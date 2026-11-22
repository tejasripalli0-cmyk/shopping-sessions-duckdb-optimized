"""
CSV dialect and timestamp rules from rules.md, expressed as DuckDB SQL.

DuckDB's own CSV parser does not follow the dialect (it repairs lines, treats
"" as an empty string, and swallows a "\\r" that precedes "\\n"), so every file is
read as *raw lines* (one VARCHAR column, no quoting, no type inference) and the
dialect is applied here in SQL:

  * a line is valid when it has exactly N comma-separated fields and every
    field is either free of double quotes or is one whole quoted field;
  * quotes around a whole field are syntax and are removed;
  * an empty field is NULL;
  * only "\\n" ends a line; "\\r" is an ordinary character.

Because DuckDB cannot be told to keep "\\r", files that contain "\\r" (or the two
bytes used for escaping) are rewritten once, byte for byte, into /scratch with
an escape sequence, and the escape is undone in SQL after the line is read.
"""
import os

ESC = b"\x1a"          # escape introducer; ESC0 = ESC itself, ESC1 = \r, ESC2 = \x01
SPECIAL = (b"\r", b"\x01", b"\x1a")
CHUNK = 16 * 1024 * 1024
RANGE = 64 * 1024 * 1024


def list_csv(dir_path):
    """Every non-empty regular *.csv file directly inside dir_path, sorted."""
    if not os.path.isdir(dir_path):
        return []
    out = []
    for name in sorted(os.listdir(dir_path)):
        path = os.path.join(dir_path, name)
        if name.endswith(".csv") and os.path.isfile(path) and os.path.getsize(path) > 0:
            out.append(path)
    return out


def _scan_range(task):
    """Worker: does bytes [start, start+length) of the file hold a special byte? -> path or None."""
    path, start, length = task
    with open(path, "rb") as f:
        f.seek(start)
        left = length
        while left > 0:
            chunk = f.read(min(CHUNK, left))
            if not chunk:
                break
            left -= len(chunk)
            if any(sp in chunk for sp in SPECIAL):
                return path
    return None


def find_special_files(paths, workers=1):
    """
    Set of files containing a byte that needs escaping. Large inputs are scanned in 64 MB slices by a
    process pool (must be called before any DuckDB threads exist); falls back to a serial scan.
    """
    tasks = []
    for p in paths:
        size = os.path.getsize(p)
        for off in range(0, size, RANGE):
            tasks.append((p, off, min(RANGE, size - off)))
    if not tasks:
        return set()
    if workers > 1 and len(tasks) > 1:
        try:
            import multiprocessing
            from concurrent.futures import ProcessPoolExecutor
            ctx = multiprocessing.get_context("fork") if "fork" in multiprocessing.get_all_start_methods() else None
            with ProcessPoolExecutor(max_workers=min(workers, len(tasks)), mp_context=ctx) as ex:
                return {r for r in ex.map(_scan_range, tasks, chunksize=1) if r}
        except (OSError, ImportError, RuntimeError):
            pass
    return {r for r in map(_scan_range, tasks) if r}


def _escape_file(src, dst):
    with open(src, "rb") as f, open(dst, "wb") as g:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            g.write(chunk.replace(b"\x1a", b"\x1a0").replace(b"\r", b"\x1a1").replace(b"\x01", b"\x1a2"))


def prepare_files(paths, scratch_dir, tag, special=None):
    """
    Return (paths_to_read, needs_unescape). Only files with special bytes are copied.
    `special` = precomputed find_special_files() result; scanned serially when omitted.
    """
    if special is None:
        special = find_special_files(paths, 1)
    out, escaped = [], False
    for i, p in enumerate(paths):
        if p in special:
            os.makedirs(scratch_dir, exist_ok=True)
            dst = os.path.join(scratch_dir, f"{tag}_{i}.csv")
            _escape_file(p, dst)
            out.append(dst)
            escaped = True
        else:
            out.append(p)
    return out, escaped


def _q(s):
    return "'" + s.replace("'", "''") + "'"


def raw_lines(paths):
    """SQL producing one row per non-empty data line (header of each file skipped)."""
    if not paths:
        return "SELECT NULL::VARCHAR AS line WHERE false"
    files = ", ".join(_q(p) for p in paths)
    return (
        "SELECT line FROM read_csv([" + files + "], columns={'line': 'VARCHAR'}, header=false, "
        "delim=chr(1), quote='', escape='', new_line='\\n', skip=1, auto_detect=false, "
        "strict_mode=false, max_line_size=33554432) WHERE line IS NOT NULL"
    )


def parsed(paths, escaped, n, names):
    """
    SQL producing the valid lines of the given files, split into n fields named
    `names`: quotes removed, empty -> NULL. Malformed lines never appear.

    A line is valid when it has exactly n comma-separated fields and, after removing the quotes that wrap a
    whole field, no field still contains a double quote.
    """
    line = "line"
    if escaped:
        line = ("replace(replace(replace(line, chr(26) || '1', chr(13)), "
                "chr(26) || '2', chr(1)), chr(26) || '0', chr(26))")
    u = ", ".join(f"ss_unq(p[{i + 1}]) AS u{i + 1}" for i in range(n))
    no_quote = " AND ".join(f"strpos(u{i + 1}, '\"') = 0" for i in range(n))
    cols = ", ".join(f"NULLIF(u{i + 1}, '') AS {names[i]}" for i in range(n))
    return (
        f"SELECT {cols} FROM (SELECT l, p, {u} FROM ("
        f"SELECT l, str_split(l, ',') AS p FROM (SELECT {line} AS l FROM ({raw_lines(paths)}))"
        f") WHERE len(p) = {n}) WHERE strpos(l, '\"') = 0 OR ({no_quote})"
    )


MACROS = [
    # A field wrapped in quotes loses them; anything else (including a lone or unbalanced quote) is
    # returned unchanged, so parsed() can reject it because a quote remains.
    """CREATE OR REPLACE MACRO ss_unq(f) AS
        CASE WHEN starts_with(f, '"') AND ends_with(f, '"') AND length(f) >= 2
             THEN substr(f, 2, length(f) - 2) ELSE f END""",
    # ts text -> epoch milliseconds (UTC), NULL for a bad timestamp.
    #   F1  YYYY-MM-DDTHH:MM:SSZ        (20 chars)
    #   F2  YYYY-MM-DDTHH:MM:SS+HH:MM   (25 chars), subtract the offset
    #   F3  ASCII digits only           (anything that fits a 64-bit integer; leading zeros fine)
    # The shape is checked first (ASCII digits, fixed positions), then every component range
    # (hour <= 23, minute/second <= 59, offset hour <= 23, offset minute <= 59), then DuckDB's own
    # TIMESTAMP cast validates the calendar (month, day, leap years). Year 0000 is rejected.
    """CREATE OR REPLACE MACRO ss_ts(s) AS (
        CASE
          WHEN s IS NULL THEN NULL
          WHEN length(s) = 20
               AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z')
            THEN CASE WHEN substr(s, 12, 2) <= '23' AND substr(s, 15, 2) <= '59' AND substr(s, 18, 2) <= '59'
                           AND substr(s, 1, 4) > '0000'
                      THEN epoch_ms(TRY_CAST(substr(s, 1, 19) AS TIMESTAMP)) END
          WHEN length(s) = 25
               AND regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}')
            THEN CASE WHEN substr(s, 12, 2) <= '23' AND substr(s, 15, 2) <= '59' AND substr(s, 18, 2) <= '59'
                           AND substr(s, 21, 2) <= '23' AND substr(s, 24, 2) <= '59' AND substr(s, 1, 4) > '0000'
                      THEN epoch_ms(TRY_CAST(substr(s, 1, 19) AS TIMESTAMP))
                           - (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
                             * (CAST(substr(s, 21, 2) AS BIGINT) * 60 + CAST(substr(s, 24, 2) AS BIGINT)) * 60000
                 END
          WHEN regexp_full_match(s, '[0-9]+') THEN TRY_CAST(s AS BIGINT)
          ELSE NULL
        END)""",
]


def register(con):
    for m in MACROS:
        con.execute(m)
