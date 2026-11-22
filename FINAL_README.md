# Shopping Sessions: complete DuckDB solution (performance-optimized version)

> **v2 (optimized).** Same outputs, same fixtures, ~1.2-1.3x less CPU time on identical data and much lower memory.
> The audit, measurements, rejected ideas and remaining bottlenecks are in
> **`results/optimization/optimization_report.md`**; the pre-optimization code is kept in `backup_original_pipeline/`
> and its original test evidence in `results/previous_baseline_evidence/`.
> New tools: `scripts/profile_statements.py` (per-statement timing), `scripts/ab_benchmark.py` (alternating A/B
> benchmark + exact output comparison), `tests/test_assist_paths.py` (forces each assist code path; env `SS_HEAVY_PAIRS`).

A complete, tested implementation of the Shopping Sessions challenge, built on the official starter
(DuckDB 1.5.6, Python 3.12, same Dockerfile / `run.sh` / scripts).

**Test status (all executed, evidence in `results/`)**

| Check | Result |
| --- | --- |
| Official fixtures (A1-A12, E1-E20, L1-L4) | **36 / 36 passed, 0 failed** (`results/fixtures_result.txt`) |
| 1M-event dev set vs official `dev-answer` (exact, both tables) | **MATCH**: 117,038 `user_daily` rows, 27,400 `campaign_daily` rows (`results/dev_comparison.txt`) |
| Output schema + consistency checks on the dev output | **VALID** (`results/dev_schema_validation.txt`): summed `sessions` 179,366 and `revenue_cents` 376,461,330 equal in both tables |
| Unit/edge tests (dialect + calendar fuzz, empty/missing/header-only files, special bytes, 1000-link chain, assist-path equivalence, ...) | **10 / 10 passed** (`results/unit_tests.txt`) |
| Pipeline vs independent oracle | **48 / 48 datasets identical** (36 fixtures + 12 generated seeds x 40k events), also with every person forced onto each of the two assist paths, and **2 / 2** at 400k events |
| Scale / memory | 24M events in **170 s** (baseline 224 s, identical output) on 1 CPU under a 3 GB limit; spill 4.4 GB -> 0.8 GB (`results/optimization/`) |

Not verified (see `results/known_limitations.md`): Docker build/run, the 8-vCPU/24 GB scorer, the full 60M-event dataset, Windows, and multi-core speedups.

---------------------------------------------------------------------------------------------------

## Architecture

Everything heavy happens inside DuckDB; Python only orchestrates, so memory stays bounded and big sorts, joins and
aggregations spill to `/scratch`. There are no Python dicts/lists over events or clicks.

```
/data  ──►  identity.py   links  ─► idmap(user_id, person)            (only IDs whose person name differs)
       ──►  events.py     events ─► dedup ─► validate ─► late ─► person ─► sessionize ─► sessions, purchases
       ──►  clicks.py     clicks ─► dedup ─► validate ─► person ─► credit (ASOF) + assists (interval merge)
                                       │
                                       ▼
                         pipeline.py  user_daily, campaign_daily ─► /output/*/part-0.parquet
```

Key ideas
* **Own CSV dialect.** DuckDB's CSV reader does not follow the rules (it repairs bad lines, turns `""` into an empty
  string and swallows a `\r` before `\n`). Every file is read as raw lines (one VARCHAR column) and the dialect is applied
  in SQL (`sql_dialect.py`): exact field count, quotes only around whole fields, empty -> NULL, `\r` is ordinary data,
  header = first line of each file only. The few files that contain `\r` (or the reserved bytes `\x01`/`\x1a`) are copied
  once to scratch with a lossless byte escape and un-escaped in SQL.
* **Timestamps** (`ss_ts` SQL macro): F1/F2/F3 exactly as specified (ASCII digits only, offsets `00-23:00-59` with colon,
  real calendar check, `-00:00` = UTC, F3 outside 64-bit = bad), result in epoch milliseconds.
* **Processing order is the rules' order.** Dedup on `event_id` keeps the copy with the smallest `ingest_ts` *before*
  validation: validity flags are computed per row but only the kept copy's flag survives the `GROUP BY` (the aggregated
  struct is never NULL, so an invalid earliest copy cannot be skipped in favour of a later valid one).
* **Identity.** Distinct valid link pairs -> integer IDs -> small integer union-find in Python (a few seconds for millions
  of links, chains of any length) -> `idmap`. The person name is the byte-wise smallest ID (DuckDB `min` on VARCHAR is
  byte-wise). Events and clicks get `COALESCE(idmap.person, user_id)`.
* **Sessions.** `lag` + running `max` over `(PARTITION BY person ORDER BY ts)`; equal timestamps are window peers, so they
  all share the session of the first of them. Day = UTC date of the session start.
* **Credit.** One click per `(person, ts)` (smallest `click_id`), then an `ASOF LEFT JOIN` of sessions to the latest
  click `<= session start`, kept only if within 7 days; otherwise `organic`.
* **Assists, two paths.** For one `(person, campaign)` the assisted sessions are those starting inside the union of
  `[click.ts, click.ts + 7d]`; intervals are merged into islands (a gap larger than 7 days starts a new one). Persons with
  few `islands x sessions` (<= 512) use a hash join on person + range test. Heavy persons (whales) cut islands at UTC
  midnight and count the sessions in each piece with per-person cumulative counts (two ASOF joins), so they cost
  O(clicks + sessions) with no click x session blow-up. Both paths are exact and tested against each other.
* **Outputs.** `BIGINT` counts, `DATE` day, nullable `top_category` (highest summed purchase `price_cents` per person-day;
  ties -> byte-wise smallest category; `unknown` competes like any category); assist-only campaign rows have 0 credited.

## Files

| Path | Purpose |
| --- | --- |
| `pipeline/pipeline.py` | Entry point (`run.sh` runs it). Settings, lookup tables, stage orchestration, daily tables, Parquet output, stage timings. Accepts `DATA_DIR OUTPUT_DIR` as arguments or env vars. |
| `pipeline/sql_dialect.py` | The CSV dialect + timestamp rules as DuckDB SQL/macros; raw-line reader; `\r`-safe file preparation. |
| `pipeline/identity.py` | Link resolution (`build_idmap`) and the plain-Python `UnionFind` used by the oracle. |
| `pipeline/events.py` | Event parsing, dedup, validation, lateness, sessionization -> `sessions`, `purchases`. |
| `pipeline/clicks.py` | Click parsing/dedup/validation, credit and assist attribution. |
| `pipeline/parse_utils.py` | Pure-Python dialect/timestamp reference (your original module, fixed: it split lines on a bare `\r`, and could overflow near year 1/9999). Used by the oracle and the fuzz tests. |
| `tests/oracle.py` | Independent brute-force Python implementation of the whole spec (small data only). |
| `tests/run_differential.py` | Pipeline vs oracle on fixtures and generator datasets. |
| `tests/test_dialect.py`, `tests/test_edge_cases.py` | Fuzz and edge-case unit tests (`unittest`). |
| `tests/validate_output.py` | Output schema/type/consistency validator (the self-checks from the rules). |
| `scripts/run_fixtures.py`, `scripts/compare.py` | Original starter scripts, unchanged interface. |
| `scripts/run_all_tests.py` | One command that runs everything and writes `results/`. |
| `scripts/perf_run.py` | Runs the pipeline and records wall time, peak RSS, peak scratch use. |
| `scripts/inspect_parquet.py` | Prints schema and first rows of the output tables. |
| `scripts/docker_run.sh`, `Dockerfile`, `run.sh`, `Makefile`, `.dockerignore`, `requirements.txt` | Starter files (Makefile gained `test` and `local-fixtures` targets). `requirements-dev.txt` adds `polars` for the comparison scripts. |
| `fixtures/` | The 36 official fixtures, untouched (expected outputs are not modified). |
| `results/` | Real test evidence, performance report, known limitations, sample Parquet outputs. |

## Install (Windows PowerShell)

Requires Python 3.10+ (3.12 recommended). Run from the project root (the folder containing `pipeline/`).

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1          # if blocked:  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
python -m pip install --upgrade pip
pip install -r requirements-dev.txt   # duckdb==1.5.6 + polars (polars only for the comparison scripts)
code .                                # open in VS Code (select the .venv interpreter)
```

Keep the line endings of `fixtures/` as they are (LF). If you copy the project through Git on Windows, set
`git config core.autocrlf false` first, because fixture E20 tests `\r` handling byte for byte.

## Run the pipeline

```powershell
# defaults: DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch (the container layout); locally pass folders:
$env:NCPU = "8"; $env:MEM_GB = "24"       # optional; defaults: all CPUs, 8 GB
python pipeline/pipeline.py C:\path\to\data C:\path\to\out
python scripts/inspect_parquet.py C:\path\to\out 10
```

The output folder receives `user_daily\part-0.parquet` and `campaign_daily\part-0.parquet`. Without `SCRATCH_DIR`, temp
files go to the system temp folder (or `/scratch` when it exists) and are deleted at the end.

## Run the tests

```powershell
# 1. The 36 official fixtures (no Docker)
python scripts/run_fixtures.py fixtures -- python pipeline/pipeline.py

# 2. Unit + edge-case tests
python -m unittest discover -s tests -p "test_*.py" -v

# 3. Pipeline vs oracle on the fixtures
python tests/run_differential.py --fixtures fixtures

# 4. Everything at once, writing results\ (add the generator / dev set if you have them, see below)
python scripts/run_all_tests.py --results results
```

Optional dev-set check (needs the official generator, built with Rust >= 1.85, and the unzipped `dev-answer`):

```powershell
cd generator; cargo build --release; cd ..
.\generator\target\release\generate.exe --seed 1 --events 1000000 --clicks 250000 --output dev
python scripts/perf_run.py dev out --cpus 8 --mem-gb 24
python scripts/compare.py out dev-answer          # folder that directly contains user_daily.csv and campaign_daily.csv
python tests/validate_output.py out
# full runner incl. generated seeds:
python scripts/run_all_tests.py --generator .\generator\target\release\generate.exe --dev dev --dev-answer dev-answer
```

Docker (as the scorer runs it; **not executed by me**): `docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .`
then, from Git Bash/WSL, `make fixtures` and `make run-dev` / `make check-dev`.

On Linux/macOS the same commands work with `python3` and `source .venv/bin/activate`; `make test` runs step 4.

## Inspect the Parquet outputs

```powershell
python scripts/inspect_parquet.py C:\path\to\out 10          # schema, row count, first rows
python -c "import duckdb; print(duckdb.sql(\"SELECT * FROM 'C:/path/to/out/user_daily/*.parquet' ORDER BY revenue_cents DESC LIMIT 5\"))"
python tests/validate_output.py C:\path\to\out               # schema + self-checks from the rules
```

Small example outputs with the exact schemas are in `results/sample_output/` (fixture A11 and the first 20 rows of each
dev table).
