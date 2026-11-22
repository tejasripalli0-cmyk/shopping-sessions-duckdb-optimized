#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline.

Reads  $DATA_DIR    (default /data)      or argv[1]
Writes $OUTPUT_DIR  (default /output)    or argv[2]: user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)   spill files, escaped copies of odd input files

Stages (each is plain SQL inside DuckDB, nothing is held in Python memory):
  1. identity.build_idmap          links  -> idmap(user_id, person)
  2. events.build_sessions         events -> sessions, purchases
  3. clicks.build_attribution      clicks -> credited, assisted
  4. this file                     user_daily, campaign_daily -> Parquet

Run from the project root:  python pipeline/pipeline.py [DATA_DIR OUTPUT_DIR]
"""
import json
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import duckdb  # noqa: E402

import clicks  # noqa: E402
import events  # noqa: E402
import identity  # noqa: E402
import parse_utils  # noqa: E402
import sql_dialect  # noqa: E402

DAY_MS = 86400000


def pick_scratch():
    """$SCRATCH_DIR if set; else /scratch when it exists and is writable; else the system temp dir."""
    wanted = os.environ.get("SCRATCH_DIR")
    if wanted is None and os.path.isdir("/scratch"):
        wanted = "/scratch"
    if wanted:
        try:
            os.makedirs(wanted, exist_ok=True)
            probe = tempfile.mkdtemp(dir=wanted)
            os.rmdir(probe)
            return wanted
        except OSError:
            pass
    return tempfile.gettempdir()


def stage(name, t0):
    now = time.time()
    print(f"[{now - t0:8.1f}s] {name}", flush=True)
    return now


def main(argv):
    data = argv[1] if len(argv) > 1 else os.environ.get("DATA_DIR", "/data")
    out = argv[2] if len(argv) > 2 else os.environ.get("OUTPUT_DIR", "/output")
    ncpu = int(os.environ.get("NCPU", os.cpu_count() or 1))
    mem_gb = float(os.environ.get("MEM_GB", "8"))
    scratch = os.path.join(pick_scratch(), "ss_work_" + str(os.getpid()))
    os.makedirs(scratch, exist_ok=True)
    t0 = time.time()

    try:
        with open(os.path.join(data, "manifest.json"), encoding="utf-8") as f:
            manifest = json.load(f)
        ws = parse_utils.parse_timestamp(manifest["window_start"])
        we = parse_utils.parse_timestamp(manifest["window_end"])
        print("window:", manifest["window_start"], "to", manifest["window_end"], flush=True)

        # Byte scan for "\\r" etc. (see sql_dialect): done by a process pool BEFORE DuckDB starts its threads.
        special = sql_dialect.find_special_files(
            sql_dialect.list_csv(os.path.join(data, "events")) + sql_dialect.list_csv(os.path.join(data, "clicks"))
            + sql_dialect.list_csv(os.path.join(data, "links")), ncpu)

        con = duckdb.connect()
        con.execute(f"SET threads = {max(1, ncpu)}")
        con.execute(f"SET memory_limit = '{max(1, int(mem_gb * 0.8 * 1024))}MB'")
        con.execute(f"SET temp_directory = '{os.path.join(scratch, 'duckdb_tmp').replace(chr(92), '/')}'")
        con.execute("SET preserve_insertion_order = false")
        sql_dialect.register(con)

        # Small, clean lookup tables.
        csv_opts = "header=true, delim=',', quote='\"', strict_mode=false, nullstr=chr(1)"
        con.execute(
            "CREATE TABLE products AS SELECT product_id, category FROM read_csv("
            f"'{os.path.join(data, 'products.csv')}', columns={{'product_id': 'VARCHAR', 'category': 'VARCHAR'}}, {csv_opts})")
        con.execute(
            "CREATE TABLE campaigns AS SELECT campaign_id, channel FROM read_csv("
            f"'{os.path.join(data, 'campaigns.csv')}', columns={{'campaign_id': 'VARCHAR', 'channel': 'VARCHAR'}}, {csv_opts})")
        t = stage("lookup tables loaded", t0)

        identity.build_idmap(con, data, scratch, special)
        t = stage("identity resolved", t0)

        events.build_sessions(con, data, scratch, ws, we, special)
        t = stage("events -> sessions", t0)

        clicks.build_attribution(con, data, scratch, ws, we, special)
        t = stage("clicks -> attribution", t0)

        # ---- user_daily / campaign_daily: written straight to Parquet, no intermediate tables ----
        user_daily = f"""
            WITH a AS (
                SELECT person, sstart // {DAY_MS} AS dn, count(*) AS sessions,
                       sum(events) AS events, sum(revenue) AS revenue_cents
                FROM sessions GROUP BY person, dn
            ), t AS (
                SELECT person, dn, cat FROM (
                    SELECT person, dn, cat, sum(price) AS s FROM (
                        SELECT person, sstart // {DAY_MS} AS dn, cat, price FROM purchases)
                    GROUP BY person, dn, cat)
                QUALIFY row_number() OVER (PARTITION BY person, dn ORDER BY s DESC, cat) = 1
            )
            SELECT a.person AS user_id, DATE '1970-01-01' + CAST(a.dn AS INTEGER) AS day,
                   CAST(a.sessions AS BIGINT) AS sessions, CAST(a.events AS BIGINT) AS events,
                   CAST(a.revenue_cents AS BIGINT) AS revenue_cents, t.cat AS top_category
            FROM a LEFT JOIN t ON a.person = t.person AND a.dn = t.dn
        """
        campaign_daily = f"""
            WITH c AS (
                SELECT campaign, sstart // {DAY_MS} AS dn, count(*) AS sessions,
                       sum(CASE WHEN revenue > 0 THEN 1 ELSE 0 END) AS purchasing_sessions,
                       sum(revenue) AS revenue_cents
                FROM credited GROUP BY campaign, dn
            )
            SELECT COALESCE(c.campaign, a.campaign) AS campaign_id,
                   CASE WHEN COALESCE(c.campaign, a.campaign) = 'organic' THEN 'organic' ELSE ch.channel END AS channel,
                   DATE '1970-01-01' + CAST(COALESCE(c.dn, a.dn) AS INTEGER) AS day,
                   CAST(COALESCE(c.sessions, 0) AS BIGINT) AS sessions,
                   CAST(COALESCE(c.purchasing_sessions, 0) AS BIGINT) AS purchasing_sessions,
                   CAST(COALESCE(c.revenue_cents, 0) AS BIGINT) AS revenue_cents,
                   CAST(COALESCE(a.assisted, 0) AS BIGINT) AS assisted_sessions
            FROM c FULL OUTER JOIN assisted a ON c.campaign = a.campaign AND c.dn = a.dn
            LEFT JOIN campaigns ch ON COALESCE(c.campaign, a.campaign) = ch.campaign_id
        """
        for table, query in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
            folder = os.path.join(out, table)
            os.makedirs(folder, exist_ok=True)
            target = os.path.join(folder, "part-0.parquet").replace(chr(92), "/")
            con.execute(f"COPY ({query}) TO '{target}' (FORMAT parquet)")
            n = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
            print(f"wrote {table}: {n} rows", flush=True)
        stage("done", t0)
        con.close()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
