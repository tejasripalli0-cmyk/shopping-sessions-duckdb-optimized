#!/usr/bin/env python3
"""
Print schema, row count and the first rows of both output tables.
Usage: inspect_parquet.py OUTPUT_DIR [ROWS]      (needs only duckdb)
"""
import os
import sys

import duckdb

out = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
con = duckdb.connect()
for table in ("user_daily", "campaign_daily"):
    glob = os.path.join(out, table, "*.parquet").replace("\\", "/")
    print(f"== {table}")
    for name, typ, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall():
        print(f"   {name:<22}{typ}")
    print("   rows:", con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0])
    for row in con.execute(f"SELECT * FROM read_parquet('{glob}') ORDER BY 1, 2, 3 LIMIT {n}").fetchall():
        print("  ", row)
