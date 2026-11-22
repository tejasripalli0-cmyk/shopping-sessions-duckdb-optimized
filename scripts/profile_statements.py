#!/usr/bin/env python3
"""
Per-statement timing of the pipeline (no code changes needed): wraps duckdb.connect so every
con.execute() is timed. Usage: profile_statements.py DATA_DIR OUTPUT_DIR   (env NCPU, MEM_GB as usual)
Prints  seconds  rows-affected-or-fetched  first words of the SQL, slowest last.
"""
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "pipeline"))
import duckdb  # noqa: E402

real_connect = duckdb.connect
LOG = []


class Proxy:
    def __init__(self, con):
        self._c = con

    def execute(self, sql, *a, **k):
        t = time.time()
        r = self._c.execute(sql, *a, **k)
        LOG.append((time.time() - t, re.sub(r"\s+", " ", sql).strip()[:110]))
        return r

    def __getattr__(self, n):
        return getattr(self._c, n)


duckdb.connect = lambda *a, **k: Proxy(real_connect(*a, **k))
import pipeline  # noqa: E402

t0 = time.time()
pipeline.main(["pipeline.py"] + sys.argv[1:3])
total = time.time() - t0
print(f"\n=== statement profile (total {total:.2f}s) ===")
for s, q in sorted(LOG, reverse=True)[:16]:
    print(f"{s:7.2f}s  {q}")
