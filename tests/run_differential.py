#!/usr/bin/env python3
"""
Differential test: production pipeline (DuckDB) vs tests/oracle.py (plain Python),
on the official fixtures and on datasets made by the official generator.

  python tests/run_differential.py --fixtures fixtures
  python tests/run_differential.py --generator /path/to/generate --seeds 2 3 4 5 --events 40000 --clicks 10000

Exact comparison of both output tables (row sets). Exit code 0 only if all match.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import oracle  # noqa: E402


def read_table(out, table, ncols):
    glob = os.path.join(out, table, "*.parquet").replace("\\", "/")
    return set(duckdb.connect().execute(f"SELECT * FROM read_parquet('{glob}')").fetchall())


def run_pipeline(data, out, mem_gb="3"):
    env = dict(os.environ, NCPU=os.environ.get("NCPU", "1"), MEM_GB=mem_gb)
    p = subprocess.run([sys.executable, os.path.join(ROOT, "pipeline", "pipeline.py"), data, out],
                       env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr[-3000:])


def compare(name, data):
    out = tempfile.mkdtemp(prefix="ss_out_")
    try:
        t = time.time()
        run_pipeline(data, out)
        t_pipe = time.time() - t
        t = time.time()
        u_exp, c_exp = oracle.run(data)
        t_orc = time.time() - t
        u_act, c_act = read_table(out, "user_daily", 6), read_table(out, "campaign_daily", 7)
        ok = u_act == u_exp and c_act == c_exp
        print(f"{'ok  ' if ok else 'FAIL'} {name}: user_daily {len(u_act)}/{len(u_exp)} rows, "
              f"campaign_daily {len(c_act)}/{len(c_exp)} rows (pipeline {t_pipe:.1f}s, oracle {t_orc:.1f}s)", flush=True)
        if not ok:
            for label, a, e in (("user_daily", u_act, u_exp), ("campaign_daily", c_act, c_exp)):
                print(f"   {label}: only in pipeline {sorted(a - e, key=str)[:3]}; only in oracle {sorted(e - a, key=str)[:3]}")
        return ok
    finally:
        shutil.rmtree(out, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixtures")
    ap.add_argument("--generator")
    ap.add_argument("--seeds", type=int, nargs="*", default=[])
    ap.add_argument("--events", type=int, default=40000)
    ap.add_argument("--clicks", type=int, default=10000)
    a = ap.parse_args()
    results = []
    if a.fixtures:
        for case in sorted(os.listdir(a.fixtures)):
            data = os.path.join(a.fixtures, case, "input")
            if os.path.isdir(data):
                results.append(compare(f"fixture {case}", data))
    for seed in a.seeds:
        d = tempfile.mkdtemp(prefix=f"ss_gen{seed}_")
        try:
            subprocess.run([a.generator, "--seed", str(seed), "--events", str(a.events), "--clicks", str(a.clicks),
                            "--output", d], check=True, capture_output=True)
            results.append(compare(f"generated seed={seed} events={a.events} clicks={a.clicks}", d))
        finally:
            shutil.rmtree(d, ignore_errors=True)
    print(f"\n{sum(results)}/{len(results)} datasets identical")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
