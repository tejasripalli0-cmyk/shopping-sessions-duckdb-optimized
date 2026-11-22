#!/usr/bin/env python3
"""
A/B benchmark of two project folders on identical data and conditions, runs alternated
(A, B, A, B, ...) so cache and noise affect both equally, and an exact comparison of their outputs.

  python scripts/ab_benchmark.py --a PATH_TO_BASELINE_PROJECT --b PATH_TO_OPTIMIZED_PROJECT \
         --data DATA_DIR --reps 3 --cpus 1 --mem-gb 3 --json results/ab.json

Each project must contain scripts/perf_run.py and scripts/compare.py (this project does).
"""
import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile


def run_once(project, data, cpus, mem_gb):
    out = tempfile.mkdtemp(prefix="ss_ab_out_")
    js = os.path.join(out, "perf.json")
    outdir = os.path.join(out, "o")
    subprocess.run([sys.executable, os.path.join(project, "scripts", "perf_run.py"), data, outdir, "--cpus", str(cpus),
                    "--mem-gb", str(mem_gb), "--json", js], capture_output=True, text=True)
    with open(js) as f:
        r = json.load(f)
    return r, outdir, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", required=True)
    ap.add_argument("--b", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--cpus", type=int, default=1)
    ap.add_argument("--mem-gb", type=float, default=3)
    ap.add_argument("--json")
    a = ap.parse_args()
    res = {"A": [], "B": []}
    last = {}
    for i in range(a.reps):
        for label, proj in (("A", a.a), ("B", a.b)):
            r, outdir, tmp = run_once(proj, a.data, a.cpus, a.mem_gb)
            res[label].append({k: r[k] for k in ("exit_code", "wall_seconds", "peak_rss_mb", "peak_scratch_mb")})
            print(f"{label} run {i + 1}: {res[label][-1]}", flush=True)
            if label in last:
                shutil.rmtree(last[label][1], ignore_errors=True)
            last[label] = (outdir, tmp)
    cmp = subprocess.run([sys.executable, os.path.join(a.b, "scripts", "compare.py"), last["A"][0], last["B"][0]],
                         capture_output=True, text=True)
    same = cmp.stdout.strip().endswith("MATCH")
    summ = {}
    for label in "AB":
        w = [r["wall_seconds"] for r in res[label]]
        summ[label] = {"median_wall_s": statistics.median(w), "min_wall_s": min(w), "runs": res[label]}
    summ["speedup_median"] = round(summ["A"]["median_wall_s"] / summ["B"]["median_wall_s"], 3)
    summ["outputs_identical"] = same
    summ["compare_output"] = cmp.stdout.strip().splitlines()
    print(json.dumps({k: v for k, v in summ.items() if k != "compare_output"}, indent=2))
    if a.json:
        with open(a.json, "w") as f:
            json.dump(summ, f, indent=2)
    for label in last:
        shutil.rmtree(last[label][1], ignore_errors=True)
    sys.exit(0 if same and all(r["exit_code"] == 0 for l in res for r in res[l]) else 1)


if __name__ == "__main__":
    main()
