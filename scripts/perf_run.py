#!/usr/bin/env python3
"""
Run the pipeline once and report wall time, peak RSS and peak scratch usage.
Usage: perf_run.py DATA_DIR OUTPUT_DIR [--cpus N] [--mem-gb G] [--json FILE]
(Works on Linux/macOS; peak RSS uses resource, on Windows it is reported as null.)
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def dir_size(path):
    total = 0
    for base, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(base, f))
            except OSError:
                pass
    return total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data")
    ap.add_argument("out")
    ap.add_argument("--cpus", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--mem-gb", type=float, default=24)
    ap.add_argument("--json")
    a = ap.parse_args()
    shutil.rmtree(a.out, ignore_errors=True)
    scratch = tempfile.mkdtemp(prefix="ss_perf_scratch_")
    env = dict(os.environ, NCPU=str(a.cpus), MEM_GB=str(a.mem_gb), SCRATCH_DIR=scratch)
    peak = [0]
    stop = threading.Event()

    def watch():
        while not stop.is_set():
            peak[0] = max(peak[0], dir_size(scratch))
            time.sleep(0.5)

    th = threading.Thread(target=watch, daemon=True)
    th.start()
    t0 = time.time()
    p = subprocess.run([sys.executable, os.path.join(ROOT, "pipeline", "pipeline.py"), a.data, a.out],
                       env=env, capture_output=True, text=True)
    wall = time.time() - t0
    stop.set()
    th.join()
    rss = None
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024.0  # MB on Linux
    except ImportError:
        pass
    shutil.rmtree(scratch, ignore_errors=True)
    result = {"exit_code": p.returncode, "wall_seconds": round(wall, 1), "peak_rss_mb": None if rss is None else round(rss),
              "peak_scratch_mb": round(peak[0] / 1e6), "cpus": a.cpus, "mem_gb_setting": a.mem_gb,
              "input_mb": round(dir_size(a.data) / 1e6), "stdout": p.stdout.strip().splitlines(), "stderr_tail": p.stderr[-1500:]}
    print(json.dumps(result, indent=2))
    if a.json:
        with open(a.json, "w") as f:
            json.dump(result, f, indent=2)
    sys.exit(p.returncode)


if __name__ == "__main__":
    main()
