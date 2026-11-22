#!/usr/bin/env python3
"""
Run every test and write the evidence into results/ (nothing is hard-coded: every
number in the reports comes from the commands executed here).

  python scripts/run_all_tests.py [--results results]
         [--generator PATH_TO_generate --seeds 2 3 4 5 --events 40000 --clicks 10000]
         [--dev DEV_DIR --dev-answer DEV_ANSWER_DIR]

Steps: 1) the 36 official fixtures  2) unit tests (dialect fuzz, edge cases)
3) pipeline-vs-oracle differential tests on fixtures (+ generated seeds when
--generator is given)  4) dev dataset run, exact diff against the official answer,
output schema/consistency validation, timing (when --dev and --dev-answer are given).
Exit code 0 only if everything passed.
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


def run(cmd, log_path, env=None):
    p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, env=env)
    text = p.stdout + (("\n--- stderr ---\n" + p.stderr) if p.stderr.strip() else "")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("$ " + " ".join(cmd) + "\n\n" + text)
    return p.returncode, p.stdout + p.stderr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results")
    ap.add_argument("--generator")
    ap.add_argument("--seeds", type=int, nargs="*", default=[2, 3, 4, 5, 6, 7, 8, 9])
    ap.add_argument("--events", type=int, default=40000)
    ap.add_argument("--clicks", type=int, default=10000)
    ap.add_argument("--dev")
    ap.add_argument("--dev-answer")
    ap.add_argument("--cpus", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--mem-gb", type=float, default=24)
    a = ap.parse_args()
    res = os.path.join(ROOT, a.results)
    os.makedirs(res, exist_ok=True)
    summary, ok_all = {}, True

    rc, out = run([PY, "scripts/run_fixtures.py", "fixtures", "--", PY, "pipeline/pipeline.py"],
                  os.path.join(res, "fixtures_result.txt"))
    m = re.search(r"(\d+)/(\d+) fixtures passed", out)
    passed, total = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    summary["official_fixtures"] = {"passed": passed, "failed": total - passed, "total": total, "ok": rc == 0}
    ok_all &= rc == 0

    rc, out = run([PY, "-m", "unittest", "discover", "-s", "tests", "-p", "test_*.py", "-v"],
                  os.path.join(res, "unit_tests.txt"))
    m = re.search(r"Ran (\d+) tests?", out)
    summary["unit_tests"] = {"ran": int(m.group(1)) if m else 0, "ok": rc == 0}
    ok_all &= rc == 0

    cmd = [PY, "tests/run_differential.py", "--fixtures", "fixtures"]
    if a.generator:
        cmd += ["--generator", a.generator, "--seeds", *map(str, a.seeds), "--events", str(a.events), "--clicks", str(a.clicks)]
    rc, out = run(cmd, os.path.join(res, "differential_tests.txt"))
    m = re.search(r"(\d+)/(\d+) datasets identical", out)
    summary["differential_vs_oracle"] = {"identical": int(m.group(1)) if m else 0, "total": int(m.group(2)) if m else 0,
                                         "generated_seeds": a.seeds if a.generator else [], "ok": rc == 0}
    ok_all &= rc == 0

    if a.generator:
        # The assist computation has two code paths (clicks.py); force each one for everybody and compare to the oracle.
        for label, heavy in (("all_pieces", "0"), ("all_direct", "1000000000")):
            rc, out = run(cmd, os.path.join(res, f"differential_tests_assist_{label}.txt"),
                          env=dict(os.environ, SS_HEAVY_PAIRS=heavy))
            m = re.search(r"(\d+)/(\d+) datasets identical", out)
            summary[f"differential_assist_{label}"] = {"identical": int(m.group(1)) if m else 0,
                                                       "total": int(m.group(2)) if m else 0, "ok": rc == 0}
            ok_all &= rc == 0

    if a.dev and a.dev_answer:
        out_dir = tempfile.mkdtemp(prefix="ss_dev_out_")
        rc, _ = run([PY, "scripts/perf_run.py", a.dev, out_dir, "--cpus", str(a.cpus), "--mem-gb", str(a.mem_gb),
                     "--json", os.path.join(res, "dev_performance.json")], os.path.join(res, "dev_run_log.txt"))
        ok_all &= rc == 0
        rc, out = run([PY, "scripts/compare.py", out_dir, a.dev_answer], os.path.join(res, "dev_comparison.txt"))
        summary["dev_vs_official_answer"] = {"match": rc == 0 and out.strip().endswith("MATCH")}
        ok_all &= rc == 0
        rc, out = run([PY, "tests/validate_output.py", out_dir, "--json", os.path.join(res, "dev_schema_validation.json")],
                      os.path.join(res, "dev_schema_validation.txt"))
        summary["dev_output_validation"] = {"valid": rc == 0}
        ok_all &= rc == 0
        shutil.rmtree(out_dir, ignore_errors=True)

    summary["all_passed"] = ok_all
    with open(os.path.join(res, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
