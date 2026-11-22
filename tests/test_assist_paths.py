#!/usr/bin/env python3
"""
The assist computation has two paths (clicks.py): a direct hash join for persons with few
islands x sessions, and a day-piece / ASOF path for heavy persons. Both must give identical
results, equal to the oracle. A synthetic dataset with a "whale" (many sessions, many campaigns,
clicks on both sides of the window start, ties, exact 7-day boundaries) plus ordinary users is
run with SS_HEAVY_PAIRS = 0 (everyone on the piece path), 512 (default mix), huge (everyone direct).
Run: python tests/test_assist_paths.py
"""
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import oracle  # noqa: E402
import run_differential as rd  # noqa: E402

WS = datetime(2026, 10, 1, tzinfo=timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build(d, seed):
    rng = random.Random(seed)
    os.makedirs(os.path.join(d, "events"))
    os.makedirs(os.path.join(d, "clicks"))
    with open(os.path.join(d, "manifest.json"), "w") as f:
        f.write('{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}')
    with open(os.path.join(d, "products.csv"), "w") as f:
        f.write("product_id,category\n" + "".join(f"p{i},cat{i % 4}\n" for i in range(6)))
    with open(os.path.join(d, "campaigns.csv"), "w") as f:
        f.write("campaign_id,channel\n" + "".join(f"c_{i},ch{i % 3}\n" for i in range(40)))
    ev, ck, n = [], [], 0
    users = ["whale"] * 1 + [f"u{i}" for i in range(60)]
    for u in users:
        sessions = 120 if u == "whale" else rng.randint(1, 6)
        starts = sorted(WS + timedelta(seconds=rng.randrange(0, 14 * 86400)) for _ in range(sessions))
        for s0 in starts:
            for k in range(rng.randint(1, 4)):
                t = s0 + timedelta(minutes=k * rng.randint(1, 20))
                n += 1
                typ = rng.choice(["view", "cart", "purchase"])
                price = rng.randint(1, 9000) if typ == "purchase" else ""
                ev.append(f"e{n},{u},{iso(t)},{iso(t + timedelta(seconds=5))},{typ},p{rng.randrange(8)},{price}\n")
            # clicks right at the 7 day boundary, at the start, 1 ms-ish neighbours (whole seconds here), ties
            for off in (7 * 86400, 7 * 86400 + 1, 0, -1, rng.randrange(0, 7 * 86400)):
                if rng.random() < (0.9 if u == "whale" else 0.5):
                    n += 1
                    ck.append(f"k{n % 977:03d}x{n},{u},c_{rng.randrange(40)},{iso(s0 - timedelta(seconds=off))}\n")
    for _ in range(400):  # whale clicks sprinkled over the whole range, some campaigns repeated
        n += 1
        ck.append(f"w{n},whale,c_{rng.randrange(40)},{iso(WS - timedelta(days=7) + timedelta(seconds=rng.randrange(0, 21 * 86400)))}\n")
    with open(os.path.join(d, "events", "a.csv"), "w") as f:
        f.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n" + "".join(ev))
    with open(os.path.join(d, "clicks", "a.csv"), "w") as f:
        f.write("click_id,user_id,campaign_id,ts\n" + "".join(ck))


class AssistPaths(unittest.TestCase):
    def test_all_paths_equal_oracle(self):
        for seed in (1, 2, 3):
            d = tempfile.mkdtemp(prefix="ss_assist_")
            try:
                build(d, seed)
                u_exp, c_exp = oracle.run(d)
                self.assertTrue(c_exp)
                for heavy in ("0", "512", "1000000000"):
                    out = tempfile.mkdtemp(prefix="ss_assist_out_")
                    try:
                        env = dict(os.environ, SS_HEAVY_PAIRS=heavy, NCPU="1", MEM_GB="2")
                        p = subprocess.run([sys.executable, os.path.join(ROOT, "pipeline", "pipeline.py"), d, out],
                                           env=env, capture_output=True, text=True)
                        self.assertEqual(p.returncode, 0, p.stderr[-2000:])
                        self.assertEqual(rd.read_table(out, "user_daily", 6), u_exp, f"seed {seed} heavy {heavy}")
                        self.assertEqual(rd.read_table(out, "campaign_daily", 7), c_exp, f"seed {seed} heavy {heavy}")
                    finally:
                        shutil.rmtree(out, ignore_errors=True)
            finally:
                shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
