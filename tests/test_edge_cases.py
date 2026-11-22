#!/usr/bin/env python3
"""
Edge-case datasets built on the fly: each runs the production pipeline and the oracle,
asserts they agree, and (where meaningful) asserts the exact expected rows.
Run: python tests/test_edge_cases.py
"""
import os
import random
import shutil
import sys
import tempfile
import unittest
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import oracle  # noqa: E402
import run_differential as rd  # noqa: E402
import validate_output  # noqa: E402

EV_H = "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
CK_H = "click_id,user_id,campaign_id,ts\n"
LK_H = "user_id_a,user_id_b\n"
MANIFEST = '{"window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z"}'


def write(path, text, binary=False):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(text if binary else text.encode("utf-8"))


class Edge(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp(prefix="ss_edge_")
        write(os.path.join(self.d, "manifest.json"), MANIFEST)
        write(os.path.join(self.d, "products.csv"), "product_id,category\np1,shoes\np2,books\n")
        write(os.path.join(self.d, "campaigns.csv"), "campaign_id,channel\nc_1,search\nc_2,social\n")

    def tearDown(self):
        shutil.rmtree(self.d, ignore_errors=True)

    def run_both(self):
        out = tempfile.mkdtemp(prefix="ss_edge_out_")
        try:
            rd.run_pipeline(self.d, out)
            ok, report = validate_output.check(out)
            self.assertTrue(ok, report)
            u_exp, c_exp = oracle.run(self.d)
            u, c = rd.read_table(out, "user_daily", 6), rd.read_table(out, "campaign_daily", 7)
            self.assertEqual(u, u_exp)
            self.assertEqual(c, c_exp)
            return u, c
        finally:
            shutil.rmtree(out, ignore_errors=True)

    def test_input_folders_missing(self):
        u, c = self.run_both()
        self.assertEqual((u, c), (set(), set()))

    def test_empty_folders_header_only_and_zero_byte_files(self):
        for sub, h in (("events", EV_H), ("clicks", CK_H), ("links", LK_H)):
            write(os.path.join(self.d, sub, "a.csv"), h)
            write(os.path.join(self.d, sub, "b.csv"), "")          # zero bytes
            write(os.path.join(self.d, sub, "c.csv"), h.rstrip("\n"))  # header without newline
            write(os.path.join(self.d, sub, "notes.txt"), "ignored")  # not *.csv
        u, c = self.run_both()
        self.assertEqual((u, c), (set(), set()))

    def test_duplicates_across_files_earliest_ingest_wins_even_if_invalid(self):
        write(os.path.join(self.d, "events", "a.csv"), EV_H +
              "e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:05:00Z,purchase,p1,700\n"      # later copy, valid
              "e2,u1,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,\n")
        write(os.path.join(self.d, "events", "b.csv"), EV_H +
              "e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,purchase,p1,abc\n"      # earliest copy, bad price
              "e2,u1,2026-10-03T10:10:00Z,2026-10-03T10:11:00Z,view,p1,\n")            # identical copy
        u, c = self.run_both()
        self.assertEqual(u, {("u1", date(2026, 10, 3), 1, 1, 0, None)})                # e1 dropped, e2 kept once

    def test_special_bytes_and_unicode_names(self):
        ev = EV_H
        users = ["a\x01b", "a\x1ab", "a\x1a0b", "a\x1a1b", "é", "z", "Z", "u\x7f", "日本"]
        for i, u in enumerate(users):
            ev += f"e{i},{u},2026-10-03T10:0{i % 9}:00Z,2026-10-03T10:30:00Z,view,p1,\n"
        ev += "eX,u1\r,2026-10-03T10:00:00Z,2026-10-03T10:30:00Z,view,p1,\n"          # \r inside a field
        ev += "eY,u1,2026-10-03T10:00:00Z\r,2026-10-03T10:30:00Z,view,p1,\n"           # bad ts
        write(os.path.join(self.d, "events", "a.csv"), ev)
        write(os.path.join(self.d, "links", "a.csv"), LK_H + "é,z\nZ,é\n日本,a\x01b\n")
        u, _ = self.run_both()
        names = {r[0] for r in u}
        self.assertIn("Z", names)            # Z < é < z byte-wise, so the person {Z, é, z} is named Z
        self.assertNotIn("é", names)
        self.assertIn("a\x01b", names)       # "a\x01b" < "日本"
        self.assertIn("u1\r", names)

    def test_long_reversed_chain_across_many_files(self):
        rng = random.Random(3)
        ids = [f"id{n:04d}" for n in range(1000)]
        links = [(ids[i + 1], ids[i]) for i in range(999)]
        rng.shuffle(links)
        for k in range(5):
            write(os.path.join(self.d, "links", f"{k}.csv"),
                  LK_H + "".join(f"{a},{b}\n" for a, b in links[k::5]))
        ev = EV_H + "".join(f"e{n},{ids[n]},2026-10-03T10:{n % 60:02d}:00Z,2026-10-03T12:00:00Z,view,p1,\n"
                            for n in range(0, 1000, 7))
        write(os.path.join(self.d, "events", "a.csv"), ev)
        u, _ = self.run_both()
        self.assertEqual({r[0] for r in u}, {"id0000"})

    def test_session_boundaries_clicks_and_assists_from_linked_ids(self):
        write(os.path.join(self.d, "links", "a.csv"), LK_H + "u2,u1\n")
        write(os.path.join(self.d, "events", "a.csv"), EV_H +
              "e1,u1,2026-10-03T10:00:00Z,2026-10-03T10:00:01Z,view,p1,\n"
              "e2,u2,2026-10-03T10:30:00Z,2026-10-03T10:30:01Z,view,p1,\n"
              "e3,u1,2026-10-03T11:00:00.001Z,2026-10-03T11:00:01Z,view,p1,\n"      # not a valid form: bad ts
              "e4,u1,1791025200001,2026-10-03T11:00:01Z,purchase,p9,1500\n")        # 11:00:00.001 UTC, new session
        write(os.path.join(self.d, "clicks", "a.csv"), CK_H +
              "k1,u2,c_1,2026-09-26T10:00:00Z\n"        # exactly 7 days before first session start: credits
              "k2,u1,c_2,2026-10-03T10:00:00Z\n"        # exactly at start: counts and wins by latest ts
              "k3,u1,c_2,2026-10-03T10:00:00Z\n")       # duplicate ts, larger id loses the tie (same campaign here)
        u, c = self.run_both()
        self.assertEqual(sum(r[2] for r in u), 2)
        credited = {r[0]: r[3] for r in c}
        self.assertEqual(credited.get("c_2"), 2)
        self.assertEqual(credited.get("c_1", 0), 0)
        self.assertEqual({r[0]: r[6] for r in c}["c_1"], 1)                      # c_1 only assisted the first session


if __name__ == "__main__":
    unittest.main(verbosity=2)
