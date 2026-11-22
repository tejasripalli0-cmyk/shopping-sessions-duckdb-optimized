#!/usr/bin/env python3
"""
Fuzz: the SQL CSV dialect / timestamp macro (production) must agree with the plain
Python versions in pipeline/parse_utils.py on thousands of random and hand-picked
inputs, including quotes, spaces, \\r, control bytes, unicode digits.
Run: python tests/test_dialect.py
"""
import os
import random
import sys
import tempfile
import unittest
from collections import Counter

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pipeline"))
import parse_utils  # noqa: E402
import sql_dialect  # noqa: E402

ALPHABET = ['a', 'B', '1', '0', ' ', ',', ',', '"', '"', '\r', '\x01', '\x1a', 'é', '٢', '２', 'Z', '-', ':', 'T']


def random_line(rng, n):
    kind = rng.random()
    if kind < 0.5:  # mostly well formed with random quoting
        fields = [''.join(rng.choice(ALPHABET[:2] + ALPHABET[2:5] + ['\r', '\x1a', 'é']) for _ in range(rng.randint(0, 4)))
                  for _ in range(n)]
        fields = [('"' + f + '"') if rng.random() < 0.3 else f for f in fields]
        return ','.join(fields)
    return ''.join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 14)))


class Dialect(unittest.TestCase):
    def test_line_parsing(self):
        rng = random.Random(7)
        lines = [random_line(rng, 3) for _ in range(6000)] + ['', ',,', '"a","b","c"', '"",,""', 'a,"b,c",d', '"a"b",c,d',
                                                           '"', '""', ' u1,"u1 ",u1\r']
        lines = [l for l in lines if "\n" not in l]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.csv")
            with open(path, "wb") as f:
                f.write(b"h1,h2,h3\n" + b"".join(l.encode() + b"\n" for l in lines))
            paths, escaped = sql_dialect.prepare_files([path], os.path.join(d, "esc"), "t")
            self.assertTrue(escaped)
            con = duckdb.connect()
            sql_dialect.register(con)
            got = Counter(con.execute(sql_dialect.parsed(paths, escaped, 3, ["a", "b", "c"])).fetchall())
        want = Counter(tuple(r) for r in (parse_utils.parse_csv_line(l, 3) for l in lines) if r is not None)
        self.assertEqual(got, want)

    def test_header_skipped_per_file_even_if_it_looks_like_data(self):
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for i in range(3):
                p = os.path.join(d, f"{i}.csv")
                with open(p, "w") as f:
                    f.write("user_id_a,user_id_b\nuser_id_a,user_id_b\nx,y")  # last line has no newline
                paths.append(p)
            con = duckdb.connect()
            sql_dialect.register(con)
            rows = con.execute(sql_dialect.parsed(paths, False, 2, ["a", "b"])).fetchall()
            self.assertEqual(sorted(rows), sorted([("user_id_a", "user_id_b"), ("x", "y")] * 3))

    def test_timestamps(self):
        rng = random.Random(11)
        cases = ["2026-10-03T23:58:10Z", "2026-10-03T19:58:10-04:00", "1791071890000", "0001791071890000",
                 "2026-10-04T01:30:00+02:00", "2026-02-30T00:00:00Z", "2028-02-29T12:00:00Z", "2027-02-29T00:00:00Z",
                 "2026-10-03T24:00:00Z", "2026-10-03T23:59:60Z", "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+05:60",
                 "2026-10-03T10:00:00+0200", "2026-10-03T10:00:00-00:00", "2026-10-03T10:00:00+23:59", "2026-10-03t10:00:00Z",
                 "2026-10-03 10:00:00", "1791071890000.0", "1.79e12", "-5", "２０２６-10-03T10:00:00Z", "٢٥٠", "86400000",
                 "999999999999999999999", "9223372036854775807", "9223372036854775808", "2026-10-03T10:00:00.5Z",
                 "2026-10-03T10:00:00z", "2026-13-01T00:00:00Z", "0000-01-01T00:00:00Z", "0001-01-01T00:00:00+12:00",
                 "9999-12-31T23:59:59-23:59", "2026-10-03T10:00:00Z\r", " 2026-10-03T10:00:00Z", "00"]
        base = ["2026-10-03T23:58:10Z", "2026-10-03T19:58:10-04:00", "1791071890000"]
        for _ in range(3000):  # random single-character mutations of valid forms
            s = list(rng.choice(base))
            i = rng.randrange(len(s))
            op = rng.random()
            if op < 0.5:
                s[i] = rng.choice("0123456789TZz:-+ .٢")
            elif op < 0.75:
                del s[i]
            else:
                s.insert(i, rng.choice("0123456789:- Z"))
            cases.append("".join(s))
        # Calendar grid (month/day limits, leap years) and component boundaries (hour 24, minute/second 60,
        # offset hour 24, offset minute 60) around every valid maximum.
        for y in (1900, 2000, 2026, 2027, 2028, 2100):
            for mo in range(0, 14):
                for dd in (0, 1, 28, 29, 30, 31, 32):
                    cases.append(f"{y:04d}-{mo:02d}-{dd:02d}T12:00:00Z")
        for hh in (0, 22, 23, 24, 25):
            for mi in (0, 58, 59, 60, 61):
                for ss in (0, 58, 59, 60, 61):
                    cases.append(f"2026-10-03T{hh:02d}:{mi:02d}:{ss:02d}Z")
                    cases.append(f"2026-10-03T{hh:02d}:{mi:02d}:{ss:02d}+01:30")
        for oh in (0, 12, 22, 23, 24, 25, 99):
            for om in (0, 30, 58, 59, 60, 61, 99):
                for sg in "+-":
                    cases.append(f"2026-10-03T10:00:00{sg}{oh:02d}:{om:02d}")
        for n in ("9223372036854775807", "9223372036854775808", "00000000001791071890", "0" * 30 + "5", "1" + "0" * 25):
            cases.append(n)
        con = duckdb.connect()
        sql_dialect.register(con)
        got = dict(con.execute("SELECT s, ss_ts(s) FROM (SELECT unnest(?) AS s)", [list(set(cases))]).fetchall())
        for s in set(cases):
            self.assertEqual(got[s], parse_utils.parse_timestamp(s), repr(s))


if __name__ == "__main__":
    unittest.main(verbosity=2)
