#!/usr/bin/env python3
"""
Output schema + self-consistency validation (rules.md, "Outputs").
Usage: validate_output.py OUTPUT_DIR [--json REPORT.json]
Checks: both tables exist as *.parquet; exact column sets; DATE type for `day`; integer
types; no duplicate keys; sum(sessions) and sum(revenue_cents) equal in both tables;
sum(assisted_sessions) >= credited non-organic sessions; assisted_sessions 0 for organic;
channel 'organic' for organic; purchasing_sessions <= sessions.
Exit code 0 when everything holds.
"""
import json
import os
import sys

import duckdb

SCHEMAS = {
    "user_daily": {"user_id": "VARCHAR", "day": "DATE", "sessions": "int", "events": "int",
                   "revenue_cents": "int", "top_category": "VARCHAR"},
    "campaign_daily": {"campaign_id": "VARCHAR", "channel": "VARCHAR", "day": "DATE", "sessions": "int",
                       "purchasing_sessions": "int", "revenue_cents": "int", "assisted_sessions": "int"},
}
INT_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"}


def check(out):
    con = duckdb.connect()
    report, ok = {}, True

    def rec(name, passed, detail=""):
        nonlocal ok
        ok &= bool(passed)
        report[name] = {"pass": bool(passed), "detail": str(detail)}

    for t, schema in SCHEMAS.items():
        folder = os.path.join(out, t)
        files = [f for f in sorted(os.listdir(folder)) if f.endswith(".parquet") and not f.startswith((".", "_"))] \
            if os.path.isdir(folder) else []
        rec(f"{t}: has parquet file(s)", files, files)
        if not files:
            continue
        glob = os.path.join(folder, "*.parquet").replace("\\", "/")
        cols = {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()}
        rec(f"{t}: exact column set", set(cols) == set(schema), sorted(cols))
        types_ok = all(cols.get(c) == ("DATE" if v == "DATE" else v) or (v == "int" and cols.get(c) in INT_TYPES)
                       for c, v in schema.items())
        rec(f"{t}: column types (day=DATE, counts=integer, strings=VARCHAR)", types_ok, cols)
        con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{glob}')")
        keys = "user_id, day" if t == "user_daily" else "campaign_id, day"
        dups = con.execute(f"SELECT count(*) - count(DISTINCT ({keys})) FROM {t}").fetchone()[0]
        rec(f"{t}: no duplicate ({keys})", dups == 0, f"{dups} duplicates")
        rec(f"{t}: row count", True, con.execute(f"SELECT count(*) FROM {t}").fetchone()[0])
    if not ok and not all(os.path.isdir(os.path.join(out, t)) for t in SCHEMAS):
        return ok, report
    s1 = con.execute("SELECT coalesce(sum(sessions),0), coalesce(sum(revenue_cents),0) FROM user_daily").fetchone()
    s2 = con.execute("SELECT coalesce(sum(sessions),0), coalesce(sum(revenue_cents),0) FROM campaign_daily").fetchone()
    rec("sum(sessions) equal in both tables", s1[0] == s2[0], f"user_daily={s1[0]} campaign_daily={s2[0]}")
    rec("sum(revenue_cents) equal in both tables", s1[1] == s2[1], f"user_daily={s1[1]} campaign_daily={s2[1]}")
    cred, asst = con.execute(
        "SELECT coalesce(sum(CASE WHEN campaign_id <> 'organic' THEN sessions END),0), coalesce(sum(assisted_sessions),0) "
        "FROM campaign_daily").fetchone()
    rec("sum(assisted_sessions) >= credited non-organic sessions", asst >= cred, f"assisted={asst} credited_non_organic={cred}")
    bad = con.execute("SELECT count(*) FROM campaign_daily WHERE campaign_id='organic' AND (assisted_sessions<>0 OR channel<>'organic')").fetchone()[0]
    rec("organic rows: channel='organic', assisted_sessions=0", bad == 0, f"{bad} bad rows")
    bad = con.execute("SELECT count(*) FROM campaign_daily WHERE purchasing_sessions > sessions OR sessions < 0 OR assisted_sessions < 0 "
                      "OR (sessions = 0 AND (purchasing_sessions <> 0 OR revenue_cents <> 0))").fetchone()[0]
    rec("campaign_daily: purchasing<=sessions, assist-only rows have 0 credited", bad == 0, f"{bad} bad rows")
    bad = con.execute("SELECT count(*) FROM campaign_daily WHERE channel IS NULL").fetchone()[0]
    rec("campaign_daily: channel never NULL", bad == 0, f"{bad} bad rows")
    bad = con.execute("SELECT count(*) FROM user_daily WHERE (revenue_cents = 0) <> (top_category IS NULL) OR sessions < 1 OR events < sessions").fetchone()[0]
    rec("user_daily: top_category NULL iff no revenue; events >= sessions >= 1", bad == 0, f"{bad} bad rows")
    return ok, report


if __name__ == "__main__":
    ok, report = check(sys.argv[1])
    for k, v in report.items():
        print(("PASS " if v["pass"] else "FAIL ") + k + ("  [" + v["detail"] + "]" if v["detail"] else ""))
    if "--json" in sys.argv:
        with open(sys.argv[sys.argv.index("--json") + 1], "w") as f:
            json.dump({"all_passed": ok, "checks": report}, f, indent=2)
    print("VALID" if ok else "INVALID")
    sys.exit(0 if ok else 1)
