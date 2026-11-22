#!/usr/bin/env python3
"""
Independent reference implementation of rules.md in plain Python (dicts and loops,
no DuckDB). Slow and memory hungry on purpose: it is only for differential tests
on small datasets (tests/run_differential.py).  Written straight from the rules,
brute force wherever the production code uses a clever algorithm.

Usage: oracle.py DATA_DIR  ->  returns / prints two sets of row tuples.
"""
import json
import os
import sys
from collections import defaultdict
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pipeline"))

import identity  # noqa: E402
import parse_utils  # noqa: E402
import sql_dialect  # noqa: E402

DAY = 86400000
WEEK = 7 * DAY
EPOCH = date(1970, 1, 1)


def csv_rows(folder, header):
    for path in sql_dialect.list_csv(folder):
        yield from parse_utils.read_csv(path, header)


def lookup(path, a, b):
    rows = {}
    for r in parse_utils.read_csv(path, [a, b]):
        rows[r[0]] = r[1]
    return rows


def run(data):
    with open(os.path.join(data, "manifest.json")) as f:
        m = json.load(f)
    ws, we = parse_utils.parse_timestamp(m["window_start"]), parse_utils.parse_timestamp(m["window_end"])
    products = lookup(os.path.join(data, "products.csv"), "product_id", "category")
    campaigns = lookup(os.path.join(data, "campaigns.csv"), "campaign_id", "channel")
    uf = identity.load_identity_links(data)

    def person(u):
        return uf.canonical(u) if u in uf.parent else u

    # ---- events -------------------------------------------------------------------
    kept = {}
    for r in csv_rows(os.path.join(data, "events"), ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]):
        eid = r[0]
        if eid is None:
            continue
        ing = parse_utils.parse_timestamp(r[3])
        key = ing if ing is not None else float("inf")
        if eid not in kept or key < kept[eid][0]:
            kept[eid] = (key, r)
    by_person = defaultdict(list)
    for key, r in kept.values():
        _, user, ts_s, _, et, prod, price_s = r
        ts = parse_utils.parse_timestamp(ts_s)
        if user is None or ts is None or not (ws <= ts < we) or key == float("inf"):
            continue
        if et not in ("view", "cart", "purchase") or prod is None:
            continue
        price = 0
        if et == "purchase":
            if price_s is None or not all(c in "0123456789" for c in price_s):
                continue
            price = int(price_s)
            if not (1 <= price <= 10_000_000):
                continue
        if key - ts > DAY:
            continue
        cat = products.get(prod, "unknown") if prod in products else "unknown"
        by_person[person(user)].append((ts, price, cat))

    # ---- sessions -----------------------------------------------------------------
    sessions = []  # dict(person, start, events, revenue, cats)
    for p, evs in by_person.items():
        evs.sort()
        cur, prev = None, None
        for ts, price, cat in evs:
            if cur is None or ts - prev > 30 * 60 * 1000:
                cur = {"person": p, "start": ts, "events": 0, "revenue": 0, "cats": defaultdict(int)}
                sessions.append(cur)
            cur["events"] += 1
            if price > 0:
                cur["revenue"] += price
                cur["cats"][cat] += price
            prev = ts

    # ---- clicks -------------------------------------------------------------------
    clicks = {}
    for r in csv_rows(os.path.join(data, "clicks"), ["click_id", "user_id", "campaign_id", "ts"]):
        if r[0] is not None and r[0] not in clicks:
            clicks[r[0]] = r
    by_user_clicks = defaultdict(list)
    for cid, (_, user, camp, ts_s) in clicks.items():
        ts = parse_utils.parse_timestamp(ts_s)
        if user is None or ts is None or not (ws - WEEK <= ts < we) or camp not in campaigns:
            continue
        by_user_clicks[person(user)].append((ts, cid, camp))

    campaign_rows = defaultdict(lambda: [0, 0, 0, 0])  # sessions, purchasing, revenue, assisted
    for s in sessions:
        eligible = [c for c in by_user_clicks.get(s["person"], []) if s["start"] - WEEK <= c[0] <= s["start"]]
        day = s["start"] // DAY
        if eligible:
            # latest ts; on a tie the byte-wise smallest click_id
            top = max(c[0] for c in eligible)
            best = min((c for c in eligible if c[0] == top), key=lambda c: c[1].encode())
            credited = best[2]
        else:
            credited = "organic"
        row = campaign_rows[(credited, day)]
        row[0] += 1
        row[1] += 1 if s["revenue"] > 0 else 0
        row[2] += s["revenue"]
        for camp in {c[2] for c in eligible}:
            campaign_rows[(camp, day)][3] += 1

    # ---- outputs ------------------------------------------------------------------
    ud = defaultdict(lambda: [0, 0, 0, defaultdict(int)])
    for s in sessions:
        row = ud[(s["person"], s["start"] // DAY)]
        row[0] += 1
        row[1] += s["events"]
        row[2] += s["revenue"]
        for cat, v in s["cats"].items():
            row[3][cat] += v
    user_daily = set()
    for (p, dn), (n, ev, rev, cats) in ud.items():
        top = None
        if cats:
            best = max(cats.values())
            top = min((c for c, v in cats.items() if v == best), key=lambda c: c.encode())
        user_daily.add((p, EPOCH + timedelta(days=dn), n, ev, rev, top))
    campaign_daily = set()
    for (camp, dn), (n, pur, rev, asst) in campaign_rows.items():
        channel = "organic" if camp == "organic" else campaigns[camp]
        campaign_daily.add((camp, channel, EPOCH + timedelta(days=dn), n, pur, rev, asst))
    return user_daily, campaign_daily


if __name__ == "__main__":
    u, c = run(sys.argv[1])
    print(len(u), "user_daily rows;", len(c), "campaign_daily rows")
