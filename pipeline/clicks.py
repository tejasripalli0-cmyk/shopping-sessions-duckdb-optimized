"""
Clicks: parse -> dedup on click_id -> validate -> person; then attribution.

Credit  : ASOF join of each session to its person's latest click <= session start
          (one click per (person, ts): the smallest click_id), kept only if it is
          within 7 days.  No click -> 'organic'.
Assists : for one (person, campaign) the sessions assisted are exactly those whose
          start lies in the union of [click.ts, click.ts + 7d] over the clicks of
          that campaign.  Those intervals are merged into islands (gap between
          neighbouring clicks > 7 d).  Per person:
            * islands x sessions small  -> hash join on person + range test, counted per (campaign, day);
            * otherwise (whales)       -> islands are cut at UTC midnight and the sessions inside each
              piece are counted with cumulative per-person session counts (two ASOF joins), so the cost
              stays O(clicks + sessions) with no click x session blow-up.
"""
import os

import sql_dialect

DAY_MS = 86400000
SEVEN_DAYS_MS = 7 * DAY_MS
# islands x sessions of one person above which the day-piece path is used (SS_HEAVY_PAIRS overrides it:
# 0 sends everyone down the piece path, a huge value sends everyone down the direct join; both must agree).
HEAVY_PAIRS = int(os.environ.get("SS_HEAVY_PAIRS", "512"))
CLICK_COLUMNS = ["click_id", "user_id", "campaign_id", "ts"]


def build_attribution(con, data_dir, scratch, window_start_ms, window_end_ms, special=None):
    paths, escaped = sql_dialect.prepare_files(
        sql_dialect.list_csv(os.path.join(data_dir, "clicks")), os.path.join(scratch, "esc"), "clicks", special)
    src = sql_dialect.parsed(paths, escaped, 4, ["c_id", "c_user", "c_camp", "c_ts"])
    lo = window_start_ms - SEVEN_DAYS_MS

    # Copies of a click have identical values; min(struct) keeps one whole row deterministically.
    con.execute(f"""
        CREATE OR REPLACE TABLE clicks_ok AS
        WITH d AS (
            SELECT c_id AS click_id, min(struct_pack(u := c_user, k := c_camp, t := c_ts)) AS r
            FROM ({src}) WHERE c_id IS NOT NULL GROUP BY c_id
        ), v AS (
            SELECT click_id, d.r.u AS u, d.r.k AS campaign, ss_ts(d.r.t) AS ts FROM d
        )
        SELECT COALESCE(m.person, v.u) AS person, v.click_id, v.campaign, v.ts
        FROM v JOIN campaigns c ON v.campaign = c.campaign_id
        LEFT JOIN idmap m ON v.u = m.user_id
        WHERE v.u IS NOT NULL AND v.ts >= {lo} AND v.ts < {window_end_ms}
          AND COALESCE(m.person, v.u) IN (SELECT person FROM sessions)
    """)

    # Credit: latest click at or before the start; ties on ts -> smallest click_id (byte-wise).
    con.execute("""
        CREATE OR REPLACE TABLE click_pt AS
        SELECT person, ts, arg_min(campaign, click_id) AS campaign
        FROM clicks_ok GROUP BY person, ts
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE credited AS
        SELECT s.sstart, s.events, s.revenue,
               CASE WHEN c.ts IS NOT NULL AND s.sstart - c.ts <= {SEVEN_DAYS_MS} THEN c.campaign
                    ELSE 'organic' END AS campaign
        FROM sessions s ASOF LEFT JOIN click_pt c ON s.person = c.person AND s.sstart >= c.ts
    """)
    con.execute("DROP TABLE click_pt")

    # Assists.
    con.execute(f"""
        CREATE OR REPLACE TABLE islands AS
        WITH x AS (
            SELECT person, campaign, ts,
                   lag(ts) OVER (PARTITION BY person, campaign ORDER BY ts) AS prev
            FROM clicks_ok
        ), y AS (
            SELECT person, campaign, ts,
                   sum(CASE WHEN prev IS NULL OR ts - prev > {SEVEN_DAYS_MS} THEN 1 ELSE 0 END)
                       OVER (PARTITION BY person, campaign ORDER BY ts) AS isl
            FROM x
        )
        SELECT person, campaign, min(ts) AS lo, max(ts) + {SEVEN_DAYS_MS} AS hi
        FROM y GROUP BY person, campaign, isl
    """)
    con.execute("DROP TABLE clicks_ok")
    # Persons whose (islands x sessions) product is small are counted with a plain hash join: every
    # (island, session) candidate pair is a real assist or a cheap range test. The few heavy persons
    # (whales) go through the day-piece / cumulative-count path below.
    con.execute(f"""
        CREATE OR REPLACE TABLE pflag AS
        SELECT i.person, (i.ni * s.ns > {HEAVY_PAIRS}) AS big
        FROM (SELECT person, count(*) AS ni FROM islands GROUP BY person) i
        JOIN (SELECT person, count(*) AS ns FROM sessions GROUP BY person) s ON i.person = s.person
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE pieces AS
        SELECT person, campaign, dn, plo, phi, plo - 1 AS plo1
        FROM (
            SELECT person, campaign, dn,
                   GREATEST(lo, dn * {DAY_MS}) AS plo,
                   LEAST(hi, dn * {DAY_MS} + {DAY_MS - 1}) AS phi
            FROM (
                SELECT person, campaign, lo, hi, unnest(generate_series(lo // {DAY_MS}, hi // {DAY_MS})) AS dn
                FROM (SELECT i.person, i.campaign, GREATEST(i.lo, {window_start_ms}) AS lo,
                             LEAST(i.hi, {window_end_ms - 1}) AS hi
                      FROM islands i JOIN pflag f ON i.person = f.person WHERE f.big)
                WHERE lo <= hi
            )
        )
    """)
    con.execute("""
        CREATE OR REPLACE TABLE cums AS
        SELECT s.person, s.sstart, row_number() OVER (PARTITION BY s.person ORDER BY s.sstart) AS cum
        FROM sessions s JOIN pflag f ON s.person = f.person WHERE f.big
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE assisted AS
        SELECT campaign, dn, sum(n) AS assisted FROM (
            SELECT i.campaign, s.sstart // {DAY_MS} AS dn, count(*) AS n
            FROM islands i
            JOIN pflag f ON i.person = f.person AND NOT f.big
            JOIN sessions s ON s.person = i.person AND s.sstart BETWEEN i.lo AND i.hi
            GROUP BY i.campaign, dn
            UNION ALL
            SELECT campaign, dn, sum(n) AS n FROM (
                SELECT p.campaign, p.dn, COALESCE(h.cum, 0) - COALESCE(l.cum, 0) AS n
                FROM pieces p
                ASOF LEFT JOIN cums h ON p.person = h.person AND p.phi >= h.sstart
                ASOF LEFT JOIN cums l ON p.person = l.person AND p.plo1 >= l.sstart
            ) GROUP BY campaign, dn
        ) WHERE n > 0 GROUP BY campaign, dn
    """)
    for t in ("islands", "pieces", "cums", "pflag"):
        con.execute(f"DROP TABLE {t}")
