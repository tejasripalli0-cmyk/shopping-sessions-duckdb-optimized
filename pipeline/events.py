"""
Events: parse -> dedup on event_id (smallest ingest_ts) -> validate the kept copy
-> drop late -> person -> sessionize.  See rules.md "Events, in this order".

Result tables (all in the DuckDB connection):
  sessions(person, sstart, events, revenue)   one row per session
  purchases(person, sstart, price, cat)       one row per surviving purchase
"""
import os

import sql_dialect

SESSION_GAP_MS = 30 * 60 * 1000
LATE_LIMIT_MS = 24 * 60 * 60 * 1000
MAX_I64 = 9223372036854775807
EVENT_COLUMNS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]


def build_sessions(con, data_dir, scratch, window_start_ms, window_end_ms, special=None):
    paths, escaped = sql_dialect.prepare_files(
        sql_dialect.list_csv(os.path.join(data_dir, "events")), os.path.join(scratch, "esc"), "events", special)
    src = sql_dialect.parsed(paths, escaped, 7, ["c_id", "c_user", "c_ts", "c_ing", "c_type", "c_prod", "c_price"])

    # Steps 1-3: parse, dedup (BEFORE validation), validate the kept copy, drop late.
    # Validity is evaluated per row, but only the flag of the copy with the smallest
    # ingest_ts survives the GROUP BY, so a later valid copy never rescues an invalid one.
    # The struct is never NULL, so arg_min cannot skip an invalid kept copy.
    con.execute(f"""
        CREATE OR REPLACE TABLE ev_kept AS
        WITH f AS (
            SELECT c_id AS event_id, c_user AS user_id, ss_ts(c_ts) AS ts, ss_ts(c_ing) AS ing,
                   c_type AS et, c_prod AS prod, c_price AS price
            FROM ({src})
            WHERE c_id IS NOT NULL
        ), g AS (
            SELECT event_id,
                   COALESCE(ing, {MAX_I64}) AS ing_key,
                   COALESCE(
                       ts >= {window_start_ms} AND ts < {window_end_ms}
                       AND ing IS NOT NULL AND user_id IS NOT NULL
                       AND et IN ('view', 'cart', 'purchase') AND prod IS NOT NULL
                       AND (et <> 'purchase' OR (regexp_full_match(price, '[0-9]+')
                            AND TRY_CAST(price AS BIGINT) BETWEEN 1 AND 10000000))
                       AND ing - ts <= {LATE_LIMIT_MS}, false) AS ok,
                   user_id, ts, prod,
                   CASE WHEN et = 'purchase' THEN TRY_CAST(price AS BIGINT) ELSE 0 END AS pr
            FROM f
        )
        SELECT arg_min(struct_pack(ok := ok,
                                   u := CASE WHEN ok THEN user_id END,
                                   t := CASE WHEN ok THEN ts END,
                                   p := CASE WHEN ok THEN pr END,
                                   prod := CASE WHEN ok AND pr > 0 THEN prod END), ing_key) AS k
        FROM g GROUP BY event_id
    """)

    # Steps 5-6, fused with link resolution: person = COALESCE(idmap.person, user_id); sessionize per person.
    # A row starts a session when it is the person's first or its gap to the previous row exceeds 30 min;
    # rows sharing a ts are peers of the window frame, so they all get the session start of the first of them.
    # Only purchases keep their product_id (needed later for the category); everything else has price 0 / prod NULL.
    con.execute(f"""
        CREATE OR REPLACE TABLE ev_sess AS
        SELECT person, price, prod,
               max(CASE WHEN prev IS NULL OR ts - prev > {SESSION_GAP_MS} THEN ts END)
                   OVER (PARTITION BY person ORDER BY ts) AS sstart
        FROM (SELECT person, ts, price, prod, lag(ts) OVER (PARTITION BY person ORDER BY ts) AS prev
              FROM (SELECT COALESCE(m.person, e.k.u) AS person, e.k.t AS ts, e.k.p AS price,
                           e.k.prod AS prod
                    FROM ev_kept e LEFT JOIN idmap m ON e.k.u = m.user_id
                    WHERE e.k.ok))
    """)
    con.execute("DROP TABLE ev_kept")
    con.execute("""
        CREATE OR REPLACE TABLE sessions AS
        SELECT person, sstart, count(*) AS events, CAST(sum(price) AS BIGINT) AS revenue
        FROM ev_sess GROUP BY person, sstart
    """)
    # Step 7: categories come from products.csv; a product not in the file is 'unknown'.
    con.execute("""
        CREATE OR REPLACE TABLE purchases AS
        SELECT e.person, e.sstart, e.price,
               CASE WHEN pr.product_id IS NULL THEN 'unknown' ELSE pr.category END AS cat
        FROM ev_sess e LEFT JOIN products pr ON e.prod = pr.product_id
        WHERE e.price > 0
    """)
    con.execute("DROP TABLE ev_sess")
