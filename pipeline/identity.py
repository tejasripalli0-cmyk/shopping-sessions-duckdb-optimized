"""
Linked IDs: every valid links line joins two IDs into one person (transitively);
a person is named by the byte-wise smallest of their IDs.

build_idmap() resolves the links inside DuckDB + a small integer union-find and
produces the table  idmap(user_id, person)  holding ONLY the IDs whose person
name differs from the ID itself. An ID missing from idmap is its own person, so
events and clicks need just a LEFT JOIN (COALESCE(person, user_id)).

UnionFind / load_identity_links are the plain-Python version used by the oracle.
"""
import os

import parse_utils
import sql_dialect


class UnionFind:
    """String union-find; canonical(x) is the smallest ID of x's person."""

    def __init__(self):
        self.parent = {}
        self.smallest = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.smallest[x] = x
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra
            self.smallest[ra] = min(self.smallest[ra], self.smallest[rb])

    def canonical(self, x):
        return self.smallest[self.find(x)]


def load_identity_links(data_dir):
    uf = UnionFind()
    links_dir = os.path.join(data_dir, "links")
    for path in sql_dialect.list_csv(links_dir):
        for a, b in parse_utils.read_csv(path, ["user_id_a", "user_id_b"]):
            if a is not None and b is not None:
                uf.union(a, b)
    return uf


def build_idmap(con, data_dir, scratch, special=None):
    paths, escaped = sql_dialect.prepare_files(
        sql_dialect.list_csv(os.path.join(data_dir, "links")), os.path.join(scratch, "esc"), "links", special)
    con.execute(
        "CREATE OR REPLACE TABLE link_edges AS SELECT DISTINCT a, b FROM ("
        + sql_dialect.parsed(paths, escaped, 2, ["a", "b"])
        + ") WHERE a IS NOT NULL AND b IS NOT NULL AND a <> b"
    )
    con.execute(
        "CREATE OR REPLACE TABLE link_ids AS SELECT id, (row_number() OVER ()) - 1 AS n FROM "
        "(SELECT a AS id FROM link_edges UNION SELECT b FROM link_edges)"
    )
    n_ids = con.execute("SELECT count(*) FROM link_ids").fetchone()[0]
    if n_ids == 0:
        con.execute("CREATE OR REPLACE TABLE idmap(user_id VARCHAR, person VARCHAR)")
        con.execute("DROP TABLE link_edges")
        con.execute("DROP TABLE link_ids")
        return
    comp_csv = os.path.join(scratch, "components.csv")
    parent = list(range(n_ids))
    cur = con.execute(
        "SELECT x.n, y.n FROM link_edges e JOIN link_ids x ON e.a = x.id JOIN link_ids y ON e.b = y.id"
    )
    while True:
        batch = cur.fetchmany(500000)
        if not batch:
            break
        for a, b in batch:
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            while parent[b] != b:
                parent[b] = parent[parent[b]]
                b = parent[b]
            if a != b:
                parent[a] = b
    with open(comp_csv, "w") as f:
        for i in range(n_ids):
            r = i
            while parent[r] != r:
                parent[r] = parent[parent[r]]
                r = parent[r]
            f.write(f"{i},{r}\n")
    del parent
    con.execute(
        "CREATE OR REPLACE TABLE idmap AS "
        "WITH x AS (SELECT i.id, c.root FROM link_ids i JOIN "
        f"read_csv('{comp_csv}', columns={{'n': 'BIGINT', 'root': 'BIGINT'}}, header=false, "
        "delim=',', auto_detect=false) c ON i.n = c.n), "
        "m AS (SELECT root, min(id) AS person FROM x GROUP BY root) "
        "SELECT x.id AS user_id, m.person FROM x JOIN m ON x.root = m.root WHERE x.id <> m.person"
    )
    os.remove(comp_csv)
    con.execute("DROP TABLE link_edges")
    con.execute("DROP TABLE link_ids")
