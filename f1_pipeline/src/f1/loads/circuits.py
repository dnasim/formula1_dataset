from ..client import F1Client
from ..parsers import parse_circuits
from ..formats import circuits_rows
from ..db import get_conn, ensure_schema, insert_rows

SCHEMA = "f1_stg"
TABLE  = f"{SCHEMA}.circuits_stg"
COLS   = ["year","circuit_id","circuit_name","city","country","latitude","longitude","wiki_url"]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  year          INTEGER,
  circuit_id    TEXT,
  circuit_name  TEXT,
  city          TEXT,
  country       TEXT,
  latitude      DOUBLE PRECISION,
  longitude     DOUBLE PRECISION,
  wiki_url      TEXT,
  PRIMARY KEY (year, circuit_id)
);
"""

def load_circuits(client: F1Client, year: int):
    data = client.get(str(year), "circuits")
    rows = circuits_rows(parse_circuits(data), year)
    with get_conn() as conn, conn.cursor() as cur:
        ensure_schema(cur, SCHEMA)
        cur.execute(DDL)
    insert_rows(TABLE, COLS, rows, upsert_on=["year","circuit_id"],
                update_cols=["circuit_name","city","country","latitude","longitude","wiki_url"])