from ..client import F1Client
from ..parsers import parse_constructors
from ..formats import constructors_rows
from ..db import get_conn, ensure_schema, insert_rows

SCHEMA = "f1_stg"
TABLE  = f"{SCHEMA}.constructors_stg"
COLS   = ["year","constructor_id","constructor_name","nationality","wiki_url"]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  year             INTEGER,
  constructor_id   TEXT,
  constructor_name TEXT,
  nationality      TEXT,
  wiki_url         TEXT,
  PRIMARY KEY (year, constructor_id)
);
"""

def load_constructors(client: F1Client, year: int):
    data = client.get(str(year), "constructors")
    rows = constructors_rows(parse_constructors(data), year)
    with get_conn() as conn, conn.cursor() as cur:
        ensure_schema(cur, SCHEMA)
        cur.execute(DDL)
    insert_rows(
        table=TABLE,
        cols=COLS,
        rows=rows,
        upsert_on=["year","constructor_id"],
        update_cols=["constructor_name","nationality","wiki_url"],
    )