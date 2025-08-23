from ..client import F1Client
from ..parsers import parse_constructor_standings
from ..formats import constructor_standings_rows
from ..db import get_conn, ensure_schema, insert_rows

SCHEMA = "f1_stg"
TABLE  = f"{SCHEMA}.constructor_standings_stg"
COLS   = ["year","position","positionText","points","wins","constructorId","url","name","nationality"]

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  year            INTEGER,
  position        INTEGER,
  positionText    TEXT,
  points          DOUBLE PRECISION,
  wins            INTEGER,
  constructorId   TEXT,
  url             TEXT,
  name            TEXT,
  nationality     TEXT,
  PRIMARY KEY (year, constructorId)
);
"""

def load_constructor_standings(client: F1Client, year: int):
    data = client.get(str(year), "constructorstandings")
    rows = constructor_standings_rows(parse_constructor_standings(data), year)
    with get_conn() as conn, conn.cursor() as cur:
        ensure_schema(cur, SCHEMA)
        cur.execute(DDL)
    insert_rows(
        table=TABLE,
        cols=COLS,
        rows=rows,
        upsert_on=["year","constructorId"],               
        update_cols=["position","positionText","points","wins","url","name","nationality"],
    )