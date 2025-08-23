import os
import psycopg2
from psycopg2.extras import execute_values

PG_DSN = {
    "dbname": os.getenv("PGDATABASE", "formula1"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
}

def get_conn():
    return psycopg2.connect(**PG_DSN)

def ensure_schema(cur, schema: str):
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

def insert_rows(table: str, cols: list[str], rows: list[dict], upsert_on: list[str], update_cols: list[str]):
    if not rows:
        return
    values = [tuple(r.get(c) for c in cols) for r in rows]
    conflict = ", ".join(upsert_on)
    setters  = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    sql = f"""
        INSERT INTO {table} ({', '.join(cols)})
        VALUES %s
        ON CONFLICT ({conflict}) DO UPDATE SET
          {setters};
    """
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=1000)
