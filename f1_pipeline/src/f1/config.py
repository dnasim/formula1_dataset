import os

PG_DSN = {
    "dbname": os.getenv("PGDATABASE", "formula1"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
}

BASE_URL = os.getenv("F1_BASE_URL", "https://api.jolpi.ca/ergast/f1")

SCHEMA_STG = os.getenv("F1_STG_SCHEMA", "f1_stg")