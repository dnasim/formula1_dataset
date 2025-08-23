import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2 import extras
import os
from typing import Any, Dict, Optional, Callable

BASE_URL = "https://api.jolpi.ca/ergast/f1" 

class F1Client:
    def __init__(self, base_url: str = BASE_URL, timeout_sec: int = 15):
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "donish-f1-client/0.1"})

        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods={"GET"}
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _get(self, *path_segments: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if len(path_segments) == 1:
            path_str = str(path_segments[0]).strip("/")
        else:
            path_str = "/".join(str(p).strip("/") for p in path_segments if p is not None)

        url = f"{self.base_url}/{path_str}/"
        resp = self.session.get(url, params=params or {}, timeout=self.timeout_sec)
        resp.raise_for_status()
        return resp.json()


# ---------------- Parsers ----------------
def parse_circuits(j: dict[str, Any]) -> list[dict[str, Any]]:
    return j["MRData"]["CircuitTable"].get("Circuits", [])

def parse_constructors(j: dict[str, Any]) -> list[dict[str, Any]]:
    return j["MRData"]["ConstructorTable"].get("Constructors", [])

def parse_constructor_standings(j: dict[str, Any]) -> list[dict[str, Any]]:
    lists = j["MRData"]["StandingsTable"].get("StandingsLists", [])
    return lists[0].get("ConstructorStandings", []) if lists else []

def parse_drivers(j: dict[str, Any]) -> list[dict[str, Any]]:
    return j["MRData"]["DriverTable"].get("Drivers", [])

def parse_driver_standings(j: dict[str, Any]) -> list[dict[str, Any]]:
    lists = j["MRData"]["StandingsTable"].get("StandingsLists", [])
    return lists[0].get("DriverStandings", []) if lists else []

def parse_races_like(j: dict[str, Any]) -> list[dict[str, Any]]:
    return j["MRData"]["RaceTable"].get("Races", [])

def parse_seasons(j: dict[str, Any]) -> list[dict[str, Any]]:
    return j["MRData"]["SeasonTable"].get("Seasons", [])

def parse_status(j: dict[str, Any]) -> list[dict[str, Any]]:
    return j["MRData"]["StatusTable"].get("Status", [])


PARSERS: dict[str, Callable[[dict[str, Any]], list[dict[str, Any]]]] = {
    "circuits": parse_circuits,
    "constructors": parse_constructors,
    "constructorstandings": parse_constructor_standings,
    "drivers": parse_drivers,
    "driverstandings": parse_driver_standings,
    "races": parse_races_like,
    "results": parse_races_like,
    "qualifying": parse_races_like,
    "sprint": parse_races_like,
    "seasons": parse_seasons,
    "status": parse_status,
}

def format_circuits(client: F1Client, year: int):
    client = F1Client() 
    raw = client._get(f"{year}/circuits")
    circuits = parse_circuits(raw)

    rows = []
    for circuit in circuits:
        loc = circuit.get("Location", {})
        rows.append({
            "year": year,
            "circuit_id": circuit.get("circuitId"),
            "circuit_name": circuit.get("circuitName"),
            "city": loc.get("locality"),
            "country": loc.get("country"),
            "latitude": float(loc["lat"]) if loc.get("lat") else None,
            "longitude": float(loc["long"]) if loc.get("long") else None,
            "wiki_url": circuit.get("url"),
        })
    return rows

def insert_circuits_stg(rows):

    cols = ["year","circuit_id","circuit_name","city","country","latitude","longitude","wiki_url"]
    
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "formula1"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS f1_stg.circuits_stg (
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
    """)

    # Convert list[dict] -> list[tuple] in column order
    values = [tuple(r.get(c) for c in cols) for r in rows]

    sql = f"""
        INSERT INTO f1_stg.circuits_stg ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (year, circuit_id) DO UPDATE SET
          year = EXCLUDED.year,
          circuit_name = EXCLUDED.circuit_name,
          city = EXCLUDED.city,
          country = EXCLUDED.country,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          wiki_url = EXCLUDED.wiki_url;
    """
    extras.execute_values(cur, sql, values, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

def format_constructors(client: F1Client, year: int):
    client = F1Client() 
    raw = client._get(f"{year}/constructors")
    constructors = parse_constructors(raw)

    rows = []
    for constructor in constructors:
        loc = constructor.get("Location", {})
        rows.append({
            "year": year,
            "constructor_id": constructor.get("constructorId"),
            "constructor_name": constructor.get("name"),
            "nationality": constructor.get("nationality"),
            "wiki_url": constructor.get("url"),
        })
    return rows

def insert_constructors_stg(rows):

    cols = ["year","constructor_id","constructor_name","nationality","wiki_url"]
    
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "formula1"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS f1_stg.constructors_stg (
          year              INTEGER,
          constructor_id    TEXT,
          constructor_name  TEXT,
          nationality       TEXT,
          wiki_url          TEXT,
          PRIMARY KEY (year, constructor_id)
        );
    """)

    # Convert list[dict] -> list[tuple] in column order
    values = [tuple(r.get(c) for c in cols) for r in rows]

    sql = f"""
        INSERT INTO f1_stg.constructors_stg ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (year, constructor_id) DO UPDATE SET
          year = EXCLUDED.year,
          constructor_id = EXCLUDED.constructor_id,
          constructor_name = EXCLUDED.constructor_name,
          nationality = EXCLUDED.nationality,
          wiki_url = EXCLUDED.wiki_url;
    """
    extras.execute_values(cur, sql, values, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

def format_constructor_standings(client: F1Client, year: int):
    client = F1Client() 
    raw = client._get(f"{year}/constructorstandings")
    constructor_standings = parse_constructor_standings(raw)

    rows = []
    for constructor_standing in constructor_standings:
        loc = constructor_standing.get("Constructor", {})
        rows.append({
            "year": year,
            "position": constructor_standing.get('position'),
            "positionText": constructor_standing.get('positionText'),
            "points": constructor_standing.get('points'),
            "wins": constructor_standing.get('wins'),
            "constructorId": loc.get("constructorId"),
            "url": loc.get("url"),
            "name": loc.get("name"),
            "nationality": loc.get("nationality"),
        })
    return rows

def insert_constructor_standings_stg(rows):

    cols = ["year","position","positionText","points","wins","constructorId","url","name","nationality"]
    
    conn = psycopg2.connect(
        dbname=os.getenv("PGDATABASE", "formula1"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS f1_stg.constructor_standings_stg (
          year              INTEGER,
          position          INTEGER,
          positionText      TEXT,
          points            INTEGER,
          wins              INTEGER,
          constructorId     TEXT,
          url               TEXT,
          name              TEXT,
          nationality       TEXT,
          PRIMARY KEY (year, constructorId)
        );
    """)

    # Convert list[dict] -> list[tuple] in column order
    values = [tuple(r.get(c) for c in cols) for r in rows]

    sql = f"""
        INSERT INTO f1_stg.constructor_standings_stg ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (year, constructorId) DO UPDATE SET
          year = EXCLUDED.year,
          position = EXCLUDED.position,
          positionText = EXCLUDED.positionText,
          points = EXCLUDED.points,
          wins = EXCLUDED.wins,
          constructorId = EXCLUDED.constructorId,
          url = EXCLUDED.url,
          name = EXCLUDED.name,
          nationality = EXCLUDED.nationality
          ;
    """
    extras.execute_values(cur, sql, values, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()

def main():
    if __name__ == "__main__":
        client = F1Client()
        cnstr = format_constructors(client, 2025)
        circ = format_circuits(client, 2025)
        consta = format_constructor_standings(client, 2025)
        insert_constructors_stg(cnstr)
        insert_circuits_stg(circ)
        insert_constructor_standings_stg(consta)
        print('Process Completed')

main()