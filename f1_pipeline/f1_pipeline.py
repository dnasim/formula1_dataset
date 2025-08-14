import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{path.strip('/')}/"
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

raw = client._get("2025/circuits")
circuits = parse_circuits(raw)
for circuit in circuits:
    print(circuit['circuitName'])