from typing import Any, Callable

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