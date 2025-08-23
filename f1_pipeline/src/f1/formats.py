from typing import Any

def circuits_rows(circuits: list[dict[str, Any]], year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in circuits:
        loc = c.get("Location", {})
        rows.append({
            "year": year,
            "circuit_id": c["circuitId"],
            "circuit_name": c["circuitName"],
            "city": loc.get("locality"),
            "country": loc.get("country"),
            "latitude": float(loc["lat"]) if loc.get("lat") else None,
            "longitude": float(loc["long"]) if loc.get("long") else None,
            "wiki_url": c.get("url"),
        })
    return rows

def constructors_rows(constructors: list[dict[str, Any]], year: int) -> list[dict[str, Any]]:
    return [{
        "year": year,
        "constructor_id": k.get("constructorId"),
        "constructor_name": k.get("name"),
        "nationality": k.get("nationality"),
        "wiki_url": k.get("url"),
    } for k in constructors]

def constructor_standings_rows(items: list[dict[str, Any]], year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for s in items:
        cons = s.get("Constructor", {})
        rows.append({
            "year": year,
            "position": int(s["position"]) if s.get("position") else None,
            "positionText": s.get("positionText"),
            "points": float(s["points"]) if s.get("points") else None,
            "wins": int(s["wins"]) if s.get("wins") else None,
            "constructorId": cons.get("constructorId"),
            "url": cons.get("url"),
            "name": cons.get("name"),
            "nationality": cons.get("nationality"),
        })
    return rows