"""Local Flask server for live-updating rare plane map."""

from __future__ import annotations

from typing import Optional, Tuple

from flask import Flask, jsonify

from fr24_client import fetch_live_flights
from rarity import assign_rarity
from map_generator import generate_map_html

app = Flask(__name__)

# Set by main.py before starting
_config = {
    "min_rarity": 10.0,
    "bounds": None,
}

# Cached rarity lookup — built once at startup, reused on every refresh
_rarity_lookup = None


def configure(min_rarity: float = 10.0, bounds: Optional[Tuple[float, ...]] = None) -> None:
    """Set server configuration and pre-cache rarity data."""
    _config["min_rarity"] = min_rarity
    _config["bounds"] = bounds

    # Pre-cache the Skycards rarity lookup so refreshes are fast
    global _rarity_lookup
    from skycards_api import build_rarity_lookup
    _rarity_lookup = build_rarity_lookup()
    print(f"  Cached {len(_rarity_lookup):,} aircraft types from Skycards")


def _fetch_rare_flights() -> list:
    """Fetch live flights, assign rarity, filter, and return as list of dicts."""
    flights_df = fetch_live_flights(_config["bounds"])
    if len(flights_df) == 0:
        return []
    enriched = assign_rarity(flights_df, lookup=_rarity_lookup)
    rare = enriched.filter(enriched["rarity"] >= _config["min_rarity"])
    rare = rare.sort("rarity", descending=True)

    results = []
    for row in rare.iter_rows(named=True):
        lat = row.get("latitude")
        lon = row.get("longitude")
        if lat is None or lon is None or (lat == 0 and lon == 0):
            continue
        results.append({
            "latitude": lat,
            "longitude": lon,
            "rarity": row.get("rarity", 0),
            "tier": row.get("tier", "Unknown"),
            "typecode": row.get("typecode", ""),
            "aircraft_name": row.get("aircraft_name", "") or "",
            "xp": row.get("xp", 0) or 0,
            "registration": row.get("registration", "") or "",
            "callsign": row.get("callsign", "") or "",
            "altitude": row.get("altitude", 0) or 0,
            "ground_speed": row.get("ground_speed", 0) or 0,
            "origin": row.get("origin", "") or "",
            "destination": row.get("destination", "") or "",
        })
    return results


@app.route("/")
def index():
    """Serve an empty map shell that auto-refreshes on load for instant startup."""
    html = generate_map_html([], min_rarity=_config["min_rarity"])
    return html


@app.route("/api/flights")
def api_flights():
    """Return current rare flights as JSON."""
    flights = _fetch_rare_flights()
    return jsonify({"flights": flights, "count": len(flights)})


def start(port: int = 5050) -> None:
    """Start the Flask server."""
    print(f"\n  Server running at http://localhost:{port}")
    print("  Press Ctrl+C to stop\n")
    app.run(host="127.0.0.1", port=port, debug=False)
