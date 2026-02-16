"""Local Flask server for live-updating rare plane map."""

from __future__ import annotations

from typing import List, Optional, Tuple

from flask import Flask, jsonify

from fr24_client import fetch_live_flights
from rarity import assign_rarity
from map_generator import generate_map_html

app = Flask(__name__)

# Set by main.py before starting
_config = {
    "min_rarity": 10.0,
    "bounds": None,
    "challenges": None,  # list of challenge text strings, or None
}

# Cached rarity lookup — built once at startup, reused on every refresh
_rarity_lookup = None

# Parsed challenge filters — built once at startup from challenge text
_parsed_challenges = None


def configure(
    min_rarity: float = 10.0,
    bounds: Optional[Tuple[float, ...]] = None,
    challenges: Optional[List[str]] = None,
) -> None:
    """Set server configuration and pre-cache rarity data."""
    _config["min_rarity"] = min_rarity
    _config["bounds"] = bounds
    _config["challenges"] = challenges

    # Pre-cache the Skycards rarity lookup so refreshes are fast
    global _rarity_lookup
    from skycards_api import build_rarity_lookup
    _rarity_lookup = build_rarity_lookup()
    print(f"  Cached {len(_rarity_lookup):,} aircraft types from Skycards")

    # Pre-parse challenges so each refresh is fast
    if challenges:
        global _parsed_challenges
        from challenges import parse_challenges
        _parsed_challenges = parse_challenges(challenges)
        for i, ch in enumerate(_parsed_challenges, 1):
            print(f"  Challenge {i}: {ch.description}")


def _flight_to_dict(row: dict, challenge_label: str = "") -> Optional[dict]:
    """Convert a flight row to a dict for JSON, or None if invalid position."""
    lat = row.get("latitude")
    lon = row.get("longitude")
    if lat is None or lon is None or (lat == 0 and lon == 0):
        return None
    d = {
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
    }
    if challenge_label:
        d["challenge"] = challenge_label
    return d


def _fetch_rare_flights() -> list:
    """Fetch live flights, assign rarity, filter, and return as list of dicts.

    Always returns rare planes (by min_rarity). When challenges are active,
    also includes challenge matches tagged with their challenge text.
    """
    flights_df = fetch_live_flights(_config["bounds"])
    if len(flights_df) == 0:
        return []
    enriched = assign_rarity(flights_df, lookup=_rarity_lookup)

    seen_ids: set = set()
    results: list = []

    # Always include rare planes
    rare = enriched.filter(enriched["rarity"] >= _config["min_rarity"])
    rare = rare.sort("rarity", descending=True)
    for row in rare.iter_rows(named=True):
        d = _flight_to_dict(row)
        if d:
            fid = row.get("flightid")
            seen_ids.add(fid)
            results.append(d)

    # Add challenge matches (tagged with challenge number, deduplicated)
    if _parsed_challenges:
        from challenges import run_challenges

        # Build a map from challenge text -> 1-indexed number
        ch_number = {ch.original_text: i for i, ch in enumerate(_parsed_challenges, 1)}

        results_by_challenge = run_challenges(enriched, _parsed_challenges)
        for challenge, matches_df in results_by_challenge:
            num = ch_number[challenge.original_text]
            for row in matches_df.iter_rows(named=True):
                fid = row.get("flightid")
                if fid in seen_ids:
                    # Already on map as rare — just tag it with challenge too
                    for existing in results:
                        if existing.get("callsign") == (row.get("callsign") or ""):
                            existing.setdefault("challenge", num)
                            break
                    continue
                seen_ids.add(fid)
                d = _flight_to_dict(row, challenge_label=num)
                if d:
                    results.append(d)

    return results


@app.route("/")
def index():
    """Serve an empty map shell that auto-refreshes on load for instant startup."""
    challenge_texts = None
    if _parsed_challenges:
        challenge_texts = [ch.original_text for ch in _parsed_challenges]
    html = generate_map_html(
        [], min_rarity=_config["min_rarity"], challenge_texts=challenge_texts
    )
    return html


@app.route("/api/flights")
def api_flights():
    """Return current rare flights as JSON."""
    flights = _fetch_rare_flights()
    resp = {"flights": flights, "count": len(flights)}
    if _parsed_challenges:
        resp["challenges"] = [
            {"text": ch.original_text, "description": ch.description}
            for ch in _parsed_challenges
        ]
    return jsonify(resp)


def start(port: int = 5050) -> None:
    """Start the Flask server."""
    # Kill any leftover server on the same port from a previous run
    import os
    import subprocess
    import time
    my_pid = str(os.getpid())
    killed = False
    try:
        pids = subprocess.check_output(
            ["lsof", "-ti", f":{port}"], stderr=subprocess.DEVNULL, text=True
        ).strip()
        if pids:
            for pid in pids.splitlines():
                if pid != my_pid:
                    subprocess.run(["kill", "-9", pid], stderr=subprocess.DEVNULL)
                    killed = True
            if killed:
                print(f"  Stopped previous server on port {port}")
                # Wait for the OS to release the port
                for _ in range(20):
                    time.sleep(0.25)
                    try:
                        subprocess.check_output(
                            ["lsof", "-ti", f":{port}"],
                            stderr=subprocess.DEVNULL, text=True,
                        )
                    except subprocess.CalledProcessError:
                        break  # port is free
    except subprocess.CalledProcessError:
        pass  # nothing on that port

    print(f"\n  Server running at http://localhost:{port}")
    print("  Press Ctrl+C to stop\n")
    app.run(host="127.0.0.1", port=port, debug=False)
