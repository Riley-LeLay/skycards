"""Fetch aircraft rarity data from the Skycards API."""

from __future__ import annotations

import json
import os
import time
import urllib.request
from typing import Dict, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODELS_CACHE = os.path.join(BASE_DIR, "models_cache.json")
MODELS_URL = "https://api.skycards.oldapes.com/models"

# Cache for 6 hours (Skycards updates rarity roughly every 6 months,
# but we check more frequently in case of updates)
CACHE_MAX_AGE_S = 6 * 3600


def _fetch_models() -> dict:
    """Fetch the full models database from the Skycards API."""
    url = f"{MODELS_URL}?updatedAt=0"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "SkyCards/3.0.0 (iPhone; iOS 18.0)")
    req.add_header("X-Client-Version", "3.0.0")
    req.add_header("Accept", "application/json")
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read())


def _load_cached_models() -> Optional[dict]:
    """Load models from local cache if fresh enough."""
    if not os.path.exists(MODELS_CACHE):
        return None
    try:
        with open(MODELS_CACHE, "r") as f:
            cached = json.load(f)
        if time.time() - cached.get("_cached_at", 0) < CACHE_MAX_AGE_S:
            return cached
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def _save_models_cache(data: dict) -> None:
    """Save models data to local cache."""
    data["_cached_at"] = time.time()
    with open(MODELS_CACHE, "w") as f:
        json.dump(data, f)


def get_models() -> dict:
    """Get the Skycards models database, using cache when available.

    Returns the raw API response with keys: updatedAt, rows, blacklist.
    Each row has: id (ICAO code), name, rareness, cardCategory, xp, ftea,
    num, manufacturer, firstFlight, and many more fields.
    """
    cached = _load_cached_models()
    if cached is not None:
        return cached

    data = _fetch_models()
    _save_models_cache(data)
    return data


def build_rarity_lookup() -> Dict[str, dict]:
    """Build a lookup dict from ICAO type code to rarity info.

    Returns:
        Dict mapping typecode -> {
            "name": str,
            "rareness": int (0-2000 scale),
            "rarity_display": float (in-game display scale, rareness/100),
            "category": str (common/uncommon/scarce/rare/ultra/...),
            "xp": int,
            "ftea": float,
            "num": int (total registered aircraft),
            "manufacturer": str,
        }
    """
    models = get_models()
    blacklist = set(models.get("blacklist", []))
    lookup = {}

    for row in models.get("rows", []):
        code = row.get("id", "")
        if not code or code in blacklist:
            continue
        # Convert raw rareness to in-game display scale (rareness / 100)
        # e.g. A320 rareness=47 -> display 0.47, R300 rareness=895 -> display 8.95
        rareness = row.get("rareness", 0)
        rarity_display = rareness / 100.0
        lookup[code] = {
            "name": row.get("name", code),
            "rareness": rareness,
            "rarity_display": rarity_display,
            "category": row.get("cardCategory", "unknown"),
            "xp": row.get("xp", 0),
            "ftea": row.get("ftea", 0),
            "num": row.get("num", 0),
            "manufacturer": row.get("manufacturer", ""),
        }

    return lookup
