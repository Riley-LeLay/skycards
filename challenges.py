"""Parse daily challenge text and filter live flights to find matches."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

import polars as pl

from skycards_api import get_models


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class ChallengeType(Enum):
    MANUFACTURER = "manufacturer"
    AIRPORT = "airport"
    AIRPORT_PAIR = "airport_pair"  # from X to Y (or back)
    ROUTE = "route"
    AIRCRAFT_TYPE = "aircraft_type"
    RARITY_TIER = "rarity_tier"
    AIRCRAFT_CLASS = "aircraft_class"
    LATITUDE_REGION = "latitude_region"


@dataclass
class ChallengeFilter:
    challenge_type: ChallengeType
    original_text: str
    description: str  # human-readable summary of what we're matching

    # Only the relevant fields are populated per challenge type
    typecodes: Optional[Set[str]] = None
    airport_icao: Optional[Set[str]] = None
    origin_codes: Optional[Set[str]] = None  # for AIRPORT_PAIR: side A codes
    destination_codes: Optional[Set[str]] = None  # for AIRPORT_PAIR: side B codes
    route_name: Optional[str] = None
    tier: Optional[str] = None
    min_lat: Optional[float] = None  # for latitude-based filters
    max_lat: Optional[float] = None


# ---------------------------------------------------------------------------
# Airport name -> ICAO lookup (notable airports likely in Skycards challenges)
# ---------------------------------------------------------------------------

AIRPORT_LOOKUP: Dict[str, str] = {
    # Extreme locations
    "longyearbyen": "ENSB",
    "svalbard": "ENSB",
    "ushuaia": "SAWH",
    "ushuai": "SAWH",
    # Major hubs
    "heathrow": "EGLL",
    "london heathrow": "EGLL",
    "gatwick": "EGKK",
    "jfk": "KJFK",
    "john f kennedy": "KJFK",
    "kennedy": "KJFK",
    "lax": "KLAX",
    "los angeles": "KLAX",
    "chicago": "KORD",
    "o'hare": "KORD",
    "ohare": "KORD",
    "atlanta": "KATL",
    "dallas": "KDFW",
    "denver": "KDEN",
    "san francisco": "KSFO",
    "sfo": "KSFO",
    "miami": "KMIA",
    "seattle": "KSEA",
    "boston": "KBOS",
    "newark": "KEWR",
    "laguardia": "KLGA",
    "narita": "RJAA",
    "tokyo narita": "RJAA",
    "haneda": "RJTT",
    "tokyo haneda": "RJTT",
    "incheon": "RKSI",
    "seoul": "RKSI",
    "beijing": "ZBAA",
    "shanghai": "ZSPD",
    "pudong": "ZSPD",
    "hong kong": "VHHH",
    "singapore": "WSSS",
    "changi": "WSSS",
    "dubai": "OMDB",
    "abu dhabi": "OMAA",
    "doha": "OTHH",
    "istanbul": "LTFM",
    "paris": "LFPG",
    "charles de gaulle": "LFPG",
    "cdg": "LFPG",
    "amsterdam": "EHAM",
    "schiphol": "EHAM",
    "frankfurt": "EDDF",
    "munich": "EDDM",
    "zurich": "LSZH",
    "rome": "LIRF",
    "fiumicino": "LIRF",
    "madrid": "LEMD",
    "barcelona": "LEBL",
    "lisbon": "LPPT",
    "sydney": "YSSY",
    "melbourne": "YMML",
    "auckland": "NZAA",
    "bangkok": "VTBS",
    "suvarnabhumi": "VTBS",
    "kuala lumpur": "WMKK",
    "jakarta": "WIII",
    "delhi": "VIDP",
    "mumbai": "VABB",
    "johannesburg": "FAOR",
    "cape town": "FACT",
    "cairo": "HECA",
    "sao paulo": "SBGR",
    "guarulhos": "SBGR",
    "mexico city": "MMMX",
    "buenos aires": "SAEZ",
    "ezeiza": "SAEZ",
    "lima": "SPJC",
    "bogota": "SKBO",
    "santiago": "SCEL",
    "toronto": "CYYZ",
    "pearson": "CYYZ",
    "vancouver": "CYVR",
    "anchorage": "PANC",
    "honolulu": "PHNL",
    "taipei": "RCTP",
    "taoyuan": "RCTP",
    "manila": "RPLL",
    "osaka": "RJBB",
    "kansai": "RJBB",
    "moscow": "UUEE",
    "sheremetyevo": "UUEE",
    "helsinki": "EFHK",
    "copenhagen": "EKCH",
    "oslo": "ENGM",
    "stockholm": "ESSA",
    "reykjavik": "BIRK",
    "keflavik": "BIKF",
}

# Common IATA -> ICAO for when users type 3-letter codes
IATA_TO_ICAO: Dict[str, str] = {
    "LYR": "ENSB", "USH": "SAWH", "LHR": "EGLL", "JFK": "KJFK",
    "LAX": "KLAX", "ORD": "KORD", "ATL": "KATL", "DFW": "KDFW",
    "DEN": "KDEN", "SFO": "KSFO", "MIA": "KMIA", "SEA": "KSEA",
    "BOS": "KBOS", "EWR": "KEWR", "LGA": "KLGA", "NRT": "RJAA",
    "HND": "RJTT", "ICN": "RKSI", "PEK": "ZBAA", "PVG": "ZSPD",
    "HKG": "VHHH", "SIN": "WSSS", "DXB": "OMDB", "AUH": "OMAA",
    "DOH": "OTHH", "IST": "LTFM", "CDG": "LFPG", "AMS": "EHAM",
    "FRA": "EDDF", "MUC": "EDDM", "ZRH": "LSZH", "FCO": "LIRF",
    "MAD": "LEMD", "BCN": "LEBL", "LIS": "LPPT", "SYD": "YSSY",
    "MEL": "YMML", "AKL": "NZAA", "BKK": "VTBS", "KUL": "WMKK",
    "CGK": "WIII", "DEL": "VIDP", "BOM": "VABB", "JNB": "FAOR",
    "CPT": "FACT", "CAI": "HECA", "GRU": "SBGR", "MEX": "MMMX",
    "EZE": "SAEZ", "LIM": "SPJC", "BOG": "SKBO", "SCL": "SCEL",
    "YYZ": "CYYZ", "YVR": "CYVR", "ANC": "PANC", "HNL": "PHNL",
    "TPE": "RCTP", "MNL": "RPLL", "KIX": "RJBB",
}

# City name -> list of ICAO codes for multi-airport cities.
# Used by AIRPORT_PAIR challenges so "from London to New York" covers all airports.
CITY_AIRPORTS: Dict[str, List[str]] = {
    "london": ["EGLL", "EGKK", "EGSS", "EGGW", "EGLC"],
    "new york": ["KJFK", "KEWR", "KLGA"],
    "tokyo": ["RJAA", "RJTT"],
    "paris": ["LFPG", "LFPO"],
    "rome": ["LIRF", "LIRA"],
    "milan": ["LIMC", "LIME"],
    "moscow": ["UUEE", "UUDD", "UUWW"],
    "seoul": ["RKSI", "RKSS"],
    "shanghai": ["ZSPD", "ZSSS"],
    "buenos aires": ["SAEZ", "SABE"],
    "sao paulo": ["SBGR", "SBSP"],
    "washington": ["KIAD", "KDCA", "KBWI"],
    "los angeles": ["KLAX", "KBUR", "KLGB", "KSNA"],
    "chicago": ["KORD", "KMDW"],
    "dallas": ["KDFW", "KDAL"],
    "houston": ["KIAH", "KHOU"],
    "san francisco": ["KSFO", "KOAK", "KSJC"],
    "miami": ["KMIA", "KFLL"],
    "bangkok": ["VTBS", "VTBD"],
    "istanbul": ["LTFM", "LTFJ"],
    "sydney": ["YSSY"],
    "melbourne": ["YMML"],
    "auckland": ["NZAA"],
    "dubai": ["OMDB", "OMDW"],
}

# Also populate IATA_TO_ICAO for the secondary airports we just added
_EXTRA_IATA: Dict[str, str] = {
    "LGW": "EGKK", "STN": "EGSS", "LTN": "EGGW", "LCY": "EGLC",
    "ORY": "LFPO", "CIA": "LIRA", "MXP": "LIMC", "BGY": "LIME",
    "DME": "UUDD", "VKO": "UUWW", "GMP": "RKSS", "SHA": "ZSSS",
    "AEP": "SABE", "CGH": "SBSP", "IAD": "KIAD", "DCA": "KDCA",
    "BWI": "KBWI", "BUR": "KBUR", "LGB": "KLGB", "SNA": "KSNA",
    "MDW": "KMDW", "DAL": "KDAL", "IAH": "KIAH", "HOU": "KHOU",
    "OAK": "KOAK", "SJC": "KSJC", "FLL": "KFLL", "DMK": "VTBD",
    "SAW": "LTFJ", "DWC": "OMDW",
}
IATA_TO_ICAO.update(_EXTRA_IATA)


# ---------------------------------------------------------------------------
# Region detection via IATA-to-region lookup + coordinate fallback
# ---------------------------------------------------------------------------

# IATA code -> region for well-known airports (covers the vast majority of flights)
# Built from the most common 200+ airports by traffic
IATA_REGIONS: Dict[str, str] = {}

def _build_iata_regions() -> None:
    """Populate IATA_REGIONS from the IATA_TO_ICAO mapping and known patterns."""
    if IATA_REGIONS:
        return  # already built

    # Map ICAO first-letter -> region (works for 4-letter ICAO codes)
    icao_prefix_to_region = {
        "K": "americas", "P": "americas", "C": "americas", "M": "americas",
        "S": "americas", "T": "americas",
        "E": "europe", "L": "europe", "B": "europe",
        "R": "asia", "Z": "asia", "V": "asia", "W": "asia", "U": "asia",
        "Y": "oceania", "N": "oceania",
        "O": "middle_east", "H": "africa", "F": "africa", "D": "africa", "G": "africa",
    }

    # Derive region from ICAO codes in our IATA_TO_ICAO mapping
    for iata, icao in IATA_TO_ICAO.items():
        if icao and len(icao) >= 1:
            region = icao_prefix_to_region.get(icao[0])
            if region:
                IATA_REGIONS[iata] = region

    # Add many more IATA codes by known region
    americas = [
        "ATL", "ORD", "DFW", "DEN", "CLT", "IAH", "PHX", "MSP", "DTW", "FLL",
        "IAD", "SLC", "MCO", "PHL", "BWI", "SAN", "TPA", "PDX", "STL", "MCI",
        "BNA", "RDU", "AUS", "IND", "CLE", "CMH", "OAK", "SJC", "SMF", "SNA",
        "SAT", "PIT", "MKE", "CVG", "JAX", "OMA", "RNO", "ABQ", "TUS", "ELP",
        "BUF", "PBI", "RSW", "ONT", "BDL", "RIC", "ORF", "SDF", "OKC", "MEM",
        "GUA", "SAL", "SJO", "PTY", "BOG", "UIO", "LIM", "SCL", "GRU", "GIG",
        "EZE", "MVD", "ASU", "CUN", "MEX", "GDL", "MTY", "TIJ", "SJD", "PVR",
        "HAV", "NAS", "MBJ", "KIN", "PUJ", "SXM", "AUA", "CUR", "BGI", "POS",
        "YUL", "YOW", "YEG", "YWG", "YHZ", "YYC", "ANC", "HNL", "OGG", "KOA",
        "LIH", "FAI", "JNU", "SIT",
    ]
    asia = [
        "NRT", "HND", "KIX", "NGO", "CTS", "FUK", "OKA", "ICN", "GMP", "CJU",
        "PUS", "PEK", "PVG", "CAN", "CTU", "SZX", "WUH", "CSX", "KMG", "XIY",
        "HGH", "NKG", "CKG", "TAO", "DLC", "TSN", "SHE", "CGO", "XMN", "FOC",
        "NNG", "KWE", "HRB", "URC", "LHW", "TNA", "ZUH", "HAK", "SYX", "YNT",
        "HKG", "MFM", "TPE", "KHH", "RMQ", "BKK", "DMK", "CNX", "HKT", "USM",
        "SGN", "HAN", "DAD", "PQC", "KUL", "PEN", "BKI", "KCH", "LGK", "SIN",
        "CGK", "DPS", "SUB", "JOG", "MNL", "CEB", "DVO", "ILO",
        "DEL", "BOM", "BLR", "MAA", "CCU", "HYD", "COK", "AMD", "GOI", "PNQ",
        "DAC", "CMB", "KTM", "ISB", "LHE", "KHI",
        "RGN", "PNH", "REP", "VTE", "LPQ", "UBN", "ULN",
    ]
    europe = [
        "LHR", "LGW", "STN", "LTN", "MAN", "EDI", "BRS", "BHX", "GLA", "BFS",
        "CDG", "ORY", "LYS", "NCE", "MRS", "TLS", "BOD", "NTE",
        "FRA", "MUC", "DUS", "TXL", "BER", "HAM", "STR", "CGN", "HAJ",
        "AMS", "BRU", "LUX",
        "MAD", "BCN", "PMI", "AGP", "ALC", "VLC", "SVQ", "IBZ", "LPA", "TFS",
        "FCO", "MXP", "LIN", "VCE", "NAP", "BLQ", "FLR", "PSA", "CTA", "PMO",
        "ZRH", "GVA", "BSL",
        "VIE", "PRG", "BUD", "WAW", "KRK", "GDN",
        "CPH", "OSL", "ARN", "GOT", "HEL", "TLL", "RIX", "VNO",
        "LIS", "OPO", "FAO", "FNC",
        "ATH", "SKG", "HER", "JTR", "CFU", "RHO", "JMK",
        "IST", "SAW", "AYT", "ADB", "ESB", "DLM", "BJV",
        "BEG", "ZAG", "LJU", "SKP", "SOF", "OTP", "CLJ", "TSR",
        "DUB", "SNN", "ORK", "KEF", "TIV", "DBV", "SPU",
        "SVO", "DME", "LED", "SVX", "KZN", "ROV",
    ]
    oceania = [
        "SYD", "MEL", "BNE", "PER", "ADL", "OOL", "CNS", "CBR", "HBA", "DRW",
        "AKL", "CHC", "WLG", "ZQN", "DUD", "NAN", "PPT", "APW", "NOU", "GUM",
    ]
    middle_east = [
        "DXB", "AUH", "SHJ", "DOH", "BAH", "KWI", "MCT", "RUH", "JED", "DMM",
        "MED", "TLV", "AMM", "BEY", "BGW", "EBL", "THR", "IFN", "MHD", "SRY",
        "ISE", "TBZ", "AWZ",
    ]
    africa = [
        "JNB", "CPT", "DUR", "CAI", "HRG", "SSH", "LXR", "ALG", "TUN", "CMN",
        "RAK", "FEZ", "TNG", "LOS", "ABV", "ACC", "DSS", "ABJ",
        "NBO", "MBA", "DAR", "ZNZ", "EBB", "KGL", "ADD", "MPM", "LAD",
        "HRE", "LUN", "MRU", "TNR", "SEZ", "WDH", "GBE",
    ]

    for code in americas:
        IATA_REGIONS.setdefault(code, "americas")
    for code in asia:
        IATA_REGIONS.setdefault(code, "asia")
    for code in europe:
        IATA_REGIONS.setdefault(code, "europe")
    for code in oceania:
        IATA_REGIONS.setdefault(code, "oceania")
    for code in middle_east:
        IATA_REGIONS.setdefault(code, "middle_east")
    for code in africa:
        IATA_REGIONS.setdefault(code, "africa")


def _get_region_for_iata(code: str) -> Optional[str]:
    """Get region for a 3-letter IATA airport code."""
    _build_iata_regions()
    return IATA_REGIONS.get(code.upper()) if code else None


ROUTE_DEFINITIONS: Dict[str, Dict[str, set]] = {
    "transpacific": {
        "side_a": {"asia", "oceania"},
        "side_b": {"americas"},
    },
    "transatlantic": {
        "side_a": {"europe", "africa", "middle_east"},
        "side_b": {"americas"},
    },
}


# ---------------------------------------------------------------------------
# Index builders (from Skycards models data)
# ---------------------------------------------------------------------------

def _build_manufacturer_index(rows: list) -> Dict[str, Tuple[str, Set[str]]]:
    """Map normalized manufacturer names -> (canonical name, set of typecodes).

    Multiple normalization keys per manufacturer allow fuzzy matching.
    """
    # First pass: group typecodes by manufacturer
    mfr_codes: Dict[str, Set[str]] = {}
    for row in rows:
        mfr = row.get("manufacturer", "")
        code = row.get("id", "")
        if mfr and code:
            mfr_codes.setdefault(mfr, set()).add(code)

    # Second pass: build index with normalized keys
    index: Dict[str, Tuple[str, Set[str]]] = {}
    for mfr, codes in mfr_codes.items():
        value = (mfr, codes)
        # Exact lowercase
        index[mfr.lower()] = value
        # Without apostrophes/hyphens
        clean = mfr.lower().replace("'", "").replace("-", " ").strip()
        index[clean] = value
        # Collapsed (no spaces/punctuation)
        collapsed = re.sub(r"[^a-z0-9]", "", mfr.lower())
        index[collapsed] = value
        # First word only (for multi-word: "GULFSTREAM AEROSPACE" -> "gulfstream")
        first_word = mfr.lower().split()[0] if " " in mfr else None
        if first_word and first_word not in index:
            index[first_word] = value

    return index


def _build_class_typecodes(rows: list) -> Dict[str, Set[str]]:
    """Build typecode sets for aircraft classes (helicopter, military, etc.)."""
    classes: Dict[str, Set[str]] = {
        "helicopter": set(),
        "military": set(),
        "gyrocopter": set(),
        "autogyro": set(),
        "tiltrotor": set(),
        "amphibian": set(),
        "glider": set(),
    }
    for row in rows:
        code = row.get("id", "")
        if not code:
            continue
        atype = row.get("type", "")
        name = row.get("name", "").lower()
        if atype == "H":
            classes["helicopter"].add(code)
        elif atype == "G":
            classes["gyrocopter"].add(code)
            classes["autogyro"].add(code)
        elif atype == "T":
            classes["tiltrotor"].add(code)
        elif atype == "A":
            classes["amphibian"].add(code)
        elif atype == "S":
            classes["glider"].add(code)
        # Also match gliders by name/ID (Skycards types them as 'L')
        if "glider" in name or code in ("GLID", "GLIM"):
            classes["glider"].add(code)
        if row.get("military", False):
            classes["military"].add(code)
    return classes


# ---------------------------------------------------------------------------
# Challenge parser
# ---------------------------------------------------------------------------

def _fuzzy_match_airport(name: str) -> Optional[str]:
    """Fuzzy-match an airport name against AIRPORT_LOOKUP using edit distance."""
    name_lower = name.lower()
    best_match = None
    best_dist = float("inf")
    for key in AIRPORT_LOOKUP:
        d = _edit_distance(name_lower, key)
        # Allow up to 2 edits, or 1 edit per 4 chars (whichever is greater)
        max_allowed = max(2, len(key) // 4)
        if d < best_dist and d <= max_allowed:
            best_dist = d
            best_match = key
    return AIRPORT_LOOKUP[best_match] if best_match else None


def _edit_distance(a: str, b: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(a) < len(b):
        return _edit_distance(b, a)
    if len(b) == 0:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            cost = 0 if ca == cb else 1
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + cost))
        prev = curr
    return prev[len(b)]


def _resolve_airport(name: str) -> Optional[str]:
    """Try to resolve an airport name/code to an ICAO code."""
    name = name.strip()
    # Direct ICAO code (4 uppercase letters)
    if re.match(r"^[A-Z]{4}$", name):
        return name
    # IATA code (3 uppercase letters)
    upper = name.upper()
    if re.match(r"^[A-Z]{3}$", upper):
        return IATA_TO_ICAO.get(upper)
    # Exact name lookup
    result = AIRPORT_LOOKUP.get(name.lower())
    if result:
        return result
    # Fuzzy match (handles typos like "madris" -> "madrid")
    return _fuzzy_match_airport(name)


def _resolve_city_airports(name: str) -> Set[str]:
    """Resolve a city/airport name to all matching ICAO + IATA codes.

    For multi-airport cities (e.g. "London" → LHR, LGW, STN, LTN, LCY),
    returns all airports. Falls back to single-airport resolution.
    """
    icao_to_iata = {v: k for k, v in IATA_TO_ICAO.items()}
    codes: Set[str] = set()

    # Check CITY_AIRPORTS first for multi-airport coverage
    city_key = name.strip().lower()
    if city_key in CITY_AIRPORTS:
        for icao in CITY_AIRPORTS[city_key]:
            codes.add(icao)
            iata = icao_to_iata.get(icao)
            if iata:
                codes.add(iata)
        return codes

    # Fall back to single-airport resolution
    icao = _resolve_airport(name)
    if icao:
        codes.add(icao)
        iata = icao_to_iata.get(icao)
        if iata:
            codes.add(iata)
    return codes


def _clean_challenge_text(text: str) -> str:
    """Strip common prefixes/suffixes from challenge text."""
    text = text.strip()
    # Remove "Catch a/an" prefix
    text = re.sub(r"^catch\s+(?:a|an)\s+", "", text, flags=re.IGNORECASE)
    return text.strip()


def parse_challenge(text: str, models_data: dict) -> ChallengeFilter:
    """Parse a single challenge text string into a ChallengeFilter.

    Uses a cascading pattern matcher to identify the challenge type.
    Falls back to best-effort model name search if no pattern matches.
    """
    original = text.strip()
    cleaned = _clean_challenge_text(original)
    rows = models_data.get("rows", [])

    # --- Route-based ---
    route_match = re.search(r"(transpacific|transatlantic)", cleaned, re.IGNORECASE)
    if route_match:
        route_name = route_match.group(1).lower()
        return ChallengeFilter(
            challenge_type=ChallengeType.ROUTE,
            original_text=original,
            description=f"Flights on {route_name} routes",
            route_name=route_name,
        )

    # --- Latitude/region-based ---
    # "north of the arctic circle", "south of the equator", etc.
    lat_regions = {
        "arctic circle": {"min_lat": 66.5, "max_lat": None, "desc": "north of the Arctic Circle (above 66.5N)"},
        "artic circle": {"min_lat": 66.5, "max_lat": None, "desc": "north of the Arctic Circle (above 66.5N)"},
        "antarctic circle": {"min_lat": None, "max_lat": -66.5, "desc": "south of the Antarctic Circle (below 66.5S)"},
        "antartic circle": {"min_lat": None, "max_lat": -66.5, "desc": "south of the Antarctic Circle (below 66.5S)"},
        "equator": None,  # handled directionally below
        "tropic of cancer": None,
        "tropic of capricorn": None,
    }
    for region_key, bounds in lat_regions.items():
        if region_key in cleaned.lower():
            if bounds is None:
                # Directional: check "north of" vs "south of"
                if region_key == "equator":
                    if re.search(r"north\s+of", cleaned, re.IGNORECASE):
                        bounds = {"min_lat": 0, "max_lat": None, "desc": "north of the Equator"}
                    else:
                        bounds = {"min_lat": None, "max_lat": 0, "desc": "south of the Equator"}
                elif region_key == "tropic of cancer":
                    bounds = {"min_lat": 23.4, "max_lat": None, "desc": "north of the Tropic of Cancer"}
                elif region_key == "tropic of capricorn":
                    bounds = {"min_lat": None, "max_lat": -23.4, "desc": "south of the Tropic of Capricorn"}
            if bounds:
                return ChallengeFilter(
                    challenge_type=ChallengeType.LATITUDE_REGION,
                    original_text=original,
                    description=f"Flights {bounds['desc']}",
                    min_lat=bounds.get("min_lat"),
                    max_lat=bounds.get("max_lat"),
                )
            break

    # --- Airport-based: "from X to Y (or back)" ---
    from_to_match = re.search(
        r"(?:flight\s+)?(?:going\s+)?from\s+(.+?)\s+to\s+(.+?)(?:\s+or\s+back)?(?:\s+airport)?\.?$",
        cleaned,
        re.IGNORECASE,
    )
    if from_to_match:
        city_a = from_to_match.group(1).strip()
        city_b = from_to_match.group(2).strip()
        codes_a = _resolve_city_airports(city_a)
        codes_b = _resolve_city_airports(city_b)
        if codes_a and codes_b:
            return ChallengeFilter(
                challenge_type=ChallengeType.AIRPORT_PAIR,
                original_text=original,
                description=f"Flights from {city_a} to {city_b} or back",
                origin_codes=codes_a,
                destination_codes=codes_b,
            )

    # --- Airport-based ---
    # "flight going to or from X or Y"
    airport_match = re.search(
        r"(?:flight\s+)?(?:going\s+)?(?:to|from)\s+(?:or\s+(?:to|from)\s+)?(.+?)(?:\s+airport)?(?:\s+in\s+the\s+world)?\.?$",
        cleaned,
        re.IGNORECASE,
    )
    if airport_match:
        airport_text = airport_match.group(1)
        # Handle compound: "the northernmost (Longyearbyen) or southernmost (Ushuai)"
        # Extract parenthetical names first
        paren_names = re.findall(r"\(([^)]+)\)", airport_text)
        if paren_names:
            airport_names = paren_names
        else:
            # Split on " or "
            airport_names = [n.strip() for n in re.split(r"\s+or\s+", airport_text)]

        icao_codes: Set[str] = set()
        resolved_names = []
        for name in airport_names:
            name = name.strip().rstrip(".")
            code = _resolve_airport(name)
            if code:
                icao_codes.add(code)
                resolved_names.append(f"{name} ({code})")
            else:
                resolved_names.append(f"{name} (unresolved)")

        if icao_codes:
            return ChallengeFilter(
                challenge_type=ChallengeType.AIRPORT,
                original_text=original,
                description=f"Flights to/from {', '.join(resolved_names)}",
                airport_icao=icao_codes,
            )

    # --- Rarity tier ---
    tier_match = re.search(
        r"\b(ultra|rare|scarce|uncommon|common|historical|fantasy)\b",
        cleaned,
        re.IGNORECASE,
    )
    if tier_match:
        tier_word = tier_match.group(1).lower()
        tier_display = {
            "ultra": "Ultra",
            "rare": "Rare",
            "scarce": "Scarce",
            "uncommon": "Uncommon",
            "common": "Common",
            "historical": "Historical",
            "fantasy": "Fantasy",
        }.get(tier_word, tier_word.title())
        return ChallengeFilter(
            challenge_type=ChallengeType.RARITY_TIER,
            original_text=original,
            description=f"{tier_display} tier aircraft",
            tier=tier_display,
        )

    # --- Aircraft class ---
    class_match = re.search(
        r"\b(helicopter|military|gyrocopter|autogyro|tiltrotor|amphibian|glider)\b",
        cleaned,
        re.IGNORECASE,
    )
    if class_match:
        class_name = class_match.group(1).lower()
        class_codes = _build_class_typecodes(rows)
        codes = class_codes.get(class_name, set())
        return ChallengeFilter(
            challenge_type=ChallengeType.AIRCRAFT_CLASS,
            original_text=original,
            description=f"{class_name.title()} aircraft ({len(codes)} types)",
            typecodes=codes,
        )

    # --- Compound "X or Y" for aircraft types ---
    # e.g. "Pilatus PC-12 or PC-24" -> parse each variant separately, merge codes
    or_match = re.search(r"\s+or\s+", cleaned, re.IGNORECASE)
    if or_match and not re.search(r"\bback\b", cleaned, re.IGNORECASE):
        # Split on " or " and try to parse each part
        parts = re.split(r"\s+or\s+", cleaned, flags=re.IGNORECASE)
        # For patterns like "Pilatus PC-12 or PC-24", the second part may lack
        # the manufacturer prefix; try to infer it from the first part
        merged_codes: Set[str] = set()
        descriptions = []
        for i, part in enumerate(parts):
            part = part.strip()
            if not part:
                continue
            # If this isn't the first part and it looks like a bare model (no spaces
            # or starts with a letter+digit like "PC-24"), prepend the manufacturer
            # from the first part
            if i > 0 and " " not in part:
                first_words = parts[0].strip().split()
                if len(first_words) > 1:
                    part = first_words[0] + " " + part
            sub = parse_challenge("Catch a " + part, models_data)
            if sub.typecodes:
                merged_codes.update(sub.typecodes)
                descriptions.append(sub.description)
        if merged_codes:
            return ChallengeFilter(
                challenge_type=ChallengeType.AIRCRAFT_TYPE,
                original_text=original,
                description=" + ".join(descriptions),
                typecodes=merged_codes,
            )

    # --- Manufacturer-based ---
    mfr_index = _build_manufacturer_index(rows)
    # Strip trailing "aircraft", "plane", "airplane"
    mfr_candidate = re.sub(
        r"\s*(?:aircraft|plane|airplane|aeroplane)s?\s*$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()

    if mfr_candidate:
        # Try several normalizations
        for key in [
            mfr_candidate.lower(),
            mfr_candidate.lower().replace("'", "").replace("-", " ").strip(),
            re.sub(r"[^a-z0-9]", "", mfr_candidate.lower()),
        ]:
            if key in mfr_index:
                canonical, codes = mfr_index[key]
                return ChallengeFilter(
                    challenge_type=ChallengeType.MANUFACTURER,
                    original_text=original,
                    description=f"{canonical} aircraft ({len(codes)} types: {', '.join(sorted(codes)[:8])}{'...' if len(codes) > 8 else ''})",
                    typecodes=codes,
                )

    # --- Manufacturer + model pattern (e.g. "Boeing 747", "Airbus A380") ---
    # Check if text starts with a known manufacturer followed by a model identifier
    search_lower = re.sub(
        r"\s*(?:aircraft|plane|airplane|aeroplane)s?\s*$", "", cleaned, flags=re.IGNORECASE
    ).strip().lower()
    # Also prepare a hyphen-stripped version for matching IDs like "SR22" from "SR-22"
    search_nohyphen = search_lower.replace("-", "")
    for key, (canonical, mfr_codes) in mfr_index.items():
        if search_lower.startswith(key) and len(search_lower) > len(key):
            model_part = search_lower[len(key):].strip()
            model_nohyphen = model_part.replace("-", "")
            if model_part:
                matched_codes: Set[str] = set()
                for row in rows:
                    if row.get("id", "") in mfr_codes:
                        rid = row.get("id", "").lower()
                        rname = row.get("name", "").lower()
                        if (model_part in rname or model_part in rid
                                or model_nohyphen in rid or model_nohyphen in rname):
                            matched_codes.add(row["id"])
                if matched_codes:
                    return ChallengeFilter(
                        challenge_type=ChallengeType.AIRCRAFT_TYPE,
                        original_text=original,
                        description=f"{canonical} {model_part.upper()} variants ({len(matched_codes)} types: {', '.join(sorted(matched_codes)[:8])}{'...' if len(matched_codes) > 8 else ''})",
                        typecodes=matched_codes,
                    )

    # --- Aircraft type (specific model search) ---
    # Search model names and IDs for a match (also try without hyphens)
    matched_codes = set()
    for row in rows:
        model_id = row.get("id", "").lower()
        model_name = row.get("name", "").lower()
        if (search_lower in model_name or search_lower == model_id
                or search_nohyphen in model_name or search_nohyphen == model_id):
            matched_codes.add(row["id"])

    if matched_codes:
        return ChallengeFilter(
            challenge_type=ChallengeType.AIRCRAFT_TYPE,
            original_text=original,
            description=f"Aircraft matching '{cleaned}' ({len(matched_codes)} types)",
            typecodes=matched_codes,
        )

    # --- Fallback: best-effort broad search ---
    # Try the full phrase against model names, then individual words
    words = [w for w in search_lower.split() if len(w) > 2]
    for row in rows:
        name_lower = row.get("name", "").lower()
        mfr_lower = row.get("manufacturer", "").lower()
        # Require ALL words to match (not just any)
        if all(w in name_lower or w in mfr_lower for w in words):
            matched_codes.add(row["id"])
    if matched_codes:
        return ChallengeFilter(
            challenge_type=ChallengeType.AIRCRAFT_TYPE,
            original_text=original,
            description=f"Best-effort match for '{cleaned}' ({len(matched_codes)} types)",
            typecodes=matched_codes,
        )

    # Nothing matched at all
    return ChallengeFilter(
        challenge_type=ChallengeType.AIRCRAFT_TYPE,
        original_text=original,
        description=f"Could not parse: '{cleaned}' — no matching aircraft found",
        typecodes=set(),
    )


# ---------------------------------------------------------------------------
# Flight filtering
# ---------------------------------------------------------------------------

def _get_region(code: str) -> Optional[str]:
    """Get geographic region for an airport code (IATA 3-letter)."""
    return _get_region_for_iata(code) if code else None


def filter_flights_for_challenge(
    flights_df: pl.DataFrame,
    challenge: ChallengeFilter,
) -> pl.DataFrame:
    """Filter enriched flights DataFrame to those matching a challenge."""
    if len(flights_df) == 0:
        return flights_df

    ct = challenge.challenge_type

    if ct in (ChallengeType.MANUFACTURER, ChallengeType.AIRCRAFT_TYPE, ChallengeType.AIRCRAFT_CLASS):
        if not challenge.typecodes:
            return flights_df.head(0)  # empty
        return flights_df.filter(pl.col("typecode").is_in(challenge.typecodes))

    if ct == ChallengeType.AIRPORT:
        if not challenge.airport_icao:
            return flights_df.head(0)
        # FR24 data uses IATA codes (e.g. "SYD"), but challenge resolution
        # produces ICAO codes (e.g. "YSSY"). Include both formats for matching.
        icao_to_iata = {v: k for k, v in IATA_TO_ICAO.items()}
        codes = set(challenge.airport_icao)
        for icao in challenge.airport_icao:
            iata = icao_to_iata.get(icao)
            if iata:
                codes.add(iata)
        return flights_df.filter(
            pl.col("origin").is_in(codes) | pl.col("destination").is_in(codes)
        )

    if ct == ChallengeType.AIRPORT_PAIR:
        if not challenge.origin_codes or not challenge.destination_codes:
            return flights_df.head(0)
        a = challenge.origin_codes
        b = challenge.destination_codes
        return flights_df.filter(
            (pl.col("origin").is_in(a) & pl.col("destination").is_in(b))
            | (pl.col("origin").is_in(b) & pl.col("destination").is_in(a))
        )

    if ct == ChallengeType.ROUTE:
        route_def = ROUTE_DEFINITIONS.get(challenge.route_name)
        if not route_def:
            return flights_df.head(0)

        side_a = route_def["side_a"]
        side_b = route_def["side_b"]

        # Build region lookup for all unique origin/destination IATA codes
        _build_iata_regions()
        all_codes = set()
        for col in ["origin", "destination"]:
            if col in flights_df.columns:
                codes = flights_df.select(pl.col(col)).filter(pl.col(col) != "").unique()
                all_codes.update(codes.to_series().to_list())

        # Build IATA -> region mapping DataFrame
        code_region_pairs = []
        for code in all_codes:
            region = _get_region(code)
            if region:
                code_region_pairs.append((code, region))

        if not code_region_pairs:
            return flights_df.head(0)

        region_df = pl.DataFrame({
            "code": [p[0] for p in code_region_pairs],
            "region": [p[1] for p in code_region_pairs],
        })

        # Join origin and destination regions
        df = flights_df.join(
            region_df.rename({"code": "origin", "region": "_o_region"}),
            on="origin", how="left",
        )
        df = df.join(
            region_df.rename({"code": "destination", "region": "_d_region"}),
            on="destination", how="left",
        )

        # Filter: origin in side_a & dest in side_b, OR vice versa
        mask = (
            (pl.col("_o_region").is_in(side_a) & pl.col("_d_region").is_in(side_b))
            | (pl.col("_o_region").is_in(side_b) & pl.col("_d_region").is_in(side_a))
        )
        result = df.filter(mask).drop(["_o_region", "_d_region"])
        return result

    if ct == ChallengeType.LATITUDE_REGION:
        mask = pl.lit(True)
        if challenge.min_lat is not None:
            mask = mask & (pl.col("latitude") >= challenge.min_lat)
        if challenge.max_lat is not None:
            mask = mask & (pl.col("latitude") <= challenge.max_lat)
        return flights_df.filter(mask)

    if ct == ChallengeType.RARITY_TIER:
        if not challenge.tier:
            return flights_df.head(0)
        return flights_df.filter(pl.col("tier") == challenge.tier)

    return flights_df.head(0)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_challenges(challenge_texts: List[str]) -> List[ChallengeFilter]:
    """Parse a list of challenge text strings into ChallengeFilter objects."""
    models_data = get_models()
    return [parse_challenge(text, models_data) for text in challenge_texts]


def run_challenges(
    flights_df: pl.DataFrame,
    challenges: List[ChallengeFilter],
) -> List[Tuple[ChallengeFilter, pl.DataFrame]]:
    """Run all challenges against enriched flight data.

    Returns list of (challenge, matching_flights_df) tuples.
    """
    results = []
    for ch in challenges:
        matches = filter_flights_for_challenge(flights_df, ch)
        matches = matches.sort("rarity", descending=True)
        results.append((ch, matches))
    return results
