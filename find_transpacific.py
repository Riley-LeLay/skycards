#!/usr/bin/env python3
"""Find transpacific flights currently in the air.

Uses the fr24 library to fetch live flights worldwide, then identifies
flights crossing the Pacific Ocean (between Asia/Oceania and the Americas).
"""

from __future__ import annotations

import asyncio
import sys
from typing import Dict, Optional, Set

import polars as pl
from fr24 import FR24, BBOXES_WORLD_STATIC, BoundingBox

# ── Region detection (adapted from challenges.py) ────────────────────────

ICAO_PREFIX_TO_REGION: Dict[str, str] = {
    "K": "americas", "P": "americas", "C": "americas", "M": "americas",
    "S": "americas", "T": "americas",
    "E": "europe", "L": "europe", "B": "europe",
    "R": "asia", "Z": "asia", "V": "asia", "W": "asia", "U": "asia",
    "Y": "oceania", "N": "oceania",
    "O": "middle_east", "H": "africa", "F": "africa", "D": "africa", "G": "africa",
}

# Extensive IATA -> region mapping for well-known airports
IATA_REGIONS: Dict[str, str] = {}

def _build_iata_regions() -> None:
    if IATA_REGIONS:
        return

    # IATA -> ICAO for deriving region from prefix
    iata_to_icao = {
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
    for iata, icao in iata_to_icao.items():
        region = ICAO_PREFIX_TO_REGION.get(icao[0])
        if region:
            IATA_REGIONS[iata] = region

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
    oceania = [
        "SYD", "MEL", "BNE", "PER", "ADL", "OOL", "CNS", "CBR", "HBA", "DRW",
        "AKL", "CHC", "WLG", "ZQN", "DUD", "NAN", "PPT", "APW", "NOU", "GUM",
    ]

    for code in americas:
        IATA_REGIONS.setdefault(code, "americas")
    for code in asia:
        IATA_REGIONS.setdefault(code, "asia")
    for code in oceania:
        IATA_REGIONS.setdefault(code, "oceania")


def get_region(code: str) -> Optional[str]:
    """Get region for a 3 or 4-letter airport code."""
    if not code:
        return None
    _build_iata_regions()
    code = code.upper().strip()
    # Try as IATA first
    if code in IATA_REGIONS:
        return IATA_REGIONS[code]
    # Try ICAO prefix
    if len(code) == 4:
        return ICAO_PREFIX_TO_REGION.get(code[0])
    return None


# ── Fetch live flights ────────────────────────────────────────────────────

async def _fetch_bbox(client, bbox: BoundingBox, sem: asyncio.Semaphore) -> Optional[pl.DataFrame]:
    async with sem:
        try:
            result = await client.live_feed.fetch(bounding_box=bbox)
            return result.to_polars()
        except Exception as e:
            print(f"  Warning: failed to fetch bbox {bbox}: {e}")
            return None


async def fetch_all_flights() -> pl.DataFrame:
    """Fetch live flights from all world bounding boxes.

    Uses a semaphore to limit concurrency to 3 requests at a time,
    avoiding rate-limiting from the FR24 gRPC API.
    """
    sem = asyncio.Semaphore(3)
    async with FR24() as client:
        results = await asyncio.gather(
            *[_fetch_bbox(client, bbox, sem) for bbox in BBOXES_WORLD_STATIC]
        )
    frames = [df for df in results if df is not None and len(df) > 0]
    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames)
    return combined.unique(subset=["flightid"])


# ── Transpacific detection ────────────────────────────────────────────────

SIDE_A = {"asia", "oceania"}   # West side of Pacific
SIDE_B = {"americas"}          # East side of Pacific


def find_transpacific(flights: pl.DataFrame) -> pl.DataFrame:
    """Filter to only transpacific flights (Asia/Oceania <-> Americas)."""
    if len(flights) == 0:
        return flights

    # Get all unique airport codes
    all_codes: Set[str] = set()
    for col in ["origin", "destination"]:
        if col in flights.columns:
            codes = flights.select(pl.col(col)).filter(pl.col(col) != "").unique()
            all_codes.update(codes.to_series().to_list())

    # Build region map
    code_region = []
    for code in all_codes:
        region = get_region(code)
        if region:
            code_region.append((code, region))

    if not code_region:
        return flights.head(0)

    region_df = pl.DataFrame({
        "code": [p[0] for p in code_region],
        "region": [p[1] for p in code_region],
    })

    # Join origin and destination regions
    df = flights.join(
        region_df.rename({"code": "origin", "region": "_o_region"}),
        on="origin", how="left",
    )
    df = df.join(
        region_df.rename({"code": "destination", "region": "_d_region"}),
        on="destination", how="left",
    )

    # Filter: origin in Asia/Oceania & dest in Americas, or vice versa
    mask = (
        (pl.col("_o_region").is_in(SIDE_A) & pl.col("_d_region").is_in(SIDE_B))
        | (pl.col("_o_region").is_in(SIDE_B) & pl.col("_d_region").is_in(SIDE_A))
    )
    result = df.filter(mask).drop(["_o_region", "_d_region"])
    return result


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    from datetime import datetime, timezone
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn

    console = Console()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    console.print(f"\n[bold cyan]Transpacific Flight Finder[/bold cyan] -- {now}\n")

    # Step 1: Fetch live flights worldwide
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching live flights from FlightRadar24 (all 26 world regions)...", total=None)
        try:
            flights_df = asyncio.run(fetch_all_flights())
        except Exception as e:
            console.print(f"[red]Error fetching flights:[/red] {e}")
            sys.exit(1)
        progress.update(task, completed=True)

    total = len(flights_df)
    console.print(f"Fetched [bold]{total:,}[/bold] live flights worldwide\n")

    if total == 0:
        console.print("[yellow]No flights found. Check your internet connection.[/yellow]")
        sys.exit(0)

    # Step 2: Filter for transpacific
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Identifying transpacific flights...", total=None)
        transpacific = find_transpacific(flights_df)
        progress.update(task, completed=True)

    count = len(transpacific)
    console.print(f"Found [bold green]{count}[/bold green] transpacific flights (Asia/Oceania <-> Americas)\n")

    if count == 0:
        console.print("[yellow]No transpacific flights detected right now.[/yellow]")
        sys.exit(0)

    # Sort by altitude descending (highest cruising flights first)
    transpacific = transpacific.sort("altitude", descending=True)

    # Step 3: Display results
    table = Table(
        title=f"Transpacific Flights ({count} found)",
        show_lines=False,
        header_style="bold cyan",
    )
    table.add_column("#", justify="right", style="dim")
    table.add_column("Callsign", style="bold")
    table.add_column("Aircraft", no_wrap=True)
    table.add_column("Registration", style="dim")
    table.add_column("Origin", style="green")
    table.add_column("Destination", style="green")
    table.add_column("Altitude (ft)", justify="right")
    table.add_column("Speed (kts)", justify="right")
    table.add_column("Position", style="dim")

    for i, row in enumerate(transpacific.iter_rows(named=True), 1):
        callsign = row.get("callsign", "") or "---"
        typecode = row.get("typecode", "") or "---"
        reg = row.get("registration", "") or "---"
        origin = row.get("origin", "") or "?"
        dest = row.get("destination", "") or "?"
        alt = row.get("altitude", 0) or 0
        speed = row.get("ground_speed", 0) or 0
        lat = row.get("latitude", 0) or 0
        lon = row.get("longitude", 0) or 0

        table.add_row(
            str(i),
            callsign,
            typecode,
            reg,
            origin,
            dest,
            f"{alt:,}" if alt else "---",
            f"{speed:,}" if speed else "---",
            f"{lat:.2f}, {lon:.2f}",
        )

        if i >= 200:  # Cap display
            break

    console.print(table)

    if count > 200:
        console.print(f"\n[dim](Showing 200 of {count} total transpacific flights)[/dim]")

    # Summary stats
    console.print(f"\n[bold]Summary:[/bold]")
    if "typecode" in transpacific.columns:
        type_counts = (
            transpacific.group_by("typecode")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        console.print(f"  [bold]Top aircraft types:[/bold]")
        for row in type_counts.iter_rows(named=True):
            console.print(f"    {row['typecode']}: {row['count']} flights")

    if "origin" in transpacific.columns:
        # Top origins
        origin_counts = (
            transpacific.filter(pl.col("origin") != "")
            .group_by("origin")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        console.print(f"\n  [bold]Top origin airports:[/bold]")
        for row in origin_counts.iter_rows(named=True):
            console.print(f"    {row['origin']}: {row['count']} departures")

    if "destination" in transpacific.columns:
        dest_counts = (
            transpacific.filter(pl.col("destination") != "")
            .group_by("destination")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        console.print(f"\n  [bold]Top destination airports:[/bold]")
        for row in dest_counts.iter_rows(named=True):
            console.print(f"    {row['destination']}: {row['count']} arrivals")

    console.print()


if __name__ == "__main__":
    main()
