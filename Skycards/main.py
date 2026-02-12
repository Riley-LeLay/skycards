#!/usr/bin/env python3
"""Skycards Rare Plane Finder — find where rare aircraft are flying right now."""

import argparse
import sys
from datetime import datetime, timezone

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn

from fr24_client import fetch_live_flights
from rarity import assign_rarity
from map_generator import generate_map


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find rare aircraft flying right now using real Skycards rarity data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Rarity tiers (matching Skycards):
  0–1.99  Common       2–3.99  Uncommon     4–5.99  Scarce
  6–7.99  Rare         8–10    Ultra        10+     Ultra+

Examples:
  python main.py                          # Ultra rare only (rarity >= 10)
  python main.py --min-rarity 8           # All ultras
  python main.py --min-rarity 6           # Rare and above
  python main.py --bounds 24,50,-125,-66  # Continental US
  python main.py --no-map                 # Terminal output only
        """,
    )
    parser.add_argument(
        "--min-rarity",
        type=float,
        default=10.0,
        help="Minimum rarity score to display (default: 10.0)",
    )
    parser.add_argument(
        "--bounds",
        type=str,
        default=None,
        help="Geographic bounding box: south,north,west,east (e.g. 24,50,-125,-66)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="rare_planes_map.html",
        help="Output HTML map filename (default: rare_planes_map.html)",
    )
    parser.add_argument(
        "--no-map",
        action="store_true",
        help="Skip map generation, terminal output only",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="Start a local web server with a live-updating map (refresh button)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    console = Console()

    # Parse bounds if provided
    bounds = None
    if args.bounds:
        try:
            parts = [float(x) for x in args.bounds.split(",")]
            if len(parts) != 4:
                raise ValueError
            bounds = tuple(parts)
        except ValueError:
            console.print(
                "[red]Error:[/red] --bounds must be 4 comma-separated numbers: south,north,west,east"
            )
            sys.exit(1)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    console.print(f"\n[bold cyan]Skycards Rare Plane Finder[/bold cyan] — {now}\n")

    # Fast path: --serve skips CLI fetch, launches server immediately
    if args.serve:
        import platform
        import subprocess
        import webbrowser
        from server import configure, start

        configure(min_rarity=args.min_rarity, bounds=bounds)
        port = 5050
        url = f"http://localhost:{port}"
        console.print(f"[bold green]Starting live map server at:[/bold green] {url}")
        console.print("[dim]Map will load flights automatically in the browser.[/dim]\n")
        if platform.system() == "Darwin":
            subprocess.Popen(["open", url])
        elif platform.system() == "Linux":
            subprocess.Popen(["xdg-open", url])
        else:
            webbrowser.open(url)
        start(port=port)
        return

    # Fetch Skycards rarity data
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Loading Skycards rarity database...", total=None
        )
        from skycards_api import build_rarity_lookup
        lookup = build_rarity_lookup()
        progress.update(task, completed=True)

    console.print(
        f"Loaded [bold]{len(lookup):,}[/bold] aircraft types from Skycards\n"
    )

    # Fetch live flights
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Fetching live flights from FlightRadar24...", total=None
        )
        try:
            flights_df = fetch_live_flights(bounds)
        except Exception as e:
            console.print(f"[red]Error fetching flights:[/red] {e}")
            sys.exit(1)
        progress.update(task, completed=True)

    total_flights = len(flights_df)
    if total_flights == 0:
        console.print("[yellow]No flights found. Check your internet connection or try again.[/yellow]")
        sys.exit(0)

    # Assign rarity scores
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Assigning rarity scores...", total=None)
        flights_df = assign_rarity(flights_df)
        progress.update(task, completed=True)

    # Filter to minimum rarity
    rare_df = flights_df.filter(flights_df["rarity"] >= args.min_rarity)
    rare_count = len(rare_df)

    console.print(
        f"\nScanned [bold]{total_flights:,}[/bold] flights — "
        f"found [bold green]{rare_count}[/bold green] with rarity >= {args.min_rarity}\n"
    )

    if rare_count == 0:
        console.print(
            f"[yellow]No planes found with rarity >= {args.min_rarity}.[/yellow]\n"
            f"[dim]Try lowering --min-rarity (e.g. --min-rarity 8 for all Ultras, "
            f"--min-rarity 6 for Rare+)[/dim]"
        )
        sys.exit(0)

    # Build rich table
    table = Table(
        title=f"Rare Planes Flying Now (rarity >= {args.min_rarity})",
        show_lines=False,
        header_style="bold cyan",
    )
    table.add_column("Rarity", justify="right", style="bold")
    table.add_column("Tier", justify="center")
    table.add_column("Aircraft", no_wrap=True)
    table.add_column("XP", justify="right", style="dim")
    table.add_column("Reg", style="dim")
    table.add_column("Position")
    table.add_column("Alt", justify="right")
    table.add_column("Route")

    tier_colors = {
        "Ultra": "bold red",
        "Historical": "bold magenta",
        "Fantasy": "bold magenta",
        "Rare": "bold yellow",
        "Scarce": "bold green",
        "Uncommon": "blue",
        "Common": "dim",
    }

    for row in rare_df.head(100).iter_rows(named=True):
        rarity = row["rarity"]
        tier = row.get("tier", "")
        aircraft = row.get("aircraft_name", row.get("typecode", "")) or ""
        typecode = row.get("typecode", "") or ""
        xp = row.get("xp", 0) or 0
        reg = row.get("registration", "") or "—"
        lat = row.get("latitude", 0) or 0
        lon = row.get("longitude", 0) or 0
        alt = row.get("altitude", 0) or 0
        origin = row.get("origin", "") or ""
        destination = row.get("destination", "") or ""

        pos = f"{lat:.1f}, {lon:.1f}"
        route = f"{origin}→{destination}" if origin and destination else origin or destination or "—"
        tier_style = tier_colors.get(tier, "")

        # Show aircraft name with typecode
        aircraft_display = f"{aircraft} ({typecode})" if aircraft != typecode else typecode

        table.add_row(
            f"{rarity:.2f}",
            f"[{tier_style}]{tier}[/{tier_style}]" if tier_style else tier,
            aircraft_display,
            f"{xp:,}",
            reg,
            pos,
            f"{alt:,}" if alt else "—",
            route,
        )

    console.print(table)

    if rare_count > 100:
        console.print(f"\n[dim](showing top 100 of {rare_count} results)[/dim]")

    # Generate map
    if not args.no_map:
        console.print()
        map_path = generate_map(rare_df, args.output)
        console.print(f"\n[bold green]Map saved to:[/bold green] {map_path}")
    else:
        console.print()


if __name__ == "__main__":
    main()
