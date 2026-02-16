# Skycards Rare Plane Finder

Find rare aircraft flying right now using real [Skycards](https://apps.apple.com/app/skycards/id6504375854) rarity data and live [FlightRadar24](https://www.flightradar24.com/) flights.

Scans ~20,000 live flights worldwide and highlights the rarest ones — with terminal output and an interactive map.

## Quick Start

**macOS — double-click to launch:**
```
Skycards.command
```
Opens the live map in your browser with a refresh button.

**Terminal:**
```bash
pip install -r requirements.txt flask
python main.py --serve
```

## Usage

```bash
python main.py                          # Ultra+ only (rarity >= 10)
python main.py --min-rarity 8           # All Ultras
python main.py --min-rarity 6           # Rare and above
python main.py --bounds 24,50,-125,-66  # Continental US only
python main.py --no-map                 # Terminal table only
python main.py --serve                  # Live map with refresh button
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--min-rarity` | `10.0` | Minimum rarity score to display |
| `--bounds` | worldwide | Geographic filter: `south,north,west,east` |
| `--output` | `rare_planes_map.html` | Output HTML map filename |
| `--no-map` | off | Skip map generation, terminal only |
| `--serve` | off | Start live web server at `localhost:5050` |

## Rarity Tiers

Matches the in-game Skycards rarity scale:

| Score | Tier |
|-------|------|
| 10+ | Ultra+ |
| 8–10 | Ultra |
| 6–8 | Rare |
| 4–6 | Scarce |
| 2–4 | Uncommon |
| 0–2 | Common |

## How It Works

1. Fetches 1,625 aircraft types with rarity scores from the Skycards API
2. Pulls ~20,000 live flights from FlightRadar24 (26 worldwide regions, fetched concurrently)
3. Joins flights to rarity data by ICAO type code
4. Filters to your minimum rarity threshold
5. Displays results in a terminal table and/or interactive dark-themed map

## Map Features

- Color-coded markers showing rarity score on each icon
- Click a marker for aircraft details, route, altitude, and FlightRadar24 link
- **Live mode** (`--serve`): refresh button fetches fresh data without restarting
- Legend with Skycards tier color coding

## Requirements

- Python 3.9+
- Dependencies: `fr24`, `folium`, `rich`, `flask`, `polars`

```bash
pip install -r requirements.txt flask
```

## Project Structure

```
main.py            CLI entry point
server.py          Flask server for live map mode
fr24_client.py     FlightRadar24 live flight fetcher
skycards_api.py    Skycards rarity API client
rarity.py          Joins rarity scores to flights
map_generator.py   Folium HTML map generation
Skycards.command   macOS double-click launcher
```
