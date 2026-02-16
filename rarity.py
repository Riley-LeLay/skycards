"""Assign Skycards rarity scores to live flights."""

from __future__ import annotations

import polars as pl

from skycards_api import build_rarity_lookup

# Skycards rarity tiers (based on in-game display scale = rareness/100)
TIERS = {
    "ultra": "Ultra",
    "rare": "Rare",
    "scarce": "Scarce",
    "uncommon": "Uncommon",
    "common": "Common",
    "historical": "Historical",
    "fantasy": "Fantasy",
}


def assign_rarity(flights_df: pl.DataFrame, lookup: dict = None) -> pl.DataFrame:
    """Assign real Skycards rarity scores to each flight.

    Fetches the official rarity database from the Skycards API and joins
    it to the live flight data by ICAO type code.

    Args:
        flights_df: DataFrame with at least a 'typecode' column.
        lookup: Optional pre-built rarity lookup dict. If None, fetches fresh.

    Returns:
        The input DataFrame with added columns:
            - rarity: float (in-game display scale, e.g. 8.95 for Robin R-300)
            - tier: str (Common/Uncommon/Scarce/Rare/Ultra/Historical/Fantasy)
            - aircraft_name: str (full aircraft name from Skycards)
            - xp: int (XP awarded for capture)
        Sorted by rarity descending.
    """
    if len(flights_df) == 0:
        return flights_df.with_columns(
            pl.lit(0.0).alias("rarity"),
            pl.lit("Common").alias("tier"),
            pl.lit("Unknown").alias("aircraft_name"),
            pl.lit(0).alias("xp"),
        )

    if lookup is None:
        lookup = build_rarity_lookup()

    # Build mapping DataFrames
    codes = list(lookup.keys())
    rarity_df = pl.DataFrame({
        "typecode": codes,
        "rarity": [lookup[c]["rarity_display"] for c in codes],
        "tier": [TIERS.get(lookup[c]["category"], lookup[c]["category"].title()) for c in codes],
        "aircraft_name": [lookup[c]["name"] for c in codes],
        "xp": [lookup[c]["xp"] for c in codes],
    })

    result = flights_df.join(rarity_df, on="typecode", how="left")

    # Fill nulls for unknown types
    result = result.with_columns(
        pl.col("rarity").fill_null(0.0),
        pl.col("tier").fill_null("Unknown"),
        pl.col("aircraft_name").fill_null(pl.col("typecode")),
        pl.col("xp").fill_null(0),
    )

    return result.sort("rarity", descending=True)
