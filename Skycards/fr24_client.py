"""Fetch live flight data from FlightRadar24 via gRPC API."""

from __future__ import annotations

import asyncio
from typing import Optional, Tuple

import polars as pl
from fr24 import FR24, BBOXES_WORLD_STATIC, BoundingBox


async def _fetch_bbox(client, bbox: BoundingBox) -> Optional[pl.DataFrame]:
    """Fetch flights for a single bounding box, returning None on failure."""
    try:
        result = await client.live_feed.fetch(bounding_box=bbox)
        return result.to_polars()
    except Exception as e:
        # Log but don't crash — some regions may have transient errors
        print(f"  Warning: failed to fetch bbox {bbox}: {e}")
        return None


async def _fetch_all_flights(
    bbox: Optional[BoundingBox] = None,
) -> pl.DataFrame:
    """Fetch live flights worldwide or within a bounding box.

    For worldwide queries, iterates through 26 predefined world bounding boxes
    (FR24 limits each query to 1500 flights).

    Returns a polars DataFrame with columns:
        timestamp, flightid, latitude, longitude, track, altitude,
        ground_speed, on_ground, callsign, source, registration,
        origin, destination, typecode, eta, squawk, vertical_speed
    """
    async with FR24() as client:
        if bbox is not None:
            result = await client.live_feed.fetch(bounding_box=bbox)
            return result.to_polars()

        # Worldwide: fetch all bounding boxes concurrently
        results = await asyncio.gather(
            *[_fetch_bbox(client, world_bbox) for world_bbox in BBOXES_WORLD_STATIC]
        )
        frames = [df for df in results if df is not None and len(df) > 0]

        if not frames:
            return pl.DataFrame()

        # Concatenate and deduplicate by flight ID
        combined = pl.concat(frames)
        return combined.unique(subset=["flightid"])


def fetch_live_flights(
    bounds: Optional[Tuple[float, float, float, float]] = None,
) -> pl.DataFrame:
    """Synchronous wrapper to fetch live flights.

    Args:
        bounds: Optional (south, north, west, east) bounding box.
                If None, fetches worldwide.

    Returns:
        polars DataFrame of live flights.
    """
    bbox = BoundingBox(*bounds) if bounds else None
    return asyncio.run(_fetch_all_flights(bbox))
