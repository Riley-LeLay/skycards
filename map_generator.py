"""Generate interactive HTML map of rare planes using Folium."""

from __future__ import annotations

import json
import os
import platform
import subprocess
import webbrowser
from typing import List

import folium
import polars as pl


def _get_marker_color(rarity: float) -> str:
    """Get marker color based on Skycards rarity tiers."""
    if rarity >= 10.0:
        return "darkred"
    elif rarity >= 8.0:
        return "red"
    elif rarity >= 6.0:
        return "orange"
    elif rarity >= 4.0:
        return "green"
    elif rarity >= 2.0:
        return "blue"
    else:
        return "gray"


def _get_marker_icon(rarity: float) -> str:
    """Get FontAwesome icon based on rarity."""
    if rarity >= 8.0:
        return "star"
    return "plane"


def generate_map(
    flights_df: pl.DataFrame,
    output_path: str = "rare_planes_map.html",
    open_browser: bool = True,
) -> str:
    """Generate an interactive HTML map of rare planes.

    Args:
        flights_df: DataFrame with columns: latitude, longitude, typecode,
                    registration, callsign, rarity, tier, aircraft_name,
                    xp, altitude, ground_speed, origin, destination
        output_path: Where to save the HTML file.
        open_browser: Whether to auto-open the map in a browser.

    Returns:
        Absolute path to the generated HTML file.
    """
    if len(flights_df) == 0:
        print("No flights to map.")
        return output_path

    # Filter out flights without valid positions
    df = flights_df.filter(
        pl.col("latitude").is_not_null()
        & pl.col("longitude").is_not_null()
        & (pl.col("latitude") != 0)
        & (pl.col("longitude") != 0)
    )

    if len(df) == 0:
        print("No flights with valid positions to map.")
        return output_path

    # Center map on the mean position
    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=3,
        tiles="CartoDB dark_matter",
    )

    # Add each flight as a marker
    for row in df.iter_rows(named=True):
        lat = row["latitude"]
        lon = row["longitude"]
        rarity = row.get("rarity", 0)
        tier = row.get("tier", "Unknown")
        typecode = row.get("typecode", "")
        aircraft_name = row.get("aircraft_name", typecode) or typecode
        xp = row.get("xp", 0) or 0
        reg = row.get("registration", "") or ""
        callsign = row.get("callsign", "") or ""
        altitude = row.get("altitude", 0) or 0
        speed = row.get("ground_speed", 0) or 0
        origin = row.get("origin", "") or ""
        destination = row.get("destination", "") or ""

        route = f"{origin} → {destination}" if origin and destination else origin or destination or "—"
        fr24_url = f"https://www.flightradar24.com/{callsign}" if callsign else ""

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; min-width: 220px;">
            <h4 style="margin: 0 0 8px 0; color: #333;">{aircraft_name}</h4>
            <table style="font-size: 13px; border-collapse: collapse;">
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Rarity</b></td>
                    <td><b>{rarity:.2f}</b> ({tier})</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>XP</b></td>
                    <td>{xp:,}</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Type</b></td>
                    <td>{typecode}</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Reg</b></td>
                    <td>{reg}</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Callsign</b></td>
                    <td>{callsign}</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Route</b></td>
                    <td>{route}</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Altitude</b></td>
                    <td>{altitude:,} ft</td></tr>
                <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Speed</b></td>
                    <td>{speed} kts</td></tr>
            </table>
            {"<a href='" + fr24_url + "' target='_blank' style='display:block;margin-top:8px;'>View on FlightRadar24</a>" if fr24_url else ""}
        </div>
        """

        color = _get_marker_color(rarity)
        icon = _get_marker_icon(rarity)

        folium.Marker(
            location=[lat, lon],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=f"{aircraft_name} | {rarity:.2f} ({tier}) | {xp:,} XP",
            icon=folium.Icon(color=color, icon=icon, prefix="fa"),
        ).add_to(m)

    # Legend matching Skycards tiers
    legend_html = """
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                background: white; padding: 12px 16px; border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3); font-family: Arial, sans-serif;">
        <h4 style="margin: 0 0 8px 0; font-size: 14px;">Skycards Rarity</h4>
        <div style="font-size: 12px;">
            <span style="color: #8B0000;">&#9679;</span> 10+ Ultra+<br>
            <span style="color: red;">&#9679;</span> 8–10 Ultra<br>
            <span style="color: orange;">&#9679;</span> 6–8 Rare<br>
            <span style="color: green;">&#9679;</span> 4–6 Scarce<br>
            <span style="color: blue;">&#9679;</span> 2–4 Uncommon<br>
            <span style="color: gray;">&#9679;</span> 0–2 Common
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    abs_path = os.path.abspath(output_path)
    m.save(abs_path)

    if open_browser:
        if platform.system() == "Darwin":
            subprocess.run(["open", abs_path])
        elif platform.system() == "Linux":
            subprocess.run(["xdg-open", abs_path])
        else:
            webbrowser.open(f"file://{abs_path}")

    return abs_path


def _get_hex_color(rarity: float) -> str:
    """Get hex color for DivIcon based on rarity tier."""
    if rarity >= 10.0:
        return "#8B0000"
    elif rarity >= 8.0:
        return "#DC143C"
    elif rarity >= 6.0:
        return "#FF8C00"
    elif rarity >= 4.0:
        return "#228B22"
    elif rarity >= 2.0:
        return "#1E90FF"
    return "#808080"


def _add_marker(m: folium.Map, f: dict) -> None:
    """Add a single flight marker to a Folium map from a flight dict."""
    lat = f["latitude"]
    lon = f["longitude"]
    rarity = f.get("rarity", 0)
    tier = f.get("tier", "Unknown")
    typecode = f.get("typecode", "")
    aircraft_name = f.get("aircraft_name", typecode) or typecode
    xp = f.get("xp", 0) or 0
    reg = f.get("registration", "") or ""
    callsign = f.get("callsign", "") or ""
    altitude = f.get("altitude", 0) or 0
    speed = f.get("ground_speed", 0) or 0
    origin = f.get("origin", "") or ""
    destination = f.get("destination", "") or ""

    route = f"{origin} \u2192 {destination}" if origin and destination else origin or destination or "\u2014"
    fr24_url = f"https://www.flightradar24.com/{callsign}" if callsign else ""

    popup_html = f"""
    <div style="font-family: Arial, sans-serif; min-width: 220px;">
        <h4 style="margin: 0 0 8px 0; color: #333;">{aircraft_name}</h4>
        <table style="font-size: 13px; border-collapse: collapse;">
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Rarity</b></td>
                <td><b>{rarity:.2f}</b> ({tier})</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>XP</b></td>
                <td>{xp:,}</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Type</b></td>
                <td>{typecode}</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Reg</b></td>
                <td>{reg}</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Callsign</b></td>
                <td>{callsign}</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Route</b></td>
                <td>{route}</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Altitude</b></td>
                <td>{altitude:,} ft</td></tr>
            <tr><td style="padding: 2px 8px 2px 0; color: #666;"><b>Speed</b></td>
                <td>{speed} kts</td></tr>
        </table>
        {"<a href='" + fr24_url + "' target='_blank' style='display:block;margin-top:8px;'>View on FlightRadar24</a>" if fr24_url else ""}
    </div>
    """

    hex_color = _get_hex_color(rarity)
    rarity_label = f"{rarity:.1f}"

    icon_html = f"""<div class="rarity-pin" style="background:{hex_color};">{rarity_label}</div>"""

    folium.Marker(
        location=[lat, lon],
        popup=folium.Popup(popup_html, max_width=300),
        tooltip=f"{aircraft_name} | {rarity:.2f} ({tier}) | {xp:,} XP",
        icon=folium.DivIcon(
            html=icon_html,
            icon_size=(40, 20),
            icon_anchor=(20, 10),
            class_name="rarity-marker",
        ),
    ).add_to(m)


# ---------------------------------------------------------------------------
# Server mode: returns HTML string with refresh button + live-update JS
# ---------------------------------------------------------------------------

# Map from rarity score to Leaflet awesome-markers color name
_COLOR_MAP = [
    (10.0, "darkred"),
    (8.0, "red"),
    (6.0, "orange"),
    (4.0, "green"),
    (2.0, "blue"),
    (0.0, "gray"),
]

# Map from rarity score to FontAwesome icon name
_ICON_MAP = [(8.0, "star"), (0.0, "plane")]


def generate_map_html(
    flights: List[dict],
    min_rarity: float = 10.0,
    challenge_texts: List[str] = None,
) -> str:
    """Generate map HTML with a refresh button for server mode.

    Args:
        flights: List of flight dicts (from server._fetch_rare_flights).
        min_rarity: Current minimum rarity threshold (for display).
        challenge_texts: Optional list of challenge text strings to display.

    Returns:
        HTML string ready to serve.
    """
    # Determine center
    if flights:
        center_lat = sum(f["latitude"] for f in flights) / len(flights)
        center_lon = sum(f["longitude"] for f in flights) / len(flights)
    else:
        center_lat, center_lon = 30.0, 0.0

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=3,
        tiles="CartoDB dark_matter",
    )

    # CSS for rarity pin markers, challenge markers, and toggle buttons
    pin_css = """
    <style>
    .rarity-marker { background: transparent !important; border: none !important; }
    .rarity-pin {
        display: flex; align-items: center; justify-content: center;
        width: 40px; height: 20px; border-radius: 10px;
        color: white; font-size: 11px; font-weight: bold;
        font-family: Arial, sans-serif;
        text-shadow: 0 1px 2px rgba(0,0,0,0.5);
        box-shadow: 0 2px 4px rgba(0,0,0,0.4);
        border: 2px solid rgba(255,255,255,0.8);
    }
    .challenge-pin {
        display: flex; align-items: center; justify-content: center;
        width: 24px; height: 24px; border-radius: 50%;
        background: #9C27B0; color: white; font-size: 13px; font-weight: bold;
        font-family: Arial, sans-serif;
        text-shadow: 0 1px 2px rgba(0,0,0,0.5);
        box-shadow: 0 2px 4px rgba(0,0,0,0.4);
        border: 2px solid rgba(255,255,255,0.8);
    }
    .toggle-btn {
        border: none; padding: 5px 10px; border-radius: 4px;
        cursor: pointer; font-size: 12px; font-weight: bold;
        font-family: Arial, sans-serif; transition: opacity 0.2s;
    }
    .toggle-btn.active { opacity: 1.0; }
    .toggle-btn.inactive { opacity: 0.4; }
    </style>
    """
    m.get_root().html.add_child(folium.Element(pin_css))

    # Add initial markers
    for f in flights:
        _add_marker(m, f)

    # Legend
    legend_html = """
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                background: white; padding: 12px 16px; border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3); font-family: Arial, sans-serif;">
        <h4 style="margin: 0 0 8px 0; font-size: 14px;">Skycards Rarity</h4>
        <div style="font-size: 12px;">
            <span style="color: #8B0000;">&#9679;</span> 10+ Ultra+<br>
            <span style="color: red;">&#9679;</span> 8&ndash;10 Ultra<br>
            <span style="color: orange;">&#9679;</span> 6&ndash;8 Rare<br>
            <span style="color: green;">&#9679;</span> 4&ndash;6 Scarce<br>
            <span style="color: blue;">&#9679;</span> 2&ndash;4 Uncommon<br>
            <span style="color: gray;">&#9679;</span> 0&ndash;2 Common
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    # Challenge panel (if challenges are active)
    if challenge_texts:
        challenge_items = "".join(
            f'<div id="ch-{i}" style="margin-bottom:4px;">'
            f'<span style="display:inline-block;width:20px;height:20px;border-radius:50%;'
            f'background:#9C27B0;color:white;font-size:11px;font-weight:bold;'
            f'text-align:center;line-height:20px;margin-right:6px;">{i + 1}</span>{text}</div>'
            for i, text in enumerate(challenge_texts)
        )
        challenge_panel = f"""
        <div id="challenge-panel" style="position: fixed; top: 12px; left: 12px; z-index: 1000;
                    background: #1a1a2e; color: #eee; padding: 14px 18px; border-radius: 10px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.5); font-family: Arial, sans-serif;
                    max-width: 340px;">
            <h4 style="margin: 0 0 10px 0; font-size: 15px; color: #fff;
                       letter-spacing: 1px;">TODAY'S MISSIONS</h4>
            <div style="font-size: 13px; line-height: 1.6;">
                {challenge_items}
            </div>
            <div id="ch-summary" style="margin-top: 8px; font-size: 12px; color: #888;"></div>
        </div>
        """
        m.get_root().html.add_child(folium.Element(challenge_panel))

    # Refresh button + status bar + toggle buttons
    has_challenges = bool(challenge_texts)
    initial_label = f"{len(flights)} planes" if flights else "Loading..."
    challenge_toggle = (
        '<button id="toggle-challenges" class="toggle-btn active"'
        ' onclick="toggleChallenges()"'
        ' style="background: #9C27B0; color: white;">Challenges</button>'
        if has_challenges else ""
    )
    refresh_ui = f"""
    <div id="refresh-bar" style="position: fixed; top: 12px; right: 12px; z-index: 1000;
                display: flex; align-items: center; gap: 8px;
                background: white; padding: 8px 14px; border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.3); font-family: Arial, sans-serif; font-size: 13px;">
        <span id="flight-count" style="font-weight: bold;">{initial_label}</span>
        <span id="last-updated" style="color: #888;"></span>
        <button id="toggle-ultra" class="toggle-btn active" onclick="toggleUltra()"
                style="background: #8B0000; color: white;">Ultra Rare</button>
        {challenge_toggle}
        <button id="refresh-btn" onclick="refreshFlights()"
                style="background: #2196F3; color: white; border: none; padding: 6px 14px;
                       border-radius: 5px; cursor: pointer; font-size: 13px; font-weight: bold;">
            Refresh
        </button>
    </div>
    """
    m.get_root().html.add_child(folium.Element(refresh_ui))

    # JavaScript for live refresh.
    # IMPORTANT: This must be injected via m.get_root().script so it runs
    # AFTER Folium's own map initialization script (which declares the map var).
    # Using m.get_root().html would place it in <body> before the map exists.
    map_var = m.get_name()

    refresh_js = f"""
    // Use setTimeout(0) to ensure this runs AFTER Folium's map init code,
    // which appears later in the same <script> block.
    setTimeout(function() {{

    // Bridge: expose the Folium map variable globally
    window.__map = {map_var};

    // Separate layer groups for toggling
    var ultraGroup = L.layerGroup().addTo(window.__map);
    var challengeGroup = L.layerGroup().addTo(window.__map);
    var showUltra = true;
    var showChallenges = true;

    // Move initial markers into ultraGroup
    (function() {{
        var layers = [];
        window.__map.eachLayer(function(layer) {{
            if (layer instanceof L.Marker) {{
                layers.push(layer);
            }}
        }});
        layers.forEach(function(layer) {{
            window.__map.removeLayer(layer);
            ultraGroup.addLayer(layer);
        }});
    }})();

    // Toggle functions
    window.toggleUltra = function() {{
        showUltra = !showUltra;
        var btn = document.getElementById('toggle-ultra');
        if (showUltra) {{
            window.__map.addLayer(ultraGroup);
            btn.className = 'toggle-btn active';
        }} else {{
            window.__map.removeLayer(ultraGroup);
            btn.className = 'toggle-btn inactive';
        }}
    }};

    window.toggleChallenges = function() {{
        showChallenges = !showChallenges;
        var btn = document.getElementById('toggle-challenges');
        if (btn) {{
            if (showChallenges) {{
                window.__map.addLayer(challengeGroup);
                btn.className = 'toggle-btn active';
            }} else {{
                window.__map.removeLayer(challengeGroup);
                btn.className = 'toggle-btn inactive';
            }}
        }}
    }};

    // Hex color mapping matching Python _get_hex_color
    function getHexColor(rarity) {{
        if (rarity >= 10.0) return '#8B0000';
        if (rarity >= 8.0)  return '#DC143C';
        if (rarity >= 6.0)  return '#FF8C00';
        if (rarity >= 4.0)  return '#228B22';
        if (rarity >= 2.0)  return '#1E90FF';
        return '#808080';
    }}

    function buildPopup(f) {{
        var route = (f.origin && f.destination) ? f.origin + ' \\u2192 ' + f.destination
                    : (f.origin || f.destination || '\\u2014');
        var fr24 = f.callsign
            ? "<a href='https://www.flightradar24.com/" + f.callsign + "' target='_blank' style='display:block;margin-top:8px;'>View on FlightRadar24</a>"
            : "";
        return '<div style="font-family:Arial,sans-serif;min-width:220px;">'
            + '<h4 style="margin:0 0 8px 0;color:#333;">' + f.aircraft_name + '</h4>'
            + '<table style="font-size:13px;border-collapse:collapse;">'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Rarity</b></td><td><b>' + f.rarity.toFixed(2) + '</b> (' + f.tier + ')</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>XP</b></td><td>' + f.xp.toLocaleString() + '</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Type</b></td><td>' + f.typecode + '</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Reg</b></td><td>' + f.registration + '</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Callsign</b></td><td>' + f.callsign + '</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Route</b></td><td>' + route + '</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Altitude</b></td><td>' + f.altitude.toLocaleString() + ' ft</td></tr>'
            + '<tr><td style="padding:2px 8px 2px 0;color:#666;"><b>Speed</b></td><td>' + f.ground_speed + ' kts</td></tr>'
            + '</table>'
            + (f.challenge ? '<div style="margin-top:6px;padding:4px 8px;background:#9C27B0;color:white;border-radius:4px;font-size:12px;">Challenge ' + f.challenge + '</div>' : '')
            + fr24 + '</div>';
    }}

    function createMarker(f) {{
        var iconHtml, iconSize, iconAnchor;
        if (f.challenge) {{
            iconHtml = '<div class="challenge-pin">' + f.challenge + '</div>';
            iconSize = [24, 24];
            iconAnchor = [12, 12];
        }} else {{
            var hexColor = getHexColor(f.rarity);
            var label = f.rarity.toFixed(1);
            iconHtml = '<div class="rarity-pin" style="background:' + hexColor + ';">' + label + '</div>';
            iconSize = [40, 20];
            iconAnchor = [20, 10];
        }}
        var marker = L.marker([f.latitude, f.longitude], {{
            icon: L.divIcon({{
                html: iconHtml,
                iconSize: iconSize,
                iconAnchor: iconAnchor,
                className: 'rarity-marker'
            }})
        }});
        marker.bindPopup(buildPopup(f), {{maxWidth: 300}});
        var tooltip = f.aircraft_name + ' | ' + f.rarity.toFixed(2) + ' (' + f.tier + ') | ' + f.xp.toLocaleString() + ' XP';
        if (f.challenge) tooltip = 'Challenge ' + f.challenge + ': ' + tooltip;
        marker.bindTooltip(tooltip);
        return marker;
    }}

    window.refreshFlights = function() {{
        var btn = document.getElementById('refresh-btn');
        var countEl = document.getElementById('flight-count');
        var updatedEl = document.getElementById('last-updated');

        btn.textContent = 'Loading...';
        btn.disabled = true;
        btn.style.background = '#999';

        fetch('/api/flights')
            .then(function(r) {{ return r.json(); }})
            .then(function(data) {{
                // Clear old markers from both groups
                ultraGroup.clearLayers();
                challengeGroup.clearLayers();

                // Add new markers to appropriate groups
                data.flights.forEach(function(f) {{
                    var marker = createMarker(f);
                    if (f.challenge) {{
                        challengeGroup.addLayer(marker);
                    }} else {{
                        ultraGroup.addLayer(marker);
                    }}
                }});

                countEl.textContent = data.count + ' planes';
                updatedEl.textContent = 'just now';
                btn.textContent = 'Refresh';
                btn.disabled = false;
                btn.style.background = '#2196F3';

                // Update challenge panel if present
                if (data.challenges) {{
                    var chCounts = {{}};
                    data.flights.forEach(function(f) {{
                        if (f.challenge) {{
                            chCounts[f.challenge] = (chCounts[f.challenge] || 0) + 1;
                        }}
                    }});
                    data.challenges.forEach(function(ch, i) {{
                        var el = document.getElementById('ch-' + i);
                        if (el) {{
                            var num = i + 1;
                            var count = chCounts[num] || 0;
                            var bgColor = count > 0 ? '#4CAF50' : '#9C27B0';
                            el.innerHTML = '<span style="display:inline-block;width:20px;height:20px;border-radius:50%;'
                                + 'background:' + bgColor + ';color:white;font-size:11px;font-weight:bold;'
                                + 'text-align:center;line-height:20px;margin-right:6px;">' + num + '</span>'
                                + ch.text + ' <span style="color:#888;">(' + count + ')</span>';
                        }}
                    }});
                    var summary = document.getElementById('ch-summary');
                    if (summary) {{
                        var total = Object.values(chCounts).reduce(function(a,b){{ return a+b; }}, 0);
                        summary.textContent = total + ' total matches';
                    }}
                }}

                // Reset the "last updated" timer
                window.__lastRefresh = Date.now();
            }})
            .catch(function(err) {{
                btn.textContent = 'Error - Retry';
                btn.disabled = false;
                btn.style.background = '#f44336';
                console.error('Refresh failed:', err);
            }});
    }};

    // Update "last updated" time every minute
    window.__lastRefresh = Date.now();
    setInterval(function() {{
        var mins = Math.floor((Date.now() - window.__lastRefresh) / 60000);
        var el = document.getElementById('last-updated');
        if (el && mins > 0) {{
            el.textContent = mins + 'm ago';
        }}
    }}, 60000);

    // Auto-refresh on page load if map is empty
    if (ultraGroup.getLayers().length === 0 && challengeGroup.getLayers().length === 0) {{
        window.refreshFlights();
    }}

    }}, 0);  // end setTimeout
    """

    m.get_root().script.add_child(folium.Element(refresh_js))

    return m.get_root().render()
