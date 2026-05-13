"""
Visualise a single country's GBIF source distribution as a 3D hexbin map.

Uses the pre-aggregated coordinate data from aggregate_country_hexbin.py
with pydeck's HexagonLayer -- the same approach as the deck.gl UK road
safety example. Each hexbin's height and colour represent record density.

Generates two maps:
  1. All sources (total record density)
  2. Self-published only (Internal records)

Usage:
    python src/visualise_country_hexbin.py --country BR
    python src/visualise_country_hexbin.py --country BR --precision 2
    python src/visualise_country_hexbin.py --country GB
"""

import argparse
import os
import re
import sys

import duckdb
import pandas as pd
import numpy as np

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "processed")
ENV_PATH = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Country view presets: (lat, lon, zoom, radius)
COUNTRY_PRESETS = {
    "BR": {"lat": -14.2, "lon": -51.9, "zoom": 3.5, "radius": 20000},
    "GB": {"lat": 53.5, "lon": -2.0, "zoom": 5, "radius": 5000},
    "AU": {"lat": -25.3, "lon": 133.8, "zoom": 3.5, "radius": 20000},
    "US": {"lat": 39.8, "lon": -98.6, "zoom": 3.5, "radius": 20000},
    "FR": {"lat": 46.6, "lon": 2.2, "zoom": 4.5, "radius": 8000},
    "DE": {"lat": 51.2, "lon": 10.4, "zoom": 5, "radius": 5000},
    "ZA": {"lat": -30.6, "lon": 22.9, "zoom": 4.5, "radius": 10000},
    "CO": {"lat": 4.6, "lon": -74.3, "zoom": 5, "radius": 5000},
    "MX": {"lat": 23.6, "lon": -102.6, "zoom": 4, "radius": 15000},
    "PE": {"lat": -9.2, "lon": -75.0, "zoom": 4.5, "radius": 10000},
    "ID": {"lat": -2.5, "lon": 118.0, "zoom": 4, "radius": 15000},
    "IN": {"lat": 20.6, "lon": 78.9, "zoom": 4, "radius": 15000},
    "ES": {"lat": 40.5, "lon": -3.7, "zoom": 5, "radius": 8000},
    "SE": {"lat": 62.0, "lon": 15.0, "zoom": 4.5, "radius": 10000},
    "NO": {"lat": 64.5, "lon": 13.0, "zoom": 4.5, "radius": 10000},
    "CA": {"lat": 56.1, "lon": -106.3, "zoom": 3, "radius": 25000},
    "JP": {"lat": 36.2, "lon": 138.3, "zoom": 5, "radius": 5000},
    "ALL": {"lat": 15, "lon": 10, "zoom": 1.2, "radius": 20000},
}


def load_mapbox_token() -> str:
    """Load MAPBOX_API_KEY from .env file."""
    token = os.getenv("MAPBOX_API_KEY", "")
    if not token and os.path.exists(ENV_PATH):
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith("MAPBOX_API_KEY="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    return token


def render_country_hexbin(
    country_code: str,
    precision: int = 1,
    mapbox_token: str = "",
) -> list[str]:
    """Render 3D hexbin maps for a single country.

    Returns list of generated HTML file paths.
    """
    import pydeck as pdk

    cc = country_code.upper()
    suffix = f"{cc.lower()}_no_aves_p{precision}"
    csv_path = os.path.join(DATA_DIR, f"hexbin_coords_{suffix}.csv")

    if not os.path.exists(csv_path):
        print(f"ERROR: Data file not found: {csv_path}")
        print(f"Run first: python src/aggregate_country_hexbin.py --country {cc} --precision {precision}")
        return []

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} hexbin cells, {df['record_count'].sum():,} total records")

    # Get view preset for this country
    preset = COUNTRY_PRESETS.get(cc, {"lat": 0, "lon": 0, "zoom": 2, "radius": 30000})

    outputs = []

    # ─── Map 1: All sources ────────────────────────────────────────
    all_layer = pdk.Layer(
        "HexagonLayer",
        data=df,
        get_position=["lon", "lat"],
        get_weight="record_count",
        elevation_scale=50,
        elevation_range=[0, 3000],
        extruded=True,
        coverage=1,
        auto_highlight=True,
        pickable=True,
        radius=preset["radius"],
        color_range=[
            [1, 152, 189],    # teal
            [73, 227, 206],   # mint
            [216, 254, 181],  # lime
            [254, 237, 177],  # yellow
            [254, 173, 84],   # orange
            [209, 55, 78],    # red
        ],
    )

    view_state = pdk.ViewState(
        longitude=preset["lon"],
        latitude=preset["lat"],
        zoom=preset["zoom"],
        min_zoom=1,
        max_zoom=15,
        pitch=40.5,
        bearing=-27,
    )

    api_keys = {"mapbox": mapbox_token} if mapbox_token else None
    deck_all = pdk.Deck(
        layers=[all_layer],
        initial_view_state=view_state,
        map_style="dark" if mapbox_token else "light",
        api_keys=api_keys,
        tooltip={
            "html": "<b>Records in cell:</b> {count}",
            "style": {
                "backgroundColor": "rgba(0, 0, 0, 0.8)",
                "color": "white",
                "fontFamily": '"Helvetica Neue", Arial',
                "fontSize": "13px",
            },
        },
    )

    out_all = os.path.join(OUTPUT_DIR, f"gbif_hexbin_all_sources_{suffix}.html")
    deck_all.to_html(out_all)
    print(f"  Saved: {out_all}")
    outputs.append(out_all)

    # ─── Map 2: Internal only ───────────────────────────────────────
    df_internal = df[df["source_type"] == "INTERNAL"].copy()
    print(f"Internal records: {df_internal['record_count'].sum():,} ({len(df_internal)} cells)")

    internal_layer = pdk.Layer(
        "HexagonLayer",
        data=df_internal,
        get_position=["lon", "lat"],
        get_weight="record_count",
        elevation_scale=50,
        elevation_range=[0, 3000],
        extruded=True,
        coverage=1,
        auto_highlight=True,
        pickable=True,
        radius=preset["radius"],
        color_range=[
            [1, 152, 189],
            [73, 227, 206],
            [216, 254, 181],
            [254, 237, 177],
            [254, 173, 84],
            [209, 55, 78],
        ],
    )

    deck_internal = pdk.Deck(
        layers=[internal_layer],
        initial_view_state=view_state,
        map_style="dark" if mapbox_token else "light",
        api_keys=api_keys,
        tooltip={
            "html": "<b>Internal records in cell:</b> {count}",
            "style": {
                "backgroundColor": "rgba(0, 0, 0, 0.8)",
                "color": "white",
                "fontFamily": '"Helvetica Neue", Arial',
                "fontSize": "13px",
            },
        },
    )

    out_internal = os.path.join(OUTPUT_DIR, f"gbif_hexbin_internal_{suffix}.html")
    deck_internal.to_html(out_internal)
    print(f"  Saved: {out_internal}")
    outputs.append(out_internal)

    return outputs


def main():
    parser = argparse.ArgumentParser(description="Render country hexbin map")
    parser.add_argument(
        "--country",
        type=str,
        default="BR",
        help="ISO 2-letter country code (e.g. BR, GB, AU)",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=1,
        help="Coordinate precision (must match the aggregated data, default 1)",
    )
    args = parser.parse_args()

    token = load_mapbox_token()
    outputs = render_country_hexbin(args.country.upper(), args.precision, token)

    if outputs:
        print(f"\nDone! Open the HTML files in a browser:")
        for p in outputs:
            print(f"  open {p}")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
