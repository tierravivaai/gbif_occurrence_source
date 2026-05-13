"""
Visualise GBIF source distribution data using kepler.gl with interactive layer toggling.

Loads enriched hexbin data (with WB income group) and creates a KeplerGl map with
separate layers for each income group and source type. Users can toggle layers on/off
interactively in the browser.

Key features:
  - Full-viewport map (fills entire browser window)
  - GBIF colour scheme matching the pydeck HexagonLayer palette
  - Separate hexbin layers per source type / income group
  - 3D extrusion enabled by default with elevation_scale proportional to data density

Usage:
    python src/visualise_kepler_hexbin.py
    python src/visualise_kepler_hexbin.py --country BR --precision 1
    python src/visualise_kepler_hexbin.py --country ALL --precision 0

Output:
    HTML file in output/ directory with interactive kepler.gl map
"""

import argparse
import json
import os
import sys
import uuid

import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "processed")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
ENV_PATH = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")

# GBIF colour palette (matching the pydeck HexagonLayer defaults)
GBIF_COLOR_RANGE = {
    "name": "GBIF Source",
    "type": "custom",
    "category": "Custom",
    "colors": [
        "#0198BD",  # teal
        "#49E3CE",  # mint
        "#D8FEB5",  # lime
        "#FEEDB1",  # yellow
        "#FEAD54",  # orange
        "#D1374E",  # red
    ],
}

# Alternative palette for Internal layer (blue tones)
INTERNAL_COLOR_RANGE = {
    "name": "Internal Records",
    "type": "custom",
    "category": "Custom",
    "colors": [
        "#0198BD",  # teal
        "#49E3CE",  # mint
        "#D8FEB5",  # lime yellow
        "#FEEDB1",  # light yellow
        "#FEAD54",  # orange
        "#D1374E",  # red
    ],
}

# Country view presets
COUNTRY_PRESETS = {
    "BR": {"lat": -14.2, "lon": -51.9, "zoom": 3.5},
    "GB": {"lat": 53.5, "lon": -2, "zoom": 5},
    "AU": {"lat": -25.3, "lon": 133.8, "zoom": 3.5},
    "US": {"lat": 39.8, "lon": -98.6, "zoom": 3.5},
    "FR": {"lat": 46.6, "lon": 2.2, "zoom": 4.5},
    "DE": {"lat": 51.2, "lon": 10.4, "zoom": 5},
    "ZA": {"lat": -30.6, "lon": 22.9, "zoom": 4.5},
    "CO": {"lat": 4.6, "lon": -74.3, "zoom": 5},
    "MX": {"lat": 23.6, "lon": -102.6, "zoom": 4},
    "ID": {"lat": -2.5, "lon": 118, "zoom": 4},
    "IN": {"lat": 20.6, "lon": 78.9, "zoom": 4},
    "ALL": {"lat": 15, "lon": 10, "zoom": 1.5},
}

# Hexbin worldUnitSize (km approx) per zoom/precision
HEXBIN_SIZES = {
    0: 40,   # ~111km precision — large hexagons at global scale
    1: 10,   # ~11km precision — medium hexagons
    2: 3,    # ~1.1km precision — small hexagons for country-level
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


def make_hexbin_layer(
    layer_id: str,
    data_id: str,
    label: str,
    color: list[int],
    color_range: dict,
    world_unit_size: float = 10,
    elevation_scale: float = 50,
    enable_3d: bool = True,
) -> dict:
    """Build a kepler.gl hexbin layer config dict."""
    return {
        "id": layer_id,
        "type": "hexbin",
        "config": {
            "dataId": data_id,
            "label": label,
            "color": color,
            "isVisible": True,
            "columns": {
                "lat": "lat",
                "lng": "lon",
            },
            "visConfig": {
                "opacity": 0.85,
                "worldUnitSize": world_unit_size,
                "resolution": 8,
                "colorRange": color_range,
                "coverage": 1,
                "sizeRange": [0, 500],
                "percentile": [0, 100],
                "elevationPercentile": [0, 100],
                "elevationScale": elevation_scale,
                "enable3d": enable_3d,
                "fixedHeight": False,
            },
        },
    }


def inject_fullpage_css(html: str) -> str:
    """Post-process kepler HTML to make the map fill the full browser window."""
    # CSS to make kepler.gl fill the entire viewport
    fullpage_css = """
    <style>
        html, body, #app-content {
            width: 100% !important;
            height: 100vh !important;
            margin: 0 !important;
            padding: 0 !important;
            overflow: hidden !important;
        }
        .kepler-gl {
            width: 100% !important;
            height: 100vh !important;
        }
        .map-container {
            width: 100% !important;
            height: 100vh !important;
        }
        /* Increase sidebar panel contrast */
        .side-panel-panel {
            background-color: #1a1a2e !important;
            color: #e0e0e0 !important;
        }
        .side-panel-panel .layer-panel-item {
            color: #e0e0e0 !important;
        }
        /* Make the layer toggle labels more readable */
        .layer-panel-item__header {
            color: #ffffff !important;
            font-size: 14px !important;
        }
        .layer-panel-item__title {
            color: #ffffff !important;
        }
        /* Style the side panel header */
        .side-panel-panel__header {
            background-color: #16213e !important;
            color: #ffffff !important;
        }
        /* Tooltip styling */
        .map-toltip {
            background-color: rgba(0, 0, 0, 0.9) !important;
            color: #ffffff !important;
            font-size: 13px !important;
            padding: 8px 12px !important;
            border-radius: 4px !important;
        }
    </style>
    """
    # Inject CSS right before </head>
    html = html.replace("</head>", fullpage_css + "\n</head>")

    # Also fix the title
    html = html.replace(
        "<title>Kepler.gl</title>",
        "<title>GBIF Source Distribution - Interactive Hexbin Map</title>",
    )

    return html


def create_kepler_map(country_code: str, precision: int) -> str:
    """Create a kepler.gl map with interactive layer toggling."""
    from keplergl import KeplerGl

    suffix = f"{country_code.lower()}_no_aves_p{precision}"
    enriched_csv = os.path.join(DATA_DIR, f"hexbin_coords_{suffix}_enriched.csv")

    if not os.path.exists(enriched_csv):
        print(f"ERROR: {enriched_csv} not found")
        print(f"Run first: python src/enrich_hexbin_with_income.py --country {country_code} --precision {precision}")
        sys.exit(1)

    print(f"Loading enriched hexbin data: {enriched_csv}")
    df = pd.read_csv(enriched_csv)
    print(f"  {len(df):,} hexbin cells, {df['record_count'].sum():,} total records")
    print(f"  Columns: {df.columns.tolist()}")

    hexbin_size = HEXBIN_SIZES.get(precision, 10)

    # Build datasets and layers
    datasets = {}
    layers = []

    # ─── All Sources ──────────────────────────────────────────────
    data_id_all = "all_sources"
    datasets["All Sources"] = df.copy()
    layers.append(make_hexbin_layer(
        layer_id=str(uuid.uuid4()),
        data_id=data_id_all,
        label="All Sources (Hexbin)",
        color=[1, 152, 189],
        color_range=GBIF_COLOR_RANGE,
        world_unit_size=hexbin_size,
        elevation_scale=50,
    ))

    # ─── Internal (Self-Published) ───────────────────────────────
    df_internal = df[df["source_type"] == "INTERNAL"].copy()
    if len(df_internal) > 0:
        data_id_internal = "internal_records"
        datasets["Internal (Self-Published)"] = df_internal
        layers.append(make_hexbin_layer(
            layer_id=str(uuid.uuid4()),
            data_id=data_id_internal,
            label="Internal / Self-Published (Hexbin)",
            color=[73, 227, 206],
            color_range=INTERNAL_COLOR_RANGE,
            world_unit_size=hexbin_size,
            elevation_scale=50,
        ))

    # ─── WB Income Groups ─────────────────────────────────────────
    income_groups = ["High income", "Upper middle income", "Lower middle income", "Low income"]
    income_colors = {
        "High income": [73, 227, 206],
        "Upper middle income": [216, 254, 181],
        "Lower middle income": [254, 237, 177],
        "Low income": [209, 55, 78],
    }

    for ig in income_groups:
        df_ig = df[df["wb_income_group"] == ig].copy()
        if len(df_ig) > 0:
            df_ig_internal = df_ig[df_ig["source_type"] == "INTERNAL"].copy()
            if len(df_ig_internal) > 0:
                data_id = f"internal_{ig.lower().replace(' ', '_')}"
                label = f"Internal - {ig}"
                datasets[label] = df_ig_internal
                layers.append(make_hexbin_layer(
                    layer_id=str(uuid.uuid4()),
                    data_id=data_id,
                    label=label,
                    color=income_colors.get(ig, [1, 152, 189]),
                    color_range=GBIF_COLOR_RANGE,
                    world_unit_size=hexbin_size,
                    elevation_scale=50,
                ))
                print(f"  {ig}: {df_ig_internal['record_count'].sum():,} internal records")

    # Load Mapbox token
    mapbox_token = load_mapbox_token()

    # Viewport
    preset = COUNTRY_PRESETS.get(country_code, COUNTRY_PRESETS["ALL"])

    # Define config with pre-configured layers
    config = {
        "mapState": {
            "latitude": preset["lat"],
            "longitude": preset["lon"],
            "zoom": preset["zoom"],
            "pitch": 40,
            "bearing": -20,
        },
        "mapStyle": {
            "styleType": "dark" if mapbox_token else "light",
        },
        "visState": {
            "filters": [],
            "layers": layers,
            "interactionConfig": {
                "tooltip": {
                    "enabled": True,
                    "fieldsToShow": {
                        data_id_all: [
                            {"name": "lat", "format": None},
                            {"name": "lon", "format": None},
                            {"name": "record_count", "format": None},
                            {"name": "source_type", "format": None},
                            {"name": "countrycode", "format": None},
                            {"name": "country_name", "format": None},
                            {"name": "wb_income_group", "format": None},
                        ],
                    },
                },
                "brush": {"enabled": False, "size": 0.5},
            },
            "layerBlending": "additive",
        },
    }

    print(f"\nCreating kepler.gl map with {len(datasets)} datasets and {len(layers)} layers...")
    map_1 = KeplerGl(height=800, data=datasets, config=config)

    # Save to HTML
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_html = os.path.join(OUTPUT_DIR, f"gbif_kepler_{suffix}.html")
    map_1.save_to_html(file_name=output_html)

    # Post-process HTML to inject full-viewport CSS and better styling
    with open(output_html, "r", encoding="utf-8") as f:
        html = f.read()

    html = inject_fullpage_css(html)

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nSaved: {output_html}")
    print(f"Open in browser: open {output_html}")

    return output_html


def main():
    parser = argparse.ArgumentParser(description="Create kepler.gl interactive hexbin map")
    parser.add_argument("--country", type=str, default="ALL", help="Country code (BR, GB, AU, ALL)")
    parser.add_argument("--precision", type=int, default=1, help="Coordinate precision (must match data)")
    args = parser.parse_args()

    create_kepler_map(args.country.upper(), args.precision)


if __name__ == "__main__":
    main()
