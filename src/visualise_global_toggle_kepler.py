"""
Global GBIF source distribution map using kepler.gl with interactive layer toggling.

Produces a single self-contained HTML file. Users click the eye icon in kepler.gl's
left-hand sidebar to toggle between layers — no coding required. Best for interactive
public presentations and policy audiences.

Layers (all toggleable in sidebar):
  All Records            -- every hexbin cell
  Self-Published         -- source_type == INTERNAL only
  High income - Internal
  Upper-middle - Internal
  Lower-middle - Internal
  Low income - Internal
  Africa - Internal
  Americas - Internal
  Asia - Internal
  Europe - Internal
  Oceania - Internal

Default visible: All Records + Self-Published only (others hidden).

Resolution:
  --precision 0  (default) -> 111km cells, ~73k rows, ~18MB HTML — good performance
  --precision 1             -> 11km cells,  ~1M rows, larger HTML — maximum detail

Usage:
    python src/visualise_global_toggle_kepler.py
    python src/visualise_global_toggle_kepler.py --precision 1
    python src/visualise_global_toggle_kepler.py --country BR

Output:
    output/gbif_kepler_global_toggle_p{N}.html
"""

import argparse
import os
import sys
import uuid

import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(PROJECT_DIR, "data", "processed")
OUTPUT_DIR  = os.path.join(PROJECT_DIR, "output")
ENV_PATH    = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")

# GBIF 6-stop colour ramp (hex strings for kepler.gl)
GBIF_COLOR_RANGE = {
    "name": "GBIF Source",
    "type": "custom",
    "category": "Custom",
    "colors": ["#0198BD", "#49E3CE", "#D8FEB5", "#FEEDB1", "#FEAD54", "#D1374E"],
}

INCOME_LAYER_COLORS = {
    "High income":          [1,   152, 189],
    "Upper middle income":  [73,  227, 206],
    "Lower middle income":  [254, 173, 84],
    "Low income":           [209, 55,  78],
}

UN_REGION_COLORS = {
    "Africa":   [255, 165, 0],
    "Americas": [100, 149, 237],
    "Asia":     [147, 112, 219],
    "Europe":   [60,  179, 113],
    "Oceania":  [255, 215, 0],
}

VIEWPORT_PRESETS = {
    "ALL": {"lat": 20,    "lon": 10,    "zoom": 1.5},
    "BR":  {"lat": -14.2, "lon": -51.9, "zoom": 3.5},
    "US":  {"lat": 39.8,  "lon": -98.6, "zoom": 3.5},
    "ZA":  {"lat": -30.6, "lon": 22.9,  "zoom": 4.5},
    "IN":  {"lat": 20.6,  "lon": 78.9,  "zoom": 4.0},
    "AU":  {"lat": -25.3, "lon": 133.8, "zoom": 3.5},
    "GB":  {"lat": 53.5,  "lon": -2.0,  "zoom": 5.0},
}

HEXBIN_WORLD_UNIT = {0: 40, 1: 10}   # km approx per precision level


def load_mapbox_token() -> str:
    token = os.getenv("MAPBOX_API_KEY", "")
    if not token and os.path.exists(ENV_PATH):
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith("MAPBOX_API_KEY="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    return token


def load_data(precision: int) -> pd.DataFrame:
    stem = f"hexbin_all_no_aves_p{precision}_enriched"
    path = os.path.join(DATA_DIR, f"{stem}.parquet")
    if not os.path.exists(path):
        print(f"ERROR: {path} not found.")
        print(f"Run first: python src/aggregate_hexbin_pipeline.py --precision {precision}")
        sys.exit(1)
    df = pd.read_parquet(path)
    print(f"Loaded {len(df):,} cells, {df['record_count'].sum():,} total records")
    return df


def _hexbin_layer(
    layer_id: str,
    data_id: str,
    label: str,
    color: list[int],
    world_unit_size: float,
    is_visible: bool = True,
    elevation_scale: float = 50,
) -> dict:
    return {
        "id": layer_id,
        "type": "hexbin",
        "config": {
            "dataId": data_id,
            "label": label,
            "color": color,
            "isVisible": is_visible,
            "columns": {"lat": "lat", "lng": "lon"},
            "visConfig": {
                "opacity": 0.85,
                "worldUnitSize": world_unit_size,
                "resolution": 8,
                "colorRange": GBIF_COLOR_RANGE,
                "coverage": 1,
                "sizeRange": [0, 500],
                "percentile": [0, 100],
                "elevationPercentile": [0, 100],
                "elevationScale": elevation_scale,
                "enable3d": True,
                "fixedHeight": False,
            },
        },
    }


def _inject_fullpage_css(html: str, title: str) -> str:
    css = """
<style>
  html, body, #app-content {
    width: 100% !important;
    height: 100vh !important;
    margin: 0 !important;
    padding: 0 !important;
    overflow: hidden !important;
  }
  .kepler-gl, .map-container { width: 100% !important; height: 100vh !important; }
  .side-panel-panel { background-color: #1a1a2e !important; color: #e0e0e0 !important; }
  .layer-panel-item__title, .layer-panel-item__header { color: #ffffff !important; font-size: 14px !important; }
  .side-panel-panel__header { background-color: #16213e !important; color: #ffffff !important; }
</style>"""
    html = html.replace("</head>", css + "\n</head>")
    html = html.replace("<title>Kepler.gl</title>", f"<title>{title}</title>")
    return html


def create_toggle_map(precision: int, country: str) -> str:
    from keplergl import KeplerGl

    df = load_data(precision)
    world_unit = HEXBIN_WORLD_UNIT.get(precision, 10)
    preset     = VIEWPORT_PRESETS.get(country.upper(), VIEWPORT_PRESETS["ALL"])

    datasets = {}
    layers   = []

    # ── All Records ────────────────────────────────────────────────────────────
    datasets["All Records"] = df.copy()
    layers.append(_hexbin_layer(
        str(uuid.uuid4()), "All Records",
        "All Records", [1, 152, 189], world_unit, is_visible=True,
    ))

    # ── Self-Published ─────────────────────────────────────────────────────────
    df_internal = df[df["source_type"] == "INTERNAL"].copy()
    if not df_internal.empty:
        datasets["Self-Published"] = df_internal
        layers.append(_hexbin_layer(
            str(uuid.uuid4()), "Self-Published",
            "Self-Published (Internal)", [73, 227, 206], world_unit, is_visible=True,
        ))

    # ── WB Income Group — Internal ─────────────────────────────────────────────
    for ig, color in INCOME_LAYER_COLORS.items():
        subset = df[(df["source_type"] == "INTERNAL") & (df["wb_income_group"] == ig)].copy()
        if not subset.empty:
            key = f"Internal - {ig}"
            datasets[key] = subset
            layers.append(_hexbin_layer(
                str(uuid.uuid4()), key,
                key, color, world_unit, is_visible=False,
            ))
            print(f"  Layer '{key}': {subset['record_count'].sum():,} records")

    # ── UN Region — Internal ───────────────────────────────────────────────────
    for region, color in UN_REGION_COLORS.items():
        subset = df[(df["source_type"] == "INTERNAL") & (df["un_region_name"] == region)].copy()
        if not subset.empty:
            key = f"{region} - Internal"
            datasets[key] = subset
            layers.append(_hexbin_layer(
                str(uuid.uuid4()), key,
                key, color, world_unit, is_visible=False,
            ))
            print(f"  Layer '{key}': {subset['record_count'].sum():,} records")

    # Tooltip fields from the primary dataset
    tooltip_fields = [
        {"name": "lat",            "format": None},
        {"name": "lon",            "format": None},
        {"name": "record_count",   "format": None},
        {"name": "source_type",    "format": None},
        {"name": "countrycode",    "format": None},
        {"name": "country_name",   "format": None},
        {"name": "wb_income_group","format": None},
        {"name": "un_region_name", "format": None},
    ]

    config = {
        "mapState": {
            "latitude":  preset["lat"],
            "longitude": preset["lon"],
            "zoom":      preset["zoom"],
            "pitch":     40,
            "bearing":  -20,
        },
        "mapStyle": {"styleType": "dark"},
        "visState": {
            "filters": [],
            "layers":  layers,
            "layerBlending": "additive",
            "interactionConfig": {
                "tooltip": {
                    "enabled": True,
                    "fieldsToShow": {"All Records": tooltip_fields},
                },
                "brush": {"enabled": False, "size": 0.5},
            },
        },
    }

    print(f"\nBuilding kepler.gl map: {len(datasets)} datasets, {len(layers)} layers...")
    map_obj = KeplerGl(height=800, data=datasets, config=config)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_name = f"gbif_kepler_global_toggle_p{precision}.html"
    out_path = os.path.join(OUTPUT_DIR, out_name)
    map_obj.save_to_html(file_name=out_path)

    title = f"GBIF Species Occurrences — Global Source Distribution (No Aves, p{precision})"
    with open(out_path, "r", encoding="utf-8") as f:
        html = f.read()
    html = _inject_fullpage_css(html, title)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nSaved: {out_path}")
    print("Tip: Use the layer eye icons in the left sidebar to toggle views.")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render kepler.gl global toggle map",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--precision", type=int, default=0,
                        help="Coordinate precision: 0=111km (fast), 1=11km (high-res)")
    parser.add_argument("--country",   default="ALL",
                        help="Starting viewport (country ISO2 or ALL)")
    args = parser.parse_args()

    create_toggle_map(args.precision, args.country.upper())


if __name__ == "__main__":
    main()
