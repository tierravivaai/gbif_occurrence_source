"""
Global GBIF species occurrence maps as flat 2D hexagon heatmaps.

Uses deck.gl HexagonLayer WITHOUT 3D extrusion (no "line heavy" effect).
Each hexagon is a flat coloured cell showing point density via colour only.
Two maps generated:
  1. All Records
  2. Self-Published (INTERNAL) Records

Usage:
    python src/visualise_global_flat_hex.py
    python src/visualise_global_flat_hex.py --mode both --precision 0
    python src/visualise_global_flat_hex.py --mode internal --precision 0 --threshold 10

Output:
    output/gbif_flathex_global_all.html
    output/gbif_flathex_global_internal.html
"""

import argparse
import os
import sys

import numpy as np
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(PROJECT_DIR, "data", "processed")
OUTPUT_DIR  = os.path.join(PROJECT_DIR, "output")
ENV_PATH    = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")

# GBIF 6-stop colour ramp [R, G, B] (0-255)
GBIF_COLORS = [
    [1,   152, 189],   # teal
    [73,  227, 206],   # mint
    [216, 254, 181],   # lime
    [254, 237, 177],   # yellow
    [254, 173, 84],    # orange
    [209, 55,  78],    # red
]

COUNTRY_PRESETS = {
    "ALL": {"lat": 20,    "lon": 10,     "zoom": 1.5, "pitch": 0, "bearing": 0},
    "BR":  {"lat": -14.2, "lon": -51.9,  "zoom": 3.5, "pitch": 0, "bearing": 0},
    "US":  {"lat": 39.8,  "lon": -98.6,  "zoom": 3.5, "pitch": 0, "bearing": 0},
    "ZA":  {"lat": -30.6, "lon": 22.9,   "zoom": 4.5, "pitch": 0, "bearing": 0},
}


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


def load_data(precision: int, mode: str = "all", threshold: int = 0) -> pd.DataFrame:
    stem = f"hexbin_all_no_aves_p{precision}_enriched"
    path = os.path.join(DATA_DIR, f"{stem}.parquet")
    if not os.path.exists(path):
        print(f"ERROR: {path} not found.")
        print(f"Run first: python src/aggregate_hexbin_pipeline.py --precision {precision}")
        sys.exit(1)

    df = pd.read_parquet(path)
    print(f"Loaded {len(df):,} cells, {df['record_count'].sum():,} total records")

    if mode == "internal":
        df = df[df["source_type"] == "INTERNAL"].copy()
        print(f"Filtered to INTERNAL: {len(df):,} cells, {df['record_count'].sum():,} records")

    if threshold > 0:
        before = len(df)
        total_before = df['record_count'].sum()
        df = df[df["record_count"] >= threshold].copy()
        total_after = df['record_count'].sum()
        pct = 100 * total_after / total_before
        print(f"Threshold >= {threshold}: dropped {before - len(df):,} cells")
        print(f"  Records retained: {total_after:,} ({pct:.1f}%)")

    return df


def render_flat_hex_map(
    df: pd.DataFrame,
    mode: str,
    mapbox_token: str,
    precision: int,
    viewport: str = "ALL",
    radius: int = 50000,
) -> str:
    import pydeck as pdk

    if mode == "internal":
        title = "GBIF Species Occurrences — Self-Published (Internal)"
        out_stem = "gbif_flathex_global_internal"
    else:
        title = "GBIF Species Occurrences — All Records"
        out_stem = "gbif_flathex_global_all"

    title += f" (No Aves, p{precision})"

    print(f"  Rendering flat hex: {len(df):,} cells, {df['record_count'].sum():,} records")

    preset = COUNTRY_PRESETS.get(viewport.upper(), COUNTRY_PRESETS["ALL"])

    # Ensure native Python types for pydeck 0.8.0 JSON
    data = df.copy()
    data["record_count"] = data["record_count"].astype(int)
    data["lat"] = data["lat"].astype(float)
    data["lon"] = data["lon"].astype(float)
    for col in ["source_type", "countrycode", "country_name", "wb_income_group", "un_region_name"]:
        if col in data.columns:
            data[col] = data[col].astype(str)

    layer = pdk.Layer(
        "HexagonLayer",
        data=data.to_dict("records"),
        get_position=["lon", "lat"],
        get_weight="record_count",
        radius=radius,
        # 2D only — NO extrusion, NO "line heavy" effect
        extruded=False,
        elevation_scale=0,
        # Colour by density
        color_range=GBIF_COLORS,
        color_aggregation="SUM",
        # Transparent for very low density
        lower_percentile=0,
        upper_percentile=100,
        coverage=1,
        auto_highlight=True,
        pickable=True,
    )

    view_state = pdk.ViewState(
        longitude=preset["lon"],
        latitude=preset["lat"],
        zoom=preset["zoom"],
        pitch=0,  # flat top-down view
        bearing=0,
        min_zoom=1,
        max_zoom=15,
    )

    api_keys = {"mapbox": mapbox_token} if mapbox_token else None

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="dark",
        api_keys=api_keys,
        tooltip={
            "html": (
                "<b>Records in hex:</b> {count}<br/>"
                "<b>Country:</b> {country_name}"
            ),
            "style": {
                "backgroundColor": "rgba(0,0,0,0.85)",
                "color": "white",
                "fontFamily": '"Helvetica Neue", Arial',
                "fontSize": "13px",
                "padding": "8px 12px",
                "borderRadius": "4px",
            },
        },
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{out_stem}.html")

    html = deck.to_html(as_string=True)
    html = html.replace("<title>", f"<title>{title} | ")
    html = html.replace("</head>", """
<style>
  html, body { width:100%; height:100vh; margin:0; padding:0; overflow:hidden; }
  #deck-container, #deck-container canvas { width:100% !important; height:100vh !important; }
</style>
</head>""")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Saved: {out_path}")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render GBIF global flat 2D hexagon heatmaps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--mode",       choices=["all", "internal", "both"], default="both")
    parser.add_argument("--precision",  type=int,  default=0,      help="0=111km, 1=11km")
    parser.add_argument("--radius",     type=int,  default=50000,  help="Hex radius in metres")
    parser.add_argument("--threshold",  type=int,  default=0,      help="Drop cells with < N records")
    parser.add_argument("--viewport",   default="ALL", help="Starting viewport country code or ALL")
    args = parser.parse_args()

    token = load_mapbox_token()
    df = load_data(args.precision, mode="all", threshold=args.threshold)

    modes = ["all", "internal"] if args.mode == "both" else [args.mode]
    for m in modes:
        plot_df = df.copy()
        if m == "internal":
            plot_df = plot_df[plot_df["source_type"] == "INTERNAL"].copy()

        if plot_df.empty:
            print(f"  Skipping {m} — no data after filtering")
            continue

        render_flat_hex_map(
            df=plot_df,
            mode=m,
            mapbox_token=token,
            precision=args.precision,
            viewport=args.viewport,
            radius=args.radius,
        )

    print("\nDone. Open the HTML files in a browser.")


if __name__ == "__main__":
    main()
