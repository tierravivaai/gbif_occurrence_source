"""
Global GBIF species occurrence maps as scatterplots.

Renders each pre-aggregated cell center (lat/lon rounded to precision)
as a coloured circle. Circle colour encodes record density via a quantile-
based colour scale (so the full rainbow is used across the data).  Radius is
fixed in screen pixels so points are visible at any zoom level.

Two maps generated:
  1. All Records — every cell
  2. Self-Published — INTERNAL source_type only

The --sample option caps the number of cells rendered (useful for quick
previews).  Default renders all cells (p0 recommended for global view).

Usage:
    python src/visualise_global_scatter.py
    python src/visualise_global_scatter.py --mode internal
    python src/visualise_global_scatter.py --mode both --precision 0
    python src/visualise_global_scatter.py --precision 0 --sample 10000
    python src/visualise_global_scatter.py --radius 8  # larger points

Output:
    output/gbif_scatter_global_all.html
    output/gbif_scatter_global_internal.html
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


def load_data(precision: int, mode: str = "all", sample: int | None = None) -> pd.DataFrame:
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
    elif mode == "external":
        df = df[df["source_type"] == "EXTERNAL"].copy()
        print(f"Filtered to EXTERNAL: {len(df):,} cells, {df['record_count'].sum():,} records")

    if sample and len(df) > sample:
        df = df.nlargest(sample, "record_count").reset_index(drop=True)
        print(f"Down-sampled to top {sample:,} cells by record_count")

    return df


def _color_for_value(val: float, color_ramp: list[list[int]]) -> list[int]:
    """Map 0..1 into the colour ramp with linear interpolation."""
    n = len(color_ramp) - 1
    idx = val * n
    lo = int(np.floor(idx))
    hi = min(lo + 1, n)
    frac = idx - lo
    return [
        int(color_ramp[lo][c] + frac * (color_ramp[hi][c] - color_ramp[lo][c]))
        for c in range(3)
    ]


def render_scatter_map(
    df: pd.DataFrame,
    mode: str,
    mapbox_token: str,
    precision: int,
    viewport: str = "ALL",
    radius_px: float = 5.0,
) -> str:
    import pydeck as pdk

    if mode == "internal":
        title = "GBIF Species Occurrences — Self-Published (Internal)"
        out_stem = "gbif_scatter_global_internal"
    elif mode == "external":
        title = "GBIF Species Occurrences — External Publishers"
        out_stem = "gbif_scatter_global_external"
    else:
        title = "GBIF Species Occurrences — All Records"
        out_stem = "gbif_scatter_global_all"

    title += f" (No Aves, p{precision})"

    print(f"  Rendering scatter: {len(df):,} points, {df['record_count'].sum():,} records")

    preset = COUNTRY_PRESETS.get(viewport.upper(), COUNTRY_PRESETS["ALL"])

    # Ensure native Python types for pydeck 0.8.0 JSON
    df = df.copy()
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)
    df["record_count"] = df["record_count"].astype(int)
    for col in ["source_type", "countrycode", "country_name", "wb_income_group", "un_region_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # --- Quantile-based colour mapping ---
    # This spreads the colour ramp evenly across the full data range
    # instead of log-compressing everything to the high end
    rc = df["record_count"].values.astype(float)
    ranks = rc.argsort().argsort()
    norm = ranks / max(len(rc) - 1, 1)
    df["_color_val"] = norm.astype(float)
    df["_color"] = df["_color_val"].apply(lambda v: _color_for_value(v, GBIF_COLORS))

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df.to_dict("records"),
        get_position=["lon", "lat"],
        get_fill_color="_color",
        # Fixed radius in screen pixels: clearly visible at any zoom
        radius_min_pixels=radius_px,
        radius_max_pixels=radius_px,
        # Stroke/outline makes points visible even on dark basemap
        stroked=True,
        get_line_color=[255, 255, 255],
        get_line_width=1,
        line_width_min_pixels=0.5,
        # High opacity: overlap creates natural density clusters
        opacity=0.85,
        pickable=True,
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        longitude=preset["lon"],
        latitude=preset["lat"],
        zoom=preset["zoom"],
        pitch=preset["pitch"],
        bearing=preset["bearing"],
        min_zoom=1,
        max_zoom=15,
    )

    api_keys = {"mapbox": mapbox_token} if mapbox_token else None

    # Always use dark basemap so bright-coloured points stand out
    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="dark",
        api_keys=api_keys,
        tooltip={
            "html": (
                "<b>Records in cell:</b> {record_count}<br/>"
                "<b>Lat:</b> {lat} | <b>Lon:</b> {lon}<br/>"
                "<b>Country:</b> {country_name}<br/>"
                "<b>Income:</b> {wb_income_group}"
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
        description="Render GBIF global scatterplot maps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--mode",       choices=["all", "internal", "external", "both"], default="both")
    parser.add_argument("--precision",  type=int,  default=0,      help="0=111km (~73K cells), 1=11km (~1M+ cells)")
    parser.add_argument("--sample",     type=int,  default=None,  help="Cap to N highest-record_count cells")
    parser.add_argument("--radius",     type=float, default=5.0,  help="Point radius in screen pixels")
    parser.add_argument("--viewport",   default="ALL", help="Starting viewport country code or ALL")
    args = parser.parse_args()

    token = load_mapbox_token()
    df = load_data(args.precision, mode="all", sample=args.sample)

    modes = ["all", "internal"] if args.mode == "both" else [args.mode]
    for m in modes:
        plot_df = df.copy()
        if m == "internal":
            plot_df = plot_df[plot_df["source_type"] == "INTERNAL"].copy()
        elif m == "external":
            plot_df = plot_df[plot_df["source_type"] == "EXTERNAL"].copy()

        if plot_df.empty:
            print(f"  Skipping {m} — no data after filtering")
            continue

        render_scatter_map(
            df=plot_df,
            mode=m,
            mapbox_token=token,
            precision=args.precision,
            viewport=args.viewport,
            radius_px=args.radius,
        )

    print("\nDone. Open the HTML files in a browser.")


if __name__ == "__main__":
    main()
