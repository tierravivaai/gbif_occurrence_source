"""
Global GBIF species occurrence maps as 3D vertical column heatmaps.

Renders each pre-aggregated cell center (lat/lon rounded to precision)
as a vertical 3D hexagonal column. Column height encodes record density
(log-scaled so low-density areas are visible alongside giants). Colour
also maps to density via the GBIF quantile scale.

Two maps generated:
  1. All Records
  2. Self-Published (INTERNAL) Records

Usage:
    python src/visualise_global_3d_columns.py
    python src/visualise_global_3d_columns.py --mode both --precision 0
    python src/visualise_global_3d_columns.py --mode internal --precision 0

Output:
    output/gbif_3dcolumns_global_all.html
    output/gbif_3dcolumns_global_internal.html
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
    "ALL": {"lat": 20,    "lon": 10,     "zoom": 1.5, "pitch": 45, "bearing": -20},
    "BR":  {"lat": -14.2, "lon": -51.9,  "zoom": 3.5, "pitch": 45, "bearing": -20},
    "US":  {"lat": 39.8,  "lon": -98.6,  "zoom": 3.5, "pitch": 45, "bearing": -20},
    "ZA":  {"lat": -30.6, "lon": 22.9,   "zoom": 4.5, "pitch": 45, "bearing": -20},
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


def render_3d_column_map(
    df: pd.DataFrame,
    mode: str,
    mapbox_token: str,
    precision: int,
    viewport: str = "ALL",
    radius: int = 30000,
    elevation_scale: float = 50,
) -> str:
    import pydeck as pdk

    if mode == "internal":
        title = "GBIF Species Occurrences — Self-Published (Internal) — 3D Columns"
        out_stem = "gbif_3dcolumns_global_internal"
    else:
        title = "GBIF Species Occurrences — All Records — 3D Columns"
        out_stem = "gbif_3dcolumns_global_all"

    title += f" (No Aves, p{precision})"

    print(f"  Rendering 3D columns: {len(df):,} cells, {df['record_count'].sum():,} records")

    preset = COUNTRY_PRESETS.get(viewport.upper(), COUNTRY_PRESETS["ALL"])

    # Ensure native Python types for pydeck 0.8.0 JSON
    data = df.copy()
    data["record_count"] = data["record_count"].astype(int)
    data["lat"] = data["lat"].astype(float)
    data["lon"] = data["lon"].astype(float)
    for col in ["source_type", "countrycode", "country_name", "wb_income_group", "un_region_name"]:
        if col in data.columns:
            data[col] = data[col].astype(str)

    # --- Quantile-based colour mapping (same as scatterplot) ---
    rc = data["record_count"].values.astype(float)
    ranks = rc.argsort().argsort()
    norm = ranks / max(len(rc) - 1, 1)
    data["_color_val"] = norm.astype(float)
    data["_color"] = data["_color_val"].apply(lambda v: _color_for_value(v, GBIF_COLORS))

    # --- Log-scaled elevation ---
    # Use log10 so low-density cells get visible columns alongside giants.
    # Elevation range: 10–1000 (before any scale multiplier).
    log_rc = np.log10(rc.clip(1))
    min_log, max_log = log_rc.min(), log_rc.max()
    if max_log > min_log:
        data["_elevation"] = ((log_rc - min_log) / (max_log - min_log) * 990 + 10).astype(float)
    else:
        data["_elevation"] = 100.0

    layer = pdk.Layer(
        "ColumnLayer",
        data=data.to_dict("records"),
        get_position=["lon", "lat"],
        get_elevation="_elevation",
        get_fill_color="_color",
        elevation_scale=1,       # elevation is now the actual height directly
        extruded=True,
        pickable=True,
        auto_highlight=True,
        disk_resolution=6,         # hexagonal cross-section
        radius=radius,
        coverage=1,
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
<script>
  // Hold shift to rotate hint
  document.addEventListener('DOMContentLoaded', function() {
    var hint = document.createElement('div');
    hint.style.cssText = 'position:fixed;bottom:20px;left:50%;transform:translateX(-50%);'
      + 'background:rgba(0,0,0,0.7);color:#fff;padding:6px 14px;border-radius:20px;'
      + 'font-family:Arial,sans-serif;font-size:12px;pointer-events:none;z-index:9999;';
    hint.textContent = 'Hold Shift + drag to rotate \u2022 Scroll to zoom';
    document.body.appendChild(hint);
    setTimeout(function(){ hint.style.opacity='0'; hint.style.transition='opacity 1s'; }, 4000);
  });
</script>
</head>""")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Saved: {out_path}")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render GBIF global 3D column heatmaps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--mode",            choices=["all", "internal", "both"], default="both")
    parser.add_argument("--precision",       type=int,  default=0,      help="0=111km, 1=11km")
    parser.add_argument("--radius",          type=int,  default=30000,  help="Column radius in metres (default 30km)")
    parser.add_argument("--elevation-scale", type=float, default=50,   help="Elevation multiplier (default 50)")
    parser.add_argument("--threshold",       type=int,  default=0,      help="Drop cells with < N records")
    parser.add_argument("--viewport",        default="ALL", help="Starting viewport country code or ALL")
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

        render_3d_column_map(
            df=plot_df,
            mode=m,
            mapbox_token=token,
            precision=args.precision,
            viewport=args.viewport,
            radius=args.radius,
            elevation_scale=args.elevation_scale,
        )

    print("\nDone. Open the HTML files in a browser.")
    print("Tip: Hold Shift + drag to rotate the 3D view.")


if __name__ == "__main__":
    main()
