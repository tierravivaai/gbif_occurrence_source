"""
Global GBIF source distribution map using pydeck HexagonLayer.

Replicates the deck.gl UK Road Safety pattern: raw lat/lon rows are fed to
HexagonLayer, which aggregates them client-side into hexagonal bins. Height
and colour represent record density. Separate HTML files are generated for
All Records and Self-Published (Internal) views.

The deck.gl HexagonLayer does client-side aggregation, so this script feeds
the raw enriched hexbin rows (lat/lon/source_type/record_count) directly —
one row per pre-rounded cell, weighted by record_count via get_weight.

Key options:
  --log-scale         Apply log10 scaling to elevation/colour so low-density
                      areas (marine, smaller countries) are visible
  --normalize-country Scale each country's hexagons relative to that country's
                      maximum, making per-country density patterns comparable
  --marine-boost      Add a fixed offset to marine (countrycode=XX) records
                      so they don't vanish against land giants

Resolution options (--precision):
  1 -> p1 enriched data, ~11km cells, full global resolution (default)
  0 -> p0 enriched data, ~111km cells, faster load

Usage:
    python src/visualise_global_deckgl.py
    python src/visualise_global_deckgl.py --mode internal
    python src/visualise_global_deckgl.py --mode both --precision 0
    python src/visualise_global_deckgl.py --income-group "Low income"
    python src/visualise_global_deckgl.py --un-region "Africa"
    python src/visualise_global_deckgl.py --log-scale
    python src/visualise_global_deckgl.py --normalize-country --log-scale

Output:
    output/gbif_deckgl_global_all.html
    output/gbif_deckgl_global_internal.html
    output/gbif_deckgl_global_internal_{filter}.html  (if filter applied)
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

# GBIF 6-stop colour ramp [R, G, B]
GBIF_COLOR_RANGE = [
    [1,   152, 189],   # teal
    [73,  227, 206],   # mint
    [216, 254, 181],   # lime
    [254, 237, 177],   # yellow
    [254, 173, 84],    # orange
    [209, 55,  78],    # red
]

COUNTRY_PRESETS = {
    "ALL": {"lat": 20,    "lon": 10,     "zoom": 1.5, "pitch": 40, "bearing": -20},
    "BR":  {"lat": -14.2, "lon": -51.9,  "zoom": 3.5, "pitch": 40, "bearing": -20},
    "US":  {"lat": 39.8,  "lon": -98.6,  "zoom": 3.5, "pitch": 40, "bearing": -20},
    "ZA":  {"lat": -30.6, "lon": 22.9,   "zoom": 4.5, "pitch": 40, "bearing": -20},
    "IN":  {"lat": 20.6,  "lon": 78.9,   "zoom": 4.0, "pitch": 40, "bearing": -20},
    "AU":  {"lat": -25.3, "lon": 133.8,  "zoom": 3.5, "pitch": 40, "bearing": -20},
    "GB":  {"lat": 53.5,  "lon": -2.0,   "zoom": 5.0, "pitch": 40, "bearing": -20},
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


def load_data(
    precision: int,
    income_group: str = "",
    un_region: str = "",
    threshold: int = 0,
    marine_boost: int = 0,
) -> pd.DataFrame:
    stem = f"hexbin_all_no_aves_p{precision}_enriched"
    path = os.path.join(DATA_DIR, f"{stem}.parquet")
    if not os.path.exists(path):
        print(f"ERROR: {path} not found.")
        print(f"Run first: python src/aggregate_hexbin_pipeline.py --precision {precision}")
        sys.exit(1)

    df = pd.read_parquet(path)
    print(f"Loaded {len(df):,} cells, {df['record_count'].sum():,} total records")

    if income_group:
        df = df[df["wb_income_group"] == income_group].copy()
        print(f"Filtered to '{income_group}': {len(df):,} cells, {df['record_count'].sum():,} records")

    if un_region:
        df = df[df["un_region_name"] == un_region].copy()
        print(f"Filtered to '{un_region}': {len(df):,} cells, {df['record_count'].sum():,} records")

    if threshold > 0:
        before = len(df)
        total_before = df['record_count'].sum()
        df = df[df["record_count"] >= threshold].copy()
        total_after = df['record_count'].sum()
        pct = 100 * total_after / total_before
        print(f"Threshold >= {threshold}: dropped {before - len(df):,} cells ({100*(before-len(df))/before:.0f}% of cells)")
        print(f"  Records retained: {total_after:,} ({pct:.1f}% of total)")

    if marine_boost > 0:
        marine_mask = df["countrycode"] == "XX"
        marine_cells = marine_mask.sum()
        if marine_cells > 0:
            df.loc[marine_mask, "record_count"] = df.loc[marine_mask, "record_count"] + marine_boost
            print(f"Marine boost (+{marine_boost}): {marine_cells:,} cells adjusted")

    return df


def _apply_scaling(
    df: pd.DataFrame,
    log_scale: bool,
    normalize_country: bool,
) -> tuple[pd.DataFrame, str]:
    """Compute the weight column used by HexagonLayer.

    - log_scale: applies log10 to record_count so low-density cells are visible
    - normalize_country: scales each country's cells by that country's max,
      making per-country density patterns comparable on the global map
    """
    df = df.copy()
    weight_col = "record_count"

    if normalize_country:
        # Scale each country's record_count by its own maximum
        country_max = df.groupby("countrycode")["record_count"].transform("max").clip(lower=1)
        df["_weight_norm"] = df["record_count"] / country_max
        weight_col = "_weight_norm"
        print(f"  Per-country normalization applied: each country scaled to 0-1")

    if log_scale:
        # Apply log10 to the weight column (raw or normalized)
        df["_weight_log"] = np.log10(df[weight_col].clip(lower=1))
        weight_col = "_weight_log"
        print(f"  Log scale applied to '{weight_col}'")

    return df, weight_col


def render_hexagon_map(
    df: pd.DataFrame,
    mode: str,
    mapbox_token: str,
    precision: int,
    radius: int,
    elevation_scale: int,
    viewport: str = "ALL",
    label_suffix: str = "",
    log_scale: bool = False,
    normalize_country: bool = False,
) -> str:
    import pydeck as pdk

    if mode == "internal":
        data = df[df["source_type"] == "INTERNAL"].copy()
        title = "GBIF Species Occurrences — Self-Published (Internal) Records"
        out_stem = f"gbif_deckgl_global_internal"
    else:
        data = df.copy()
        title = "GBIF Species Occurrences — All Records"
        out_stem = "gbif_deckgl_global_all"

    if label_suffix:
        out_stem += f"_{label_suffix}"
        title += f" — {label_suffix.replace('_', ' ').title()}"

    # Build scaling suffix for filename
    scale_suffix = ""
    if log_scale:
        scale_suffix += "_log"
    if normalize_country:
        scale_suffix += "_norm"
    if scale_suffix:
        out_stem += scale_suffix
        title += f" ({scale_suffix.replace('_', ', ').strip(', ')} scaled)"

    title += f" (No Aves, p{precision})"

    print(f"  Rendering {mode}: {len(data):,} cells, {data['record_count'].sum():,} records")

    preset = COUNTRY_PRESETS.get(viewport.upper(), COUNTRY_PRESETS["ALL"])

    # pydeck 0.8.0 JSON serialization fails on numpy dtypes; coerce to native Python
    data["record_count"] = data["record_count"].astype(int)
    data["lat"] = data["lat"].astype(float)
    data["lon"] = data["lon"].astype(float)
    for col in ["source_type", "countrycode", "country_name", "wb_income_group", "un_region_name"]:
        if col in data.columns:
            data[col] = data[col].astype(str)

    # Apply scaling
    data, weight_col = _apply_scaling(data, log_scale, normalize_country)
    data[weight_col] = data[weight_col].astype(float)

    layer = pdk.Layer(
        "HexagonLayer",
        data=data.to_dict("records"),
        get_position=["lon", "lat"],
        get_weight=weight_col,
        radius=radius,
        elevation_scale=elevation_scale,
        elevation_range=[0, 3000],
        extruded=True,
        coverage=1,
        auto_highlight=True,
        pickable=True,
        color_range=GBIF_COLOR_RANGE,
        upper_percentile=100,
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
        map_style="dark" if mapbox_token else "light",
        api_keys=api_keys,
        tooltip={
            "html": (
                "<b>Records in cell:</b> {count}<br/>"
                "<b>Hold Shift</b> to rotate  |  <b>Scroll</b> to zoom"
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
        description="Render GBIF global HexagonLayer map (deck.gl / UK Road Safety pattern)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--mode",            choices=["all", "internal", "both"], default="both")
    parser.add_argument("--precision",       type=int,  default=1,      help="0=111km, 1=11km")
    parser.add_argument("--radius",          type=int,  default=None, help="Hexagon radius in metres (default: 50000 for p0, 20000 for p1)")
    parser.add_argument("--elevation-scale", type=int,  default=None, help="3D extrusion scale (default: 50 for p0, 100 for p1)")
    parser.add_argument("--income-group",    default="", help="Filter: 'High income', 'Low income', etc.")
    parser.add_argument("--un-region",       default="", help="Filter: 'Africa', 'Americas', etc.")
    parser.add_argument("--viewport",        default="ALL", help="Starting viewport country code or ALL")
    parser.add_argument("--threshold",       type=int,  default=0,      help="Drop cells with fewer than N records (default 0 = keep all)")
    parser.add_argument("--log-scale",       action="store_true", help="Apply log10 scaling to weights (makes low-density areas visible)")
    parser.add_argument("--normalize-country", action="store_true", help="Normalize each country's weights by its max (comparable per-country density)")
    parser.add_argument("--marine-boost",    type=int,  default=0,      help="Add N to all marine (countrycode=XX) cell counts so they don't vanish")
    args = parser.parse_args()

    # Smart defaults based on precision
    radius = args.radius if args.radius is not None else {0: 50000, 1: 20000}.get(args.precision, 50000)
    elevation_scale = args.elevation_scale if args.elevation_scale is not None else {0: 50, 1: 100}.get(args.precision, 50)

    token = load_mapbox_token()
    df = load_data(args.precision, args.income_group, args.un_region, threshold=args.threshold, marine_boost=args.marine_boost)

    label = ""
    if args.income_group:
        label = args.income_group.lower().replace(" ", "_")
    elif args.un_region:
        label = args.un_region.lower().replace(" ", "_")

    modes = ["all", "internal"] if args.mode == "both" else [args.mode]
    for m in modes:
        render_hexagon_map(
            df=df,
            mode=m,
            mapbox_token=token,
            precision=args.precision,
            radius=radius,
            elevation_scale=elevation_scale,
            viewport=args.viewport,
            label_suffix=label,
            log_scale=args.log_scale,
            normalize_country=args.normalize_country,
        )

    print("\nDone. Open the HTML files in a browser.")
    print("Tip: Hold Shift + drag to rotate the 3D view.")


if __name__ == "__main__":
    main()
