"""
Country-level GBIF source distribution map using pydeck HexagonLayer.

Deep-dive into a single country's occurrence record distribution at 11km
resolution. Supersedes visualise_country_hexbin.py with extended filter support.

Generates two HTML files by default:
  1. All records (all source_type values)
  2. Self-published only (source_type == INTERNAL)

Additional filters:
  --filter all|internal|external    subset source_type
  --income-group                    subset by WB income group
  --un-region                       subset by UN region

Scaling options:
  --log-scale                       Apply log10 to weights (makes low-density visible)

Data requirement:
  hexbin_{cc}_no_aves_p1_enriched.parquet must exist in data/processed/.
  If missing, run: python src/aggregate_hexbin_pipeline.py --countries {CC}

Usage:
    python src/visualise_country_drilldown.py --country BR
    python src/visualise_country_drilldown.py --country ZA --filter internal
    python src/visualise_country_drilldown.py --country IN --income-group "Lower middle income"
    python src/visualise_country_drilldown.py --country US --filter all --log-scale

Output:
    output/gbif_drilldown_{cc}_all.html
    output/gbif_drilldown_{cc}_internal.html
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

GBIF_COLOR_RANGE = [
    [1,   152, 189],
    [73,  227, 206],
    [216, 254, 181],
    [254, 237, 177],
    [254, 173, 84],
    [209, 55,  78],
]

COUNTRY_PRESETS = {
    "BR": {"lat": -14.2, "lon": -51.9,  "zoom": 3.5, "radius": 20000},
    "US": {"lat": 39.8,  "lon": -98.6,  "zoom": 3.5, "radius": 20000},
    "ZA": {"lat": -30.6, "lon": 22.9,   "zoom": 4.5, "radius": 10000},
    "IN": {"lat": 20.6,  "lon": 78.9,   "zoom": 4.0, "radius": 15000},
    "CO": {"lat": 4.6,   "lon": -74.3,  "zoom": 5.0, "radius": 5000},
    "MX": {"lat": 23.6,  "lon": -102.6, "zoom": 4.0, "radius": 15000},
    "AU": {"lat": -25.3, "lon": 133.8,  "zoom": 3.5, "radius": 20000},
    "GB": {"lat": 53.5,  "lon": -2.0,   "zoom": 5.0, "radius": 5000},
    "FR": {"lat": 46.6,  "lon": 2.2,    "zoom": 4.5, "radius": 8000},
    "DE": {"lat": 51.2,  "lon": 10.4,   "zoom": 5.0, "radius": 5000},
    "ID": {"lat": -2.5,  "lon": 118.0,  "zoom": 4.0, "radius": 15000},
    "JP": {"lat": 36.2,  "lon": 138.3,  "zoom": 5.0, "radius": 5000},
    "CA": {"lat": 56.1,  "lon": -106.3, "zoom": 3.0, "radius": 25000},
    "PE": {"lat": -9.2,  "lon": -75.0,  "zoom": 4.5, "radius": 10000},
    "SE": {"lat": 62.0,  "lon": 15.0,   "zoom": 4.5, "radius": 10000},
    "NO": {"lat": 64.5,  "lon": 13.0,   "zoom": 4.5, "radius": 10000},
    "ES": {"lat": 40.5,  "lon": -3.7,   "zoom": 5.0, "radius": 8000},
}
DEFAULT_PRESET = {"lat": 0, "lon": 0, "zoom": 4.0, "radius": 20000}


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
    country_code: str,
    filter_source: str = "all",
    income_group: str = "",
    un_region: str = "",
) -> pd.DataFrame:
    cc = country_code.upper()
    stem = f"hexbin_{cc.lower()}_no_aves_p1_enriched"
    path = os.path.join(DATA_DIR, f"{stem}.parquet")

    if not os.path.exists(path):
        print(f"ERROR: data file not found:\n  {path}")
        print(f"\nTo build it, run:")
        print(f"  python src/aggregate_hexbin_pipeline.py --countries {cc} --skip-global")
        sys.exit(1)

    df = pd.read_parquet(path)
    print(f"Loaded {len(df):,} cells, {df['record_count'].sum():,} records for {cc}")

    if filter_source == "internal":
        df = df[df["source_type"] == "INTERNAL"].copy()
    elif filter_source == "external":
        df = df[df["source_type"] == "EXTERNAL"].copy()

    if income_group:
        df = df[df["wb_income_group"] == income_group].copy()
        print(f"  Income filter '{income_group}': {len(df):,} cells")

    if un_region:
        df = df[df["un_region_name"] == un_region].copy()
        print(f"  UN region filter '{un_region}': {len(df):,} cells")

    if df.empty:
        print("WARNING: no data after filtering — check filter values")

    return df


def render_drilldown(
    df: pd.DataFrame,
    mode: str,
    country_code: str,
    mapbox_token: str,
    label_suffix: str = "",
    log_scale: bool = False,
) -> str:
    import pydeck as pdk

    cc = country_code.upper()
    preset = COUNTRY_PRESETS.get(cc, DEFAULT_PRESET)

    if mode == "internal":
        data = df[df["source_type"] == "INTERNAL"].copy()
        mode_label = "Self-Published (Internal)"
        out_stem = f"gbif_drilldown_{cc.lower()}_internal"
    else:
        data = df.copy()
        mode_label = "All Records"
        out_stem = f"gbif_drilldown_{cc.lower()}_all"

    if label_suffix:
        out_stem += f"_{label_suffix}"

    scale_suffix = ""
    if log_scale:
        scale_suffix += "_log"
    if scale_suffix:
        out_stem += scale_suffix

    title = f"GBIF Species Occurrences — {cc} — {mode_label} (No Aves)"

    print(f"  {mode_label}: {len(data):,} cells, {data['record_count'].sum():,} records")

    if data.empty:
        print(f"  Skipping {mode} map — no data")
        return ""

    data = data.copy()
    # pydeck 0.8.0 JSON serialization: convert DataFrame to plain Python dicts
    data["record_count"] = data["record_count"].astype(int)
    data["lat"] = data["lat"].astype(float)
    data["lon"] = data["lon"].astype(float)
    for col in ["countrycode", "country_name", "wb_income_group", "un_region_name", "un_sub_region_name"]:
        if col in data.columns:
            data[col] = data[col].fillna("").astype(str)

    # Apply log scaling if requested
    weight_col = "record_count"
    if log_scale:
        data["_weight_log"] = np.log10(data["record_count"].clip(lower=1))
        weight_col = "_weight_log"
        data[weight_col] = data[weight_col].astype(float)
        print(f"  Log scale applied to weights")

    records = data.to_dict("records")  # native Python types only

    layer = pdk.Layer(
        "HexagonLayer",
        data=records,
        get_position=["lon", "lat"],
        get_weight=weight_col,
        radius=preset["radius"],
        elevation_scale=50,
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
        pitch=40.5,
        bearing=-27,
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
                "<b>Hold Shift</b> to rotate"
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
        description="Render country-level GBIF HexagonLayer drilldown map",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--country",      default="BR",  help="ISO2 country code")
    parser.add_argument("--filter",       choices=["all", "internal", "external", "both"],
                        default="both",  help="Source type filter")
    parser.add_argument("--income-group", default="",   help="Filter by WB income group")
    parser.add_argument("--un-region",    default="",   help="Filter by UN region")
    parser.add_argument("--log-scale",      action="store_true", help="Apply log10 scaling to weights")
    args = parser.parse_args()

    cc = args.country.upper()
    if cc not in COUNTRY_PRESETS:
        print(f"Note: no preset for {cc} — using default zoom/radius. "
              f"Known presets: {', '.join(sorted(COUNTRY_PRESETS))}")

    token = load_mapbox_token()

    filter_source = "all" if args.filter == "both" else args.filter
    df = load_data(cc, filter_source=filter_source,
                   income_group=args.income_group, un_region=args.un_region)

    label = ""
    if args.income_group:
        label = args.income_group.lower().replace(" ", "_")
    elif args.un_region:
        label = args.un_region.lower().replace(" ", "_")

    modes = ["all", "internal"] if args.filter == "both" else [args.filter]
    outputs = []
    for m in modes:
        path = render_drilldown(df, m, cc, token, label_suffix=label, log_scale=args.log_scale)
        if path:
            outputs.append(path)

    if outputs:
        print(f"\nDone. Open in browser:")
        for p in outputs:
            print(f"  open '{p}'")


if __name__ == "__main__":
    main()
