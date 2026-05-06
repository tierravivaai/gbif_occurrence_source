"""
Global choropleth map of GBIF source distribution using pydeck GeoJsonLayer.

Fills each country polygon with a colour based on internal_percentage (self-published
share) or total_count (record density). Best for policy audiences — immediately shows
which countries self-publish vs depend on external publishers.

Colour ramp:
  internal_percentage mode: grey (0%) -> GBIF teal (100%)
  total_count mode:         grey (low) -> GBIF red (high, log scale)

Usage:
    python src/visualise_global_choropleth.py
    python src/visualise_global_choropleth.py --mode internal
    python src/visualise_global_choropleth.py --mode total
    python src/visualise_global_choropleth.py --income-group "Low income"
    python src/visualise_global_choropleth.py --un-region "Africa"

Output:
    output/gbif_choropleth_internal.html
    output/gbif_choropleth_total.html
"""

import argparse
import json
import math
import os
import sys

import duckdb
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEOJSON_PATH = os.path.join(PROJECT_DIR, "data", "processed", "ne_110m_admin_0_countries.geojson")
SOURCE_CSV   = os.path.join(PROJECT_DIR, "data", "processed", "source_by_country.csv")
OUTPUT_DIR   = os.path.join(PROJECT_DIR, "output")
ENV_PATH     = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")

# GBIF 6-stop colour ramps as [R,G,B] lists
RAMP_TEAL = [
    [220, 220, 220],   # grey    0%
    [1,   152, 189],   # teal   20%
    [73,  227, 206],   # mint   40%
    [216, 254, 181],   # lime   60%
    [254, 237, 177],   # yellow 80%
    [209, 55,  78],    # red   100%
]

INCOME_COLORS = {
    "High income":          [1,   152, 189],
    "Upper middle income":  [73,  227, 206],
    "Lower middle income":  [254, 173, 84],
    "Low income":           [209, 55,  78],
    "Not classified":       [160, 160, 160],
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


def _lerp_color(ramp: list[list[int]], t: float) -> list[int]:
    """Linearly interpolate through a colour ramp for t in [0, 1]."""
    t = max(0.0, min(1.0, t))
    idx = t * (len(ramp) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ramp) - 1)
    frac = idx - lo
    return [int(ramp[lo][i] + frac * (ramp[hi][i] - ramp[lo][i])) for i in range(3)]


def load_source_data(income_group: str = "", un_region: str = "") -> pd.DataFrame:
    con = duckdb.connect()
    where_clauses = ["iso2c IS NOT NULL"]
    if income_group:
        where_clauses.append(f"wb_income_group = '{income_group}'")
    if un_region:
        where_clauses.append(f"un_region_name = '{un_region}'")
    where_sql = " AND ".join(where_clauses)

    df = con.execute(f"""
        SELECT
            iso2c,
            iso3c,
            country_name,
            un_region_name,
            un_sub_region_name,
            wb_income_group,
            is_cbd_party,
            is_ldc,
            is_sids,
            internal_count,
            external_count,
            total_count,
            internal_percentage,
            external_percentage
        FROM read_csv('{SOURCE_CSV}')
        WHERE {where_sql}
    """).df()
    con.close()
    return df


def build_geojson_with_data(source_df: pd.DataFrame, mode: str) -> dict:
    """
    Join source_df into the Natural Earth GeoJSON feature properties so pydeck
    GeoJsonLayer can read colour and tooltip fields directly from each feature.
    """
    with open(GEOJSON_PATH) as f:
        geojson = json.load(f)

    lookup = source_df.set_index("iso2c").to_dict(orient="index")

    max_total = source_df["total_count"].max()

    for feature in geojson["features"]:
        props = feature["properties"]
        iso2 = props.get("ISO_A2_EH") or props.get("ISO_A2") or ""
        row = lookup.get(iso2)

        if row:
            props["country_name"]      = row["country_name"]
            props["total_count"]       = int(row["total_count"])
            props["internal_count"]    = int(row["internal_count"])
            props["external_count"]    = int(row["external_count"])
            props["internal_pct"]      = round(float(row["internal_percentage"] or 0), 1)
            props["external_pct"]      = round(float(row["external_percentage"] or 0), 1)
            props["wb_income_group"]   = str(row["wb_income_group"] or "Not classified")
            props["un_region"]         = str(row["un_region_name"] or "")
            props["is_cbd_party"]      = str(row["is_cbd_party"])

            if mode == "internal":
                t = props["internal_pct"] / 100.0
                rgb = _lerp_color(RAMP_TEAL, t)
            else:
                # log-scale total_count
                t = math.log10(max(row["total_count"], 1)) / math.log10(max(max_total, 2))
                rgb = _lerp_color(RAMP_TEAL, t)

            props["fill_color"] = rgb + [200]   # [R, G, B, A]
        else:
            # No data — dark grey
            props["country_name"]    = props.get("NAME", "Unknown")
            props["total_count"]     = 0
            props["internal_count"]  = 0
            props["external_count"]  = 0
            props["internal_pct"]    = 0.0
            props["external_pct"]    = 0.0
            props["wb_income_group"] = "No data"
            props["un_region"]       = ""
            props["is_cbd_party"]    = "Unknown"
            props["fill_color"]      = [60, 60, 60, 160]

    return geojson


def render_choropleth(mode: str, mapbox_token: str, suffix: str = "") -> str:
    import pydeck as pdk

    source_df = load_source_data()
    geojson   = build_geojson_with_data(source_df, mode)

    geojson_layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson,
        get_fill_color="properties.fill_color",
        get_line_color=[255, 255, 255, 80],
        line_width_min_pixels=0.5,
        pickable=True,
        auto_highlight=True,
        opacity=0.85,
    )

    view_state = pdk.ViewState(
        longitude=10,
        latitude=20,
        zoom=1.5,
        pitch=0,
        bearing=0,
    )

    if mode == "internal":
        title = "GBIF Species Occurrences — Self-Published Share by Country (No Aves)"
        tooltip_metric = "<b>Self-published:</b> {properties.internal_pct}%<br/>"
    else:
        title = "GBIF Species Occurrences — Total Record Density by Country (No Aves)"
        tooltip_metric = "<b>Total records:</b> {properties.total_count}<br/>"

    api_keys = {"mapbox": mapbox_token} if mapbox_token else None

    deck = pdk.Deck(
        layers=[geojson_layer],
        initial_view_state=view_state,
        map_style="dark" if mapbox_token else "light",
        api_keys=api_keys,
        tooltip={
            "html": f"""
                <b>{{properties.country_name}}</b><br/>
                {tooltip_metric}
                <b>Internal count:</b> {{properties.internal_count}}<br/>
                <b>External count:</b> {{properties.external_count}}<br/>
                <b>WB income group:</b> {{properties.wb_income_group}}<br/>
                <b>UN region:</b> {{properties.un_region}}<br/>
                <b>CBD party:</b> {{properties.is_cbd_party}}
            """,
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
    out_name = f"gbif_choropleth_{mode}{('_' + suffix) if suffix else ''}.html"
    out_path = os.path.join(OUTPUT_DIR, out_name)

    html = deck.to_html(as_string=True)
    html = html.replace("<title>", f"<title>{title} | ")
    # Full-viewport CSS
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
        description="Render GBIF global choropleth map",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--mode", choices=["internal", "total", "both"], default="both")
    parser.add_argument("--income-group", default="", help="Filter to one WB income group")
    parser.add_argument("--un-region",    default="", help="Filter to one UN region")
    args = parser.parse_args()

    for path, name in [(GEOJSON_PATH, "Natural Earth GeoJSON"), (SOURCE_CSV, "source_by_country.csv")]:
        if not os.path.exists(path):
            print(f"ERROR: {path} not found ({name})")
            sys.exit(1)

    token = load_mapbox_token()
    suffix = ""
    if args.income_group:
        suffix = args.income_group.lower().replace(" ", "_")
    elif args.un_region:
        suffix = args.un_region.lower().replace(" ", "_")

    modes = ["internal", "total"] if args.mode == "both" else [args.mode]
    for m in modes:
        print(f"\nRendering choropleth: mode={m}")
        render_choropleth(m, token, suffix)

    print("\nDone. Open the HTML files in a browser.")


if __name__ == "__main__":
    main()
