"""
Visualise GBIF source distribution as a 3D hexbin map using pydeck.

Uses the pre-computed country-level source data (source_by_country_no_aves.csv)
with Natural Earth country centroids. Produces two HTML maps:
  1. All records density (total_count per country)
  2. Self-published share (internal_percentage per country)

Each country is rendered as a hexbin point weighted by the metric,
with elevation showing the same metric in 3D.

Usage:
    python src/visualise_source_hexbin.py
    python src/visualise_source_hexbin.py --mode internal   # self-published share
    python src/visualise_source_hexbin.py --mode both        # generate both maps

Requires:
    pip install pydeck pandas duckdb
    MAPBOX_API_KEY in /Users/pauloldham/hermes-secure-runner/hermes-data/.env
"""

import argparse
import json
import os
import sys

import duckdb
import pandas as pd

# ─── Paths ───────────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "processed")
ENV_PATH = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")

SOURCE_CSV = os.path.join(DATA_DIR, "source_by_country_no_aves.csv")
SOURCE_ALL_CSV = os.path.join(DATA_DIR, "source_by_country.csv")
NE_GEOJSON = os.path.join(DATA_DIR, "ne_110m_admin_0_countries.geojson")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


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
    if not token:
        print("WARNING: MAPBOX_API_KEY not found. Map will use blank basemap.")
    return token


# ─── Manual centroids for countries missing from Natural Earth 110m ─────────
MANUAL_CENTROIDS = {
    "SJM": (78.0, 16.0),       # Svalbard
    "IMN": (54.2, -4.5),       # Isle of Man
    "GUF": (3.9, -53.1),       # French Guiana
    "ALA": (60.1, 20.0),      # Aland Islands
    "REU": (-21.1, 55.5),     # Reunion
    "VIR": (18.3, -64.9),     # Virgin Islands U.S.
    "PYF": (-17.6, -149.4),  # French Polynesia
    "HKG": (22.3, 114.2),     # Hong Kong
    "ASM": (-14.3, -170.7),  # American Samoa
    "SGP": (1.3, 103.8),      # Singapore
    "GLP": (16.3, -61.6),     # Guadeloupe
    "UMI": (19.3, 166.6),     # U.S. Minor Outlying Islands
    "GGY": (49.5, -2.6),      # Guernsey
    "SGS": (-54.3, -37.0),   # South Georgia
    "SYC": (-4.7, 55.5),     # Seychelles
    "FSM": (6.9, 158.2),      # Micronesia
    "MNP": (15.2, 145.7),     # Northern Mariana Islands
    "BMU": (32.3, -64.8),     # Bermuda
    "MTQ": (14.6, -61.0),     # Martinique
    "JEY": (49.2, -2.1),      # Jersey
    "COK": (-21.2, -159.8),   # Cook Islands
    "NIU": (-19.1, -169.9),   # Niue
    "TUV": (-8.0, 178.0),     # Tuvalu
    "KNA": (17.3, -62.7),     # St Kitts and Nevis
    "LCA": (14.0, -61.0),     # St Lucia
    "VCT": (13.2, -61.2),    # St Vincent and Grenadines
    "GRD": (12.1, -61.7),     # Grenada
    "ATG": (17.0, -61.8),     # Antigua and Barbuda
    "DMA": (15.4, -61.4),     # Dominica
    "MCO": (43.7, 7.4),       # Monaco
    "SMR": (43.9, 12.5),      # San Marino
    "VAT": (41.9, 12.5),      # Vatican City
    "LIE": (47.1, 9.6),       # Liechtenstein
    "AND": (42.5, 1.5),        # Andorra
    "PLW": (7.5, 134.6),      # Palau
    "MHL": (7.1, 171.2),      # Marshall Islands
    "KIR": (1.9, 173.0),      # Kiribati
    "NFK": (-29.0, 168.0),    # Norfolk Island
    "SPM": (46.8, -56.3),     # Saint Pierre and Miquelon
    "BLM": (17.9, -62.8),     # Saint Barthelemy
    "MAF": (18.1, -63.1),     # Saint Martin (French)
    "SXM": (18.0, -63.1),     # Sint Maarten
    "CXR": (-10.4, 105.7),   # Christmas Island
    "CCK": (-12.2, 96.8),     # Cocos Islands
    "IOT": (-7.4, 72.4),       # British Indian Ocean Territory
    "PCN": (-25.1, -130.1),   # Pitcairn Islands
    "SHN": (-15.9, -5.7),     # Saint Helena
    "TCA": (21.5, -71.7),     # Turks and Caicos
    "AIA": (18.2, -63.0),     # Anguilla
    "MSR": (16.7, -62.2),     # Montserrat
    "GIB": (36.1, -5.4),       # Gibraltar
    "FLK": (-51.8, -59.0),    # Falkland Islands
    "WLF": (-14.3, -178.0),  # Wallis and Futuna
    "NCL": (-22.3, 166.9),   # New Caledonia
    "MYT": (-12.8, 45.2),     # Mayotte
    "BES": (12.2, -68.3),     # Bonaire, Sint Eustatius and Saba
    "ABW": (12.5, -70.0),     # Aruba
    "CUW": (12.1, -68.9),     # Curacao
    "SAB": (-5.0, -55.0),     # Suriname (approx)
    "GUM": (13.4, 144.8),     # Guam
    "PRI": (18.2, -66.5),     # Puerto Rico
    "TKL": (-9.2, -171.8),    # Tokelau
    "XKX": (42.6, 20.9),      # Kosovo
    "TWN": (23.7, 121.0),     # Taiwan
    "PSE": (31.9, 35.2),      # Palestine
    "MLT": (35.9, 14.4),       # Malta
    "BRB": (13.2, -59.5),     # Barbados
    "BHR": (26.1, 50.6),       # Bahrain
    "COM": (-11.9, 43.9),      # Comoros
    "CPV": (16.0, -24.0),     # Cape Verde
    "CYM": (19.3, -81.4),      # Cayman Islands
    "FRO": (62.0, -6.8),       # Faroe Islands
    "HMD": (-53.1, 73.0),     # Heard Island and McDonald Islands
    "MAC": (22.2, 113.5),      # Macao
    "MDV": (3.2, 73.2),        # Maldives
    "MUS": (-20.3, 57.5),      # Mauritius
    "NRU": (-0.5, 166.9),      # Nauru
    "STP": (0.2, 6.6),         # Sao Tome and Principe
    "TON": (-21.2, -175.2),    # Tonga
    "VGB": (18.4, -64.6),      # British Virgin Islands
    "WSM": (-13.8, -172.0),    # Samoa
    "BVT": (-54.4, 3.4),       # Bouvet Island
}


def extract_centroids(geojson_path: str) -> dict[str, tuple[float, float]]:
    """Extract country centroids (label point) from Natural Earth GeoJSON.

    Returns dict mapping ISO_A3 -> (lat, lon). Includes manual fallbacks.
    """
    with open(geojson_path) as f:
        data = json.load(f)

    centroids = {}
    for feature in data["features"]:
        props = feature["properties"]
        # Try multiple ISO code fields
        iso3 = props.get("ISO_A3_EH") or props.get("ISO_A3") or props.get("ADM0_A3") or ""
        label_lat = props.get("LABEL_Y") or props.get("LABEL_LAT")
        label_lon = props.get("LABEL_X") or props.get("LABEL_LON")

        if iso3 and iso3 != "-99":
            if label_lat is not None and label_lon is not None:
                centroids[iso3] = (float(label_lat), float(label_lon))
            else:
                # Fallback: compute centroid from geometry bounds
                geom = feature["geometry"]
                coords = _flatten_coords(geom)
                if coords:
                    lats = [c[1] for c in coords]
                    lons = [c[0] for c in coords]
                    centroids[iso3] = (sum(lats) / len(lats), sum(lons) / len(lons))

    # Add manual centroids for missing territories
    for iso3, coord in MANUAL_CENTROIDS.items():
        if iso3 not in centroids:
            centroids[iso3] = coord

    return centroids


def _flatten_coords(geom: dict) -> list[tuple[float, float]]:
    """Extract all coordinate pairs from a GeoJSON geometry."""
    coords = []
    geom_type = geom["type"]
    if geom_type == "Point":
        return [tuple(geom["coordinates"])]
    elif geom_type in ("MultiPoint", "LineString"):
        return [tuple(c) for c in geom["coordinates"]]
    elif geom_type == "MultiLineString":
        for line in geom["coordinates"]:
            coords.extend(tuple(c) for c in line)
        return coords
    elif geom_type == "Polygon":
        for ring in geom["coordinates"]:
            coords.extend(tuple(c) for c in ring)
        return coords
    elif geom_type == "MultiPolygon":
        for polygon in geom["coordinates"]:
            for ring in polygon:
                coords.extend(tuple(c) for c in ring)
        return coords
    return coords


def merge_source_with_centroids(
    source_csv: str, centroids: dict[str, tuple[float, float]]
) -> pd.DataFrame:
    """Join source data with country centroids, returning DataFrame with lat/lon."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT
            iso3c,
            iso2c,
            country_name,
            un_region_name,
            wb_income_group,
            is_cbd_party,
            is_ldc,
            is_sids,
            internal_count,
            regional_count,
            sub_regional_count,
            external_count,
            unknown_count,
            total_count,
            internal_percentage,
            external_percentage
        FROM read_csv('{source_csv}')
        WHERE iso3c IS NOT NULL
        ORDER BY total_count DESC
    """).df()
    con.close()

    # Add centroids
    df["lat"] = df["iso3c"].map(lambda x: centroids.get(x, (None, None))[0])
    df["lon"] = df["iso3c"].map(lambda x: centroids.get(x, (None, None))[1])

    # Drop rows without centroids
    before = len(df)
    df = df.dropna(subset=["lat", "lon"]).reset_index(drop=True)
    print(f"  Countries with centroids: {len(df)} (dropped {before - len(df)} without)")

    return df


def render_hexbin_map(
    df: pd.DataFrame,
    mode: str = "total",
    mapbox_token: str = "",
    suffix: str = "no_aves",
) -> str:
    """Render a 3D column map using pydeck ColumnLayer and return the HTML file path.

    Each country is an extruded hexagonal column whose height is proportional to
    the chosen metric (total_count or internal_count). Colour maps from a
    sequential scale.

    Args:
        df: DataFrame with lat, lon, and count columns
        mode: 'total' for total_count, 'internal' for internal_count/percentage
        mapbox_token: Mapbox API key
        suffix: Filename suffix (e.g. 'no_aves', 'all_taxa')
    """
    import pydeck as pdk
    import numpy as np

    if mode == "internal":
        height_col = "internal_count"
        title = "GBIF Self-Published (Internal) Records — " + suffix.replace("_", " ").title()
    else:
        height_col = "total_count"
        title = "GBIF Occurrence Record Density — " + suffix.replace("_", " ").title()

    # Normalise height: use log scale so small countries are visible alongside giants
    # like the US (1.1B) vs Seychelles (247K)
    max_val = df[height_col].max()
    df["_elevation"] = np.log10(df[height_col].clip(lower=1)) / np.log10(max_val) * 3000

    # Colour: map height_col to a 0-1 range then to the colour ramp
    df["_colour_val"] = np.log10(df[height_col].clip(lower=1)) / np.log10(max_val)

    def _color_scale(val: float) -> list:
        """Map 0..1 to a sequential teal-to-red colour ramp."""
        ramp = [
            (1, 152, 189),    # teal
            (73, 227, 206),   # mint
            (216, 254, 181),  # lime
            (254, 237, 177),  # yellow
            (254, 173, 84),   # orange
            (209, 55, 78),    # red
        ]
        idx = min(int(val * (len(ramp) - 1)), len(ramp) - 1)
        return list(ramp[idx])

    df["_color"] = df["_colour_val"].apply(_color_scale)

    # ─── Column Layer (3D extruded hexagonal columns) ────────────────
    column_layer = pdk.Layer(
        "ColumnLayer",
        data=df,
        get_position=["lon", "lat"],
        get_elevation="_elevation",
        getFillColor="_color",
        elevation_scale=2,
        extruded=True,
        pickable=True,
        auto_highlight=True,
        disk_resolution=12,     # 12 sides = hexagonal cross-section
        radius=100000,          # 100km column radius for global visibility
        coverage=1,
    )

    # ─── View State (pitch 60° for dramatic 3D) ─────────────────────
    view_state = pdk.ViewState(
        longitude=10,
        latitude=15,
        zoom=1.2,
        min_zoom=1,
        max_zoom=10,
        pitch=60,
        bearing=20,
    )

    # ─── Deck ───────────────────────────────────────────────────────
    api_keys = {"mapbox": mapbox_token} if mapbox_token else None

    deck = pdk.Deck(
        layers=[column_layer],
        initial_view_state=view_state,
        map_style="dark" if mapbox_token else "light",
        api_keys=api_keys,
        tooltip={
            "html": """
                <b>{country_name}</b><br/>
                Records: {total_count:,}<br/>
                Internal: {internal_count:,} ({internal_percentage:.1f}%)<br/>
                External: {external_count:,} ({external_percentage:.1f}%)<br/>
                UN Region: {un_region_name}<br/>
                Income: {wb_income_group}
            """,
            "style": {
                "backgroundColor": "rgba(0, 0, 0, 0.8)",
                "color": "white",
                "fontFamily": '"Helvetica Neue", Arial',
                "fontSize": "13px",
            },
        },
    )

    # ─── Export HTML ────────────────────────────────────────────────
    mode_suffix = "internal" if mode == "internal" else "total"
    out_path = os.path.join(OUTPUT_DIR, f"gbif_hexbin_{mode_suffix}_{suffix}.html")
    html_string = deck.to_html(as_string=True)
    # Inject title into the HTML
    html_string = html_string.replace(
        "<title>kepler.gl</title>" if "<title>kepler.gl</title>" in html_string else "<head>",
        f"<title>{title}</title>" if "<title>kepler.gl</title>" in html_string else f"<head><title>{title}</title>",
    )
    with open(out_path, "w") as f:
        f.write(html_string)
    print(f"  Saved: {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Render GBIF source hexbin map")
    parser.add_argument(
        "--mode",
        choices=["total", "internal", "both"],
        default="both",
        help="Which map to generate: total count, internal share, or both",
    )
    parser.add_argument(
        "--radius",
        type=int,
        default=50000,
        help="Hexagon radius in metres (default 50000 = 50km)",
    )
    args = parser.parse_args()

    print("Loading country centroids from Natural Earth...")
    centroids = extract_centroids(NE_GEOJSON)
    print(f"  Found {len(centroids)} country centroids")

    print("Merging with source data (No Aves)...")
    df = merge_source_with_centroids(SOURCE_CSV, centroids)
    print(f"  {len(df)} countries ready for visualisation")

    token = load_mapbox_token()

    if args.mode in ("total", "both"):
        print("\nRendering: Total Record Density (No Aves)...")
        render_hexbin_map(df, mode="total", mapbox_token=token, suffix="no_aves")

    if args.mode in ("internal", "both"):
        print("\nRendering: Internal Percentage (Self-Published, No Aves)...")
        render_hexbin_map(df, mode="internal", mapbox_token=token, suffix="no_aves")

    # Also merge all-taxa data for completeness
    print("\nMerging with source data (All taxa)...")
    df_all = merge_source_with_centroids(SOURCE_ALL_CSV, centroids)
    print(f"  {len(df_all)} countries ready for visualisation")

    if args.mode in ("total", "both"):
        print("\nRendering: Total Record Density (All taxa)...")
        render_hexbin_map(df_all, mode="total", mapbox_token=token, suffix="all_taxa")

    if args.mode in ("internal", "both"):
        print("\nRendering: Internal Percentage (Self-Published, All taxa)...")
        render_hexbin_map(df_all, mode="internal", mapbox_token=token, suffix="all_taxa")

    print("\nDone! Open the HTML files in a browser to view the maps.")


if __name__ == "__main__":
    main()
