"""
Visualise GBIF source distribution data using kepler.gl with interactive layer toggling.

Loads enriched hexbin data (with WB income group) and creates a KeplerGl map with
separate layers for each income group and source type. Users can toggle layers on/off
interactively in the browser.

Datasets loaded:
  1. All sources (hexbin density layer)
  2. Internal/self-published only (hexbin density layer)
  3. Filtered by WB income group (one dataset per group)

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

import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "processed")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output")
ENV_PATH = os.path.expanduser("~/hermes-secure-runner/hermes-data/.env")


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

    # Split into datasets by source type and income group
    datasets = {}

    # All sources - full dataset
    datasets["All Sources"] = df.copy()

    # Internal only
    df_internal = df[df["source_type"] == "INTERNAL"].copy()
    if len(df_internal) > 0:
        datasets["Internal (Self-Published)"] = df_internal

    # WB income groups
    income_groups = ["High income", "Upper middle income", "Lower middle income", "Low income"]
    for ig in income_groups:
        df_ig = df[df["wb_income_group"] == ig].copy()
        if len(df_ig) > 0:
            datasets[f"Internal - {ig}"] = df_ig[df_ig["source_type"] == "INTERNAL"].copy()
            if len(datasets[f"Internal - {ig}"]) > 0:
                print(f"  {ig}: {datasets[f'Internal - {ig}']['record_count'].sum():,} internal records")
            else:
                del datasets[f"Internal - {ig}"]

    # Load Mapbox token
    mapbox_token = load_mapbox_token()

    # Define config for the map
    config = {
        "mapState": {
            "latitude": 15,
            "longitude": 10,
            "zoom": 1.5,
            "pitch": 40,
            "bearing": -20,
        },
        "mapStyle": {
            "styleType": "dark" if mapbox_token else "light",
        },
        "visState": {
            "filters": [],
            "layerBlending": "additive",
        },
    }

    # Adjust viewport for country-specific views
    if country_code == "BR":
        config["mapState"] = {"latitude": -14.2, "longitude": -51.9, "zoom": 3.5, "pitch": 40, "bearing": -20}
    elif country_code == "GB":
        config["mapState"] = {"latitude": 53.5, "longitude": -2, "zoom": 5, "pitch": 40, "bearing": -20}
    elif country_code == "AU":
        config["mapState"] = {"latitude": -25.3, "longitude": 133.8, "zoom": 3.5, "pitch": 40, "bearing": -20}

    print(f"\nCreating kepler.gl map with {len(datasets)} datasets...")
    map_1 = KeplerGl(height=800, data=datasets, config=config)

    # Save to HTML
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_html = os.path.join(OUTPUT_DIR, f"gbif_kepler_{suffix}.html")
    map_1.save_to_html(file_name=output_html)
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
