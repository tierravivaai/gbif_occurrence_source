"""
Enrich hexbin coordinate data with country metadata and WB income groups.

Reverse-geocodes each lat/lon cell to a country using the Natural Earth 110m
boundaries, then joins to World Bank income group classification from the
enriched source_by_country table.

Outputs:
  - hexbin_coords_{suffix}_enriched.parquet (with country/wb_income_group columns)
  - hexbin_coords_{suffix}_enriched.csv (same, for kepler.gl CSV import)

Usage:
    python src/enrich_hexbin_with_income.py --country ALL --precision 1
    python src/enrich_hexbin_with_income.py --country BR --precision 1
"""

import argparse
import os
import sys

import duckdb
import pandas as pd
import numpy as np

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "processed")
GEOJSON_PATH = os.path.join(DATA_DIR, "ne_110m_admin_0_countries.geojson")
SOURCE_BY_COUNTRY = os.path.join(DATA_DIR, "source_by_country.csv")


def enrich_hexbin_data(country_code: str, precision: int) -> None:
    """Join hexbin data with country boundaries and WB income groups."""
    suffix = f"{country_code.lower()}_no_aves_p{precision}"
    input_parquet = os.path.join(DATA_DIR, f"hexbin_coords_{suffix}.parquet")
    output_parquet = os.path.join(DATA_DIR, f"hexbin_coords_{suffix}_enriched.parquet")
    output_csv = os.path.join(DATA_DIR, f"hexbin_coords_{suffix}_enriched.csv")

    if not os.path.exists(input_parquet):
        print(f"ERROR: {input_parquet} not found")
        print(f"Run first: python src/aggregate_country_hexbin.py --country {country_code} --precision {precision}")
        sys.exit(1)

    print(f"Loading hexbin data: {input_parquet}")
    df = pd.read_parquet(input_parquet)
    print(f"  {len(df):,} hexbin cells, {df['record_count'].sum():,} total records")

    # ─── Reverse-geocode using DuckDB + GeoJSON ───────────────────────────
    # Use point-in-polygon via DuckDB spatial extension
    print("Reverse-geocoding lat/lon cells to countries...")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Load hexbin data
    con.execute(f"CREATE OR REPLACE TABLE hexbin AS SELECT * FROM read_parquet('{input_parquet}')")

    # Load country boundaries from GeoJSON (DuckDB spatial flattens properties into columns)
    con.execute(f"""
        CREATE OR REPLACE TABLE countries AS
        SELECT
            ISO_A2_EH AS iso2c,
            NAME AS country_name,
            geom
        FROM ST_Read('{GEOJSON_PATH}')
    """)

    # Load WB income groups from enriched source_by_country
    con.execute(f"""
        CREATE OR REPLACE TABLE income AS
        SELECT iso2c, wb_income_group
        FROM read_csv('{SOURCE_BY_COUNTRY}')
    """)

    # Point-in-polygon join: for each hexbin cell, find which country polygon contains it
    print("  Running point-in-polygon join (this may take a minute)...")
    con.execute("""
        CREATE OR REPLACE TABLE enriched AS
        SELECT
            h.lat,
            h.lon,
            h.source_type,
            h.record_count,
            COALESCE(c.iso2c, 'XX') AS countrycode,
            COALESCE(c.country_name, 'Unknown') AS country_name,
            COALESCE(inc.wb_income_group, 'Not classified') AS wb_income_group
        FROM hexbin h
        LEFT JOIN countries c
          ON ST_Contains(c.geom, ST_Point(h.lon, h.lat))
        LEFT JOIN income inc
          ON c.iso2c = inc.iso2c
    """)

    result = con.execute("SELECT * FROM enriched ORDER BY record_count DESC").df()
    con.close()

    # Stats
    print(f"\nEnriched data: {len(result):,} rows")
    matched = (result["countrycode"] != "XX").sum()
    print(f"  Country matched: {matched:,} cells ({100.0 * matched / len(result):.1f}%)")

    print("\nWB income group breakdown:")
    for grp in ["High income", "Upper middle income", "Lower middle income", "Low income", "Not classified"]:
        sub = result[result["wb_income_group"] == grp]
        total = sub["record_count"].sum()
        print(f"  {grp:25s}: {total:>15,} records ({len(sub):>10,} cells)")

    # Save
    result.to_parquet(output_parquet, index=False)
    result.to_csv(output_csv, index=False)
    print(f"\nSaved: {output_parquet}")
    print(f"Saved: {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Enrich hexbin data with country/WB income group")
    parser.add_argument("--country", type=str, default="ALL", help="Country code or ALL")
    parser.add_argument("--precision", type=int, default=1, help="Coordinate precision (must match aggregated data)")
    args = parser.parse_args()
    enrich_hexbin_data(args.country.upper(), args.precision)


if __name__ == "__main__":
    main()
