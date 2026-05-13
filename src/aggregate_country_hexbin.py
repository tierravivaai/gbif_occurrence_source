"""
Aggregate occurrence coordinates for a single country for 3D hexbin visualisation.

Queries only one country's records from the 3.7B occurrence parquet, rounds
coordinates, groups by source_type, and outputs a lightweight CSV/parquet
ready for pydeck HexagonLayer or kepler.gl.

Much faster than the full global scan (~1-3 min per country vs 10+ min global).

Usage:
    python src/aggregate_country_hexbin.py --country BR
    python src/aggregate_country_hexbin.py --country BR --precision 2
    python src/aggregate_country_hexbin.py --country GB  # United Kingdom
    python src/aggregate_country_hexbin.py --country AU  # Australia
    python src/aggregate_country_hexbin.py --country ALL  # global (slower)
"""

import argparse
import os
import sys
import time

import duckdb
import pandas as pd

OCC_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*"
REGISTRY_PATH = "data/gbif_registry_lookup.parquet"
COUNTRY_CODE_PATH = "data-raw/countrycode.csv"
OUTPUT_DIR = "data/processed"

CORE_BASIS_OF_RECORD = (
    "LIVING_SPECIMEN",
    "OBSERVATION",
    "HUMAN_OBSERVATION",
    "MACHINE_OBSERVATION",
    "OCCURRENCE",
    "MATERIAL_SAMPLE",
)

BASIS_SQL = ",".join(f"'{b}'" for b in CORE_BASIS_OF_RECORD)


def aggregate_country(country_code: str, precision: int = 1) -> None:
    """Aggregate coordinates for a single country (or ALL) into hexbin-ready data."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect()

    print("Loading country metadata...")
    con.execute(
        f"CREATE OR REPLACE TABLE country_metadata AS "
        f"SELECT * FROM read_csv('{COUNTRY_CODE_PATH}', ALL_VARCHAR=TRUE)"
    )

    where_country = ""
    country_label = "all"
    if country_code != "ALL":
        where_country = f"AND occ.countrycode = '{country_code}'"
        # Look up country name
        try:
            name = con.execute(
                f"SELECT country_name FROM country_metadata WHERE iso2c = '{country_code}' LIMIT 1"
            ).fetchone()
            country_label = country_code
            if name:
                country_label = f"{name[0]} ({country_code})"
        except Exception:
            country_label = country_code
        print(f"Country: {country_label}")
    else:
        print("Country: ALL (global)")

    # Build the classification query for this country only
    # This is much faster than building a global view because DuckDB can
    # filter the parquet files early (partition pruning)
    aves_filter = "AND (occ.class != 'Aves' OR occ.class IS NULL)"

    print("Querying and classifying occurrences (No Aves)...")
    start = time.time()

    query = f"""
        SELECT
            ROUND(occ.decimallatitude::DOUBLE, {precision}) AS lat,
            ROUND(occ.decimallongitude::DOUBLE, {precision}) AS lon,
            CASE
                WHEN reg.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN occ.countrycode = reg.resolved_country THEN 'INTERNAL'
                WHEN occ_region.un_region_name = pub_region.un_region_name THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END as source_type,
            COUNT(*) AS record_count
        FROM read_parquet('{OCC_PATH}') occ
        LEFT JOIN (
            SELECT original_key, resolved_country
            FROM read_parquet('{REGISTRY_PATH}')
            WHERE type = 'organization'
        ) reg ON occ.publishingorgkey = reg.original_key
        LEFT JOIN country_metadata occ_region ON occ.countrycode = occ_region.iso2c
        LEFT JOIN country_metadata pub_region ON reg.resolved_country = pub_region.iso2c
        WHERE occ.decimallatitude IS NOT NULL
          AND occ.decimallatitude BETWEEN -90 AND 90
          AND occ.decimallongitude IS NOT NULL
          AND occ.taxonrank = 'SPECIES'
          AND occ.occurrencestatus = 'PRESENT'
          AND occ.basisofrecord IN ({BASIS_SQL})
          {where_country}
          {aves_filter}
        GROUP BY 1, 2, 3
        ORDER BY record_count DESC
    """

    df = con.sql(query).df()
    elapsed = time.time() - start
    print(f"  -> {len(df):,} hexbin cells in {elapsed:.1f}s")
    print(f"  -> Total records covered: {df['record_count'].sum():,}")

    # Save
    suffix = f"{country_code.lower()}_no_aves_p{precision}"
    out_parquet = f"{OUTPUT_DIR}/hexbin_coords_{suffix}.parquet"
    out_csv = f"{OUTPUT_DIR}/hexbin_coords_{suffix}.csv"
    df.to_parquet(out_parquet)
    df.to_csv(out_csv, index=False)
    print(f"  Saved: {out_parquet}")
    print(f"  Saved: {out_csv}")

    # Also show source_type breakdown
    if len(df) > 0:
        summary = df.groupby("source_type")["record_count"].sum().sort_values(ascending=False)
        print("\n  Source type breakdown:")
        for st, cnt in summary.items():
            pct = 100.0 * cnt / df["record_count"].sum()
            print(f"    {st:12s}: {cnt:>15,} ({pct:.1f}%)")

    con.close()
    return out_csv


def main():
    parser = argparse.ArgumentParser(description="Aggregate country coordinates for hexbin visualisation")
    parser.add_argument(
        "--country",
        type=str,
        default="BR",
        help="ISO 2-letter country code (e.g. BR, GB, AU) or 'ALL' for global",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=1,
        help="Decimal places for rounding lat/lon (1=~11km, 2=~1.1km)",
    )
    args = parser.parse_args()

    try:
        aggregate_country(args.country.upper(), args.precision)
    except Exception as e:
        import traceback
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
