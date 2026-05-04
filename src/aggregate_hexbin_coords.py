"""
Aggregate GBIF occurrence coordinates for 3D hexbin visualisation.

Pre-computes rounded lat/lon counts by source_type, outputs cached parquet
files for fast loading by pydeck/kepler.gl. Assumes the GBIF occurrence
parquet and registry lookup are already available.

Runs take ~5-10 min against the full 3.7B record dataset.

Usage:
    python src/aggregate_hexbin_coords.py
    python src/aggregate_hexbin_coords.py --precision 2  # finer grid (~1.1km)
    python src/aggregate_hexbin_coords.py --max-rows 500000  # limit output size
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


def create_occurrence_classification(con: duckdb.DuckDBPyConnection) -> None:
    """Build the occurrence_classification view with source_type and coordinates."""
    print("Loading country metadata...")
    con.execute(
        f"CREATE OR REPLACE TABLE country_metadata AS "
        f"SELECT * FROM read_csv('{COUNTRY_CODE_PATH}', ALL_VARCHAR=TRUE)"
    )

    print("Classifying occurrences with coordinates (Internal / Regional / External / Unknown)...")
    con.execute(f"""
        CREATE OR REPLACE VIEW occurrence_classification AS
        SELECT
            occ.countrycode,
            occ.kingdom,
            occ.class,
            occ.decimallatitude,
            occ.decimallongitude,
            occ.publishingorgkey,
            CASE
                WHEN reg.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN occ.countrycode = reg.resolved_country THEN 'INTERNAL'
                WHEN occ_region.un_region_name = pub_region.un_region_name THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END as source_type
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
          AND occ.basisofrecord IN ({','.join(f"'{b}'" for b in CORE_BASIS_OF_RECORD)})
    """)


def aggregate_hexbin(
    con: duckdb.DuckDBPyConnection,
    precision: int = 1,
    exclude_aves: bool = True,
    include_kingdom: bool = False,
    max_rows: int | None = None,
) -> pd.DataFrame:
    """Aggregate occurrence coordinates into hexbin-ready rows."""
    where_clauses = []
    if exclude_aves:
        where_clauses.append("(class != 'Aves' OR class IS NULL)")

    where_sql = f" AND {chr(10)}    ".join(where_clauses) if where_clauses else ""

    select_kingdom = ", kingdom" if include_kingdom else ""
    group_kingdom = ", kingdom" if include_kingdom else ""

    query = f"""
        SELECT
            ROUND(decimallatitude::DOUBLE, {precision}) AS lat,
            ROUND(decimallongitude::DOUBLE, {precision}) AS lon,
            source_type{select_kingdom},
            COUNT(*) AS record_count
        FROM occurrence_classification
        {f'WHERE {where_sql}' if where_sql else ''}
        GROUP BY 1, 2, 3{group_kingdom}
        ORDER BY record_count DESC
    """

    if max_rows:
        query += f"\n        LIMIT {max_rows}"

    print(f"Running aggregation (precision={precision}, exclude_aves={exclude_aves}, kingdom={include_kingdom})...")
    start = time.time()
    df = con.sql(query).df()
    elapsed = time.time() - start
    print(f"  -> {len(df):,} rows in {elapsed:.1f}s")
    return df


def run_aggregation(precision: int = 1, max_rows: int | None = None) -> None:
    """Full pipeline: create view, aggregate, save parquet files."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    con = duckdb.connect()
    create_occurrence_classification(con)

    # 1. No Aves (default view)
    print("\n--- Aggregating: No Aves ---")
    df_no_aves = aggregate_hexbin(
        con, precision=precision, exclude_aves=True, include_kingdom=False, max_rows=max_rows
    )
    out_no_aves = f"{OUTPUT_DIR}/hexbin_coords_no_aves.parquet"
    df_no_aves.to_parquet(out_no_aves)
    df_no_aves.to_csv(out_no_aves.replace(".parquet", ".csv"), index=False)
    print(f"  Saved: {out_no_aves} ({len(df_no_aves):,} rows)")

    # 2. No Aves with kingdom (for future filtering)
    print("\n--- Aggregating: No Aves + Kingdom ---")
    df_no_aves_kingdom = aggregate_hexbin(
        con, precision=precision, exclude_aves=True, include_kingdom=True, max_rows=max_rows
    )
    out_kingdom = f"{OUTPUT_DIR}/hexbin_coords_no_aves_kingdom.parquet"
    df_no_aves_kingdom.to_parquet(out_kingdom)
    df_no_aves_kingdom.to_csv(out_kingdom.replace(".parquet", ".csv"), index=False)
    print(f"  Saved: {out_kingdom} ({len(df_no_aves_kingdom):,} rows)")

    # 3. All taxa including Aves
    print("\n--- Aggregating: All taxa (incl. Aves) ---")
    df_all = aggregate_hexbin(
        con, precision=precision, exclude_aves=False, include_kingdom=False, max_rows=max_rows
    )
    out_all = f"{OUTPUT_DIR}/hexbin_coords_all.parquet"
    df_all.to_parquet(out_all)
    df_all.to_csv(out_all.replace(".parquet", ".csv"), index=False)
    print(f"  Saved: {out_all} ({len(df_all):,} rows)")

    con.close()
    print("\nAggregation complete.")


def main():
    parser = argparse.ArgumentParser(description="Aggregate GBIF coordinates for hexbin visualisation")
    parser.add_argument(
        "--precision",
        type=int,
        default=1,
        help="Decimal places for rounding lat/lon (1=~11km, 2=~1.1km)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Limit output rows per file (for testing)",
    )
    args = parser.parse_args()

    try:
        run_aggregation(precision=args.precision, max_rows=args.max_rows)
    except Exception as e:
        import traceback

        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
