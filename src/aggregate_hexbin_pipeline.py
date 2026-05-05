"""
Rebuild all hexbin coordinate data from scratch for the GBIF visualisation pipeline.

Single entry-point data preparation script. Queries the raw GBIF occurrence parquet,
classifies records as INTERNAL/REGIONAL/EXTERNAL/UNKNOWN, rounds coordinates to the
requested precision, and enriches with WB income group and UN region via
point-in-polygon lookup against Natural Earth 110m country boundaries.

Classification logic:
  INTERNAL  -- occurrence country == publisher country
  REGIONAL  -- same UN region, different country
  EXTERNAL  -- different UN region
  UNKNOWN   -- publisher country not in registry

Record filters applied:
  taxonrank        = 'SPECIES'
  occurrencestatus = 'PRESENT'
  basisofrecord    IN (OBSERVATION, HUMAN_OBSERVATION, MACHINE_OBSERVATION,
                       LIVING_SPECIMEN, OCCURRENCE, MATERIAL_SAMPLE)
  class            != 'Aves'  (birds excluded -- 2B+ records would skew results)
  lat/lon valid and non-null

Coordinate precision:
  0 -> round to 1 degree  (~111 km cells) -- lightweight global view
  1 -> round to 0.1 degree (~11 km cells) -- high-resolution global/country view

Outputs (data/processed/):
  hexbin_all_no_aves_p{N}_enriched.parquet/.csv   Global grids
  hexbin_{cc}_no_aves_p1_enriched.parquet/.csv    Per-country grids

Usage:
    python src/aggregate_hexbin_pipeline.py
    python src/aggregate_hexbin_pipeline.py --skip-global
    python src/aggregate_hexbin_pipeline.py --skip-countries
    python src/aggregate_hexbin_pipeline.py --countries BR,US,ZA
    python src/aggregate_hexbin_pipeline.py --precision 0
"""

import argparse
import os
import sys
import time

import duckdb
import pandas as pd

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OCC_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*"
REGISTRY_PATH = os.path.join(PROJECT_DIR, "data", "gbif_registry_lookup.parquet")
COUNTRY_CODE_PATH = os.path.join(PROJECT_DIR, "data-raw", "countrycode.csv")
GEOJSON_PATH = os.path.join(PROJECT_DIR, "data", "processed", "ne_110m_admin_0_countries.geojson")
SOURCE_BY_COUNTRY = os.path.join(PROJECT_DIR, "data", "processed", "source_by_country.csv")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "data", "processed")

CORE_BASIS_OF_RECORD = (
    "LIVING_SPECIMEN",
    "OBSERVATION",
    "HUMAN_OBSERVATION",
    "MACHINE_OBSERVATION",
    "OCCURRENCE",
    "MATERIAL_SAMPLE",
)
BASIS_SQL = ",".join(f"'{b}'" for b in CORE_BASIS_OF_RECORD)

DEFAULT_COUNTRIES = [
    "BR", "US", "ZA", "IN", "CO", "MX", "AU", "GB", "FR", "DE", "ID", "JP", "CA",
]


# ── Query builders ─────────────────────────────────────────────────────────────

def _build_classification_query(precision: int, country_code: str = "") -> str:
    where_country = f"AND occ.countrycode = '{country_code}'" if country_code else ""
    return f"""
        SELECT
            ROUND(occ.decimallatitude::DOUBLE,  {precision}) AS lat,
            ROUND(occ.decimallongitude::DOUBLE, {precision}) AS lon,
            CASE
                WHEN reg.resolved_country IS NULL                              THEN 'UNKNOWN'
                WHEN occ.countrycode = reg.resolved_country                    THEN 'INTERNAL'
                WHEN occ_r.un_region_name = pub_r.un_region_name               THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END AS source_type,
            COUNT(*) AS record_count
        FROM read_parquet('{OCC_PATH}') occ
        LEFT JOIN (
            SELECT original_key, resolved_country
            FROM read_parquet('{REGISTRY_PATH}')
            WHERE type = 'organization'
        ) reg ON occ.publishingorgkey = reg.original_key
        LEFT JOIN country_metadata occ_r ON occ.countrycode       = occ_r.iso2c
        LEFT JOIN country_metadata pub_r ON reg.resolved_country   = pub_r.iso2c
        WHERE occ.decimallatitude  IS NOT NULL
          AND occ.decimallatitude  BETWEEN -90 AND 90
          AND occ.decimallongitude IS NOT NULL
          AND occ.taxonrank        = 'SPECIES'
          AND occ.occurrencestatus = 'PRESENT'
          AND occ.basisofrecord    IN ({BASIS_SQL})
          AND (occ.class != 'Aves' OR occ.class IS NULL)
          {where_country}
        GROUP BY 1, 2, 3
        ORDER BY record_count DESC
    """


# ── Enrichment ─────────────────────────────────────────────────────────────────

def enrich_with_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach country, WB income group, and UN region to each hexbin cell via
    point-in-polygon join (DuckDB Spatial) against Natural Earth 110m boundaries.

    Cells over open ocean or polar regions get countrycode='XX',
    wb_income_group='Not classified', un_region_name='Unknown'.
    """
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute("CREATE OR REPLACE TABLE hexbin AS SELECT * FROM df")
        con.execute(f"""
            CREATE OR REPLACE TABLE country_polys AS
            SELECT ISO_A2_EH AS iso2c, NAME AS country_name, geom
            FROM ST_Read('{GEOJSON_PATH}')
        """)
        con.execute(f"""
            CREATE OR REPLACE TABLE country_meta AS
            SELECT iso2c, un_region_name, un_sub_region_name, wb_income_group
            FROM read_csv('{SOURCE_BY_COUNTRY}')
        """)

        print("  Running point-in-polygon join (1-3 min for global data)...")
        t0 = time.time()
        result = con.execute("""
            SELECT
                h.lat,
                h.lon,
                h.source_type,
                h.record_count,
                COALESCE(c.iso2c,             'XX')             AS countrycode,
                COALESCE(c.country_name,      'Unknown')        AS country_name,
                COALESCE(m.wb_income_group,   'Not classified') AS wb_income_group,
                COALESCE(m.un_region_name,    'Unknown')        AS un_region_name,
                COALESCE(m.un_sub_region_name,'Unknown')        AS un_sub_region_name
            FROM hexbin h
            LEFT JOIN country_polys c ON ST_Contains(c.geom, ST_Point(h.lon, h.lat))
            LEFT JOIN country_meta  m ON c.iso2c = m.iso2c
            ORDER BY h.record_count DESC
        """).df()
        print(f"  Enrichment complete in {time.time() - t0:.0f}s  ({len(result):,} rows)")
    finally:
        con.close()
    return result


# ── Helpers ────────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, stem: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(os.path.join(OUTPUT_DIR, f"{stem}.parquet"), index=False)
    df.to_csv(os.path.join(OUTPUT_DIR, f"{stem}.csv"), index=False)
    print(f"  -> {stem}  ({len(df):,} rows, {df['record_count'].sum():,} records)")


def _print_summary(df: pd.DataFrame) -> None:
    total = df["record_count"].sum()
    print("  Source type:")
    for st, cnt in df.groupby("source_type")["record_count"].sum().sort_values(ascending=False).items():
        print(f"    {st:12s}: {cnt:>15,}  ({100*cnt/total:.1f}%)")
    if "wb_income_group" in df.columns:
        print("  WB income group:")
        for ig, cnt in df.groupby("wb_income_group")["record_count"].sum().sort_values(ascending=False).items():
            print(f"    {ig:30s}: {cnt:>15,}")


# ── Pipeline stages ────────────────────────────────────────────────────────────

def build_global(precision: int, con: duckdb.DuckDBPyConnection) -> None:
    label = f"all_no_aves_p{precision}"
    km = {0: "~111km", 1: "~11km", 2: "~1km"}.get(precision, "?")
    print(f"\n{'='*60}")
    print(f"GLOBAL hexbin  precision={precision} ({km} cells)")
    print(f"{'='*60}")

    t0 = time.time()
    df_raw = con.execute(_build_classification_query(precision)).df()
    print(f"  Aggregated: {len(df_raw):,} cells in {time.time()-t0:.0f}s")
    _save(df_raw, f"hexbin_{label}")

    df_enriched = enrich_with_metadata(df_raw)
    _print_summary(df_enriched)
    _save(df_enriched, f"hexbin_{label}_enriched")


def build_country(country_code: str, precision: int, con: duckdb.DuckDBPyConnection) -> None:
    label = f"{country_code.lower()}_no_aves_p{precision}"
    print(f"\n--- {country_code} hexbin  precision={precision} ---")

    t0 = time.time()
    df_raw = con.execute(_build_classification_query(precision, country_code=country_code)).df()
    print(f"  Aggregated: {len(df_raw):,} cells in {time.time()-t0:.0f}s")

    if df_raw.empty:
        print(f"  WARNING: no data for {country_code} — skipping")
        return

    _save(df_raw, f"hexbin_{label}")

    df_enriched = enrich_with_metadata(df_raw)
    _print_summary(df_enriched)
    _save(df_enriched, f"hexbin_{label}_enriched")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rebuild all GBIF hexbin data from scratch",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--skip-global",    action="store_true", help="Skip global aggregations")
    parser.add_argument("--skip-countries", action="store_true", help="Skip per-country aggregations")
    parser.add_argument(
        "--countries",
        default=",".join(DEFAULT_COUNTRIES),
        help="Comma-separated ISO2 codes for per-country files",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=None,
        help="Override precision for all builds (0 or 1). Default: global builds both 0 and 1.",
    )
    args = parser.parse_args()

    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]

    for path, name in [
        (REGISTRY_PATH,    "GBIF registry"),
        (COUNTRY_CODE_PATH,"country codes"),
        (GEOJSON_PATH,     "Natural Earth GeoJSON"),
        (SOURCE_BY_COUNTRY,"source_by_country.csv"),
    ]:
        if not os.path.exists(path):
            print(f"ERROR: required file not found: {path}  ({name})")
            sys.exit(1)

    print("Connecting to DuckDB and loading country metadata...")
    con = duckdb.connect()
    con.execute(
        f"CREATE OR REPLACE TABLE country_metadata AS "
        f"SELECT * FROM read_csv('{COUNTRY_CODE_PATH}', ALL_VARCHAR=TRUE)"
    )

    if not args.skip_global:
        if args.precision is not None:
            build_global(args.precision, con)
        else:
            build_global(0, con)
            build_global(1, con)

    if not args.skip_countries:
        p = args.precision if args.precision is not None else 1
        print(f"\nBuilding per-country grids (precision={p}): {', '.join(countries)}")
        for cc in countries:
            try:
                build_country(cc, p, con)
            except Exception as exc:
                print(f"  ERROR building {cc}: {exc}")

    con.close()
    print("\nPipeline complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        import traceback
        print(f"FATAL: {exc}")
        traceback.print_exc()
        sys.exit(1)
