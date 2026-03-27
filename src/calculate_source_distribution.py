import duckdb
import pandas as pd
import os

OCC_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*"
REGISTRY_PATH = "data/gbif_registry_lookup.parquet"
OUTPUT_DIR = "data/processed"

COUNTRY_CODE_PATH = "data-raw/countrycode.csv"

def run_analysis():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    con = duckdb.connect()
    
    # Load country codes (handling NA for Namibia)
    print("Loading country metadata...")
    con.execute(f"CREATE OR REPLACE TABLE country_metadata AS SELECT * FROM read_csv('{COUNTRY_CODE_PATH}', ALL_VARCHAR=TRUE)")
    
    # 1. Base classification table (In-Memory)
    print("Classifying occurrences (Internal vs External)...")
    con.execute(f"""
        CREATE OR REPLACE VIEW occurrence_classification AS
        SELECT 
            occ.countrycode,
            occ.kingdom,
            occ.class,
            CASE 
                WHEN reg.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN occ.countrycode = reg.resolved_country THEN 'INTERNAL'
                ELSE 'EXTERNAL'
            END as source_type
        FROM read_parquet('{OCC_PATH}') occ
        LEFT JOIN (
            SELECT original_key, resolved_country 
            FROM read_parquet('{REGISTRY_PATH}') 
            WHERE type = 'organization'
        ) reg ON occ.publishingorgkey = reg.original_key
    """)
    
    # 2. Report A: By Country
    print("Generating Report A: Source by Country (Enriched)...")
    df_country = con.sql("""
        SELECT 
            m.*,
            c.internal_count,
            c.external_count,
            c.unknown_count,
            c.total_count,
            c.internal_percentage,
            c.external_percentage
        FROM (
            SELECT 
                countrycode,
                count(*) filter (where source_type = 'INTERNAL') as internal_count,
                count(*) filter (where source_type = 'EXTERNAL') as external_count,
                count(*) filter (where source_type = 'UNKNOWN') as unknown_count,
                count(*) as total_count,
                round(100.0 * internal_count / total_count, 2) as internal_percentage,
                round(100.0 * external_count / total_count, 2) as external_percentage
            FROM occurrence_classification
            GROUP BY 1
        ) c
        LEFT JOIN country_metadata m ON c.countrycode = m.iso2c
        ORDER BY c.total_count DESC
    """).df()
    df_country.to_parquet(f"{OUTPUT_DIR}/source_by_country.parquet")
    df_country.to_csv(f"{OUTPUT_DIR}/source_by_country.csv", index=False)

    # 2.1 Report A_NO_AVES: By Country (EXCLUDING AVES)
    print("Generating Report A_NO_AVES: Source by Country (EXCLUDING AVES) (Enriched)...")
    df_country_no_aves = con.sql("""
        SELECT 
            m.*,
            c.internal_count,
            c.external_count,
            c.unknown_count,
            c.total_count,
            c.internal_percentage,
            c.external_percentage
        FROM (
            SELECT 
                countrycode,
                count(*) filter (where source_type = 'INTERNAL') as internal_count,
                count(*) filter (where source_type = 'EXTERNAL') as external_count,
                count(*) filter (where source_type = 'UNKNOWN') as unknown_count,
                count(*) as total_count,
                round(100.0 * internal_count / total_count, 2) as internal_percentage,
                round(100.0 * external_count / total_count, 2) as external_percentage
            FROM occurrence_classification
            WHERE class != 'Aves' OR class IS NULL
            GROUP BY 1
        ) c
        LEFT JOIN country_metadata m ON c.countrycode = m.iso2c
        ORDER BY c.total_count DESC
    """).df()
    df_country_no_aves.to_parquet(f"{OUTPUT_DIR}/source_by_country_no_aves.parquet")
    df_country_no_aves.to_csv(f"{OUTPUT_DIR}/source_by_country_no_aves.csv", index=False)
    
    # 3. Report B: By Country and Kingdom
    print("Generating Report B: Source by Country and Kingdom (Enriched)...")
    df_kingdom = con.sql("""
        SELECT 
            m.*,
            c.kingdom,
            c.internal_count,
            c.external_count,
            c.unknown_count,
            c.total_count,
            c.internal_percentage,
            c.external_percentage
        FROM (
            SELECT 
                countrycode,
                kingdom,
                count(*) filter (where source_type = 'INTERNAL') as internal_count,
                count(*) filter (where source_type = 'EXTERNAL') as external_count,
                count(*) filter (where source_type = 'UNKNOWN') as unknown_count,
                count(*) as total_count,
                round(100.0 * internal_count / total_count, 2) as internal_percentage,
                round(100.0 * external_count / total_count, 2) as external_percentage
            FROM occurrence_classification
            GROUP BY 1, 2
        ) c
        LEFT JOIN country_metadata m ON c.countrycode = m.iso2c
        ORDER BY c.total_count DESC
    """).df()
    df_kingdom.to_parquet(f"{OUTPUT_DIR}/source_by_country_kingdom.parquet")
    df_kingdom.to_csv(f"{OUTPUT_DIR}/source_by_country_kingdom.csv", index=False)

    # 4. Report C: By Country and Kingdom (EXCLUDING AVES) (Enriched)
    print("Generating Report C: Source by Country and Kingdom (EXCLUDING AVES) (Enriched)...")
    df_kingdom_no_aves = con.sql("""
        SELECT 
            m.*,
            c.kingdom,
            c.internal_count,
            c.external_count,
            c.unknown_count,
            c.total_count,
            c.internal_percentage,
            c.external_percentage
        FROM (
            SELECT 
                countrycode,
                kingdom,
                count(*) filter (where source_type = 'INTERNAL') as internal_count,
                count(*) filter (where source_type = 'EXTERNAL') as external_count,
                count(*) filter (where source_type = 'UNKNOWN') as unknown_count,
                count(*) as total_count,
                round(100.0 * internal_count / total_count, 2) as internal_percentage,
                round(100.0 * external_count / total_count, 2) as external_percentage
            FROM occurrence_classification
            WHERE class != 'Aves' OR class IS NULL
            GROUP BY 1, 2
        ) c
        LEFT JOIN country_metadata m ON c.countrycode = m.iso2c
        ORDER BY c.total_count DESC
    """).df()
    df_kingdom_no_aves.to_parquet(f"{OUTPUT_DIR}/source_by_country_kingdom_no_aves.parquet")
    df_kingdom_no_aves.to_csv(f"{OUTPUT_DIR}/source_by_country_kingdom_no_aves.csv", index=False)
    df_kingdom_no_aves.to_parquet(f"{OUTPUT_DIR}/source_by_country_kingdom_no_aves.parquet")
    df_kingdom_no_aves.to_csv(f"{OUTPUT_DIR}/source_by_country_kingdom_no_aves.csv", index=False)

    print(f"Analysis complete. Results saved to {OUTPUT_DIR}/")

if __name__ == "__main__":
    try:
        run_analysis()
    except Exception as e:
        import traceback
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        exit(1)
