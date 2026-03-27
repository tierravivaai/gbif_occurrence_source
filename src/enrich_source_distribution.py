import duckdb
import pandas as pd
import os

PROCESSED_DIR = "data/processed"
COUNTRY_CODE_PATH = "data-raw/countrycode.csv"
COUNTRY_OVERLAY_PATH = "data-raw/country_overlay.csv"

def enrich_files():
    con = duckdb.connect()
    
    # Load country metadata (handling NA for Namibia)
    print("Loading country metadata...")
    con.execute(f"CREATE OR REPLACE TABLE country_metadata AS SELECT * FROM read_csv('{COUNTRY_CODE_PATH}', ALL_VARCHAR=TRUE)")
    
    # Load overlay metadata (CBD and WB Income)
    print("Loading country overlay...")
    con.execute(f"CREATE OR REPLACE TABLE country_overlay AS SELECT iso3c, is_cbd_party, wb_income_group FROM read_csv('{COUNTRY_OVERLAY_PATH}', ALL_VARCHAR=TRUE)")
    
    files_to_enrich = [
        "source_by_country.parquet",
        "source_by_country_kingdom.parquet",
        "source_by_country_kingdom_no_aves.parquet"
    ]
    
    for filename in files_to_enrich:
        base_name = filename.replace(".parquet", "")
        print(f"Enriching {filename}...")
        
        con.execute(f"CREATE OR REPLACE TABLE current_data AS SELECT * FROM read_parquet('{PROCESSED_DIR}/{filename}')")
        
        # Determine join column
        # Note: Previous step already added iso3c from countrycode.csv
        enriched_df = con.sql("""
            SELECT 
                c.*,
                o.is_cbd_party,
                o.wb_income_group
            FROM current_data c
            LEFT JOIN country_overlay o ON c.iso3c = o.iso3c
            ORDER BY c.total_count DESC
        """).df()
        
        # Save enriched results
        enriched_df.to_parquet(f"{PROCESSED_DIR}/{base_name}.parquet")
        enriched_df.to_csv(f"{PROCESSED_DIR}/{base_name}.csv", index=False)
        print(f"  Saved enriched {base_name}")

if __name__ == "__main__":
    enrich_files()
