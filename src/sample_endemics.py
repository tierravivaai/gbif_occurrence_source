import duckdb
import os

DB_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/gbifdump_20260101.db"
TAXA_AU_PATH = "/Volumes/Mybook18/wikidata/data/taxa_australia.csv"
OUTPUT_PATH = "map-data/au_endemics_250k.parquet"

def sample_endemics():
    print(f"Connecting to DuckDB: {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    
    print(f"Counting Australian Endemic records using reference: {TAXA_AU_PATH}")
    count_query = f"""
    SELECT COUNT(*) 
    FROM gbif_cleaned 
    WHERE countrycode = 'AU' 
    AND canonical_lower IN (SELECT DISTINCT LOWER(scientific_name) FROM read_csv_auto('{TAXA_AU_PATH}'))
    """
    count = con.execute(count_query).fetchone()[0]
    print(f"Total Endemic AU records found: {count:,}")
    
    if count > 0:
        print(f"Sampling 250,000 records to {OUTPUT_PATH}...")
        sample_query = f"""
        COPY (
            SELECT species, kingdom, decimallatitude as latitude, decimallongitude as longitude 
            FROM gbif_cleaned 
            WHERE countrycode = 'AU' 
            AND canonical_lower IN (SELECT DISTINCT LOWER(scientific_name) FROM read_csv_auto('{TAXA_AU_PATH}'))
            USING SAMPLE 250000 ROWS
        ) TO '{OUTPUT_PATH}' (FORMAT PARQUET)
        """
        con.execute(sample_query)
        print("Success!")
    else:
        print("No records found to sample.")

if __name__ == "__main__":
    sample_endemics()
