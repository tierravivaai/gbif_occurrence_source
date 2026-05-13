import duckdb
import pandas as pd
import os

OCC_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*"
PROCESSED_DIR = "data/processed"

def run_validation():
    con = duckdb.connect()
    
    print("Fetching actual occurrence counts from source...")
    # 1. Total records
    total_source = con.execute(f"SELECT count(*) FROM read_parquet('{OCC_PATH}')").fetchone()[0]
    # 2. Total records excluding Aves
    total_source_no_aves = con.execute(f"SELECT count(*) FROM read_parquet('{OCC_PATH}') WHERE class != 'Aves' OR class IS NULL").fetchone()[0]
    
    print("Reading counts from processed reports...")
    # Report A
    df_country = pd.read_csv(f"{PROCESSED_DIR}/source_by_country.csv")
    sum_country = df_country['total_count'].sum()
    
    # Report B
    df_kingdom = pd.read_csv(f"{PROCESSED_DIR}/source_by_country_kingdom.csv")
    sum_kingdom = df_kingdom['total_count'].sum()
    
    # Report C (No Aves)
    df_no_aves = pd.read_csv(f"{PROCESSED_DIR}/source_by_country_kingdom_no_aves.csv")
    sum_no_aves = df_no_aves['total_count'].sum()
    
    # CBD Summary (All Taxa)
    df_cbd = pd.read_csv(f"{PROCESSED_DIR}/cbd_parties_all_taxa_un_region_summary.csv")
    sum_cbd = df_cbd['total_count'].sum()

    # Create Validation Report
    with open("VALIDATION_REPORT.md", "w") as f:
        f.write("# Validation Report: GBIF Source Analysis (2026)\n\n")
        f.write("This report compares the raw counts from the source dataset against the aggregated counts in the processed output files.\n\n")
        
        f.write("## 1. Primary Analysis Totals\n")
        f.write("| File | Calculated Total | Source Total | Difference |\n")
        f.write("|------|------------------|--------------|------------|\n")
        f.write(f"| `source_by_country.csv` | {sum_country:,} | {total_source:,} | {sum_country - total_source:,} |\n")
        f.write(f"| `source_by_country_kingdom.csv` | {sum_kingdom:,} | {total_source:,} | {sum_kingdom - total_source:,} |\n")
        
        f.write("\n## 2. Taxonomic Filtering (Excluding Aves)\n")
        f.write("| File | Calculated Total | Source Total (No Aves) | Difference |\n")
        f.write("|------|------------------|------------------------|------------|\n")
        f.write(f"| `source_by_country_kingdom_no_aves.csv` | {sum_no_aves:,} | {total_source_no_aves:,} | {sum_no_aves - total_source_no_aves:,} |\n")
        
        f.write("\n## 3. CBD Party Summary (Subset)\n")
        f.write("This summary only includes records where `is_cbd_party` is TRUE.\n\n")
        f.write(f"- **Total Records in CBD Party Summary:** {sum_cbd:,}\n")
        f.write(f"- **Percentage of Total Records from CBD Parties:** {round(100.0 * sum_cbd / total_source, 2)}%\n")

    print("Validation report generated: VALIDATION_REPORT.md")

if __name__ == "__main__":
    run_validation()
