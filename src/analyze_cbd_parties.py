import pandas as pd
import os

PROCESSED_DIR = "data/processed"

def generate_summaries(df, prefix):
    # Grouping columns
    groups = [
        ['un_region_name'],
        ['un_region_name', 'un_intermediate_region_name'],
        ['un_developed_or_developing_countries'],
        ['wb_income_group']
    ]
    
    file_map = {
        "un_region_name": "un_region",
        "un_intermediate_region_name": "un_intermediate_region",
        "un_developed_or_developing_countries": "development_status",
        "wb_income_group": "income_group"
    }
    
    for group in groups:
        # Weighted aggregation
        summary = df.groupby(group).agg({
            'internal_count': 'sum',
            'external_count': 'sum',
            'unknown_count': 'sum',
            'total_count': 'sum'
        }).reset_index()
        
        # Calculate percentages on aggregated totals
        summary['internal_percentage'] = round(100.0 * summary['internal_count'] / summary['total_count'], 2)
        summary['external_percentage'] = round(100.0 * summary['external_count'] / summary['total_count'], 2)
        
        # Determine filename based on grouping
        group_key = "_".join([file_map.get(g, g) for g in group])
        filename_csv = f"{PROCESSED_DIR}/cbd_parties_{prefix}_{group_key}_summary.csv"
        filename_parquet = f"{PROCESSED_DIR}/cbd_parties_{prefix}_{group_key}_summary.parquet"
        summary.to_csv(filename_csv, index=False)
        summary.to_parquet(filename_parquet)
        print(f"  Saved {filename_csv} and {filename_parquet}")

def run_cbd_analysis():
    # 1. Full Analysis (All Taxa)
    print("Analyzing CBD Parties (All Taxa)...")
    df_all = pd.read_parquet(f"{PROCESSED_DIR}/source_by_country.parquet")
    # Ensure is_cbd_party is boolean
    df_all['is_cbd_party'] = df_all['is_cbd_party'].astype(str).str.lower() == 'true'
    cbd_all = df_all[df_all['is_cbd_party']].copy()
    generate_summaries(cbd_all, "all_taxa")
    
    # 2. No Aves Analysis
    print("Analyzing CBD Parties (Excluding Aves)...")
    df_no_aves = pd.read_parquet(f"{PROCESSED_DIR}/source_by_country_kingdom_no_aves.parquet")
    # Aggregate kingdom-level records back to country level for this summary
    df_no_aves_country = df_no_aves.groupby(['iso2c', 'iso3c', 'country_name', 'un_region_name', 'un_intermediate_region_name', 'un_developed_or_developing_countries', 'wb_income_group', 'is_cbd_party']).agg({
        'internal_count': 'sum',
        'external_count': 'sum',
        'unknown_count': 'sum',
        'total_count': 'sum'
    }).reset_index()
    
    df_no_aves_country['is_cbd_party'] = df_no_aves_country['is_cbd_party'].astype(str).str.lower() == 'true'
    cbd_no_aves = df_no_aves_country[df_no_aves_country['is_cbd_party']].copy()
    generate_summaries(cbd_no_aves, "no_aves")

if __name__ == "__main__":
    run_cbd_analysis()
