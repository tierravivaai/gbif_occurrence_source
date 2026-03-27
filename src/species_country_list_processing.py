import duckdb
import os

# Paths
INPUT_PATH = "data/gbif_country_index.parquet"
OUTPUT_PATH = "data/species_country_list.parquet"

def main():
    print("Starting species-country list aggregation with DuckDB...")
    con = duckdb.connect()

    # Ensure output directory exists
    output_dir = os.path.dirname(OUTPUT_PATH)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    query = f"""
    COPY (
        SELECT
            species,
            canonical_lower,
            kingdom,
            string_agg(DISTINCT country_code, ', ') as country_code_list,
            COUNT(DISTINCT country_code) as country_count,
            string_agg(DISTINCT un_region, ', ') as un_region_list,
            string_agg(DISTINCT un_sub_region, ', ') as un_sub_region_list,
            string_agg(DISTINCT un_intermediate_region, ', ') as un_intermediate_region_list,
            string_agg(DISTINCT un_development_status, ', ') as un_development_status_list,
            string_agg(DISTINCT un_ldc, ', ') as un_ldc_list,
            string_agg(DISTINCT un_sids, ', ') as un_sids_list,
            -- Metrics (add the maximum values across all countries/regions for the species)
            SUM(country_count) as total_occurrence_count,
            MAX(country_breadth) as country_breadth,
            MAX(un_region_breadth) as un_region_breadth,
            MAX(significant_breadth) as significant_breadth,
            MAX(likelihood_country) as max_likelihood_country,
            MAX(likelihood_un_region) as max_likelihood_un_region,
            MAX(localisation_score) as max_localisation_score
        FROM read_parquet('{INPUT_PATH}')
        GROUP BY species, canonical_lower, kingdom
    ) TO '{OUTPUT_PATH}' (FORMAT PARQUET)
    """

    try:
        con.execute(query)
        print(f"Success! Output saved to {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error during processing: {e}")

if __name__ == "__main__":
    main()
