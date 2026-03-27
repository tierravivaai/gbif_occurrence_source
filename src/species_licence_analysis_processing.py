import duckdb
import os

# Paths
OCCURRENCE_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump/gbifdump/gbif-dump-2026-01-01/occurrence.parquet/[0-9][0-9][0-9][0-9][0-9][0-9]"
COUNTRY_CODE_PATH = "data-raw/country_code.parquet"
OUTPUT_PATH = "data/species_country_region_occurrence_cc0-ccby.parquet"

def main():
    print("Starting processing for CC0 and CC-BY only...")
    con = duckdb.connect()
    
    # Using the same logic as the main script but adding license filter
    query = f"""
    COPY (
        WITH base AS (
            SELECT 
                species, 
                kingdom, 
                countrycode, 
                basisofrecord,
                COUNT(*) as country_count
            FROM read_parquet('{OCCURRENCE_PATH}')
            WHERE UPPER(taxonrank) = 'SPECIES' 
              AND (class IS NULL OR UPPER(class) != 'AVES')
              AND species IS NOT NULL
              AND UPPER(basisofrecord) IN ('LIVING_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION', 'OCCURRENCE')
              AND license IN ('CC_BY_4_0', 'CC0_1_0')
            GROUP BY species, kingdom, countrycode, basisofrecord
        ),
        agg_base AS (
            SELECT 
                species, 
                kingdom, 
                countrycode,
                SUM(country_count) as country_count
            FROM base
            GROUP BY species, kingdom, countrycode
        )
        SELECT 
            b.species, 
            b.kingdom, 
            b.countrycode as country_code, 
            b.country_count,
            c.un_region_clean as un_region_name_clean,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_region_clean) as un_region_name_clean_count,
            c.un_sub_region_clean as un_sub_region_name_clean,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_sub_region_clean) as un_sub_region_name_clean_count,
            c.un_intermediate_region_clean,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_intermediate_region_clean) as un_intermediate_region_clean_count,
            c.un_developed_or_developing_countries,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_developed_or_developing_countries) as un_developed_or_developing_countries_count,
            c.un_least_developed_countries_ldc,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_least_developed_countries_ldc) as un_least_developed_countries_ldc_count,
            c.un_small_island_developing_states_sids as un_small_island_developing_countries_lldc,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_small_island_developing_states_sids) as un_small_island_developing_countries_lldc_count,
            
            CAST(b.country_count AS DOUBLE) / SUM(b.country_count) OVER(PARTITION BY b.species) as likelihood_country,
            SUM(b.country_count) OVER(PARTITION BY b.species, c.un_region_clean) / SUM(b.country_count) OVER(PARTITION BY b.species) as likelihood_un_region,
            
            (0.7 * (SUM(b.country_count) OVER(PARTITION BY b.species, c.un_region_clean) / SUM(b.country_count) OVER(PARTITION BY b.species))) + 
            (0.3 * (CAST(b.country_count AS DOUBLE) / SUM(b.country_count) OVER(PARTITION BY b.species))) as localisation_score,
            
            COUNT(DISTINCT b.countrycode) OVER(PARTITION BY b.species) as country_breadth,
            COUNT(DISTINCT c.un_region_clean) OVER(PARTITION BY b.species) as un_region_breadth
        FROM agg_base b
        LEFT JOIN read_parquet('{COUNTRY_CODE_PATH}') c ON b.countrycode = c.iso2c
    ) TO '{OUTPUT_PATH}' (FORMAT PARQUET)
    """
    
    try:
        con.execute(query)
        print(f"Success! Output saved to {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error during processing: {e}")

if __name__ == "__main__":
    main()
