import duckdb
import os

try:
    from src.endemic_annotations import (
        AU_ENDEMIC_REFERENCE,
        ZA_ENDEMIC_REFERENCE,
        build_endemic_ctes,
        build_endemic_joins,
        build_endemic_select_columns,
    )
except ImportError:  # pragma: no cover
    from endemic_annotations import (
        AU_ENDEMIC_REFERENCE,
        ZA_ENDEMIC_REFERENCE,
        build_endemic_ctes,
        build_endemic_joins,
        build_endemic_select_columns,
    )

# Paths
DB_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/gbifdump_20260101.db"
COUNTRY_CODE_PATH = "data-raw/country_code.parquet"
OUTPUT_PATH = "data/gbif_country_index.parquet"

# Validation Constants
NAMIBIA_TOTAL_EXPECTED = 1917503
NAMIBIA_TOTAL_ALLOWED_DIFF = 150000

# Filter Constants
MIN_OCCURRENCES_PER_COUNTRY = 2

def validate_namibia(con):
    print("Validating Namibia occurrence counts...")
    
    # Total count (including Aves) for comparison with GBIF online
    query_total = "SELECT COUNT(*) FROM occurrence WHERE countrycode = 'NA'"
    total_count = con.execute(query_total).fetchone()[0]
    
    # Aves count
    query_aves = "SELECT COUNT(*) FROM occurrence WHERE countrycode = 'NA' AND UPPER(class) = 'AVES'"
    aves_count = con.execute(query_aves).fetchone()[0]
    
    diff_total = abs(total_count - NAMIBIA_TOTAL_EXPECTED)
    print(f"Namibia Total count: {total_count} (Expected: {NAMIBIA_TOTAL_EXPECTED}, Diff: {diff_total})")
    print(f"Namibia Aves count: {aves_count} ({aves_count/total_count*100:.1f}% of total)")
    
    if diff_total > NAMIBIA_TOTAL_ALLOWED_DIFF:
        print(f"WARNING: Namibia total count diff {diff_total} exceeds allowed threshold {NAMIBIA_TOTAL_ALLOWED_DIFF}!")
        return False
    return True

def main():
    print("Starting processing with DuckDB...")
    con = duckdb.connect(DB_PATH)
    
    if not validate_namibia(con):
        print("Continuing despite Namibia validation warning.")

    # Ensure output directory exists
    output_dir = os.path.dirname(OUTPUT_PATH)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    query = f"""
    COPY (
        WITH base AS (
            SELECT 
                species, 
                canonical_lower,
                kingdom, 
                countrycode, 
                basisofrecord,
                COUNT(*) as country_count
            FROM gbif_cleaned
            GROUP BY species, canonical_lower, kingdom, countrycode, basisofrecord
        ),
        agg_base AS (
            SELECT 
                canonical_lower,
                kingdom, 
                countrycode,
                ANY_VALUE(species) as species,
                SUM(country_count) as country_count
            FROM base
            GROUP BY canonical_lower, kingdom, countrycode
        ),
        scored_base AS (
            SELECT 
                b.*,
                c.un_region_clean as un_region_name_clean,
                c.un_sub_region_clean as un_sub_region_name_clean,
                c.un_intermediate_region_clean,
                c.un_developed_or_developing_countries,
                c.un_least_developed_countries_ldc,
                c.un_small_island_developing_states_sids as un_small_island_developing_countries_lldc,
                
                -- Distinctiveness (Likelihood) Metrics
                CAST(b.country_count AS DOUBLE) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower) as likelihood_country,
                SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_region_clean) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower) as likelihood_un_region,
                
                -- Distribution Breadth with Threshold (Refinement Plan)
                COUNT(DISTINCT CASE WHEN b.country_count >= 300 THEN b.countrycode END) OVER(PARTITION BY b.canonical_lower) as breadth_300_count,
                
                -- Cosmopolitanism (Breadth) Metrics
                COUNT(DISTINCT b.countrycode) OVER(PARTITION BY b.canonical_lower) as country_breadth,
                COUNT(DISTINCT c.un_region_clean) OVER(PARTITION BY b.canonical_lower) as un_region_breadth
            FROM agg_base b
            LEFT JOIN read_parquet('{COUNTRY_CODE_PATH}') c ON b.countrycode = c.iso2c
        ),
        {build_endemic_ctes([AU_ENDEMIC_REFERENCE, ZA_ENDEMIC_REFERENCE])}
        SELECT 
            b.species, 
            b.canonical_lower,
            b.kingdom, 
            b.countrycode as country_code, 
            b.country_count,
            b.un_region_name_clean as un_region,
            b.un_sub_region_name_clean as un_sub_region,
            b.un_intermediate_region_clean as un_intermediate_region,
            b.un_developed_or_developing_countries as un_development_status,
            b.un_least_developed_countries_ldc as un_ldc,
            b.un_small_island_developing_countries_lldc as un_sids,
            
            -- Distribution Breadth with Threshold
            b.breadth_300_count as significant_breadth,
            
            -- Cosmopolitanism (Breadth) Metrics
            b.country_breadth,
            b.un_region_breadth,

            -- Endemic Status
            {build_endemic_select_columns([AU_ENDEMIC_REFERENCE, ZA_ENDEMIC_REFERENCE], table_alias="b")},

            -- Adjusted Metrics (Refinement Plan v2)
            b.likelihood_country * (1.0 / sqrt(GREATEST(b.breadth_300_count, 1))) as likelihood_country,
            b.likelihood_un_region * (1.0 / sqrt(GREATEST(b.breadth_300_count, 1))) as likelihood_un_region,
            (0.7 * b.likelihood_un_region + 0.3 * b.likelihood_country) * (1.0 / sqrt(GREATEST(b.breadth_300_count, 1))) as localisation_score
        FROM scored_base b
        {build_endemic_joins([AU_ENDEMIC_REFERENCE, ZA_ENDEMIC_REFERENCE], table_alias="b")}
    ) TO '{OUTPUT_PATH}' (FORMAT PARQUET)
    """
    
    try:
        con.execute(query)
        print(f"Success! Output saved to {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error during processing: {e}")

if __name__ == "__main__":
    main()
