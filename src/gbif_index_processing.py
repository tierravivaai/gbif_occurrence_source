import duckdb
import os

# Paths
OCCURRENCE_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump/gbifdump/gbif-dump-2024-04-01/occurrence/*"
COUNTRY_CODE_PATH = "data-raw/country_code.parquet"
OUTPUT_PATH = "data/gbif_country_index.parquet"

# Validation Constants
NAMIBIA_TOTAL_EXPECTED = 2104928
NAMIBIA_TOTAL_ALLOWED_DIFF = 150000

def validate_namibia(con):
    print("Validating Namibia occurrence counts...")
    query_total = f"SELECT COUNT(*) FROM read_parquet('{OCCURRENCE_PATH}') WHERE countrycode = 'NA'"
    total_count = con.execute(query_total).fetchone()[0]
    
    query_aves = f"SELECT COUNT(*) FROM read_parquet('{OCCURRENCE_PATH}') WHERE countrycode = 'NA' AND UPPER(class) = 'AVES'"
    aves_count = con.execute(query_aves).fetchone()[0]
    
    diff_total = abs(total_count - NAMIBIA_TOTAL_EXPECTED)
    print(f"Namibia Total count: {total_count} (Expected: {NAMIBIA_TOTAL_EXPECTED}, Diff: {diff_total})")
    print(f"Namibia Aves count: {aves_count} ({aves_count/total_count*100:.1f}% of total)")
    
    if diff_total > NAMIBIA_TOTAL_ALLOWED_DIFF:
        print(f"WARNING: Namibia total count diff {diff_total} exceeds allowed threshold {NAMIBIA_TOTAL_ALLOWED_DIFF}!")
        return False
    return True

def run_processing(license_filter=None, output_suffix=""):
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='32GB'")
    
    actual_output_path = OUTPUT_PATH
    if output_suffix:
        actual_output_path = OUTPUT_PATH.replace(".parquet", f"_{output_suffix}.parquet")
    
    license_clause = ""
    if license_filter:
        license_clause = f"AND license IN ({', '.join([f'{repr(l)}' for l in license_filter])})"

    print(f"Starting processing for {actual_output_path}...")

    query = f"""
    COPY (
        WITH base AS (
            SELECT 
                gbifid,
                species, 
                LOWER(str_split(species, ' ')[1] || ' ' || str_split(species, ' ')[2]) as canonical_lower,
                kingdom, 
                countrycode, 
                basisofrecord,
                license
            FROM read_parquet('{OCCURRENCE_PATH}')
            WHERE UPPER(taxonrank) = 'SPECIES' 
              AND (class IS NULL OR UPPER(class) != 'AVES')
              AND species IS NOT NULL
              AND UPPER(basisofrecord) IN ('LIVING_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION', 'OCCURRENCE', 'MATERIAL_SAMPLE')
              {license_clause}
        ),
        agg_base AS (
            SELECT 
                canonical_lower,
                kingdom, 
                countrycode,
                ANY_VALUE(species) as species,
                COUNT(*) as country_count,
                -- Removed gbifids and licenses lists due to memory issues
                -- LIST(gbifid) as gbifids, 
                -- LIST(license) as licenses
            FROM base
            GROUP BY canonical_lower, kingdom, countrycode
        )
        SELECT 
            b.species, 
            b.canonical_lower,
            b.kingdom, 
            b.countrycode as country_code, 
            b.country_count,
            -- b.gbifids,
            -- b.licenses,
            c.un_region_clean as un_region_name_clean,
            SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_region_clean) as un_region_name_clean_count,
            c.un_sub_region_clean as un_sub_region_name_clean,
            SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_sub_region_clean) as un_sub_region_name_clean_count,
            c.un_intermediate_region_clean,
            SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_intermediate_region_clean) as un_intermediate_region_clean_count,
            
            CAST(b.country_count AS DOUBLE) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower) as likelihood_country,
            SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_region_clean) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower) as likelihood_un_region,
            
            (0.7 * (SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_region_clean) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower))) + 
            (0.3 * (CAST(b.country_count AS DOUBLE) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower))) as localisation_score,
            
            COUNT(DISTINCT b.countrycode) OVER(PARTITION BY b.canonical_lower) as country_breadth,
            COUNT(DISTINCT c.un_region_clean) OVER(PARTITION BY b.canonical_lower) as un_region_breadth
        FROM agg_base b
        LEFT JOIN read_parquet('{COUNTRY_CODE_PATH}') c ON b.countrycode = c.iso2c
    ) TO '{actual_output_path}' (FORMAT PARQUET)
    """
    
    try:
        con.execute(query)
        print(f"Success! Output saved to {actual_output_path}")
    except Exception as e:
        print(f"Error during processing: {e}")

def main():
    con = duckdb.connect()
    if not validate_namibia(con):
        print("Namibia validation failed. Check source data.")
        return

    # 1. Full Dataset
    run_processing()

    # 2. CC0 and CC-BY only
    run_processing(license_filter=['CC_BY_4_0', 'CC0_1_0'], output_suffix="cc0-ccby")

if __name__ == "__main__":
    main()
