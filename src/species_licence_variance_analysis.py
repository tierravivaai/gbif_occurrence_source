import duckdb

def analyze_variance():
    con = duckdb.connect()
    
    print("Analyzing variance between full dataset and CC0/CC-BY filtered dataset (2024-04-01)...")
    
    query = """
    WITH full_ds AS (
        SELECT kingdom, COUNT(DISTINCT canonical_lower) as species_count, SUM(country_count) as total_occurrences
        FROM read_parquet('data/gbif_country_index.parquet')
        GROUP BY kingdom
    ),
    filtered_ds AS (
        SELECT kingdom, COUNT(DISTINCT canonical_lower) as species_count, SUM(country_count) as total_occurrences
        FROM read_parquet('data/gbif_country_index_cc0-ccby.parquet')
        GROUP BY kingdom
    )
    SELECT 
        f.kingdom,
        f.species_count as full_species,
        l.species_count as filtered_species,
        (CAST(f.species_count AS DOUBLE) - l.species_count) / f.species_count * 100 as species_loss_pct,
        f.total_occurrences as full_occurrences,
        l.total_occurrences as filtered_occurrences,
        (CAST(f.total_occurrences AS DOUBLE) - l.total_occurrences) / f.total_occurrences * 100 as occurrence_loss_pct
    FROM full_ds f
    LEFT JOIN filtered_ds l ON f.kingdom = l.kingdom
    ORDER BY occurrence_loss_pct DESC
    """
    
    results = con.execute(query).fetchall()
    print("\nKingdom Variance Analysis:")
    print(f"{'Kingdom':<15} | {'Species Loss %':<15} | {'Occurrence Loss %':<15}")
    print("-" * 50)
    for row in results:
        print(f"{row[0]:<15} | {row[3]:>14.2f}% | {row[6]:>16.2f}%")

    # Analyze region impact for Plantae specifically
    query_plantae = """
    WITH full_plantae AS (
        SELECT un_region_name_clean as region, SUM(country_count) as occurrences
        FROM read_parquet('data/gbif_country_index.parquet')
        WHERE kingdom = 'Plantae'
        GROUP BY un_region_name_clean
    ),
    filtered_plantae AS (
        SELECT un_region_name_clean as region, SUM(country_count) as occurrences
        FROM read_parquet('data/gbif_country_index_cc0-ccby.parquet')
        WHERE kingdom = 'Plantae'
        GROUP BY un_region_name_clean
    )
    SELECT 
        f.region,
        (CAST(f.occurrences AS DOUBLE) - COALESCE(l.occurrences, 0)) / f.occurrences * 100 as loss_pct
    FROM full_plantae f
    LEFT JOIN filtered_plantae l ON f.region = l.region
    ORDER BY loss_pct DESC
    """
    
    results_plantae = con.execute(query_plantae).fetchall()
    print("\nPlantae Regional Occurrence Loss (CC-BY-NC exclusion impact):")
    for row in results_plantae:
        print(f"{row[0] or 'Unknown':<20} : {row[1]:>6.2f}% loss")

if __name__ == "__main__":
    analyze_variance()
