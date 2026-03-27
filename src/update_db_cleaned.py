import duckdb

DB_PATH = '/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/gbifdump_20260101.db'

def update_db():
    print(f"Connecting to {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    
    print("Dropping existing gbif_cleaned table...")
    con.execute("DROP TABLE IF EXISTS gbif_cleaned")
    
    print("Recreating gbif_cleaned table with MATERIAL_SAMPLE included...")
    # Based on the original filters but adding MATERIAL_SAMPLE
    query = """
    CREATE TABLE gbif_cleaned AS 
    SELECT 
        species, 
        LOWER(str_split(species, ' ')[1] || ' ' || str_split(species, ' ')[2]) as canonical_lower,
        kingdom, 
        phylum, 
        class, 
        "order", 
        family, 
        genus, 
        countrycode, 
        basisofrecord, 
        occurrencestatus, 
        decimallatitude, 
        decimallongitude, 
        issue 
    FROM occurrence 
    WHERE UPPER(taxonrank) = 'SPECIES' 
      AND (class IS NULL OR UPPER(class) != 'AVES') 
      AND species IS NOT NULL 
      AND UPPER(basisofrecord) IN ('LIVING_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION', 'OCCURRENCE', 'MATERIAL_SAMPLE') 
      AND UPPER(occurrencestatus) = 'PRESENT' 
      AND decimallatitude IS NOT NULL 
      AND decimallongitude IS NOT NULL
    """
    con.execute(query)
    print("Table creation complete.")
    
    # Verify count for AU Aspergillus niger
    print("Verifying Aspergillus niger in Australia...")
    res = con.execute("SELECT basisofrecord, COUNT(*) FROM gbif_cleaned WHERE species = 'Aspergillus niger' AND countrycode = 'AU' GROUP BY basisofrecord").fetchall()
    print(res)

if __name__ == "__main__":
    update_db()
