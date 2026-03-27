import pandas as pd
import duckdb

def create_lookup():
    # Load raw registry data
    orgs = pd.read_parquet("data-raw/registry/organizations.parquet")
    insts = pd.read_parquet("data-raw-registry/institutions.parquet" if os.path.exists("data-raw-registry/institutions.parquet") else "data-raw/registry/institutions.parquet")
    colls = pd.read_parquet("data-raw/registry/collections.parquet")
    
    lookup_rows = []
    
    # 1. Organizations (by Key and Name)
    for _, row in orgs.iterrows():
        # Map by UUID key
        lookup_rows.append({
            'original_key': row['key'],
            'resolved_name': row['title'],
            'resolved_country': row.get('country'),
            'type': 'organization'
        })
        # Map by Name (for rightsholder lookup)
        lookup_rows.append({
            'original_key': row['title'],
            'resolved_name': row['title'],
            'resolved_country': row.get('country'),
            'type': 'rightsholder'
        })
        
    # 2. Institutions (by Key and Code)
    for _, row in insts.iterrows():
        # Map by UUID key
        lookup_rows.append({
            'original_key': row['key'],
            'resolved_name': row['name'],
            'resolved_country': row.get('address', {}).get('country') if isinstance(row.get('address'), dict) else None,
            'type': 'institution'
        })
        # Map by Code
        if row.get('code'):
            lookup_rows.append({
                'original_key': row['code'],
                'resolved_name': row['name'],
                'resolved_country': row.get('address', {}).get('country') if isinstance(row.get('address'), dict) else None,
                'type': 'institution'
            })

    # 3. Collections (by Key and Code)
    for _, row in colls.iterrows():
        # Map by UUID key
        lookup_rows.append({
            'original_key': row['key'],
            'resolved_name': row['name'],
            'resolved_country': row.get('address', {}).get('country') if isinstance(row.get('address'), dict) else None,
            'type': 'collection'
        })
        # Map by Code
        if row.get('code'):
            lookup_rows.append({
                'original_key': row['code'],
                'resolved_name': row['name'],
                'resolved_country': row.get('address', {}).get('country') if isinstance(row.get('address'), dict) else None,
                'type': 'collection'
            })

    df_lookup = pd.DataFrame(lookup_rows)
    # Remove duplicates (e.g., if multiple institutions share a code, we'll take the first for now)
    df_lookup = df_lookup.drop_duplicates(subset=['original_key', 'type'])
    
    output_path = "data/gbif_registry_lookup.parquet"
    df_lookup.to_parquet(output_path)
    print(f"Created lookup table with {len(df_lookup)} entries at {output_path}")

if __name__ == "__main__":
    import os
    create_lookup()
