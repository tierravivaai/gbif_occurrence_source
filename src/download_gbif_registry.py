import requests
import pandas as pd
import os

BASE_URL = "https://api.gbif.org/v1"

def fetch_all(endpoint, name):
    results = []
    offset = 0
    limit = 1000
    
    print(f"Downloading {name}...")
    while True:
        url = f"{BASE_URL}/{endpoint}?offset={offset}&limit={limit}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        batch = data.get("results", [])
        if not batch:
            break
            
        results.extend(batch)
        print(f"  Fetched {len(results)} records...")
        
        if data.get("endOfRecords"):
            break
        offset += limit
        
    df = pd.DataFrame(results)
    output_path = f"data-raw/registry/{name}.parquet"
    df.to_parquet(output_path)
    print(f"Saved {len(results)} {name} to {output_path}")

if __name__ == "__main__":
    fetch_all("organization", "organizations")
    fetch_all("grscicoll/institution", "institutions")
    fetch_all("grscicoll/collection", "collections")
