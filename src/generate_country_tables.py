"""Generate per-country tables for CBD Party publisher country share (Excluding Aves).

Produces:
  - data/processed/cbd_parties_no_aves_country_table.csv
  - data/processed/cbd_parties_no_aves_country_table.parquet
  - Section in the CBD report with alphabetical country table
"""

import os
import pandas as pd

PROCESSED_DIR = "data/processed"


def generate_country_table():
    df = pd.read_parquet(f"{PROCESSED_DIR}/source_by_country_no_aves.parquet")

    # Filter to CBD parties only
    df['is_cbd_party'] = df['is_cbd_party'].astype(str).str.lower() == 'true'
    cbd = df[df['is_cbd_party']].copy()

    # Select and rename columns
    table = cbd[[
        'country_name', 'iso3c', 'iso2c',
        'un_region_name', 'un_sub_region_name',
        'wb_income_group', 'is_ldc', 'is_sids',
        'internal_count', 'regional_count', 'external_count', 'unknown_count',
        'total_count',
        'internal_percentage', 'regional_percentage', 'external_percentage',
    ]].copy()

    # Sort alphabetically by country name
    table = table.sort_values('country_name').reset_index(drop=True)

    # Format boolean columns
    table['is_ldc'] = table['is_ldc'].apply(lambda x: 'Yes' if str(x).lower() == 'true' else '')
    table['is_sids'] = table['is_sids'].apply(lambda x: 'Yes' if str(x).lower() == 'true' else '')

    # Flag anomalies
    flags = []
    for _, row in table.iterrows():
        row_flags = []
        if row['total_count'] > 0 and row['unknown_count'] / row['total_count'] > 0.01:
            row_flags.append('High unknown%')
        if row['total_count'] < 1000:
            row_flags.append('Low record count')
        if row['internal_count'] == 0 and row['total_count'] > 100000:
            row_flags.append('No domestic publisher')
        flags.append('; '.join(row_flags))
    table['data_flags'] = flags

    # Save
    table.to_csv(f"{PROCESSED_DIR}/cbd_parties_no_aves_country_table.csv", index=False)
    table.to_parquet(f"{PROCESSED_DIR}/cbd_parties_no_aves_country_table.parquet", index=False)

    print(f"Country table: {len(table)} CBD parties")
    flagged = table[table['data_flags'] != '']
    if len(flagged) > 0:
        print(f"  Flagged countries: {len(flagged)}")
        for _, row in flagged.iterrows():
            print(f"    {row['country_name']}: {row['data_flags']}")

    return table


if __name__ == "__main__":
    generate_country_table()
