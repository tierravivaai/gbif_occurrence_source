# AGENTS.md - GBIF Species Occurrence Source Analysis

## Project Overview
This project analyzes the geographic distribution of data publishing for the GBIF 2026 occurrence dataset (3.7 billion records). It classifies records as **Internal** (published by an organization in the same country as the occurrence) or **External** (published from a different country) and segments the results by taxon, UN region, and CBD status.

## Source Data & Locations
- **GBIF Occurrence Data (2026-01-01)**: `/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/`
- **GBIF Registry Lookup**: `data/gbif_registry_lookup.parquet` (Generated from GBIF API)
- **Country Metadata**: `data-raw/countrycode.csv` and `data-raw/country_code.parquet`
- **Citation**: GBIF.org (1 January 2026) GBIF Occurrence Download https://doi.org/10.15468/dl.vp6jpz

## Key Scripts & Workflow
1.  **Registry Reconciliation**:
    - `src/download_gbif_registry.py`: Fetches Organizations, Institutions, and Collections from GBIF API.
    - `src/create_registry_lookup.py`: Processes registry data into a fast lookup table.
2.  **Source Classification**:
    - `src/calculate_source_distribution.py`: Uses DuckDB to perform Internal/External classification at scale.
3.  **Enrichment & Analysis**:
    - `src/enrich_source_distribution.py`: Joins results with UN and World Bank metadata.
    - `src/analyze_cbd_parties.py`: Generates summaries for CBD Parties, UN Regions, and economic groups.
4.  **Specialized Processing**:
    - `src/gbif_2024_processing.py`: Pipeline for 2024 snapshot analysis.
    - `src/species_country_region_processing.py`: Regional species count and metrics (likelihood/breadth).

## Processing Logic
- **Internal**: `occurrence.countrycode == publisher.country`
- **External**: `occurrence.countrycode != publisher.country`
- **Filtering**: Records are filtered for `taxonrank == 'SPECIES'`, `occurrencestatus == 'PRESENT'`, and specific `basisofrecord` types.
- **Aves Exclusion**: Analysis often provides a "No Aves" version to prevent bird observations (2B+ records) from skewing results.

## Specialized Droids
- **gbif-data-reconciliation-specialist**: Project-level droid for ultra-fast, local-first reconciliation of `institutioncode`, `collectioncode`, `publishingorgkey`, and `rightsholder`. It prioritizes the local `data/gbif_registry_lookup.parquet` and uses fuzzy matching to reconcile the dataset with minimal API calls.
