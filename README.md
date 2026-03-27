# Methods: GBIF Species Occurrence Source Analysis (2026)

This document describes the methodology used to analyze the geographic distribution of data publishing for the GBIF 2026 occurrence dataset (3.7 billion records).

## Objectives
1.  **Classify Source**: Determine if an occurrence record was published by an organization within the same country (**Internal Source**) or from a different country (**External Source**).
2.  **Taxonomic Segmentation**: Analyze trends across all taxa and specifically excluding **Class Aves** (birds), which account for over 2 billion records.
3.  **Policy Alignment**: Analyze distribution patterns specifically for **CBD Parties**, segmented by UN Region, Development Status, and World Bank Income Groups.

## Data Citation
**GBIF Occurrence Data**: GBIF.org (1 January 2026) GBIF Occurrence Download [https://doi.org/10.15468/dl.vp6jpz](https://doi.org/10.15468/dl.vp6jpz)

## Input Data
- **GBIF Occurrence Data (2026-01-01)**: `/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/` (3.7B rows)
- **GBIF Registry Snapshot**: `data-raw/registry/*.parquet` (Fetched via GBIF API)
- **Country Metadata**: `data-raw/countrycode.csv`
- **Country Overlay**: `data-raw/country_overlay.csv` (CBD status, Income groups)

## Methodology

### 1. Registry Reconciliation
We fetched the complete GBIF registry (Organizations, Institutions, and Collections) to create a local lookup table. This allowed for ultra-fast joining with the multi-billion row dataset without millions of API calls.
- **Script**: `src/download_gbif_registry.py` & `src/create_registry_lookup.py`
- **Lookup File**: `data/gbif_registry_lookup.parquet`

### 2. Source Classification & Aggregation
Using DuckDB, we joined each occurrence record with the registry lookup table based on `publishingorgkey`.
- **Logic**:
    - **Internal**: `occurrence.countrycode == publisher.country`
    - **External**: `occurrence.countrycode != publisher.country`
- **Script**: `src/calculate_source_distribution.py`

### 3. Enrichment
The aggregated counts by country and kingdom were joined with UN and World Bank metadata for regional and economic analysis.
- **Script**: `src/enrich_source_distribution.py`

### 4. CBD Party Analysis
Filtered the enriched data for CBD Parties and generated weighted summary statistics.
- **Script**: `src/analyze_cbd_parties.py`

## Output Data (data/processed/)

### Primary Distribution Tables
- `source_by_country.parquet/.csv`: Overall internal/external source distribution by country.
- `source_by_country_kingdom.parquet/.csv`: Distribution by country and kingdom.
- `source_by_country_kingdom_no_aves.parquet/.csv`: Distribution by country and kingdom (Excluding birds).

### CBD Party Summaries (Filtered & Aggregated)
- `cbd_parties_[all_taxa|no_aves]_un_region_summary.parquet/.csv`
- `cbd_parties_[all_taxa|no_aves]_un_region_un_intermediate_region_summary.parquet/.csv`
- `cbd_parties_[all_taxa|no_aves]_development_status_summary.parquet/.csv`
- `cbd_parties_[all_taxa|no_aves]_income_group_summary.parquet/.csv`

## Processing Environment
- **Engine**: DuckDB (for high-performance Parquet processing)
- **Language**: Python 3.9
- **Libraries**: pandas, requests, pyarrow, duckdb
