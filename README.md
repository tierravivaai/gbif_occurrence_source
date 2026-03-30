# Methods: GBIF Species Occurrence Source Analysis (2026)

This document describes the methodology used to analyze the geographic distribution of data publishing for the GBIF 2025 occurrence dataset (3.7 billion records).

The aim of the exercise is to quantify the source of species occurrence records by country and whether the publisher is from the country (internal source) or external to the country (external source). This data, calculated as the percentage of a country's taxonomic records published in country and those published externally, can be combined with classifications of countries by economic status. This should provide wider insights into whether there is a correlation between income and the number and origin of species occurrence records. Taken together this can provide insights into *where gaps exist in internal taxonomic capacity* among Parties to the Convention on Biological Diversity and potential priorities for future support as part of implementation of the Global Biodiversity Framework.

It should be emphasised that there are likely to be limitations to this approach:

- a) GBIF species occurrence records are dominated by bird observation data (class Aves) submitted by citizen scientists. There are over 2 billion occurrence records for birds in GBIF data. In some cases bird data may be submitted by a citizen scientist in one country but is classified as published by the country where the publisher is located (e.g. for eBird and iNaturalist and similar initiatives). One solution to the skew in the data is to exclude class Aves from the calculations altogether. In the data generated for this approach the counts with Aves and non-Aves are both presented.
- b) It will be useful to understand occurrence records by species and data is compiled by country and by kingdom. An important potential limitation here is that species occurrence records may be limited for some kingdoms in GBIF data (e.g. bacteria and viruses). This merits further investigation.
- c) Assessing gaps in taxonomic capacity would also benefit from calculations of how many species one might reasonably be expected to be found in a given country for a given category (e.g. a country dominated by arid and semi-arid ecosystems would have a different expected profile to a tropical forest country). As such, the assessment of gaps in taxonomic capacity using this method is likely to be somwhat crude in the absence of estimates of expected taxonomic coverage (e.g. given what we know about similar countries with assumed high levels of coverage, what is it reasonable to expect). 

## Objectives
1.  **Classify Source**: Determine if an occurrence record was published by an organization within the same country (**Internal Source**) or from a different country (**External Source**).
2.  **Taxonomic Segmentation**: Analyze trends across all taxa and specifically excluding **Class Aves** (birds), which account for over 2 billion records.
3.  **Policy Alignment**: Analyze distribution patterns specifically for **CBD Parties**, segmented by UN Region, Development Status, and World Bank Income Groups.

## Data Citation
**GBIF Occurrence Data**: GBIF.org (1 June 2025) GBIF Occurrence Download [https://doi.org/10.15468/dl.jsevhc](https://doi.org/10.15468/dl.jsevhc). Downloaded 1st January 2026. 

## Input Data
- **GBIF Occurrence Data (2025-06-01)**: `/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/` (3.7B rows). Note that the file title refers to the download date and not the data of the snapshot.
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

**Field Mapping**:
- **Occurrence Record**: `countrycode` (ISO 2-letter country code where the specimen was collected)
- **Publisher Record**: `publishingorgkey` (UUID of the publishing organization) → joined to registry lookup → `resolved_country` (ISO 2-letter country code of the publisher)

**Classification Logic**:
```sql
CASE 
    WHEN registry.resolved_country IS NULL THEN 'UNKNOWN'
    WHEN occurrence.countrycode = registry.resolved_country THEN 'INTERNAL'
    ELSE 'EXTERNAL'
END as source_type
```

- **INTERNAL**: The occurrence was published by an organization in the same country as the specimen's location
- **EXTERNAL**: The occurrence was published by an organization in a different country
- **UNKNOWN**: No matching registry entry found for the `publishingorgkey`

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

## Note on the use of AI

The analysis in this repository was designed by Paul Oldham and implemented in Droid from Factory AI. The main models used in code 
generation and testing were Gemini 3 Flash with planning performed in GPT-5.4 Codex.
