# Annex 

## Methodology 

## Data Sources and Quality

**Note on coverage:** 

GBIF data is routinely updated. Individual snapshots may vary as new data comes in or old data is updated. For reference, approximately 43 million
records in the 1 June 2025 reference snapshot (roughly 1.36% of 3.17 billion) had no occurrence country code and therefore cannot be assigned to any CBD Party. 
These records are excluded from analysis. 

In the current 1 April 2026 snapshot roughly 1.27% of 3.58 billion had no occurrence country code and cannot be assigned to 
any CBD Party. These records are excluded from analysis.

**Data Sources:** 

Two snapshots may be used for analysis:

1. A Reference snapshot: GBIF.org (1 June 2025) GBIF Occurrence Download https://doi.org/10.15468/dl.jsevhc, downloaded 1 January 2026.

2. The current snapshot GBIF.org (01 April 2026) GBIF Occurrence Download https://doi.org/10.15468/dl.9z6p8m, downloaded 21 April 2026
Licence CC BY-NC 4.0
File 274 GB Simple Parquet
Involved datasets 89,635
Involved publishers 2,595
Involved publishing countries 147

2026 snapshot data notes: The public AWS snapshot that resolves to DOI https://doi.org/10.15468/dl.9z6p8m contained 3,582,425,982 records compared with the 3,769,914,580
reported at the DOI reference page. This is a difference of 349 part files and 187.5M rows. On the 21st of April the GBIF home page listed 3,679,734,966 records - which is
normally updated daily - suggesting that in the corresponding period one or more datasets containing the 187.5M were removed from GBIF by published for reasons that are unknown (e.g duplication, 
regeneration etc).

---

## Methods

For full methodology details, see the online code repository and its [README.md](https://github.com/tierravivaai/gbif_occurrence_source).

In summary:

1. The GBIF registry was downloaded and a local lookup table (`data/gbif_registry_lookup.parquet`) was built mapping `publishingorgkey` to `resolved_country`.
2. Each occurrence record was classified as Internal (publisher country = occurrence country) or External (publisher country differs) or Unknown (no registry match).
3. Results were enriched with UN regional, development status, and World Bank income group metadata.
4. CBD party records were filtered and aggregated into the summary tables used in this report.
5. Aggregates are validated in a multi-step procedure

**Processing scripts:**
- `src/calculate_source_distribution.py` — Source classification and aggregation
- `src/create_registry_lookup.py` — Registry reconciliation
- `src/enrich_source_distribution.py` — Metadata enrichment
- `src/analyze_cbd_parties.py` — CBD Parties summaries
- `src/validate_publisher_country.py` - Main Validation script for Party Publisher counts
- `src/validate_analysis.py` - Additional validation

---