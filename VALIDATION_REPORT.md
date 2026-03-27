# Validation Report: GBIF Source Analysis (2026)

This report compares the raw counts from the source dataset against the aggregated counts in the processed output files.

## 1. Primary Analysis Totals (All Taxa)
| File | Calculated Total | Source Total | Difference |
|------|------------------|--------------|------------|
| `source_by_country.csv` | 3,171,723,948 | 3,171,723,948 | 0 |
| `source_by_country_kingdom.csv` | 3,171,723,948 | 3,171,723,948 | 0 |

## 2. Taxonomic Filtering (Excluding Aves)
| File | Calculated Total | Source Total (No Aves) | Difference |
|------|------------------|------------------------|------------|
| `source_by_country_no_aves.csv` | 1,149,135,774 | 1,149,135,774 | 0 |
| `source_by_country_kingdom_no_aves.csv` | 1,149,135,774 | 1,149,135,774 | 0 |

## 3. CBD Party Summary (Subset)
This summary only includes records where `is_cbd_party` is TRUE.

### All Taxa
- **Total Records in CBD Party Summary:** 1,968,908,210
- **Percentage of Total Records from CBD Parties:** 62.08%

### No Aves
- **Total Records in CBD Party Summary (No Aves):** 104,623,816
- **Percentage of Total No Aves Records from CBD Parties:** 9.1%
