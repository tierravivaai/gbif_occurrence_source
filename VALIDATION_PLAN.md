# Validation Plan: Publisher Country Share Analysis

## Problem Statement

The current validation (`src/validate_analysis.py`) only checks **row count conservation** — that the sum of aggregated outputs equals the source parquet total. While necessary, this is insufficient to establish that the Internal/External/Unknown classification is factually correct. A mislabelled publisher country could shift millions of records between categories without changing the total.

This plan defines a layered validation approach to establish factual correctness of the publisher country classification at increasing levels of specificity.

---

## Layer 1: Aggregate Integrity (Existing — Passes)

| Check | Status | Result |
|-------|--------|--------|
| Total row count: source_by_country | Pass | 3,171,723,948 = 3,171,723,948 |
| Total row count: source_by_country_kingdom | Pass | 3,171,723,948 = 3,171,723,948 |
| Total row count: no-aves | Pass | 1,149,135,774 = 1,149,135,774 |
| CBD party fraction | Pass | 62.08% of total |

**Gap**: Confirms no records were lost, but not that they were classified correctly.

---

## Layer 2: Per-Country Bucket Arithmetic

**Purpose**: Verify that inside + outside + missing = total for every country, and that percentages are correctly derived.

**Implementation**: `src/validate_publisher_country.py` — `check_bucket_arithmetic()`

- For each row in `source_by_country.csv`, assert:
  - `internal_count + external_count + unknown_count == total_count`
  - `round(100.0 * internal_count / total_count, 2) == internal_percentage`
  - `round(100.0 * external_count / total_count, 2) == external_percentage`
- Fail on any mismatch > 1 (rounding tolerance)
- Also run on `source_by_country_kingdom.csv` and `source_by_country_kingdom_no_aves.csv`

---

## Layer 3: Per-Country Source Count Reconciliation

**Purpose**: Verify that the `total_count` for each country in the CSV matches the actual count in the source parquet.

**Implementation**: `src/validate_publisher_country.py` — `check_country_totals()`

- For a sample of countries (top 20 by record count + all CBD parties with >1M records), query the source parquet:
  ```sql
  SELECT count(*) FROM read_parquet('...') WHERE countrycode = '{CC}'
  ```
- Compare to `total_count` in CSV
- Fail on any difference > 0
- This is expensive (full table scan per country), so limit to ~30 countries

**Expected runtime**: ~30-60 seconds per country × 30 = 15-30 minutes

---

## Layer 4: Registry Lookup Accuracy (Spot-Check)

**Purpose**: Verify that the GBIF Registry API returns the same country as our cached `gbif_registry_lookup.parquet`.

**Implementation**: `src/validate_publisher_country.py` — `check_registry_accuracy()`

- Identify the top 50 `publishingorgkey` values by record count from the source parquet
- For each, call `GET https://api.gbif.org/v1/organization/{key}`
- Compare the `country` field in the API response to the `resolved_country` in the registry lookup
- Report mismatches with: org key, org name, expected country, actual country, record count affected
- Also check for org keys present in the data but missing from the lookup

**Why this matters**: The 2026-01-01 registry cache was built at a point in time. Publishers may have changed country, or new publishers may have been added since. A single mislabelled large publisher (e.g., iNaturalist with ~100M records) could shift a country's internal% by several points.

---

## Layer 5: Known-Publisher Verification

**Purpose**: Independently verify the classification of well-known publishers against external knowledge.

**Implementation**: `src/validate_publisher_country.py` — `check_known_publishers()`

Hard-code a list of well-known publishers with their expected country:

| Publisher | Expected Country | publishingorgkey (to be resolved) |
|-----------|-----------------|----------------------------------|
| iNaturalist | US | `28eb1a3f-0924-4e00-84be-7090c015eb9a` |
| eBird/Cornell Lab | US | `e2375770-86c0-43c2-93c4-786d66958540` |
| MNHN | FR | `2a625078-6e53-42c2-889c-7007893822f0` |
| Kew Gardens | GB | `635f5e82-4751-4ca3-8e9a-669e9c6c3c4f` |
| SANBI | ZA | `b5f4e498-a905-4e4c-8f65-7d5f81a3d8e1` |
| Atlas of Living Australia | AU | `4625657f-87e1-44cb-8544-fb9e5a56e8f0` |
| GBIF Spain | ES | `1cfb4a1c-c427-4421-b584-78e84be7c0c6` |
| SiB Colombia | CO | `9f8a8e90-4bfb-4d9e-b787-bf672437f4e6` |
| CONABIO (Mexico) | MX | `6293c3cd-3cc9-4e5c-9f7e-6c93dc4e456e` |

- For each, verify: (a) the registry lookup returns the expected country, (b) records with this org key are classified correctly (Internal if countrycode matches, External otherwise)

---

## Layer 6: Inferred Country Resolution Accuracy

**Purpose**: The `publisher_institution_country_share.py` module adds inferred country resolution by text-parsing `institutioncode`, `collectioncode`, and `rightsholder`. This is inherently noisier than the registry lookup and needs separate validation.

**Implementation**: `src/validate_publisher_country.py` — `check_inferred_accuracy()`

- Sample 200 unique `(institutioncode, collectioncode, rightsholder)` combinations from the source parquet where `publishingorgkey` has no registry match (i.e., would rely on inference)
- For each, run the inference logic and manually verify a subset:
  - Does the inferred country match the org's actual country (if verifiable via API)?
  - Are there false positives from ISO2 codes (e.g., "IN" matching India when it means "Institution")?
  - Are there false positives from country name substrings (e.g., "INDIANA" matching India)?
- Report: false positive rate, false negative rate, and specific failure patterns

**Known risks**:
- The ISO2 blacklist already excludes common English words (IN, AS, OR, NO, BE, TO, US, IS, IT, AN, ON, AT, BY, DO, IF, ME, MY, SO, UP, WE, HE, AM) but may miss others
- Long-form country name matching (`"kenya" in text_lower`) could match substrings

---

## Layer 7: Cross-Pipeline Comparison

**Purpose**: The two classification pipelines use different methods. Comparing them reveals the inference's impact.

| Pipeline | Script | Method |
|----------|--------|--------|
| Explicit only | `calculate_source_distribution.py` | Registry lookup on `publishingorgkey` only |
| Explicit + Inferred | `publisher_institution_country_share.py` | Registry + text parsing of institutioncode/collectioncode/rightsholder |

**Implementation**: `src/validate_publisher_country.py` — `check_pipeline_comparison()`

- For a sample country (e.g., Kenya, South Africa, Australia), run both pipelines
- Compare: explicit-only internal% vs explicit+inferred internal%
- If inference changes a country's internal% by more than 5 points, flag for manual review
- Document which specific records are reclassified by inference (list the org keys and text values)

---

## Layer 8: Plausibility Bounds & Anomaly Detection

**Purpose**: Flag statistically anomalous results for manual review without requiring ground truth.

**Implementation**: `src/validate_publisher_country.py` — `check_plausibility()`

Flag countries where:
- `unknown_pct > 1%` (potential registry coverage gap)
- `internal_pct < 1%` with `total_count > 1M` (implausible — large biodiverse countries should have some domestic publishers)
- `internal_pct > 99.5%` with `total_count > 10M` (implausible — even the US has some foreign-published records)
- `internal_pct` differs by >20 points between all-taxa and no-aves (suggests Aves records are skewing the classification)

---

## Layer 9: Deep-Dive on High-Stakes Countries

**Purpose**: For countries where the classification has policy implications (CBD negotiations), provide transparent evidence of correctness.

**Implementation**: `src/validate_publisher_country.py` — `deep_dive(country_code)`

For a given country:
1. List the top 20 publishing orgs by record count, with: org name, resolved country, record count, classification (inside/outside)
2. For each "outside" org in the top 10, verify via API that it is genuinely foreign
3. For each "inside" org in the top 10, verify via API that it is genuinely domestic
4. List the top inferred matches and their confidence
5. Produce a per-country validation report section

**Priority countries** (CBD parties with low internal%):
- Canada (9.32%), India (1.35%), Peru (0.24%), Malaysia (0.11%), Indonesia (1.89%), Kenya (8.18%), Ecuador (3.70%), Costa Rica (12.57%), Mexico (22.10%), Colombia (36.85%)

---

## Implementation Plan

### Script: `src/validate_publisher_country.py`

A single script with subcommands matching the validation layers:

```
python3 src/validate_publisher_country.py --layer arithmetic
python3 src/validate_publisher_country.py --layer country-totals
python3 src/validate_publisher_country.py --layer registry-accuracy
python3 src/validate_publisher_country.py --layer known-publishers
python3 src/validate_publisher_country.py --layer inferred-accuracy
python3 src/validate_publisher_country.py --layer pipeline-comparison
python3 src/validate_publisher_country.py --layer plausibility
python3 src/validate_publisher_country.py --layer deep-dive --country KE
python3 src/validate_publisher_country.py --layer all
```

Output: Appends to `VALIDATION_REPORT.md` with structured sections per layer.

### Execution Order

1. **Layer 2** (bucket arithmetic) — instant, no source data access
2. **Layer 8** (plausibility) — instant, only reads CSVs
3. **Layer 5** (known publishers) — fast, ~9 API calls
4. **Layer 4** (registry accuracy) — moderate, ~50 API calls + source query for top orgs
5. **Layer 3** (country totals) — expensive, ~30 full table scans
6. **Layer 6** (inferred accuracy) — moderate, requires sampling source data
7. **Layer 7** (pipeline comparison) — moderate, runs both pipelines for sample countries
8. **Layer 9** (deep-dive) — on-demand per country

Layers 2, 5, 8 can be run immediately. Layer 3 is the most expensive and should be run last.

### Dependencies

- `duckdb`, `pandas`, `urllib` (all already available)
- Source parquet access at `/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*`
- Internet access for API calls (layers 4, 5, 6)
- No new packages required

---

## Success Criteria

The validation is considered adequate when:

1. **All arithmetic checks pass** (Layer 2) — zero tolerance
2. **Country totals match source** (Layer 3) — zero tolerance for sampled countries
3. **Registry accuracy ≥ 99%** (Layer 4) — allow for legitimate changes since cache was built
4. **All known publishers verified correct** (Layer 5) — zero tolerance
5. **Inferred accuracy ≥ 90%** (Layer 6) — inference is inherently noisy, 90% is acceptable
6. **No unexplained pipeline differences > 5 points** (Layer 7)
7. **All anomalies investigated and documented** (Layer 8)
8. **Deep-dives completed for all priority countries** (Layer 9)
