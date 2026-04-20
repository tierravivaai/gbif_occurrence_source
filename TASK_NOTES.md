# Task Notes: Publisher Country Share Analysis

## Completed

- [x] Regression test for Aves/non-Aves scope separation (`tests/test_report_aves_non_aves_separation.py`)
- [x] Report restructured: Excluding Aves as primary, All Taxa as Annex
- [x] Key Findings updated to scope all percentage claims

## Pending

### a) Internal-to-Region vs External-to-Region Classification

**Problem**: The current Internal/External classification is country-to-country. A US-published record for Canada is "External", even though both are in the Americas. This makes the Americas appear overwhelmingly externally-published when in fact most "external" publishers are within the same UN region (primarily US-based orgs).

**Proposed three-tier classification**:
- **Internal**: Publisher country = Occurrence country (same as now)
- **Regional**: Publisher country differs from Occurrence country, but both are in the same UN Region
- **External**: Publisher country is in a different UN Region from the Occurrence country

**Why this matters**: Regional GBIF nodes (e.g., GBIF America, GBIF Africa, GBIF Asia) may represent a different form of domestic/regional capacity than truly external publishing. A regional classification would distinguish between "data published within the region" and "data published from outside the region."

**Implementation**:
- Modify `src/calculate_source_distribution.py` to add a `source_type_regional` column
- Join occurrence `countrycode` and publisher `resolved_country` to `countrycode.csv` to get UN regions for both
- Classify: Internal (same country), Regional (same UN region, different country), External (different UN region)
- Update CBD summaries and report generation

**Data needed**: Already available — `countrycode.csv` has `un_region_name` for all countries.

---

### b) Word Formatting: Black and White Colour Scheme

**Problem**: The current .docx uses `Light Shading Accent 1` table style, which applies coloured (blue/teal) table headers and alternating row shading.

**Required**: Black and white throughout — no colour. Suitable for printing and institutional documents.

**Implementation**:
- In `src/convert_cbd_report_to_docx.py`:
  - Change table style from `'Light Shading Accent 1'` to `'TableGrid'` or a custom black-and-white style
  - Set header row: black background, white text, bold
  - Set body rows: white background, black text
  - Remove any coloured shading from headings or emphasis
- Remove `RGBColor` usage if any coloured elements are added

---

### c) Country Tables Excluding Aves (Alphabetical, with LDC and SIDS)

**Problem**: The current report only provides aggregate tables by UN Region, Development Status, and Income Group. For policy analysis, per-country tables are needed.

**Requirements**:
- One row per CBD Party country
- Alphabetical order by country name
- Columns: Country Name, ISO3C, UN Region, WB Income Group, LDC Status, SIDS Status, Internal Count, External Count, Unknown Count, Total Count, Internal %, External %
- Excluding Aves data only (primary analysis scope)
- Flag countries where data appears incomplete or anomalous

**Data needed**:
- **LDC (Least Developed Countries) status**: NOT currently in `data-raw/country_overlay.csv` or `data-raw/countrycode.csv`. Needs to be sourced from UN/OHRILS list and added to `country_overlay.csv`.
- **SIDS (Small Island Developing States) status**: NOT currently in either file. Needs to be sourced from UN/OHRILS list and added to `country_overlay.csv`.
- **Per-country data**: Available in `data/processed/source_by_country_kingdom_no_aves.csv` (has country-level counts, but needs filtering for CBD parties and aggregation across kingdoms).

**Implementation**:
1. Source LDC and SIDS classification lists and add as columns to `data-raw/country_overlay.csv`
2. Modify `src/enrich_source_distribution.py` to propagate LDC and SIDS fields
3. Create new script `src/generate_country_tables.py` to produce per-country tables
4. Add country table section to the CBD report (between aggregate tables and Key Findings)
5. Flag countries where:
   - `unknown_count > 1% of total_count`
   - `total_count < 1000` (statistically unreliable)
   - No domestic publisher found (`internal_count = 0`)

**Missing data to flag**:
- [x] LDC classification — sourced from UN DESA (44 countries, December 2024 list)
- [x] SIDS classification — sourced from UN-OHRLLS (39 countries)

**SIDS coverage note**: The official UN SIDS list has 39 member states, but only 36 appear in the Excluding Aves country table. Three Caribbean SIDS — Saint Kitts and Nevis (KNA), Saint Lucia (LCA), and Saint Vincent and the Grenadines (VCT) — have insufficient GBIF records in the no-aves dataset to produce a row. These countries do appear in the all-taxa source data with very small counts. The country table should note that 3 SIDS are absent due to low record counts rather than misclassification.
