# Changelog

All notable changes to the GBIF Occurrence Source Analysis project.

## [Unreleased] — 2026-04-16

### Added
- `src/publisher_institution_country_share.py` — Modular computation of publisher country share for any country, with explicit (GBIF Registry) and inferred (text parsing) country resolution, scoped results (all records vs exclude Aves), and percentage breakdowns (inside/outside/missing).
- `src/generate_cbd_report.py` — Generates `CBD_Publisher_Country_Share_Report.md` from the CBD party summary CSVs, with formatted tables and interpretive commentary.
- `src/convert_cbd_report_to_docx.py` — Converts the markdown report to Word (.docx) using python-docx, with proper table styling, heading hierarchy, and YAML front matter handling.
- `tests/test_publisher_institution_country_share.py` — Unit tests for the publisher country share module using in-memory DuckDB fixtures for Kenya (KE).
- `CBD_Publisher_Country_Share_Report.md` — Generated markdown report on CBD Party publisher country shares, with tables for UN Region, Intermediate Region, Development Status, and Income Group (all taxa and excluding Aves).
- `CBD_Publisher_Country_Share_Report.docx` — Word version of the report.
- `CHANGELOG.md` — This file.

### Fixed
- `src/analyze_cbd_parties.py` — Added `dropna=False` to all `groupby()` calls. Previously, pandas default `dropna=True` silently dropped Europe, Asia, and Oceania from the Excluding Aves CBD summaries because these regions have `None` in `un_intermediate_region_name`.
- Updated CBD party summary CSV/Parquet files with corrected data now including all five UN regions in both all-taxa and no-aves breakdowns.

### Changed
- `VALIDATION_REPORT.md` — Updated to reflect current data totals.
- `src/validate_analysis.py` — Updated validation script with improved formatting.

## [0.3.0] — 2026-03-30

### Added
- AI usage acknowledgment in README.
- Detailed source classification logic in README methodology section.
- Refined README objectives and formatting.

### Changed
- README updated with comprehensive methodology documentation including SQL classification logic, field mapping, and limitations discussion.

## [0.2.0] — 2026-03-27

### Added
- Country-level analysis excluding Aves occurrences (`source_by_country_no_aves`).
- No Aves version of CBD party summaries.
- Parquet versions of all CBD party summary tables alongside CSVs.
- CSV version of GBIF registry lookup.
- Validation report comparing processed totals against raw source counts.
- `src/enrich_source_distribution.py` — Enriches source distribution with CBD party status and World Bank income group.
- `src/validate_analysis.py` — Cross-checks processed totals against raw source.
- `data-raw/country_overlay.csv` — CBD party status and WB income group overlay.
- `data-raw/countrycode.csv` — Country metadata with UN regional classifications.

### Changed
- Updated processing scripts to support country-level analysis excluding Aves.
- Removed scripts not involved in producing `data/processed` outputs.

## [0.1.0] — 2026-03-27

### Added
- Initial commit with core analysis pipeline.
- `src/calculate_source_distribution.py` — Classifies GBIF occurrence records as Internal/External/Unknown source using registry lookup.
- `src/create_registry_lookup.py` — Builds local lookup table from GBIF registry data (organizations, institutions, collections).
- `src/download_gbif_registry.py` — Downloads complete GBIF registry via API.
- `src/analyze_cbd_parties.py` — Generates CBD party summary statistics.
- `data/gbif_registry_lookup.parquet` — Local registry lookup table.
- `data/processed/source_by_country.csv/.parquet` — Source distribution by country.
- `data/processed/source_by_country_kingdom.csv/.parquet` — Source distribution by country and kingdom.
- `data/processed/source_by_country_kingdom_no_aves.csv/.parquet` — Source distribution excluding Aves.
- `data-raw/registry/` — Raw GBIF registry data (organizations, institutions, collections).
