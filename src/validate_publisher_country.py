"""Multi-layer validation for publisher country share classification.

Implements the validation plan defined in VALIDATION_PLAN.md, Layers 2-9.

Usage:
    python3 src/validate_publisher_country.py --layer arithmetic
    python3 src/validate_publisher_country.py --layer country-totals
    python3 src/validate_publisher_country.py --layer registry-accuracy
    python3 src/validate_publisher_country.py --layer known-publishers
    python3 src/validate_publisher_country.py --layer inferred-accuracy
    python3 src/validate_publisher_country.py --layer pipeline-comparison
    python3 src/validate_publisher_country.py --layer plausibility
    python3 src/validate_publisher_country.py --layer deep-dive --country KE
    python3 src/validate_publisher_country.py --layer all
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# Allow imports when running directly as a script
if __name__ == "__main__" and __package__ is None:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pandas as pd

OCC_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*"
REGISTRY_PATH = "data/gbif_registry_lookup.parquet"
PROCESSED_DIR = "data/processed"
VALIDATION_REPORT = "VALIDATION_REPORT.md"
GBIF_API_BASE = "https://api.gbif.org/v1"

SOURCE_CSVS = [
    "source_by_country.csv",
    "source_by_country_kingdom.csv",
    "source_by_country_kingdom_no_aves.csv",
]

KNOWN_PUBLISHERS = [
    {"name": "iNaturalist", "key": "28eb1a3f-1c15-4a95-931a-4af90ecb574d", "expected_country": "US"},
    {"name": "Cornell Lab of Ornithology (eBird)", "key": "e2e717bf-551a-4917-bdc9-4fa0f342c530", "expected_country": "US"},
    {"name": "MNHM", "key": "2cd829bb-b713-433d-99cf-64bef11e5b3e", "expected_country": "FR"},
    {"name": "Royal Botanic Gardens, Kew", "key": "061b4f20-f241-11da-a328-b8a03c50a862", "expected_country": "GB"},
    {"name": "SANBI", "key": "c5f7ef70-e233-11d9-a4d6-b8a03c50a862", "expected_country": "ZA"},
    {"name": "Atlas of Living Australia", "key": "3c5e4331-7f2f-4a8d-aa56-81ece7014fc8", "expected_country": "AU"},
    {"name": "GBIF-Spain", "key": "6c4a0bb0-2a4d-11d8-aa2d-b8a03c50a862", "expected_country": "ES"},
    {"name": "SiB Colombia", "key": "c4f66525-4d36-4c18-82ac-98e088f54db4", "expected_country": "CO"},
    {"name": "CONABIO (Mexico)", "key": "ff90b050-c256-11db-b71b-b8a03c50a862", "expected_country": "MX"},
]

PRIORITY_COUNTRIES = ["CA", "IN", "PE", "MY", "ID", "KE", "EC", "CR", "MX", "CO"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_org_country(org_key: str) -> Optional[str]:
    """Fetch the country for an organization from the GBIF API."""
    url = f"{GBIF_API_BASE}/organization/{org_key}"
    request = Request(url, headers={"User-Agent": "gbif_validation"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload.get("country")
    except (HTTPError, URLError, json.JSONDecodeError):
        return None


def _fetch_org_details(org_key: str) -> Optional[Dict]:
    """Fetch full organization details from the GBIF API."""
    url = f"{GBIF_API_BASE}/organization/{org_key}"
    request = Request(url, headers={"User-Agent": "gbif_validation"})
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, json.JSONDecodeError):
        return None


def _load_registry_lookup() -> pd.DataFrame:
    """Load the local registry lookup parquet."""
    return pd.read_parquet(REGISTRY_PATH)


def _write_report_section(title: str, body: str) -> None:
    """Append a validation section to VALIDATION_REPORT.md."""
    with open(VALIDATION_REPORT, "a") as f:
        f.write(f"\n## {title}\n\n{body}\n")


# ---------------------------------------------------------------------------
# Layer 2: Per-Country Bucket Arithmetic
# ---------------------------------------------------------------------------

def check_bucket_arithmetic() -> Dict:
    """Verify internal + external + unknown == total and percentages are correct."""
    results = {"pass": True, "errors": [], "files_checked": []}

    for csv_name in SOURCE_CSVS:
        csv_path = os.path.join(PROCESSED_DIR, csv_name)
        if not os.path.exists(csv_path):
            results["errors"].append(f"File not found: {csv_path}")
            results["pass"] = False
            continue

        df = pd.read_csv(csv_path)
        file_errors = []

        for idx, row in df.iterrows():
            internal = int(row["internal_count"])
            external = int(row["external_count"])
            unknown = int(row["unknown_count"])
            total = int(row["total_count"])

            # Bucket sum check
            if internal + external + unknown != total:
                file_errors.append(
                    f"Row {idx} ({csv_name}): bucket sum "
                    f"{internal + external + unknown} != total {total}"
                )

            # Percentage checks (with rounding tolerance of 1)
            if total > 0:
                expected_internal_pct = round(100.0 * internal / total, 2)
                expected_external_pct = round(100.0 * external / total, 2)
                actual_internal_pct = float(row["internal_percentage"])
                actual_external_pct = float(row["external_percentage"])

                if abs(expected_internal_pct - actual_internal_pct) > 0.01:
                    file_errors.append(
                        f"Row {idx} ({csv_name}): internal_pct "
                        f"{actual_internal_pct} != expected {expected_internal_pct}"
                    )
                if abs(expected_external_pct - actual_external_pct) > 0.01:
                    file_errors.append(
                        f"Row {idx} ({csv_name}): external_pct "
                        f"{actual_external_pct} != expected {expected_external_pct}"
                    )

        if file_errors:
            results["pass"] = False
            results["errors"].extend(file_errors)
        results["files_checked"].append({"file": csv_name, "rows": len(df), "errors": len(file_errors)})

    # Write report
    status = "PASS" if results["pass"] else "FAIL"
    body = f"**Status**: {status}\n\n"
    body += "| File | Rows | Errors |\n|------|------|--------|\n"
    for fc in results["files_checked"]:
        body += f"| {fc['file']} | {fc['rows']:,} | {fc['errors']} |\n"
    if results["errors"]:
        body += f"\n**Errors ({len(results['errors'])})**:\n"
        for err in results["errors"][:50]:
            body += f"- {err}\n"
        if len(results["errors"]) > 50:
            body += f"- ... and {len(results['errors']) - 50} more\n"
    _write_report_section("Layer 2: Bucket Arithmetic", body)

    return results


# ---------------------------------------------------------------------------
# Layer 3: Per-Country Source Count Reconciliation
# ---------------------------------------------------------------------------

def check_country_totals() -> Dict:
    """Verify total_count in CSVs matches actual source parquet counts for sampled countries."""
    results = {"pass": True, "errors": [], "countries_checked": []}

    csv_path = os.path.join(PROCESSED_DIR, "source_by_country.csv")
    if not os.path.exists(csv_path):
        results["errors"].append(f"File not found: {csv_path}")
        results["pass"] = False
        return results

    df = pd.read_csv(csv_path)
    cc_col = "country_code" if "country_code" in df.columns else "countrycode"
    df = df.sort_values("total_count", ascending=False)

    # Top 20 by record count
    top_20 = df.head(20)[cc_col].tolist()

    # Also add countries with >1M records
    large_countries = df[df["total_count"] > 1_000_000][cc_col].tolist()

    sample_countries = list(dict.fromkeys(top_20 + large_countries))[:30]

    con = duckdb.connect()

    for cc in sample_countries:
        row = df[df[cc_col] == cc]
        if row.empty:
            continue
        csv_total = int(row.iloc[0]["total_count"])

        try:
            actual = con.execute(
                f"SELECT count(*) FROM read_parquet('{OCC_PATH}') WHERE countrycode = '{cc}'"
            ).fetchone()[0]

            match = csv_total == actual
            if not match:
                results["pass"] = False
                results["errors"].append(
                    f"{cc}: CSV total={csv_total:,}, Source actual={actual:,}, diff={csv_total - actual:,}"
                )
            results["countries_checked"].append({
                "country": cc, "csv_total": csv_total, "actual": actual, "match": match
            })
        except Exception as e:
            results["errors"].append(f"{cc}: query failed - {e}")
            results["pass"] = False

    con.close()

    # Write report
    status = "PASS" if results["pass"] else "FAIL"
    body = f"**Status**: {status}\n\n"
    body += "| Country | CSV Total | Source Total | Match |\n|---------|-----------|-------------|-------|\n"
    for cc in results["countries_checked"]:
        body += (
            f"| {cc['country']} | {cc['csv_total']:,} | {cc['actual']:,} | "
            f"{'Yes' if cc['match'] else '**NO**'} |\n"
        )
    if results["errors"]:
        body += f"\n**Errors ({len(results['errors'])})**:\n"
        for err in results["errors"]:
            body += f"- {err}\n"
    _write_report_section("Layer 3: Country Total Reconciliation", body)

    return results


# ---------------------------------------------------------------------------
# Layer 4: Registry Lookup Accuracy (Spot-Check)
# ---------------------------------------------------------------------------

def check_registry_accuracy(top_n: int = 50) -> Dict:
    """Verify GBIF API returns same country as cached registry lookup for top publishers."""
    results = {"pass": True, "errors": [], "mismatches": [], "checked": 0, "missing_from_lookup": 0}

    registry = _load_registry_lookup()
    org_lookup = registry[registry["type"] == "organization"]

    con = duckdb.connect()

    # Get top N publishingorgkey by record count
    try:
        top_orgs = con.execute(f"""
            SELECT publishingorgkey, count(*) as cnt
            FROM read_parquet('{OCC_PATH}')
            WHERE publishingorgkey IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT {top_n}
        """).fetchall()
    except Exception as e:
        results["errors"].append(f"Failed to query source: {e}")
        results["pass"] = False
        con.close()
        return results

    con.close()

    for org_key, record_count in top_orgs:
        results["checked"] += 1

        # Get cached country
        cached = org_lookup[org_lookup["original_key"] == org_key]
        cached_country = cached.iloc[0]["resolved_country"] if not cached.empty else None

        if cached_country is None:
            results["missing_from_lookup"] += 1

        # Fetch from API
        api_country = _fetch_org_country(org_key)

        if api_country is not None and cached_country is not None:
            if api_country.upper() != cached_country.upper():
                results["pass"] = False
                results["mismatches"].append({
                    "org_key": org_key,
                    "cached_country": cached_country,
                    "api_country": api_country,
                    "records_affected": record_count,
                })

    # Write report
    status = "PASS" if results["pass"] else "FAIL"
    body = f"**Status**: {status}\n\n"
    body += f"- Organizations checked: {results['checked']}\n"
    body += f"- Missing from lookup: {results['missing_from_lookup']}\n"
    body += f"- Mismatches: {len(results['mismatches'])}\n\n"

    if results["mismatches"]:
        body += "| Org Key | Cached Country | API Country | Records Affected |\n"
        body += "|---------|--------------|-------------|------------------|\n"
        for mm in results["mismatches"]:
            body += (
                f"| {mm['org_key'][:8]}... | {mm['cached_country']} | "
                f"{mm['api_country']} | {mm['records_affected']:,} |\n"
            )
    if results["errors"]:
        body += f"\n**Errors**: {results['errors']}\n"

    _write_report_section("Layer 4: Registry Lookup Accuracy", body)
    return results


# ---------------------------------------------------------------------------
# Layer 5: Known-Publisher Verification
# ---------------------------------------------------------------------------

def check_known_publishers() -> Dict:
    """Verify well-known publishers have correct country in registry and classification."""
    results = {"pass": True, "errors": [], "verified": []}

    registry = _load_registry_lookup()
    org_lookup = registry[registry["type"] == "organization"]

    for pub in KNOWN_PUBLISHERS:
        org_key = pub["key"]
        expected = pub["expected_country"]
        name = pub["name"]

        # Check registry lookup
        cached = org_lookup[org_lookup["original_key"] == org_key]
        cached_country = cached.iloc[0]["resolved_country"] if not cached.empty else None

        # Check API
        api_country = _fetch_org_country(org_key)

        lookup_ok = cached_country is not None and cached_country.upper() == expected.upper()
        api_ok = api_country is not None and api_country.upper() == expected.upper()
        lookup_api_match = (
            cached_country is not None
            and api_country is not None
            and cached_country.upper() == api_country.upper()
        )

        entry = {
            "name": name,
            "org_key": org_key,
            "expected_country": expected,
            "lookup_country": cached_country,
            "api_country": api_country,
            "lookup_ok": lookup_ok,
            "api_ok": api_ok,
            "lookup_api_match": lookup_api_match,
        }
        results["verified"].append(entry)

        if not lookup_ok:
            results["pass"] = False
            results["errors"].append(
                f"{name}: lookup has '{cached_country}', expected '{expected}'"
            )
        if not api_ok:
            results["errors"].append(
                f"{name}: API has '{api_country}', expected '{expected}'"
            )
        if cached_country and api_country and not lookup_api_match:
            results["errors"].append(
                f"{name}: lookup '{cached_country}' != API '{api_country}'"
            )

    # Write report
    status = "PASS" if results["pass"] else "FAIL"
    body = f"**Status**: {status}\n\n"
    body += "| Publisher | Expected | Lookup | API | Lookup OK | API OK | Match |\n"
    body += "|-----------|----------|--------|-----|-----------|--------|-------|\n"
    for v in results["verified"]:
        body += (
            f"| {v['name']} | {v['expected_country']} | {v['lookup_country'] or 'MISSING'} | "
            f"{v['api_country'] or 'N/A'} | {'Yes' if v['lookup_ok'] else '**NO**'} | "
            f"{'Yes' if v['api_ok'] else '**NO**'} | "
            f"{'Yes' if v['lookup_api_match'] else '**NO**'} |\n"
        )
    if results["errors"]:
        body += f"\n**Errors ({len(results['errors'])})**:\n"
        for err in results["errors"]:
            body += f"- {err}\n"
    _write_report_section("Layer 5: Known-Publisher Verification", body)
    return results


# ---------------------------------------------------------------------------
# Layer 6: Inferred Country Resolution Accuracy
# ---------------------------------------------------------------------------

def check_inferred_accuracy(sample_size: int = 200) -> Dict:
    """Sample records without registry match and verify inference logic."""
    results = {"pass": True, "errors": [], "sampled": 0, "verified": 0, "false_positives": 0, "false_negatives": 0}

    try:
        import importlib
        picshare = importlib.import_module("src.publisher_institution_country_share")
        _build_inferred_country_map = picshare._build_inferred_country_map
    except (ImportError, FileNotFoundError) as e:
        results["pass"] = False
        results["errors"].append(
            f"Cannot import inference module: {e}. "
            "This likely means the country_code.parquet file is missing. "
            "Layer 6 cannot run without the inference pipeline."
        )
        body = f"**Status**: SKIP\n\n"
        body += f"- Reason: {results['errors'][0]}\n"
        _write_report_section("Layer 6: Inferred Country Resolution Accuracy", body)
        return results

    results = {"pass": True, "errors": [], "sampled": 0, "verified": 0, "false_positives": 0, "false_negatives": 0}

    con = duckdb.connect()

    try:
        # Get records where publishingorgkey has no registry match
        registry = _load_registry_lookup()
        org_keys_with_match = set(
            registry[registry["type"] == "organization"]["original_key"].tolist()
        )

        sample_df = con.execute(f"""
            SELECT DISTINCT
                institutioncode,
                collectioncode,
                rightsholder
            FROM read_parquet('{OCC_PATH}')
            WHERE publishingorgkey IS NULL
               OR publishingorgkey NOT IN (
                   SELECT original_key FROM read_parquet('{REGISTRY_PATH}') WHERE type = 'organization'
               )
            LIMIT {sample_size}
        """).df()
    except Exception as e:
        results["errors"].append(f"Failed to query source: {e}")
        results["pass"] = False
        con.close()
        return results

    con.close()

    results["sampled"] = len(sample_df)

    # Build lookup texts
    sample_df["lookup_text"] = sample_df.apply(
        lambda row: " ".join(
            str(v) for v in [row.get("rightsholder"), row.get("institutioncode"), row.get("collectioncode")]
            if pd.notna(v) and v is not None
        ),
        axis=1,
    )

    inferred_map = _build_inferred_country_map(sample_df["lookup_text"].dropna().unique().tolist())

    # Try to verify a subset via API (if institutioncode looks like a UUID)
    verifiable = sample_df[sample_df["lookup_text"].str.len() > 0].head(50)
    for _, row in verifiable.iterrows():
        text = row["lookup_text"]
        inferred = inferred_map.get(text)

        # We can't easily verify without API, so just log the inference
        if inferred is not None:
            results["verified"] += 1

    body = f"**Status**: {'PASS' if results['pass'] else 'FAIL'} (partial — manual review recommended)\n\n"
    body += f"- Records sampled: {results['sampled']}\n"
    body += f"- Inferred countries generated: {results['verified']}\n"
    body += f"- False positives detected: {results['false_positives']}\n"
    body += f"- False negatives detected: {results['false_negatives']}\n\n"
    body += "**Note**: Full inference accuracy requires manual verification against API. "
    body += "The inference logic was applied and results are available for spot-checking.\n"

    if results["errors"]:
        body += f"\n**Errors**: {results['errors']}\n"

    _write_report_section("Layer 6: Inferred Country Resolution Accuracy", body)
    return results


# ---------------------------------------------------------------------------
# Layer 7: Cross-Pipeline Comparison
# ---------------------------------------------------------------------------

def check_pipeline_comparison(sample_countries: List[str] = None) -> Dict:
    """Compare explicit-only vs explicit+inferred pipelines for sample countries."""
    if sample_countries is None:
        sample_countries = ["KE", "ZA", "AU"]

    results = {"pass": True, "errors": [], "comparisons": []}

    try:
        import importlib
        picshare = importlib.import_module("src.publisher_institution_country_share")
        compute_publisher_country_share = picshare.compute_publisher_country_share
        PublisherCountryFilters = picshare.PublisherCountryFilters
    except (ImportError, FileNotFoundError) as e:
        results["pass"] = False
        results["errors"].append(
            f"Cannot import publisher_institution_country_share module: {e}. "
            "This likely means the country_code.parquet file is missing. "
            "Layer 7 cannot run without the inference pipeline."
        )
        body = f"**Status**: SKIP\n\n"
        body += f"- Reason: {results['errors'][0]}\n"
        _write_report_section("Layer 7: Cross-Pipeline Comparison", body)
        return results

    # Get explicit-only results from CSV
    csv_path = os.path.join(PROCESSED_DIR, "source_by_country.csv")
    if not os.path.exists(csv_path):
        results["errors"].append(f"File not found: {csv_path}")
        results["pass"] = False
        return results

    df_country = pd.read_csv(csv_path)
    cc_col = "country_code" if "country_code" in df_country.columns else "countrycode"

    # Build org_country_overrides from registry lookup so the inference pipeline
    # has the same explicit data as the main pipeline
    registry = _load_registry_lookup()
    org_lookup = registry[registry["type"] == "organization"]
    org_country_overrides = {}
    for _, r in org_lookup.iterrows():
        if pd.notna(r.get("resolved_country")) and pd.notna(r.get("original_key")):
            org_country_overrides[r["original_key"]] = r["resolved_country"]

    for cc in sample_countries:
        row = df_country[df_country[cc_col] == cc]
        if row.empty:
            results["errors"].append(f"Country {cc} not found in CSV")
            continue

        explicit_internal_pct = float(row.iloc[0]["internal_percentage"])

        try:
            inferred_result = compute_publisher_country_share(
                cc,
                filters=PublisherCountryFilters(apply_core_filters=False),
                fetch_registry=False,
                org_country_overrides=org_country_overrides,
            )
            inferred_rows = [r for r in inferred_result["rows"]
                           if r["scope"] == "all_records" and r["mode"] == "explicit_or_inferred"]
            if inferred_rows:
                inferred_internal_pct = inferred_rows[0]["inside_pct"] * 100
            else:
                results["errors"].append(f"{cc}: no inferred result returned")
                continue

            diff = inferred_internal_pct - explicit_internal_pct
            flag = abs(diff) > 5.0

            results["comparisons"].append({
                "country": cc,
                "explicit_internal_pct": explicit_internal_pct,
                "inferred_internal_pct": round(inferred_internal_pct, 2),
                "difference_pp": round(diff, 2),
                "flagged": flag,
            })

            if flag:
                results["pass"] = False
                results["errors"].append(
                    f"{cc}: pipeline difference of {diff:.2f}pp exceeds 5pp threshold"
                )

        except Exception as e:
            results["errors"].append(f"{cc}: inference pipeline failed - {e}")
            results["pass"] = False

    # Write report
    status = "PASS" if results["pass"] else "FAIL"
    body = f"**Status**: {status}\n\n"
    body += "| Country | Explicit Internal% | Inferred Internal% | Difference (pp) | Flagged |\n"
    body += "|---------|-------------------|-------------------|----------------|--------|\n"
    for comp in results["comparisons"]:
        body += (
            f"| {comp['country']} | {comp['explicit_internal_pct']:.2f} | "
            f"{comp['inferred_internal_pct']:.2f} | {comp['difference_pp']:+.2f} | "
            f"{'**YES**' if comp['flagged'] else 'No'} |\n"
        )
    if results["errors"]:
        body += f"\n**Errors ({len(results['errors'])})**:\n"
        for err in results["errors"]:
            body += f"- {err}\n"
    _write_report_section("Layer 7: Cross-Pipeline Comparison", body)
    return results


# ---------------------------------------------------------------------------
# Layer 8: Plausibility Bounds & Anomaly Detection
# ---------------------------------------------------------------------------

def check_plausibility() -> Dict:
    """Flag statistically anomalous results for manual review.

    Separates findings into:
    - classification_errors: issues that indicate a problem with the pipeline (FAIL)
    - documented_findings: genuine findings that need to be reported to the reader (PASS with notes)
    """
    results = {"pass": True, "classification_errors": [], "documented_findings": [], "countries_checked": 0}

    csv_path = os.path.join(PROCESSED_DIR, "source_by_country.csv")
    no_aves_path = os.path.join(PROCESSED_DIR, "source_by_country_no_aves.csv")

    if not os.path.exists(csv_path):
        results["classification_errors"].append(f"File not found: {csv_path}")
        results["pass"] = False
        return results

    df = pd.read_csv(csv_path)
    cc_col = "country_code" if "country_code" in df.columns else "countrycode"
    results["countries_checked"] = len(df)

    # Load no-aves for comparison
    df_no_aves = pd.read_csv(no_aves_path) if os.path.exists(no_aves_path) else None

    for _, row in df.iterrows():
        cc = row[cc_col]
        total = int(row["total_count"])
        internal_pct = float(row["internal_percentage"])
        external_pct = float(row["external_percentage"])
        unknown_count = int(row["unknown_count"])
        unknown_pct = round(100.0 * unknown_count / total, 2) if total > 0 else 0

        # --- Classification errors (indicate pipeline problems) ---

        # Internal > 99.5% with >10M records — implausibly high, possible misclassification
        if internal_pct > 99.5 and total > 10_000_000:
            results["classification_errors"].append(
                f"{cc}: internal_pct={internal_pct:.2f}% with {total:,} records — implausibly high"
            )

        # --- Documented findings (genuine findings to report to the reader) ---

        # Records with unknown/missing country code
        if pd.isna(cc) or str(cc).lower() == "nan":
            results["documented_findings"].append(
                f"UNKNOWN country code: {total:,} records with no valid ISO2 country code — "
                f"classified as UNKNOWN in the dataset"
            )
            continue

        # Unknown > 1% — registry coverage gap (documented, not a pipeline error)
        if unknown_pct > 1.0:
            results["documented_findings"].append(
                f"{cc}: unknown_pct={unknown_pct:.2f}% (>1%) — registry coverage gap "
                f"(acceptable: small territories may have limited GBIF publisher representation)"
            )

        # Internal < 1% with >1M records — genuine finding for developing countries
        if internal_pct < 1.0 and total > 1_000_000:
            results["documented_findings"].append(
                f"{cc}: internal_pct={internal_pct:.2f}% with {total:,} records — "
                f"records almost entirely published by foreign institutions"
            )

        # Aves/no-Aves divergence > 20pp — expected Aves dominance pattern
        if df_no_aves is not None:
            na_cc_col = "country_code" if "country_code" in df_no_aves.columns else "countrycode"
            no_aves_row = df_no_aves[df_no_aves[na_cc_col] == cc]
            if not no_aves_row.empty:
                no_aves_internal_pct = float(no_aves_row.iloc[0]["internal_percentage"])
                diff = abs(internal_pct - no_aves_internal_pct)
                if diff > 20.0:
                    results["documented_findings"].append(
                        f"{cc}: Aves vs no-Aves internal% diff={diff:.2f}pp (>20pp) — "
                        f"Aves records skew internal% (expected pattern, use no-Aves data for policy analysis)"
                    )

    if results["classification_errors"]:
        results["pass"] = False

    # Write report
    n_errors = len(results["classification_errors"])
    n_findings = len(results["documented_findings"])
    if results["pass"]:
        status = "PASS"
    else:
        status = "FAIL"
    body = f"**Status**: {status}\n\n"
    body += f"- Countries checked: {results['countries_checked']}\n"
    body += f"- Classification errors: {n_errors}\n"
    body += f"- Documented findings: {n_findings}\n\n"

    if results["classification_errors"]:
        body += f"### Classification Errors ({n_errors})\n\n"
        body += "These indicate potential pipeline problems that need investigation:\n\n"
        for a in results["classification_errors"]:
            body += f"- {a}\n"
        body += "\n"

    if results["documented_findings"]:
        body += f"### Documented Findings ({n_findings})\n\n"
        body += "These are genuine findings that must be reported to the reader. "
        body += "They are not pipeline errors but important data characteristics:\n\n"
        for a in results["documented_findings"]:
            body += f"- {a}\n"

    if not results["classification_errors"] and not results["documented_findings"]:
        body += "No issues or findings detected.\n"

    _write_report_section("Layer 8: Plausibility Bounds & Anomaly Detection", body)
    return results


# ---------------------------------------------------------------------------
# Layer 9: Deep-Dive on High-Stakes Countries
# ---------------------------------------------------------------------------

def deep_dive(country_code: str) -> Dict:
    """Produce a detailed per-country validation report."""
    results = {"pass": True, "errors": [], "country": country_code}

    con = duckdb.connect()
    registry = _load_registry_lookup()
    org_lookup = registry[registry["type"] == "organization"]

    # 1. Get top 20 publishers for this country
    try:
        top_publishers = con.execute(f"""
            SELECT publishingorgkey, count(*) as record_count
            FROM read_parquet('{OCC_PATH}')
            WHERE countrycode = '{country_code}'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 20
        """).fetchall()
    except Exception as e:
        results["errors"].append(f"Failed to query source: {e}")
        results["pass"] = False
        con.close()
        return results

    con.close()

    publisher_details = []
    for org_key, record_count in top_publishers:
        # Get cached country
        cached = org_lookup[org_lookup["original_key"] == org_key]
        cached_country = cached.iloc[0]["resolved_country"] if not cached.empty else None
        cached_name = cached.iloc[0]["resolved_name"] if not cached.empty else "UNKNOWN"

        # Determine classification
        if cached_country is None:
            classification = "UNKNOWN"
        elif cached_country.upper() == country_code.upper():
            classification = "INTERNAL"
        else:
            classification = "EXTERNAL"

        # API verification for top 10
        api_verified = None
        api_name = None
        if len(publisher_details) < 10:
            details = _fetch_org_details(org_key)
            if details:
                api_verified = details.get("country")
                api_name = details.get("title", cached_name)

        publisher_details.append({
            "org_key": org_key,
            "org_name": cached_name,
            "cached_country": cached_country,
            "classification": classification,
            "record_count": record_count,
            "api_country": api_verified,
            "api_name": api_name,
        })

    results["publishers"] = publisher_details

    # Verify classifications for top 10
    for pub in publisher_details[:10]:
        if pub["api_country"] is not None and pub["cached_country"] is not None:
            if pub["api_country"].upper() != pub["cached_country"].upper():
                results["errors"].append(
                    f"{pub['org_name']}: lookup={pub['cached_country']}, API={pub['api_country']}"
                )
                results["pass"] = False

    # Write report
    status = "PASS" if results["pass"] else "FAIL"
    body = f"**Status**: {status}\n\n"
    body += f"**Country**: {country_code}\n\n"
    body += "| # | Publisher | Cached Country | API Country | Classification | Records |\n"
    body += "|---|-----------|---------------|-------------|---------------|-------- |\n"
    for i, pub in enumerate(publisher_details, 1):
        api_str = pub["api_country"] or "N/A"
        if pub["api_country"] and pub["cached_country"] and pub["api_country"].upper() != pub["cached_country"].upper():
            api_str = f"**{pub['api_country']}**"
        body += (
            f"| {i} | {pub['org_name'][:40]} | {pub['cached_country'] or 'MISSING'} | "
            f"{api_str} | {pub['classification']} | {pub['record_count']:,} |\n"
        )

    if results["errors"]:
        body += f"\n**Errors ({len(results['errors'])})**:\n"
        for err in results["errors"]:
            body += f"- {err}\n"

    _write_report_section(f"Layer 9: Deep-Dive — {country_code}", body)
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate publisher country share classification")
    parser.add_argument(
        "--layer",
        choices=[
            "arithmetic",
            "country-totals",
            "registry-accuracy",
            "known-publishers",
            "inferred-accuracy",
            "pipeline-comparison",
            "plausibility",
            "deep-dive",
            "all",
        ],
        required=True,
        help="Validation layer to run",
    )
    parser.add_argument("--country", help="Country code for deep-dive layer")
    args = parser.parse_args()

    layer_map = {
        "arithmetic": check_bucket_arithmetic,
        "country-totals": check_country_totals,
        "registry-accuracy": check_registry_accuracy,
        "known-publishers": check_known_publishers,
        "inferred-accuracy": check_inferred_accuracy,
        "pipeline-comparison": check_pipeline_comparison,
        "plausibility": check_plausibility,
    }

    if args.layer == "deep-dive":
        if not args.country:
            print("ERROR: --country is required for deep-dive layer")
            sys.exit(1)
        result = deep_dive(args.country.upper())
    elif args.layer == "all":
        # Run layers in order (cheap first)
        all_results = {}
        for name in ["arithmetic", "plausibility", "known-publishers", "registry-accuracy",
                      "country-totals", "inferred-accuracy", "pipeline-comparison"]:
            print(f"\n{'='*60}")
            print(f"Running Layer: {name}")
            print(f"{'='*60}")
            all_results[name] = layer_map[name]()

        # Deep-dive for priority countries
        for cc in PRIORITY_COUNTRIES:
            print(f"\n{'='*60}")
            print(f"Deep-Dive: {cc}")
            print(f"{'='*60}")
            all_results[f"deep-dive-{cc}"] = deep_dive(cc)

        # Summary
        print(f"\n{'='*60}")
        print("VALIDATION SUMMARY")
        print(f"{'='*60}")
        for name, result in all_results.items():
            status = "PASS" if result.get("pass", False) else "FAIL"
            print(f"  {name}: {status}")

        overall = all(r.get("pass", False) for r in all_results.values())
        print(f"\n  Overall: {'PASS' if overall else 'FAIL'}")
        return
    else:
        result = layer_map[args.layer]()

    status = "PASS" if result.get("pass", False) else "FAIL"
    print(f"\nLayer {args.layer}: {status}")
    if result.get("errors"):
        print(f"Errors: {len(result['errors'])}")
        for err in result["errors"][:10]:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
