"""Unit tests for validate_publisher_country.py — Layers 2, 5, 8.

These tests use small fixture CSV data and mock API calls so they don't
require the source parquet.
"""
import os
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest

from src.validate_publisher_country import (
    check_bucket_arithmetic,
    check_known_publishers,
    check_plausibility,
    KNOWN_PUBLISHERS,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def processed_dir(tmp_path, monkeypatch):
    """Create a temp processed dir with fixture CSVs for Layers 2 and 8."""
    proc = tmp_path / "processed"
    proc.mkdir()
    monkeypatch.setattr("src.validate_publisher_country.PROCESSED_DIR", str(proc))

    # source_by_country.csv
    df_country = pd.DataFrame([
        {
            "country_code": "US",
            "internal_count": 800_000,
            "external_count": 150_000,
            "unknown_count": 50_000,
            "total_count": 1_000_000,
            "internal_percentage": 80.0,
            "external_percentage": 15.0,
        },
        {
            "country_code": "KE",
            "internal_count": 50_000,
            "external_count": 400_000,
            "unknown_count": 50_000,
            "total_count": 500_000,
            "internal_percentage": 10.0,
            "external_percentage": 80.0,
        },
        {
            "country_code": "NO",
            "internal_count": 950_000,
            "external_count": 4_000,
            "unknown_count": 1_000,
            "total_count": 955_000,
            "internal_percentage": round(100.0 * 950_000 / 955_000, 2),
            "external_percentage": round(100.0 * 4_000 / 955_000, 2),
        },
    ])
    df_country.to_csv(proc / "source_by_country.csv", index=False)

    # source_by_country_kingdom.csv
    df_kingdom = pd.DataFrame([
        {
            "country_code": "US",
            "kingdom": "Animalia",
            "internal_count": 500_000,
            "external_count": 100_000,
            "unknown_count": 30_000,
            "total_count": 630_000,
            "internal_percentage": round(100.0 * 500_000 / 630_000, 2),
            "external_percentage": round(100.0 * 100_000 / 630_000, 2),
        },
    ])
    df_kingdom.to_csv(proc / "source_by_country_kingdom.csv", index=False)

    # source_by_country_kingdom_no_aves.csv
    df_no_aves = pd.DataFrame([
        {
            "country_code": "US",
            "kingdom": "Animalia",
            "internal_count": 400_000,
            "external_count": 80_000,
            "unknown_count": 20_000,
            "total_count": 500_000,
            "internal_percentage": 80.0,
            "external_percentage": 16.0,
        },
    ])
    df_no_aves.to_csv(proc / "source_by_country_kingdom_no_aves.csv", index=False)

    # source_by_country_no_aves.csv for plausibility check
    df_country_no_aves = pd.DataFrame([
        {
            "country_code": "US",
            "internal_count": 600_000,
            "external_count": 120_000,
            "unknown_count": 30_000,
            "total_count": 750_000,
            "internal_percentage": 80.0,
            "external_percentage": 16.0,
        },
        {
            "country_code": "KE",
            "internal_count": 40_000,
            "external_count": 350_000,
            "unknown_count": 10_000,
            "total_count": 400_000,
            "internal_percentage": 10.0,
            "external_percentage": 87.5,
        },
        {
            "country_code": "NO",
            "internal_count": 940_000,
            "external_count": 4_000,
            "unknown_count": 1_000,
            "total_count": 945_000,
            "internal_percentage": round(100.0 * 940_000 / 945_000, 2),
            "external_percentage": round(100.0 * 4_000 / 945_000, 2),
        },
    ])
    df_country_no_aves.to_csv(proc / "source_by_country_no_aves.csv", index=False)

    # Minimal VALIDATION_REPORT.md
    report_path = tmp_path / "VALIDATION_REPORT.md"
    report_path.write_text("# Validation Report\n")
    monkeypatch.setattr("src.validate_publisher_country.VALIDATION_REPORT", str(report_path))

    return proc


# ---------------------------------------------------------------------------
# Layer 2: Bucket Arithmetic
# ---------------------------------------------------------------------------

class TestBucketArithmetic:

    def test_valid_data_passes(self, processed_dir):
        result = check_bucket_arithmetic()
        assert result["pass"] is True
        assert result["errors"] == []

    def test_bad_bucket_sum_fails(self, processed_dir):
        # Corrupt one row
        csv_path = processed_dir / "source_by_country.csv"
        df = pd.read_csv(csv_path)
        df.loc[0, "internal_count"] = 999  # wrong value
        df.to_csv(csv_path, index=False)

        result = check_bucket_arithmetic()
        assert result["pass"] is False
        assert any("bucket sum" in e for e in result["errors"])

    def test_bad_percentage_fails(self, processed_dir):
        csv_path = processed_dir / "source_by_country.csv"
        df = pd.read_csv(csv_path)
        df.loc[0, "internal_percentage"] = 50.0  # wrong value
        df.to_csv(csv_path, index=False)

        result = check_bucket_arithmetic()
        assert result["pass"] is False
        assert any("internal_pct" in e for e in result["errors"])


# ---------------------------------------------------------------------------
# Layer 5: Known Publishers
# ---------------------------------------------------------------------------

class TestKnownPublishers:

    @patch("src.validate_publisher_country._fetch_org_country")
    @patch("src.validate_publisher_country._load_registry_lookup")
    def test_all_correct_passes(self, mock_lookup, mock_api, processed_dir):
        # Build a registry lookup that matches all expected countries
        rows = []
        for pub in KNOWN_PUBLISHERS:
            rows.append({
                "original_key": pub["key"],
                "resolved_name": pub["name"],
                "resolved_country": pub["expected_country"],
                "type": "organization",
            })
        mock_lookup.return_value = pd.DataFrame(rows)

        # API returns same as expected
        def api_side_effect(key):
            for pub in KNOWN_PUBLISHERS:
                if pub["key"] == key:
                    return pub["expected_country"]
            return None
        mock_api.side_effect = api_side_effect

        result = check_known_publishers()
        assert result["pass"] is True

    @patch("src.validate_publisher_country._fetch_org_country")
    @patch("src.validate_publisher_country._load_registry_lookup")
    def test_mismatch_fails(self, mock_lookup, mock_api, processed_dir):
        rows = []
        for pub in KNOWN_PUBLISHERS:
            rows.append({
                "original_key": pub["key"],
                "resolved_name": pub["name"],
                "resolved_country": "XX",  # wrong country
                "type": "organization",
            })
        mock_lookup.return_value = pd.DataFrame(rows)
        mock_api.return_value = "US"

        result = check_known_publishers()
        assert result["pass"] is False


# ---------------------------------------------------------------------------
# Layer 8: Plausibility
# ---------------------------------------------------------------------------

class TestPlausibility:

    def test_no_anomalies_passes(self, processed_dir):
        # Fix all rows so no anomalies are triggered
        csv_path = processed_dir / "source_by_country.csv"
        df = pd.read_csv(csv_path)
        # US: reduce unknown to < 1%
        df.loc[0, "unknown_count"] = 500
        df.loc[0, "total_count"] = 950_500
        df.loc[0, "internal_percentage"] = round(100.0 * 800_000 / 950_500, 2)
        df.loc[0, "external_percentage"] = round(100.0 * 150_000 / 950_500, 2)
        # KE: reduce unknown to < 1%
        df.loc[1, "unknown_count"] = 500
        df.loc[1, "total_count"] = 450_500
        df.loc[1, "internal_percentage"] = round(100.0 * 50_000 / 450_500, 2)
        df.loc[1, "external_percentage"] = round(100.0 * 400_000 / 450_500, 2)
        df.to_csv(csv_path, index=False)

        # Also fix no-aves
        csv_no_aves = processed_dir / "source_by_country_no_aves.csv"
        df_na = pd.read_csv(csv_no_aves)
        df_na.loc[0, "unknown_count"] = 500
        df_na.loc[0, "total_count"] = 750_500
        df_na.loc[0, "internal_percentage"] = round(100.0 * 600_000 / 750_500, 2)
        df_na.loc[0, "external_percentage"] = round(100.0 * 150_000 / 750_500, 2)
        df_na.loc[1, "unknown_count"] = 500
        df_na.loc[1, "total_count"] = 390_500
        df_na.loc[1, "internal_percentage"] = round(100.0 * 40_000 / 390_500, 2)
        df_na.loc[1, "external_percentage"] = round(100.0 * 350_000 / 390_500, 2)
        df_na.to_csv(csv_no_aves, index=False)

        result = check_plausibility()
        assert result["pass"] is True

    def test_high_unknown_flagged(self, processed_dir):
        # KE has unknown_pct = 50_000/500_000 = 10% which is > 1%
        result = check_plausibility()
        # Unknown > 1% is a documented finding, not a classification error
        assert result["pass"] is True
        assert any("registry coverage gap" in a for a in result["documented_findings"])

    def test_low_internal_flagged(self, processed_dir):
        # Add a country with internal_pct < 1% and >1M records
        csv_path = processed_dir / "source_by_country.csv"
        df = pd.read_csv(csv_path)
        new_row = {
            "country_code": "XX",
            "internal_count": 5_000,
            "external_count": 2_000_000,
            "unknown_count": 0,
            "total_count": 2_005_000,
            "internal_percentage": round(100.0 * 5_000 / 2_005_000, 2),
            "external_percentage": round(100.0 * 2_000_000 / 2_005_000, 2),
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(csv_path, index=False)

        result = check_plausibility()
        # Low internal% is a documented finding, not a classification error
        assert any("foreign institutions" in a for a in result["documented_findings"])

    def test_aves_no_aves_divergence_flagged(self, processed_dir):
        # Make KE have a huge divergence between all-taxa and no-aves
        csv_path = processed_dir / "source_by_country.csv"
        df = pd.read_csv(csv_path)
        # KE all-taxa: internal = 10%
        df.loc[1, "internal_count"] = 50_000
        df.loc[1, "external_count"] = 400_000
        df.loc[1, "unknown_count"] = 50_000
        df.loc[1, "total_count"] = 500_000
        df.loc[1, "internal_percentage"] = 10.0
        df.loc[1, "external_percentage"] = 80.0
        df.to_csv(csv_path, index=False)

        csv_no_aves = processed_dir / "source_by_country_no_aves.csv"
        df_na = pd.read_csv(csv_no_aves)
        # KE no-aves: internal = 60% — 50pp divergence
        df_na.loc[1, "internal_count"] = 240_000
        df_na.loc[1, "external_count"] = 150_000
        df_na.loc[1, "unknown_count"] = 10_000
        df_na.loc[1, "total_count"] = 400_000
        df_na.loc[1, "internal_percentage"] = 60.0
        df_na.loc[1, "external_percentage"] = 37.5
        df_na.to_csv(csv_no_aves, index=False)

        result = check_plausibility()
        # Aves skew is a documented finding, not a classification error
        assert any("Aves" in a for a in result["documented_findings"])
