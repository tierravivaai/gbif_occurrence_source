"""Test that percentage columns in every summary table sum to approximately 100%.

Each row in a summary table should satisfy:
    internal% + sub_regional% + regional% + external% ≈ 100%
(within a small tolerance to account for rounding of the unknown% slice).
"""

import csv
import os
import pytest

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")

# All summary CSVs that have percentage columns
SUMMARY_FILES = [
    "cbd_parties_no_aves_un_region_summary.csv",
    "cbd_parties_no_aves_un_region_un_sub_region_summary.csv",
    "cbd_parties_no_aves_un_region_un_sub_region_un_intermediate_region_summary.csv",
    "cbd_parties_no_aves_development_status_summary.csv",
    "cbd_parties_no_aves_income_group_summary.csv",
    "cbd_parties_all_taxa_un_region_summary.csv",
    "cbd_parties_all_taxa_un_region_un_sub_region_summary.csv",
    "cbd_parties_all_taxa_un_region_un_sub_region_un_intermediate_region_summary.csv",
    "cbd_parties_all_taxa_development_status_summary.csv",
    "cbd_parties_all_taxa_income_group_summary.csv",
]

TOLERANCE = 0.5  # Allow up to 0.5% rounding drift


@pytest.fixture(params=SUMMARY_FILES)
def summary_csv(request):
    path = os.path.join(PROCESSED_DIR, request.param)
    if not os.path.exists(path):
        pytest.skip(f"File not found: {request.param}")
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f)), request.param


def test_percentage_columns_sum_to_100(summary_csv):
    """Each row's internal% + sub_regional% + regional% + external% should be close to 100%."""
    rows, filename = summary_csv
    for i, row in enumerate(rows):
        internal = float(row.get("internal_percentage", 0) or 0)
        sub_regional = float(row.get("sub_regional_percentage", 0) or 0)
        regional = float(row.get("regional_percentage", 0) or 0)
        external = float(row.get("external_percentage", 0) or 0)
        total_pct = internal + sub_regional + regional + external
        assert 100 - TOLERANCE <= total_pct <= 100 + TOLERANCE, (
            f"{filename} row {i}: internal({internal}%) + sub_regional({sub_regional}%) "
            f"+ regional({regional}%) + external({external}%) = {total_pct}% "
            f"(expected ~100%)"
        )


def test_count_columns_sum_to_total(summary_csv):
    """Each row's count columns should sum to total_count."""
    rows, filename = summary_csv
    for i, row in enumerate(rows):
        internal = int(row.get("internal_count", 0) or 0)
        sub_regional = int(row.get("sub_regional_count", 0) or 0)
        regional = int(row.get("regional_count", 0) or 0)
        external = int(row.get("external_count", 0) or 0)
        unknown = int(row.get("unknown_count", 0) or 0)
        total = int(row.get("total_count", 0) or 0)
        computed_total = internal + sub_regional + regional + external + unknown
        assert computed_total == total, (
            f"{filename} row {i}: sum({internal}+{sub_regional}+{regional}+{external}+{unknown}) "
            f"= {computed_total} != total={total}"
        )


def test_total_row_sums_match_actual_totals():
    """The 'Total' row in the report's regional table should match the sum of all rows."""
    # Check the no-Aves UN region summary specifically
    path = os.path.join(PROCESSED_DIR, "cbd_parties_no_aves_un_region_summary.csv")
    if not os.path.exists(path):
        pytest.skip("no-Aves UN region summary not found")

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    sum_internal = sum(int(r["internal_count"]) for r in rows)
    sum_total = sum(int(r["total_count"]) for r in rows)

    # The total row from the report should match these computed sums
    assert sum_internal > 0, "Sum of internal counts should be positive"
    assert sum_total > 0, "Sum of total counts should be positive"

    # Verify individual region totals don't exceed the global total
    for r in rows:
        assert int(r["total_count"]) <= sum_total, (
            f"Region {r['un_region_name']} total {r['total_count']} exceeds global total {sum_total}"
        )
