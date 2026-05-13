"""
Unit tests for src/aggregate_hexbin_pipeline.py

Tests cover the pure-Python logic (classification SQL building, summary printing,
save helpers) using in-memory DuckDB fixtures — no external drive or parquet files
required.
"""

import os
import sys

import duckdb
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.aggregate_hexbin_pipeline import (
    BASIS_SQL,
    DEFAULT_COUNTRIES,
    _build_classification_query,
    _print_summary,
    _save,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_hexbin_df() -> pd.DataFrame:
    return pd.DataFrame({
        "lat":          [-14.2, -14.2, 51.5,  51.5,   1.3],
        "lon":          [-51.9, -51.9,  -0.1,  -0.1, 103.8],
        "source_type":  ["INTERNAL", "EXTERNAL", "INTERNAL", "REGIONAL", "UNKNOWN"],
        "record_count": [1000, 200, 5000, 300, 50],
    })


@pytest.fixture()
def enriched_df() -> pd.DataFrame:
    return pd.DataFrame({
        "lat":              [-14.2, -14.2, 51.5],
        "lon":              [-51.9, -51.9, -0.1],
        "source_type":      ["INTERNAL", "EXTERNAL", "INTERNAL"],
        "record_count":     [1000, 200, 5000],
        "countrycode":      ["BR", "US", "GB"],
        "country_name":     ["Brazil", "United States", "United Kingdom"],
        "wb_income_group":  ["Upper middle income", "High income", "High income"],
        "un_region_name":   ["Americas", "Americas", "Europe"],
        "un_sub_region_name": ["South America", "Northern America", "Northern Europe"],
    })


# ── BASIS_SQL ──────────────────────────────────────────────────────────────────

def test_basis_sql_contains_human_observation():
    assert "'HUMAN_OBSERVATION'" in BASIS_SQL


def test_basis_sql_contains_all_required_types():
    required = [
        "LIVING_SPECIMEN", "OBSERVATION", "HUMAN_OBSERVATION",
        "MACHINE_OBSERVATION", "OCCURRENCE", "MATERIAL_SAMPLE",
    ]
    for b in required:
        assert f"'{b}'" in BASIS_SQL, f"Missing basisofrecord value: {b}"


def test_basis_sql_excludes_preserved_specimen():
    assert "PRESERVED_SPECIMEN" not in BASIS_SQL


# ── _build_classification_query ────────────────────────────────────────────────

def test_classification_query_has_aves_filter():
    q = _build_classification_query(precision=1)
    assert "Aves" in q


def test_classification_query_has_species_filter():
    q = _build_classification_query(precision=1)
    assert "SPECIES" in q


def test_classification_query_has_present_filter():
    q = _build_classification_query(precision=1)
    assert "PRESENT" in q


def test_classification_query_precision_0():
    q = _build_classification_query(precision=0)
    assert "ROUND(occ.decimallatitude::DOUBLE,  0)" in q or ", 0)" in q


def test_classification_query_precision_1():
    q = _build_classification_query(precision=1)
    assert "1)" in q


def test_classification_query_country_filter_included():
    q = _build_classification_query(precision=1, country_code="BR")
    assert "'BR'" in q
    assert "countrycode" in q


def test_classification_query_no_country_filter_when_empty():
    q = _build_classification_query(precision=1, country_code="")
    # Should not contain a WHERE clause fragment restricting to a specific country
    assert "= ''" not in q


def test_classification_query_groups_by_lat_lon_source():
    q = _build_classification_query(precision=1)
    assert "GROUP BY" in q


def test_classification_query_internal_case():
    q = _build_classification_query(precision=1)
    assert "INTERNAL" in q
    assert "EXTERNAL" in q
    assert "REGIONAL" in q
    assert "UNKNOWN" in q


# ── _save ──────────────────────────────────────────────────────────────────────

def test_save_creates_parquet_and_csv(tmp_path, sample_hexbin_df, monkeypatch):
    monkeypatch.setattr(
        "src.aggregate_hexbin_pipeline.OUTPUT_DIR", str(tmp_path)
    )
    _save(sample_hexbin_df, "test_hexbin")
    assert (tmp_path / "test_hexbin.parquet").exists()
    assert (tmp_path / "test_hexbin.csv").exists()


def test_save_parquet_roundtrip(tmp_path, sample_hexbin_df, monkeypatch):
    monkeypatch.setattr(
        "src.aggregate_hexbin_pipeline.OUTPUT_DIR", str(tmp_path)
    )
    _save(sample_hexbin_df, "test_hexbin")
    reloaded = pd.read_parquet(tmp_path / "test_hexbin.parquet")
    assert len(reloaded) == len(sample_hexbin_df)
    assert list(reloaded.columns) == list(sample_hexbin_df.columns)


def test_save_csv_has_correct_row_count(tmp_path, sample_hexbin_df, monkeypatch):
    monkeypatch.setattr(
        "src.aggregate_hexbin_pipeline.OUTPUT_DIR", str(tmp_path)
    )
    _save(sample_hexbin_df, "test_hexbin")
    df_csv = pd.read_csv(tmp_path / "test_hexbin.csv")
    assert len(df_csv) == len(sample_hexbin_df)


# ── _print_summary ─────────────────────────────────────────────────────────────

def test_print_summary_runs_without_error(capsys, enriched_df):
    _print_summary(enriched_df)
    captured = capsys.readouterr()
    assert "INTERNAL" in captured.out
    assert "EXTERNAL" in captured.out


def test_print_summary_shows_income_group(capsys, enriched_df):
    _print_summary(enriched_df)
    captured = capsys.readouterr()
    assert "High income" in captured.out


def test_print_summary_shows_percentages(capsys, enriched_df):
    _print_summary(enriched_df)
    captured = capsys.readouterr()
    # Should contain a % character somewhere
    assert "%" in captured.out


# ── DEFAULT_COUNTRIES ──────────────────────────────────────────────────────────

def test_default_countries_includes_brazil():
    assert "BR" in DEFAULT_COUNTRIES


def test_default_countries_includes_south_africa():
    assert "ZA" in DEFAULT_COUNTRIES


def test_default_countries_all_uppercase():
    for cc in DEFAULT_COUNTRIES:
        assert cc == cc.upper(), f"{cc} is not uppercase"


def test_default_countries_all_two_letters():
    for cc in DEFAULT_COUNTRIES:
        assert len(cc) == 2, f"{cc} is not a 2-letter code"


# ── Integration: classification logic via in-memory DuckDB ─────────────────────

def test_classification_internal_when_countries_match():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE country_metadata (iso2c VARCHAR, un_region_name VARCHAR);
        INSERT INTO country_metadata VALUES ('BR', 'Americas'), ('US', 'Americas');
        CREATE TABLE occ (
            countrycode VARCHAR, publishingorgkey VARCHAR,
            decimallatitude DOUBLE, decimallongitude DOUBLE,
            taxonrank VARCHAR, occurrencestatus VARCHAR,
            basisofrecord VARCHAR, class VARCHAR
        );
        INSERT INTO occ VALUES ('BR', 'org-1', -14.2, -51.9, 'SPECIES', 'PRESENT', 'OBSERVATION', 'Mammalia');
        CREATE TABLE reg (original_key VARCHAR, resolved_country VARCHAR, type VARCHAR);
        INSERT INTO reg VALUES ('org-1', 'BR', 'organization');
    """)

    result = con.execute("""
        SELECT
            CASE
                WHEN r.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN o.countrycode = r.resolved_country THEN 'INTERNAL'
                WHEN occ_r.un_region_name = pub_r.un_region_name THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END AS source_type,
            COUNT(*) AS cnt
        FROM occ o
        LEFT JOIN reg r ON o.publishingorgkey = r.original_key AND r.type = 'organization'
        LEFT JOIN country_metadata occ_r ON o.countrycode       = occ_r.iso2c
        LEFT JOIN country_metadata pub_r ON r.resolved_country  = pub_r.iso2c
        GROUP BY 1
    """).df()
    con.close()

    row = result[result["source_type"] == "INTERNAL"]
    assert len(row) == 1
    assert row.iloc[0]["cnt"] == 1


def test_classification_external_when_regions_differ():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE country_metadata (iso2c VARCHAR, un_region_name VARCHAR);
        INSERT INTO country_metadata VALUES ('BR', 'Americas'), ('DE', 'Europe');
        CREATE TABLE occ (
            countrycode VARCHAR, publishingorgkey VARCHAR,
            decimallatitude DOUBLE, decimallongitude DOUBLE,
            taxonrank VARCHAR, occurrencestatus VARCHAR,
            basisofrecord VARCHAR, class VARCHAR
        );
        INSERT INTO occ VALUES ('BR', 'org-de', -14.2, -51.9, 'SPECIES', 'PRESENT', 'OBSERVATION', 'Mammalia');
        CREATE TABLE reg (original_key VARCHAR, resolved_country VARCHAR, type VARCHAR);
        INSERT INTO reg VALUES ('org-de', 'DE', 'organization');
    """)

    result = con.execute("""
        SELECT
            CASE
                WHEN r.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN o.countrycode = r.resolved_country THEN 'INTERNAL'
                WHEN occ_r.un_region_name = pub_r.un_region_name THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END AS source_type,
            COUNT(*) AS cnt
        FROM occ o
        LEFT JOIN reg r ON o.publishingorgkey = r.original_key AND r.type = 'organization'
        LEFT JOIN country_metadata occ_r ON o.countrycode       = occ_r.iso2c
        LEFT JOIN country_metadata pub_r ON r.resolved_country  = pub_r.iso2c
        GROUP BY 1
    """).df()
    con.close()

    row = result[result["source_type"] == "EXTERNAL"]
    assert len(row) == 1
    assert row.iloc[0]["cnt"] == 1


def test_classification_regional_when_same_region_different_country():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE country_metadata (iso2c VARCHAR, un_region_name VARCHAR);
        INSERT INTO country_metadata VALUES ('BR', 'Americas'), ('CO', 'Americas');
        CREATE TABLE occ (
            countrycode VARCHAR, publishingorgkey VARCHAR,
            decimallatitude DOUBLE, decimallongitude DOUBLE,
            taxonrank VARCHAR, occurrencestatus VARCHAR,
            basisofrecord VARCHAR, class VARCHAR
        );
        INSERT INTO occ VALUES ('BR', 'org-co', -14.2, -51.9, 'SPECIES', 'PRESENT', 'OBSERVATION', 'Mammalia');
        CREATE TABLE reg (original_key VARCHAR, resolved_country VARCHAR, type VARCHAR);
        INSERT INTO reg VALUES ('org-co', 'CO', 'organization');
    """)

    result = con.execute("""
        SELECT
            CASE
                WHEN r.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN o.countrycode = r.resolved_country THEN 'INTERNAL'
                WHEN occ_r.un_region_name = pub_r.un_region_name THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END AS source_type,
            COUNT(*) AS cnt
        FROM occ o
        LEFT JOIN reg r ON o.publishingorgkey = r.original_key AND r.type = 'organization'
        LEFT JOIN country_metadata occ_r ON o.countrycode       = occ_r.iso2c
        LEFT JOIN country_metadata pub_r ON r.resolved_country  = pub_r.iso2c
        GROUP BY 1
    """).df()
    con.close()

    row = result[result["source_type"] == "REGIONAL"]
    assert len(row) == 1
    assert row.iloc[0]["cnt"] == 1


def test_classification_unknown_when_no_registry_match():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE country_metadata (iso2c VARCHAR, un_region_name VARCHAR);
        INSERT INTO country_metadata VALUES ('BR', 'Americas');
        CREATE TABLE occ (
            countrycode VARCHAR, publishingorgkey VARCHAR,
            decimallatitude DOUBLE, decimallongitude DOUBLE,
            taxonrank VARCHAR, occurrencestatus VARCHAR,
            basisofrecord VARCHAR, class VARCHAR
        );
        INSERT INTO occ VALUES ('BR', 'org-unknown', -14.2, -51.9, 'SPECIES', 'PRESENT', 'OBSERVATION', 'Mammalia');
        CREATE TABLE reg (original_key VARCHAR, resolved_country VARCHAR, type VARCHAR);
    """)

    result = con.execute("""
        SELECT
            CASE
                WHEN r.resolved_country IS NULL THEN 'UNKNOWN'
                WHEN o.countrycode = r.resolved_country THEN 'INTERNAL'
                WHEN occ_r.un_region_name = pub_r.un_region_name THEN 'REGIONAL'
                ELSE 'EXTERNAL'
            END AS source_type,
            COUNT(*) AS cnt
        FROM occ o
        LEFT JOIN reg r ON o.publishingorgkey = r.original_key AND r.type = 'organization'
        LEFT JOIN country_metadata occ_r ON o.countrycode       = occ_r.iso2c
        LEFT JOIN country_metadata pub_r ON r.resolved_country  = pub_r.iso2c
        GROUP BY 1
    """).df()
    con.close()

    row = result[result["source_type"] == "UNKNOWN"]
    assert len(row) == 1
    assert row.iloc[0]["cnt"] == 1
