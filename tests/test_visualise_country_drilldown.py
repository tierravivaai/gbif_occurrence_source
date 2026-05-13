"""
Unit tests for src/visualise_country_drilldown.py

Tests cover country preset coverage, data loading with filters, and HTML
rendering output — without requiring a Mapbox token or real parquet files.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.visualise_country_drilldown import (
    COUNTRY_PRESETS,
    DEFAULT_PRESET,
    GBIF_COLOR_RANGE,
    load_data,
    render_drilldown,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def brazil_df() -> pd.DataFrame:
    """Minimal Brazil hexbin data with both INTERNAL and EXTERNAL records."""
    return pd.DataFrame({
        "lat":              [-14.2, -14.2, -10.0, -10.0, -5.0],
        "lon":              [-51.9, -51.9, -50.0, -50.0, -45.0],
        "source_type":      ["INTERNAL", "EXTERNAL", "INTERNAL", "REGIONAL", "EXTERNAL"],
        "record_count":     [1000, 200, 800, 150, 50],
        "countrycode":      ["BR", "US", "BR", "CO", "DE"],
        "country_name":     ["Brazil", "United States", "Brazil", "Colombia", "Germany"],
        "wb_income_group":  ["Upper middle income", "High income", "Upper middle income",
                             "Upper middle income", "High income"],
        "un_region_name":   ["Americas", "Americas", "Americas", "Americas", "Europe"],
        "un_sub_region_name": ["South America", "Northern America", "South America",
                               "South America", "Western Europe"],
    })


# ── GBIF_COLOR_RANGE ───────────────────────────────────────────────────────────

def test_gbif_color_range_has_six_stops():
    assert len(GBIF_COLOR_RANGE) == 6


def test_gbif_color_range_all_rgb():
    for stop in GBIF_COLOR_RANGE:
        assert len(stop) == 3
        for v in stop:
            assert 0 <= v <= 255


# ── COUNTRY_PRESETS ────────────────────────────────────────────────────────────

def test_country_presets_has_brazil():
    assert "BR" in COUNTRY_PRESETS


def test_country_presets_has_south_africa():
    assert "ZA" in COUNTRY_PRESETS


def test_country_presets_required_keys():
    for code, preset in COUNTRY_PRESETS.items():
        for key in ("lat", "lon", "zoom", "radius"):
            assert key in preset, f"{code} missing key: {key}"


def test_country_presets_lat_in_range():
    for code, preset in COUNTRY_PRESETS.items():
        assert -90 <= preset["lat"] <= 90, f"{code} lat out of range"


def test_country_presets_lon_in_range():
    for code, preset in COUNTRY_PRESETS.items():
        assert -180 <= preset["lon"] <= 180, f"{code} lon out of range"


def test_country_presets_radius_positive():
    for code, preset in COUNTRY_PRESETS.items():
        assert preset["radius"] > 0, f"{code} radius is not positive"


def test_default_preset_has_required_keys():
    for key in ("lat", "lon", "zoom", "radius"):
        assert key in DEFAULT_PRESET


# ── load_data ──────────────────────────────────────────────────────────────────

def test_load_data_returns_dataframe(tmp_path, brazil_df, monkeypatch):
    path = tmp_path / "hexbin_br_no_aves_p1_enriched.parquet"
    brazil_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    df = load_data("BR")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(brazil_df)


def test_load_data_filter_internal(tmp_path, brazil_df, monkeypatch):
    path = tmp_path / "hexbin_br_no_aves_p1_enriched.parquet"
    brazil_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    df = load_data("BR", filter_source="internal")
    assert (df["source_type"] == "INTERNAL").all()


def test_load_data_filter_external(tmp_path, brazil_df, monkeypatch):
    path = tmp_path / "hexbin_br_no_aves_p1_enriched.parquet"
    brazil_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    df = load_data("BR", filter_source="external")
    assert (df["source_type"] == "EXTERNAL").all()


def test_load_data_filter_all_keeps_all_rows(tmp_path, brazil_df, monkeypatch):
    path = tmp_path / "hexbin_br_no_aves_p1_enriched.parquet"
    brazil_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    df = load_data("BR", filter_source="all")
    assert len(df) == len(brazil_df)


def test_load_data_income_group_filter(tmp_path, brazil_df, monkeypatch):
    path = tmp_path / "hexbin_br_no_aves_p1_enriched.parquet"
    brazil_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    df = load_data("BR", income_group="High income")
    assert (df["wb_income_group"] == "High income").all()


def test_load_data_missing_file_exits(tmp_path, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    with pytest.raises(SystemExit):
        load_data("BR")


def test_load_data_lowercase_country_code(tmp_path, brazil_df, monkeypatch):
    """Country code should be upper-cased in file lookup."""
    path = tmp_path / "hexbin_br_no_aves_p1_enriched.parquet"
    brazil_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_country_drilldown.DATA_DIR", str(tmp_path))
    df = load_data("br")   # lower-case input
    assert len(df) == len(brazil_df)


# ── render_drilldown ───────────────────────────────────────────────────────────

def test_render_drilldown_all_creates_html(tmp_path, brazil_df, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "all", "BR", mapbox_token="")
    assert os.path.exists(out)
    assert out.endswith(".html")


def test_render_drilldown_internal_creates_html(tmp_path, brazil_df, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "internal", "BR", mapbox_token="")
    assert os.path.exists(out)


def test_render_drilldown_html_contains_gbif(tmp_path, brazil_df, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "all", "BR", mapbox_token="")
    with open(out) as f:
        html = f.read()
    assert "GBIF" in html


def test_render_drilldown_html_is_fullviewport(tmp_path, brazil_df, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "all", "BR", mapbox_token="")
    with open(out) as f:
        html = f.read()
    assert "100vh" in html


def test_render_drilldown_label_suffix_in_filename(tmp_path, brazil_df, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "all", "BR", mapbox_token="", label_suffix="high_income")
    assert "high_income" in os.path.basename(out)


def test_render_drilldown_internal_filters_data(tmp_path, brazil_df, monkeypatch):
    """Render internal map from mixed df — should not crash, INTERNAL rows only used."""
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "internal", "BR", mapbox_token="")
    assert os.path.exists(out)


def test_render_drilldown_empty_internal_returns_empty_string(tmp_path, monkeypatch):
    """If there are no INTERNAL rows, render should return empty string gracefully."""
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    df_no_internal = pd.DataFrame({
        "lat": [-14.2], "lon": [-51.9],
        "source_type": ["EXTERNAL"], "record_count": [100],
        "countrycode": ["US"], "country_name": ["United States"],
        "wb_income_group": ["High income"], "un_region_name": ["Americas"],
        "un_sub_region_name": ["Northern America"],
    })
    result = render_drilldown(df_no_internal, "internal", "BR", mapbox_token="")
    assert result == ""


def test_render_drilldown_unknown_country_uses_default_preset(tmp_path, brazil_df, monkeypatch):
    """A country code with no preset should still render using DEFAULT_PRESET."""
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "all", "XY", mapbox_token="")
    assert os.path.exists(out)


def test_render_drilldown_country_code_in_filename(tmp_path, brazil_df, monkeypatch):
    monkeypatch.setattr("src.visualise_country_drilldown.OUTPUT_DIR", str(tmp_path))
    out = render_drilldown(brazil_df, "all", "ZA", mapbox_token="")
    assert "za" in os.path.basename(out)
