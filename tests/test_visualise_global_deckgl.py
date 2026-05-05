"""
Unit tests for src/visualise_global_deckgl.py

Tests cover data loading, filtering, colour range structure, and HTML
rendering output — without requiring a Mapbox token or real parquet files.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.visualise_global_deckgl import (
    COUNTRY_PRESETS,
    GBIF_COLOR_RANGE,
    load_data,
    render_hexagon_map,
)

VIEWPORT_PRESETS = COUNTRY_PRESETS  # alias — same dict, different name in tests


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_enriched_df() -> pd.DataFrame:
    return pd.DataFrame({
        "lat":              [-14.2, -14.2, 51.5, 51.5,  1.3,  1.3],
        "lon":              [-51.9, -51.9, -0.1, -0.1, 103.8, 103.8],
        "source_type":      ["INTERNAL", "EXTERNAL", "INTERNAL", "REGIONAL", "INTERNAL", "EXTERNAL"],
        "record_count":     [1000, 200, 5000, 300, 150, 50],
        "countrycode":      ["BR", "US", "GB", "DE", "SG", "MY"],
        "country_name":     ["Brazil", "United States", "United Kingdom", "Germany", "Singapore", "Malaysia"],
        "wb_income_group":  ["Upper middle income", "High income", "High income",
                             "High income", "High income", "Upper middle income"],
        "un_region_name":   ["Americas", "Americas", "Europe", "Europe", "Asia", "Asia"],
        "un_sub_region_name": ["South America", "Northern America", "Northern Europe",
                               "Western Europe", "South-Eastern Asia", "South-Eastern Asia"],
    })


# ── GBIF_COLOR_RANGE ───────────────────────────────────────────────────────────

def test_gbif_color_range_has_six_stops():
    assert len(GBIF_COLOR_RANGE) == 6


def test_gbif_color_range_all_rgb_triples():
    for stop in GBIF_COLOR_RANGE:
        assert len(stop) == 3
        for v in stop:
            assert 0 <= v <= 255


def test_gbif_color_range_first_stop_is_teal():
    assert GBIF_COLOR_RANGE[0] == [1, 152, 189]


def test_gbif_color_range_last_stop_is_red():
    assert GBIF_COLOR_RANGE[-1] == [209, 55, 78]


# ── VIEWPORT_PRESETS ───────────────────────────────────────────────────────────

def test_viewport_presets_contains_global():
    assert "ALL" in VIEWPORT_PRESETS


def test_viewport_presets_contains_brazil():
    assert "BR" in VIEWPORT_PRESETS


def test_viewport_presets_have_required_keys():
    for code, preset in VIEWPORT_PRESETS.items():
        assert "lat"     in preset, f"{code} missing lat"
        assert "lon"     in preset, f"{code} missing lon"
        assert "zoom"    in preset, f"{code} missing zoom"
        assert "pitch"   in preset, f"{code} missing pitch"
        assert "bearing" in preset, f"{code} missing bearing"


def test_viewport_presets_lat_in_range():
    for code, preset in VIEWPORT_PRESETS.items():
        assert -90 <= preset["lat"] <= 90, f"{code} lat out of range"


def test_viewport_presets_lon_in_range():
    for code, preset in VIEWPORT_PRESETS.items():
        assert -180 <= preset["lon"] <= 180, f"{code} lon out of range"


# ── load_data — with monkeypatched parquet ─────────────────────────────────────

def test_load_data_filters_internal(tmp_path, sample_enriched_df, monkeypatch):
    parquet_path = tmp_path / "hexbin_all_no_aves_p1_enriched.parquet"
    sample_enriched_df.to_parquet(parquet_path, index=False)
    monkeypatch.setattr("src.visualise_global_deckgl.DATA_DIR", str(tmp_path))

    df = load_data(precision=1, income_group="", un_region="")
    assert len(df) == len(sample_enriched_df)


def test_load_data_income_filter(tmp_path, sample_enriched_df, monkeypatch):
    parquet_path = tmp_path / "hexbin_all_no_aves_p1_enriched.parquet"
    sample_enriched_df.to_parquet(parquet_path, index=False)
    monkeypatch.setattr("src.visualise_global_deckgl.DATA_DIR", str(tmp_path))

    df = load_data(precision=1, income_group="High income")
    assert (df["wb_income_group"] == "High income").all()


def test_load_data_un_region_filter(tmp_path, sample_enriched_df, monkeypatch):
    parquet_path = tmp_path / "hexbin_all_no_aves_p1_enriched.parquet"
    sample_enriched_df.to_parquet(parquet_path, index=False)
    monkeypatch.setattr("src.visualise_global_deckgl.DATA_DIR", str(tmp_path))

    df = load_data(precision=1, un_region="Europe")
    assert (df["un_region_name"] == "Europe").all()


def test_load_data_missing_file_exits(tmp_path, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.DATA_DIR", str(tmp_path))
    with pytest.raises(SystemExit):
        load_data(precision=1)


# ── render_hexagon_map ─────────────────────────────────────────────────────────

def test_render_hexagon_map_all_creates_html(tmp_path, sample_enriched_df, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    out = render_hexagon_map(
        df=sample_enriched_df,
        mode="all",
        mapbox_token="",
        precision=1,
        radius=50000,
        elevation_scale=50,
    )
    assert os.path.exists(out)
    assert out.endswith(".html")


def test_render_hexagon_map_internal_creates_html(tmp_path, sample_enriched_df, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    out = render_hexagon_map(
        df=sample_enriched_df,
        mode="internal",
        mapbox_token="",
        precision=1,
        radius=50000,
        elevation_scale=50,
    )
    assert os.path.exists(out)


def test_render_hexagon_map_html_contains_title(tmp_path, sample_enriched_df, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    out = render_hexagon_map(
        df=sample_enriched_df,
        mode="all",
        mapbox_token="",
        precision=1,
        radius=50000,
        elevation_scale=50,
    )
    with open(out) as f:
        html = f.read()
    assert "GBIF" in html


def test_render_hexagon_map_label_suffix_in_filename(tmp_path, sample_enriched_df, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    out = render_hexagon_map(
        df=sample_enriched_df,
        mode="internal",
        mapbox_token="",
        precision=1,
        radius=50000,
        elevation_scale=50,
        label_suffix="high_income",
    )
    assert "high_income" in os.path.basename(out)


def test_render_hexagon_map_internal_mode_filters_source_type(tmp_path, sample_enriched_df, monkeypatch):
    """Internal mode should not crash even with mixed source_type rows."""
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    out = render_hexagon_map(
        df=sample_enriched_df,
        mode="internal",
        mapbox_token="",
        precision=0,
        radius=50000,
        elevation_scale=50,
    )
    assert os.path.exists(out)


def test_render_hexagon_map_html_is_fullviewport(tmp_path, sample_enriched_df, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    out = render_hexagon_map(
        df=sample_enriched_df,
        mode="all",
        mapbox_token="",
        precision=1,
        radius=50000,
        elevation_scale=50,
    )
    with open(out) as f:
        html = f.read()
    assert "100vh" in html


def test_render_hexagon_map_different_viewports(tmp_path, sample_enriched_df, monkeypatch):
    monkeypatch.setattr("src.visualise_global_deckgl.OUTPUT_DIR", str(tmp_path))
    for vp in ["ALL", "BR", "ZA"]:
        out = render_hexagon_map(
            df=sample_enriched_df,
            mode="all",
            mapbox_token="",
            precision=1,
            radius=50000,
            elevation_scale=50,
            viewport=vp,
        )
        assert os.path.exists(out)
