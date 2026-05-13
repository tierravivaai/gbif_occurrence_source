"""
Unit tests for src/visualise_global_toggle_kepler.py

Tests cover config constants, layer builder, CSS injection, and the full
map creation pipeline using a monkeypatched parquet file — no Mapbox token
or real GBIF data required.
"""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.visualise_global_toggle_kepler import (
    GBIF_COLOR_RANGE,
    HEXBIN_WORLD_UNIT,
    INCOME_LAYER_COLORS,
    UN_REGION_COLORS,
    VIEWPORT_PRESETS,
    _hexbin_layer,
    _inject_fullpage_css,
    load_data,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "lat":              [-14.2, 51.5,  -30.6,  20.6,   1.3],
        "lon":              [-51.9, -0.1,   22.9,  78.9, 103.8],
        "source_type":      ["INTERNAL", "INTERNAL", "EXTERNAL", "INTERNAL", "REGIONAL"],
        "record_count":     [1000, 5000, 200, 800, 100],
        "countrycode":      ["BR", "GB", "ZA", "IN", "SG"],
        "country_name":     ["Brazil", "United Kingdom", "South Africa", "India", "Singapore"],
        "wb_income_group":  ["Upper middle income", "High income", "Upper middle income",
                             "Lower middle income", "High income"],
        "un_region_name":   ["Americas", "Europe", "Africa", "Asia", "Asia"],
        "un_sub_region_name": ["South America", "Northern Europe", "Southern Africa",
                               "Southern Asia", "South-Eastern Asia"],
    })


# ── GBIF_COLOR_RANGE ───────────────────────────────────────────────────────────

def test_gbif_color_range_has_six_colors():
    assert len(GBIF_COLOR_RANGE["colors"]) == 6


def test_gbif_color_range_is_hex_strings():
    for color in GBIF_COLOR_RANGE["colors"]:
        assert color.startswith("#")
        assert len(color) == 7


def test_gbif_color_range_has_name():
    assert "name" in GBIF_COLOR_RANGE
    assert GBIF_COLOR_RANGE["name"]


# ── HEXBIN_WORLD_UNIT ──────────────────────────────────────────────────────────

def test_hexbin_world_unit_has_p0():
    assert 0 in HEXBIN_WORLD_UNIT


def test_hexbin_world_unit_has_p1():
    assert 1 in HEXBIN_WORLD_UNIT


def test_hexbin_world_unit_p0_larger_than_p1():
    assert HEXBIN_WORLD_UNIT[0] > HEXBIN_WORLD_UNIT[1]


# ── INCOME_LAYER_COLORS ────────────────────────────────────────────────────────

def test_income_layer_colors_covers_all_four_groups():
    expected = {"High income", "Upper middle income", "Lower middle income", "Low income"}
    assert expected.issubset(set(INCOME_LAYER_COLORS.keys()))


def test_income_layer_colors_are_rgb_triples():
    for group, color in INCOME_LAYER_COLORS.items():
        assert len(color) == 3, f"{group} color is not an RGB triple"
        for v in color:
            assert 0 <= v <= 255


# ── UN_REGION_COLORS ───────────────────────────────────────────────────────────

def test_un_region_colors_covers_major_regions():
    for region in ["Africa", "Americas", "Asia", "Europe", "Oceania"]:
        assert region in UN_REGION_COLORS, f"Missing region: {region}"


def test_un_region_colors_are_rgb_triples():
    for region, color in UN_REGION_COLORS.items():
        assert len(color) == 3
        for v in color:
            assert 0 <= v <= 255


# ── VIEWPORT_PRESETS ───────────────────────────────────────────────────────────

def test_viewport_presets_has_global():
    assert "ALL" in VIEWPORT_PRESETS


def test_viewport_presets_lat_lon_zoom():
    for code, p in VIEWPORT_PRESETS.items():
        assert "lat"  in p
        assert "lon"  in p
        assert "zoom" in p


# ── _hexbin_layer ──────────────────────────────────────────────────────────────

def test_hexbin_layer_returns_dict():
    layer = _hexbin_layer("id-1", "All Records", "All Records", [1, 152, 189], 40)
    assert isinstance(layer, dict)


def test_hexbin_layer_type_is_hexbin():
    layer = _hexbin_layer("id-1", "All Records", "All Records", [1, 152, 189], 40)
    assert layer["type"] == "hexbin"


def test_hexbin_layer_has_config():
    layer = _hexbin_layer("id-1", "All Records", "All Records", [1, 152, 189], 40)
    assert "config" in layer


def test_hexbin_layer_visible_by_default():
    layer = _hexbin_layer("id-1", "data-id", "label", [1, 152, 189], 40, is_visible=True)
    assert layer["config"]["isVisible"] is True


def test_hexbin_layer_hidden_when_specified():
    layer = _hexbin_layer("id-1", "data-id", "label", [1, 152, 189], 40, is_visible=False)
    assert layer["config"]["isVisible"] is False


def test_hexbin_layer_has_lat_lng_columns():
    layer = _hexbin_layer("id-1", "data-id", "label", [1, 152, 189], 40)
    cols = layer["config"]["columns"]
    assert "lat" in cols
    assert "lng" in cols


def test_hexbin_layer_world_unit_size_set():
    layer = _hexbin_layer("id-1", "data-id", "label", [1, 152, 189], 25)
    assert layer["config"]["visConfig"]["worldUnitSize"] == 25


def test_hexbin_layer_enable3d_true():
    layer = _hexbin_layer("id-1", "data-id", "label", [1, 152, 189], 40)
    assert layer["config"]["visConfig"]["enable3d"] is True


# ── _inject_fullpage_css ───────────────────────────────────────────────────────

def test_inject_fullpage_css_adds_style_tag():
    html = "<html><head></head><body></body></html>"
    result = _inject_fullpage_css(html, "Test Title")
    assert "<style>" in result


def test_inject_fullpage_css_sets_100vh():
    html = "<html><head></head><body></body></html>"
    result = _inject_fullpage_css(html, "Test Title")
    assert "100vh" in result


def test_inject_fullpage_css_replaces_title():
    html = "<html><head><title>Kepler.gl</title></head><body></body></html>"
    result = _inject_fullpage_css(html, "My Custom Title")
    assert "My Custom Title" in result
    assert "Kepler.gl" not in result


def test_inject_fullpage_css_preserves_body():
    html = "<html><head></head><body><div>content</div></body></html>"
    result = _inject_fullpage_css(html, "Title")
    assert "<div>content</div>" in result


# ── load_data ──────────────────────────────────────────────────────────────────

def test_load_data_returns_dataframe(tmp_path, sample_df, monkeypatch):
    path = tmp_path / "hexbin_all_no_aves_p0_enriched.parquet"
    sample_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_global_toggle_kepler.DATA_DIR", str(tmp_path))
    df = load_data(precision=0)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(sample_df)


def test_load_data_missing_file_exits(tmp_path, monkeypatch):
    monkeypatch.setattr("src.visualise_global_toggle_kepler.DATA_DIR", str(tmp_path))
    with pytest.raises(SystemExit):
        load_data(precision=0)


def test_load_data_has_required_columns(tmp_path, sample_df, monkeypatch):
    path = tmp_path / "hexbin_all_no_aves_p0_enriched.parquet"
    sample_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_global_toggle_kepler.DATA_DIR", str(tmp_path))
    df = load_data(precision=0)
    for col in ["lat", "lon", "source_type", "record_count", "wb_income_group", "un_region_name"]:
        assert col in df.columns, f"Missing column: {col}"


# ── create_toggle_map — integration (keplergl must be installed) ────────────────

def test_create_toggle_map_produces_html(tmp_path, sample_df, monkeypatch):
    pytest.importorskip("keplergl")
    path = tmp_path / "hexbin_all_no_aves_p0_enriched.parquet"
    sample_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_global_toggle_kepler.DATA_DIR",   str(tmp_path))
    monkeypatch.setattr("src.visualise_global_toggle_kepler.OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr("src.visualise_global_toggle_kepler.load_mapbox_token", lambda: "")

    from src.visualise_global_toggle_kepler import create_toggle_map
    out = create_toggle_map(precision=0, country="ALL")
    assert os.path.exists(out)
    assert out.endswith(".html")


def test_create_toggle_map_html_contains_gbif(tmp_path, sample_df, monkeypatch):
    pytest.importorskip("keplergl")
    path = tmp_path / "hexbin_all_no_aves_p0_enriched.parquet"
    sample_df.to_parquet(path, index=False)
    monkeypatch.setattr("src.visualise_global_toggle_kepler.DATA_DIR",   str(tmp_path))
    monkeypatch.setattr("src.visualise_global_toggle_kepler.OUTPUT_DIR", str(tmp_path))
    monkeypatch.setattr("src.visualise_global_toggle_kepler.load_mapbox_token", lambda: "")

    from src.visualise_global_toggle_kepler import create_toggle_map
    out = create_toggle_map(precision=0, country="ALL")
    with open(out) as f:
        html = f.read()
    assert "GBIF" in html
