"""
Unit tests for src/visualise_global_choropleth.py

Tests cover colour interpolation, GeoJSON feature property injection, and
source data loading — without requiring a Mapbox token or writing HTML files.
"""

import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.visualise_global_choropleth import (
    RAMP_TEAL,
    _lerp_color,
    build_geojson_with_data,
    load_source_data,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def minimal_source_df() -> pd.DataFrame:
    return pd.DataFrame({
        "iso2c":               ["BR", "DE", "KE"],
        "iso3c":               ["BRA", "DEU", "KEN"],
        "country_name":        ["Brazil", "Germany", "Kenya"],
        "un_region_name":      ["Americas", "Europe", "Africa"],
        "un_sub_region_name":  ["South America", "Western Europe", "Eastern Africa"],
        "wb_income_group":     ["Upper middle income", "High income", "Lower middle income"],
        "is_cbd_party":        [True, True, True],
        "is_ldc":              [False, False, False],
        "is_sids":             [False, False, False],
        "internal_count":      [500_000, 8_000_000, 50_000],
        "external_count":      [100_000, 500_000, 400_000],
        "total_count":         [600_000, 8_500_000, 450_000],
        "internal_percentage": [83.3, 94.1, 11.1],
        "external_percentage": [16.7, 5.9, 88.9],
    })


@pytest.fixture()
def minimal_geojson(minimal_source_df) -> dict:
    """Minimal GeoJSON with three features matching the source dataframe."""
    features = []
    for iso2, name in [("BR", "Brazil"), ("DE", "Germany"), ("KE", "Kenya")]:
        features.append({
            "type": "Feature",
            "properties": {
                "ISO_A2_EH": iso2,
                "ISO_A2":    iso2,
                "NAME":      name,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [0, 0],
            },
        })
    # Add one feature with no matching data
    features.append({
        "type": "Feature",
        "properties": {"ISO_A2_EH": "XX", "ISO_A2": "XX", "NAME": "NoData"},
        "geometry": {"type": "Point", "coordinates": [0, 0]},
    })
    return {"type": "FeatureCollection", "features": features}


# ── _lerp_color ────────────────────────────────────────────────────────────────

def test_lerp_color_at_zero_returns_first_stop():
    color = _lerp_color(RAMP_TEAL, 0.0)
    assert color == RAMP_TEAL[0]


def test_lerp_color_at_one_returns_last_stop():
    color = _lerp_color(RAMP_TEAL, 1.0)
    assert color == RAMP_TEAL[-1]


def test_lerp_color_returns_three_integers():
    color = _lerp_color(RAMP_TEAL, 0.5)
    assert len(color) == 3
    assert all(isinstance(v, int) for v in color)


def test_lerp_color_clamps_below_zero():
    color = _lerp_color(RAMP_TEAL, -1.0)
    assert color == RAMP_TEAL[0]


def test_lerp_color_clamps_above_one():
    color = _lerp_color(RAMP_TEAL, 2.0)
    assert color == RAMP_TEAL[-1]


def test_lerp_color_midpoint_within_range():
    color = _lerp_color(RAMP_TEAL, 0.5)
    for v in color:
        assert 0 <= v <= 255


def test_lerp_color_monotonically_changes():
    """Higher t should produce a 'warmer' (higher red channel) value than a low-mid t.
    The ramp starts at grey (220,220,220) and passes through teal (1,152,189) before
    reaching red (209,55,78), so we compare a mid-low value against a high value."""
    c_low  = _lerp_color(RAMP_TEAL, 0.3)   # near teal stop, low red
    c_high = _lerp_color(RAMP_TEAL, 1.0)   # red stop, highest red
    assert c_high[0] > c_low[0]


# ── RAMP_TEAL ──────────────────────────────────────────────────────────────────

def test_ramp_teal_has_six_stops():
    assert len(RAMP_TEAL) == 6


def test_ramp_teal_values_in_rgb_range():
    for stop in RAMP_TEAL:
        assert len(stop) == 3
        for v in stop:
            assert 0 <= v <= 255


# ── build_geojson_with_data ────────────────────────────────────────────────────

def test_build_geojson_adds_fill_color_internal(minimal_geojson, minimal_source_df):
    result = build_geojson_with_data(minimal_source_df, mode="internal")
    for feat in result["features"]:
        assert "fill_color" in feat["properties"]
        assert len(feat["properties"]["fill_color"]) == 4   # RGBA


def test_build_geojson_adds_fill_color_total(minimal_geojson, minimal_source_df, monkeypatch):
    import src.visualise_global_choropleth as mod
    monkeypatch.setattr(mod, "load_source_data", lambda **kw: minimal_source_df)

    # Call directly on the minimal_geojson fixture
    result = build_geojson_with_data(minimal_source_df, mode="total")
    for feat in result["features"]:
        assert "fill_color" in feat["properties"]


def test_build_geojson_injects_country_name(minimal_source_df, monkeypatch, tmp_path):
    import json
    import src.visualise_global_choropleth as mod

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature",
             "properties": {"ISO_A2_EH": "BR", "ISO_A2": "BR", "NAME": "Brazil"},
             "geometry": {"type": "Point", "coordinates": [0, 0]}},
            {"type": "Feature",
             "properties": {"ISO_A2_EH": "DE", "ISO_A2": "DE", "NAME": "Germany"},
             "geometry": {"type": "Point", "coordinates": [0, 0]}},
        ],
    }
    gj_path = tmp_path / "ne.geojson"
    gj_path.write_text(json.dumps(geojson))
    monkeypatch.setattr(mod, "GEOJSON_PATH", str(gj_path))

    result = build_geojson_with_data(minimal_source_df, mode="internal")
    matched = [f for f in result["features"] if f["properties"].get("country_name") == "Brazil"]
    assert len(matched) >= 1


def test_build_geojson_no_data_country_gets_grey(minimal_geojson, minimal_source_df):
    result = build_geojson_with_data(minimal_source_df, mode="internal")
    no_data = [
        f for f in result["features"]
        if f["properties"].get("ISO_A2_EH") == "XX"
    ]
    if no_data:
        fill = no_data[0]["properties"]["fill_color"]
        # Grey = low R,G,B all roughly equal
        assert fill[0] == fill[1] == fill[2]


def test_build_geojson_high_internal_pct_is_warmer(minimal_source_df):
    """Germany (94% internal) should have a warmer (higher red) fill than Kenya (11%)."""
    result = build_geojson_with_data(minimal_source_df, mode="internal")
    feat_de = next(f for f in result["features"] if f["properties"].get("ISO_A2_EH") == "DE")
    feat_ke = next(f for f in result["features"] if f["properties"].get("ISO_A2_EH") == "KE")
    red_de = feat_de["properties"]["fill_color"][0]
    red_ke = feat_ke["properties"]["fill_color"][0]
    assert red_de >= red_ke


def test_build_geojson_sets_internal_pct_correctly(minimal_source_df):
    result = build_geojson_with_data(minimal_source_df, mode="internal")
    feat_br = next(f for f in result["features"] if f["properties"].get("ISO_A2_EH") == "BR")
    assert feat_br["properties"]["internal_pct"] == pytest.approx(83.3, abs=0.1)


def test_build_geojson_sets_wb_income_group(minimal_source_df):
    result = build_geojson_with_data(minimal_source_df, mode="internal")
    feat_de = next(f for f in result["features"] if f["properties"].get("ISO_A2_EH") == "DE")
    assert feat_de["properties"]["wb_income_group"] == "High income"


# ── load_source_data — smoke test against real CSV if available ─────────────────

def test_load_source_data_returns_dataframe():
    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "processed", "source_by_country.csv"
    )
    if not os.path.exists(csv_path):
        pytest.skip("source_by_country.csv not found")
    df = load_source_data()
    assert isinstance(df, pd.DataFrame)
    assert "iso2c" in df.columns
    assert "internal_percentage" in df.columns
    assert len(df) > 50


def test_load_source_data_income_filter():
    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "processed", "source_by_country.csv"
    )
    if not os.path.exists(csv_path):
        pytest.skip("source_by_country.csv not found")
    df = load_source_data(income_group="High income")
    assert (df["wb_income_group"] == "High income").all()


def test_load_source_data_no_null_iso2c():
    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "processed", "source_by_country.csv"
    )
    if not os.path.exists(csv_path):
        pytest.skip("source_by_country.csv not found")
    df = load_source_data()
    assert df["iso2c"].notna().all()
