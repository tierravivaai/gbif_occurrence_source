import duckdb
import pandas as pd
import pytest

from src.publisher_institution_country_share import (
    PublisherCountryFilters,
    compute_publisher_country_share,
)


def _build_occurrence_db(tmp_path):
    db_path = tmp_path / "occurrence.db"
    records = [
        {
            "countrycode": "KE",
            "class": "Mammalia",
            "publishingorgkey": "org-1",
            "institutioncode": "KE-NMK",
            "collectioncode": None,
            "rightsholder": "Kenya Wildlife Service",
            "taxonrank": "SPECIES",
            "species": "Panthera leo",
            "basisofrecord": "HUMAN_OBSERVATION",
            "occurrencestatus": "PRESENT",
            "decimallatitude": 1.0,
            "decimallongitude": 36.0,
        },
        {
            "countrycode": "KE",
            "class": "Aves",
            "publishingorgkey": "org-1",
            "institutioncode": None,
            "collectioncode": None,
            "rightsholder": "Kenya Wildlife Service",
            "taxonrank": "SPECIES",
            "species": "Struthio camelus",
            "basisofrecord": "HUMAN_OBSERVATION",
            "occurrencestatus": "PRESENT",
            "decimallatitude": 0.5,
            "decimallongitude": 35.0,
        },
        {
            "countrycode": "KE",
            "class": "Mammalia",
            "publishingorgkey": "org-2",
            "institutioncode": None,
            "collectioncode": None,
            "rightsholder": "Smithsonian Institution",
            "taxonrank": "SPECIES",
            "species": "Loxodonta africana",
            "basisofrecord": "HUMAN_OBSERVATION",
            "occurrencestatus": "PRESENT",
            "decimallatitude": -1.0,
            "decimallongitude": 37.0,
        },
        {
            "countrycode": "KE",
            "class": "Mammalia",
            "publishingorgkey": "org-3",
            "institutioncode": "NMK",
            "collectioncode": None,
            "rightsholder": "National Museums of Kenya",
            "taxonrank": "SPECIES",
            "species": "Equus quagga",
            "basisofrecord": "HUMAN_OBSERVATION",
            "occurrencestatus": "PRESENT",
            "decimallatitude": 1.5,
            "decimallongitude": 36.5,
        },
        {
            "countrycode": "KE",
            "class": "Mammalia",
            "publishingorgkey": "org-3",
            "institutioncode": "NMK",
            "collectioncode": None,
            "rightsholder": "National Museums of Kenya",
            "taxonrank": "SPECIES",
            "species": "Equus quagga",
            "basisofrecord": "HUMAN_OBSERVATION",
            "occurrencestatus": "PRESENT",
            "decimallatitude": 1.6,
            "decimallongitude": 36.6,
        },
        {
            "countrycode": "KE",
            "class": "Mammalia",
            "publishingorgkey": "org-4",
            "institutioncode": None,
            "collectioncode": None,
            "rightsholder": None,
            "taxonrank": "SPECIES",
            "species": "Civettictis civetta",
            "basisofrecord": "HUMAN_OBSERVATION",
            "occurrencestatus": "PRESENT",
            "decimallatitude": 0.1,
            "decimallongitude": 36.2,
        },
    ]
    df = pd.DataFrame(records)
    con = duckdb.connect(str(db_path))
    con.register("source_df", df)
    con.execute("CREATE TABLE occurrence AS SELECT * FROM source_df")
    con.close()
    return db_path


def _rows_by_key(rows):
    return {(row["scope"], row["mode"]): row for row in rows}


def test_country_share_explicit_and_inferred(tmp_path):
    db_path = _build_occurrence_db(tmp_path)
    result = compute_publisher_country_share(
        "KE",
        occurrence_source_path=str(db_path),
        use_db=True,
        filters=PublisherCountryFilters(apply_core_filters=False),
        org_country_overrides={"org-1": "KE", "org-2": "US"},
        fetch_registry=False,
    )

    rows = _rows_by_key(result["rows"])
    explicit_all = rows[("all_records", "explicit")]
    assert explicit_all["inside_count"] == 2
    assert explicit_all["outside_count"] == 1
    assert explicit_all["missing_count"] == 3
    assert explicit_all["total"] == 6

    explicit_exclude = rows[("exclude_aves", "explicit")]
    assert explicit_exclude["inside_count"] == 1
    assert explicit_exclude["outside_count"] == 1
    assert explicit_exclude["missing_count"] == 3
    assert explicit_exclude["total"] == 5

    inferred_all = rows[("all_records", "explicit_or_inferred")]
    assert inferred_all["inside_count"] == 4
    assert inferred_all["outside_count"] == 1
    assert inferred_all["missing_count"] == 1
    assert inferred_all["total"] == 6
    assert inferred_all["inside_pct"] == pytest.approx(4 / 6)

    inferred_exclude = rows[("exclude_aves", "explicit_or_inferred")]
    assert inferred_exclude["inside_count"] == 3
    assert inferred_exclude["outside_count"] == 1
    assert inferred_exclude["missing_count"] == 1
    assert inferred_exclude["total"] == 5
