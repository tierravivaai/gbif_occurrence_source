from __future__ import annotations

from dataclasses import dataclass
import json
import os
import re
from typing import Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
import pandas as pd

COUNTRY_CODE_PATH = "data-raw/country_code.parquet"
DEFAULT_OCCURRENCE_PARQUET_PATH = (
    "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/*"
)

CORE_BASIS_OF_RECORD = (
    "LIVING_SPECIMEN",
    "OBSERVATION",
    "HUMAN_OBSERVATION",
    "MACHINE_OBSERVATION",
    "OCCURRENCE",
    "MATERIAL_SAMPLE",
)

ISO2_BLACKLIST = {
    "IN",
    "AS",
    "OR",
    "NO",
    "BE",
    "TO",
    "US",
    "IS",
    "IT",
    "AN",
    "ON",
    "AT",
    "BY",
    "DO",
    "IF",
    "ME",
    "MY",
    "SO",
    "UP",
    "WE",
    "HE",
    "AM",
}


@dataclass
class PublisherCountryFilters:
    apply_core_filters: bool = False


def compute_publisher_country_share(
    country_code: str,
    occurrence_source_path: str = DEFAULT_OCCURRENCE_PARQUET_PATH,
    use_db: bool = False,
    filters: PublisherCountryFilters = PublisherCountryFilters(),
    org_country_cache_path: Optional[str] = None,
    org_country_overrides: Optional[Dict[str, str]] = None,
    fetch_registry: bool = True,
    registry_base_url: str = "https://api.gbif.org/v1",
) -> Dict[str, List[Dict[str, float]]]:
    clean_country = country_code.upper()
    con, source_expr = _build_source_expression(occurrence_source_path, use_db)
    condition_sql = _build_conditions(clean_country, filters)

    query = f"""
    WITH source AS (
        SELECT
            countrycode,
            class,
            publishingorgkey,
            institutioncode,
            collectioncode,
            rightsholder
        FROM {source_expr}
        WHERE {condition_sql}
    ),
    scoped AS (
        SELECT 'all_records' AS scope, * FROM source
        UNION ALL
        SELECT 'exclude_aves' AS scope, * FROM source
        WHERE class IS NULL OR UPPER(class) != 'AVES'
    )
    SELECT
        scope,
        publishingorgkey,
        institutioncode,
        collectioncode,
        rightsholder,
        COUNT(*)::BIGINT AS record_count
    FROM scoped
    GROUP BY scope, publishingorgkey, institutioncode, collectioncode, rightsholder
    """

    grouped = con.execute(query).df()
    grouped["publishingorgkey"] = grouped["publishingorgkey"].where(
        grouped["publishingorgkey"].notna(), None
    )
    grouped["institutioncode"] = grouped["institutioncode"].where(
        grouped["institutioncode"].notna(), None
    )
    grouped["collectioncode"] = grouped["collectioncode"].where(
        grouped["collectioncode"].notna(), None
    )
    grouped["rightsholder"] = grouped["rightsholder"].where(
        grouped["rightsholder"].notna(), None
    )

    org_country_map = build_publishing_org_country_map(
        grouped["publishingorgkey"].dropna().unique().tolist(),
        org_country_cache_path=org_country_cache_path,
        org_country_overrides=org_country_overrides,
        fetch_registry=fetch_registry,
        registry_base_url=registry_base_url,
    )

    grouped["org_country_explicit"] = (
        grouped["publishingorgkey"].map(org_country_map).str.upper()
    )

    grouped["lookup_text"] = grouped.apply(
        lambda row: " ".join(
            str(value)
            for value in [
                row.get("rightsholder"),
                row.get("institutioncode"),
                row.get("collectioncode"),
            ]
            if value is not None and pd.notna(value)
        ),
        axis=1,
    )

    inferred_map = _build_inferred_country_map(grouped["lookup_text"].unique().tolist())
    grouped["org_country_inferred"] = grouped["lookup_text"].map(inferred_map)

    explicit_rows = _aggregate_buckets(
        grouped,
        clean_country,
        mode="explicit",
    )
    inferred_rows = _aggregate_buckets(
        grouped,
        clean_country,
        mode="explicit_or_inferred",
    )

    return {"rows": explicit_rows + inferred_rows}


def _build_source_expression(occurrence_source_path: str, use_db: bool):
    if use_db:
        con = duckdb.connect(occurrence_source_path, read_only=True)
        return con, "occurrence"
    con = duckdb.connect()
    return con, f"read_parquet('{occurrence_source_path}')"


def _build_conditions(country_code: str, filters: PublisherCountryFilters) -> str:
    conditions = [f"countrycode = '{country_code}'"]
    if filters.apply_core_filters:
        conditions.extend(
            [
                "UPPER(taxonrank) = 'SPECIES'",
                "species IS NOT NULL",
                "UPPER(basisofrecord) IN ('LIVING_SPECIMEN', 'OBSERVATION', "
                "'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION', 'OCCURRENCE', 'MATERIAL_SAMPLE')",
                "UPPER(occurrencestatus) = 'PRESENT'",
                "decimallatitude IS NOT NULL",
                "decimallongitude IS NOT NULL",
            ]
        )
    return " AND ".join(conditions)


def build_publishing_org_country_map(
    org_keys: Iterable[str],
    org_country_cache_path: Optional[str],
    org_country_overrides: Optional[Dict[str, str]],
    fetch_registry: bool,
    registry_base_url: str,
) -> Dict[str, Optional[str]]:
    mapping: Dict[str, Optional[str]] = {}
    if org_country_cache_path and os.path.exists(org_country_cache_path):
        mapping.update(_load_country_cache(org_country_cache_path))
    if org_country_overrides:
        mapping.update({key: value for key, value in org_country_overrides.items()})

    missing = [key for key in org_keys if key and key not in mapping]
    if fetch_registry:
        for key in missing:
            mapping[key] = _fetch_org_country(key, registry_base_url)

    if org_country_cache_path and missing:
        _write_country_cache(org_country_cache_path, mapping)

    return mapping


def _load_country_cache(cache_path: str) -> Dict[str, Optional[str]]:
    if cache_path.lower().endswith(".parquet"):
        df = pd.read_parquet(cache_path)
    else:
        df = pd.read_csv(cache_path)
    return {
        str(row["publishingorgkey"]): row["country"]
        for _, row in df.iterrows()
        if row.get("publishingorgkey")
    }


def _write_country_cache(cache_path: str, mapping: Dict[str, Optional[str]]) -> None:
    df = pd.DataFrame(
        [
            {"publishingorgkey": key, "country": value}
            for key, value in mapping.items()
            if key
        ]
    )
    if cache_path.lower().endswith(".parquet"):
        df.to_parquet(cache_path, index=False)
    else:
        df.to_csv(cache_path, index=False)


def _fetch_org_country(org_key: str, registry_base_url: str) -> Optional[str]:
    url = f"{registry_base_url.rstrip('/')}/organization/{org_key}"
    request = Request(url, headers={"User-Agent": "gbif_country_index"})
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, json.JSONDecodeError):
        return None
    return payload.get("country")


def _build_inferred_country_map(text_values: Iterable[str]) -> Dict[str, Optional[str]]:
    country_df = pd.read_parquet(COUNTRY_CODE_PATH)
    candidates = country_df[
        country_df["country_lower"].notna() & country_df["iso2c"].notna()
    ]
    name_pairs = sorted(
        (
            (row["country_lower"], row["iso2c"].upper())
            for _, row in candidates.iterrows()
        ),
        key=lambda item: len(item[0]),
        reverse=True,
    )
    iso2_map = {
        row["iso2c"].upper(): row["iso2c"].upper()
        for _, row in candidates.iterrows()
        if isinstance(row["iso2c"], str)
    }
    iso3_map = {
        row["iso3c"].upper(): row["iso2c"].upper()
        for _, row in candidates.iterrows()
        if isinstance(row["iso3c"], str)
    }

    inferred: Dict[str, Optional[str]] = {}
    for text in text_values:
        if not text:
            inferred[text] = None
            continue
        inferred[text] = _infer_country_from_text(text, name_pairs, iso2_map, iso3_map)
    return inferred


def _infer_country_from_text(
    text: str,
    name_pairs: List[tuple],
    iso2_map: Dict[str, str],
    iso3_map: Dict[str, str],
) -> Optional[str]:
    text_lower = text.lower()
    for name, iso2 in name_pairs:
        if name and name in text_lower:
            return iso2

    upper_text = text.upper()
    iso3_hits = re.findall(r"\b[A-Z]{3}\b", upper_text)
    for token in iso3_hits:
        match = iso3_map.get(token)
        if match:
            return match

    iso2_hits = re.findall(r"\b[A-Z]{2}\b", upper_text)
    for token in iso2_hits:
        if token in ISO2_BLACKLIST:
            continue
        match = iso2_map.get(token)
        if match:
            return match
    return None


def _aggregate_buckets(
    grouped: pd.DataFrame,
    target_country: str,
    mode: str,
) -> List[Dict[str, float]]:
    if grouped.empty:
        return _empty_rows(mode)

    if mode == "explicit":
        resolved = grouped["org_country_explicit"]
    else:
        resolved = grouped["org_country_explicit"].where(
            grouped["org_country_explicit"].notna(), grouped["org_country_inferred"]
        )

    def classify(value: Optional[str]) -> str:
        if value is None or pd.isna(value):
            return "missing"
        return "inside" if value == target_country else "outside"

    bucketed = grouped.assign(bucket=resolved.apply(classify))
    grouped_counts = (
        bucketed.groupby(["scope", "bucket"], dropna=False)["record_count"]
        .sum()
        .reset_index()
    )

    rows = []
    for scope in ["all_records", "exclude_aves"]:
        scope_counts = grouped_counts[grouped_counts["scope"] == scope]
        counts = {bucket: 0 for bucket in ["inside", "outside", "missing"]}
        for _, row in scope_counts.iterrows():
            counts[row["bucket"]] = int(row["record_count"])
        total = sum(counts.values())
        rows.append(
            {
                "scope": scope,
                "mode": mode,
                "inside_count": counts["inside"],
                "outside_count": counts["outside"],
                "missing_count": counts["missing"],
                "inside_pct": counts["inside"] / total if total else 0.0,
                "outside_pct": counts["outside"] / total if total else 0.0,
                "missing_pct": counts["missing"] / total if total else 0.0,
                "total": total,
            }
        )
    return rows


def _empty_rows(mode: str) -> List[Dict[str, float]]:
    return [
        {
            "scope": scope,
            "mode": mode,
            "inside_count": 0,
            "outside_count": 0,
            "missing_count": 0,
            "inside_pct": 0.0,
            "outside_pct": 0.0,
            "missing_pct": 0.0,
            "total": 0,
        }
        for scope in ["all_records", "exclude_aves"]
    ]
