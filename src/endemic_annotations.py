from dataclasses import dataclass


@dataclass(frozen=True)
class EndemicReference:
    code: str
    source_reader: str
    source_path: str
    source_key_expr: str


AU_ENDEMIC_REFERENCE = EndemicReference(
    code="au",
    source_reader="read_csv_auto",
    source_path="/Volumes/Mybook18/wikidata/data/taxa_australia.csv",
    source_key_expr="LOWER(scientific_name)",
)

ZA_ENDEMIC_REFERENCE = EndemicReference(
    code="za",
    source_reader="read_parquet",
    source_path="/Users/pauloldham/Documents/knowledgebase/data/south_africa_sanbi_endemics.parquet",
    source_key_expr="canonical_lower",
)


def build_endemic_cte(reference: EndemicReference) -> str:
    return (
        f"{reference.code}_endemics AS (\n"
        f"            SELECT DISTINCT {reference.source_key_expr} as canonical_lower, TRUE as is_endemic_{reference.code}\n"
        f"            FROM {reference.source_reader}('{reference.source_path}')\n"
        f"        )"
    )


def build_endemic_join(reference: EndemicReference, table_alias: str = "b") -> str:
    return (
        f"LEFT JOIN {reference.code}_endemics {reference.code} "
        f"ON {table_alias}.canonical_lower = {reference.code}.canonical_lower"
    )


def build_endemic_columns(reference: EndemicReference, table_alias: str = "b") -> str:
    code = reference.code
    return (
        f"COALESCE({code}.is_endemic_{code}, FALSE) as is_endemic_{code},\n"
        f"            CASE \n"
        f"                WHEN COALESCE({code}.is_endemic_{code}, FALSE) AND {table_alias}.breadth_300_count <= 2 THEN 'Endemic (Localized)'\n"
        f"                WHEN COALESCE({code}.is_endemic_{code}, FALSE) AND {table_alias}.breadth_300_count > 2 THEN 'Endemic (Widespread)'\n"
        f"                WHEN NOT COALESCE({code}.is_endemic_{code}, FALSE) AND {table_alias}.breadth_300_count <= 2 THEN 'Localized (Non-Endemic)'\n"
        f"                ELSE 'Widespread (Non-Endemic)'\n"
        f"            END as endemic_status_{code}"
    )


def build_endemic_ctes(references) -> str:
    return ",\n        ".join(build_endemic_cte(reference) for reference in references)


def build_endemic_joins(references, table_alias: str = "b") -> str:
    return "\n        ".join(build_endemic_join(reference, table_alias=table_alias) for reference in references)


def build_endemic_select_columns(references, table_alias: str = "b") -> str:
    return ",\n            ".join(build_endemic_columns(reference, table_alias=table_alias) for reference in references)
