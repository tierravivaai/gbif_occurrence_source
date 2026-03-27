import duckdb
import os
from datetime import date

try:
    from src.endemic_annotations import (
        AU_ENDEMIC_REFERENCE,
        ZA_ENDEMIC_REFERENCE,
        build_endemic_ctes,
        build_endemic_joins,
        build_endemic_select_columns,
    )
except ImportError:  # pragma: no cover
    from endemic_annotations import (
        AU_ENDEMIC_REFERENCE,
        ZA_ENDEMIC_REFERENCE,
        build_endemic_ctes,
        build_endemic_joins,
        build_endemic_select_columns,
    )

OCCURRENCE_PATH = "/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump/gbif-dump-2024-04-01/occurrence.parquet"
COUNTRY_CODE_PATH = "data-raw/country_code.parquet"
GBIF_COUNTRY_INDEX_PATH = "data/gbif_2024/gbif_country_index_2024.parquet"


def ensure_parent_dir(path):
    output_dir = os.path.dirname(path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)


def build_country_index(output_path=GBIF_COUNTRY_INDEX_PATH):
    print("Starting processing for 2024 snapshot with DuckDB...")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='32GB'")
    ensure_parent_dir(output_path)

    query = f"""
    COPY (
        WITH base AS (
            SELECT
                species,
                LOWER(str_split(species, ' ')[1] || ' ' || str_split(species, ' ')[2]) as canonical_lower,
                kingdom,
                countrycode,
                basisofrecord,
                occurrencestatus,
                decimallatitude,
                decimallongitude
            FROM read_parquet('{OCCURRENCE_PATH}')
            WHERE UPPER(taxonrank) = 'SPECIES'
              AND (class IS NULL OR UPPER(class) != 'AVES')
              AND species IS NOT NULL
              AND UPPER(basisofrecord) IN ('LIVING_SPECIMEN', 'OBSERVATION', 'HUMAN_OBSERVATION', 'MACHINE_OBSERVATION', 'OCCURRENCE', 'MATERIAL_SAMPLE')
              AND UPPER(occurrencestatus) = 'PRESENT'
              AND decimallatitude IS NOT NULL
              AND decimallongitude IS NOT NULL
        ),
        agg_base AS (
            SELECT
                canonical_lower,
                kingdom,
                countrycode,
                ANY_VALUE(species) as species,
                COUNT(*) as country_count
            FROM base
            GROUP BY canonical_lower, kingdom, countrycode
        ),
        scored_base AS (
            SELECT
                b.*,
                c.un_region_clean as un_region_name_clean,
                c.un_sub_region_clean as un_sub_region_name_clean,
                c.un_intermediate_region_clean,
                c.un_developed_or_developing_countries,
                c.un_least_developed_countries_ldc,
                c.un_small_island_developing_states_sids as un_small_island_developing_countries_lldc,
                CAST(b.country_count AS DOUBLE) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower) as likelihood_country,
                SUM(b.country_count) OVER(PARTITION BY b.canonical_lower, c.un_region_clean) / SUM(b.country_count) OVER(PARTITION BY b.canonical_lower) as likelihood_un_region,
                COUNT(DISTINCT CASE WHEN b.country_count >= 300 THEN b.countrycode END) OVER(PARTITION BY b.canonical_lower) as breadth_300_count,
                COUNT(DISTINCT b.countrycode) OVER(PARTITION BY b.canonical_lower) as country_breadth,
                COUNT(DISTINCT c.un_region_clean) OVER(PARTITION BY b.canonical_lower) as un_region_breadth
            FROM agg_base b
            LEFT JOIN read_parquet('{COUNTRY_CODE_PATH}') c ON b.countrycode = c.iso2c
        ),
        {build_endemic_ctes([AU_ENDEMIC_REFERENCE, ZA_ENDEMIC_REFERENCE])}
        SELECT
            b.species,
            b.canonical_lower,
            b.kingdom,
            b.countrycode as country_code,
            b.country_count,
            b.un_region_name_clean as un_region,
            b.un_sub_region_name_clean as un_sub_region,
            b.un_intermediate_region_clean as un_intermediate_region,
            b.un_developed_or_developing_countries as un_development_status,
            b.un_least_developed_countries_ldc as un_ldc,
            b.un_small_island_developing_countries_lldc as un_sids,
            b.breadth_300_count as significant_breadth,
            b.country_breadth,
            b.un_region_breadth,
            {build_endemic_select_columns([AU_ENDEMIC_REFERENCE, ZA_ENDEMIC_REFERENCE], table_alias="b")},
            b.likelihood_country * (1.0 / sqrt(GREATEST(b.breadth_300_count, 1))) as likelihood_country,
            b.likelihood_un_region * (1.0 / sqrt(GREATEST(b.breadth_300_count, 1))) as likelihood_un_region,
            (0.7 * b.likelihood_un_region + 0.3 * b.likelihood_country) * (1.0 / sqrt(GREATEST(b.breadth_300_count, 1))) as localisation_score
        FROM scored_base b
        {build_endemic_joins([AU_ENDEMIC_REFERENCE, ZA_ENDEMIC_REFERENCE], table_alias="b")}
    ) TO '{output_path}' (FORMAT PARQUET)
    """

    try:
        con.execute(query)
        print(f"Success! Output saved to {output_path}")
    except Exception as e:
        print(f"Error during processing: {e}")


def _fmt_int(value):
    return f"{int(value):,}"


def _render_markdown_table(headers, rows):
    if not rows:
        return "_No data found._"

    def cell(value):
        if isinstance(value, float) and value.is_integer():
            return _fmt_int(value)
        if isinstance(value, (int,)):
            return _fmt_int(value)
        return str(value)

    table = ["| " + " | ".join(headers) + " |"]
    table.append("| " + " | ".join(["---" for _ in headers]) + " |")
    for row in rows:
        table.append("| " + " | ".join(cell(value) for value in row) + " |")
    return "\n".join(table)


def infer_source_data_files(occurrence_source_path, country_index_path, endemic_path):
    source_files = [occurrence_source_path]
    if occurrence_source_path.lower().endswith(".db"):
        derived_occurrence = os.path.join(os.path.dirname(occurrence_source_path), "occurrence.parquet")
        if os.path.exists(derived_occurrence):
            source_files.append(derived_occurrence)
    source_files.extend([country_index_path, endemic_path])
    return source_files


def generate_south_africa_report(
    output_path="South_Africa_Biodiversity_2024.md",
    snapshot_label="2024",
    snapshot_description="April 2024",
    data_source_citation="GBIF.org (1 April 2024) GBIF Occurrence Download",
    processing_script="src/gbif_2024_processing.py",
    country_code="ZA",
    country_name="South Africa",
    country_adjective="South African",
    occurrence_source_path="/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump/country_occurrences_ZA.csv",
    endemic_path="/Users/pauloldham/Documents/knowledgebase/data/south_africa_sanbi_endemics.parquet",
    country_index_path=GBIF_COUNTRY_INDEX_PATH,
    source_data_files=None,
):
    clean_country_code = country_code.upper()
    source_is_db = occurrence_source_path.lower().endswith(".db")
    con = duckdb.connect(occurrence_source_path) if source_is_db else duckdb.connect()
    source_expr = "occurrence" if source_is_db else (
        f"read_csv_auto('{occurrence_source_path}')" if occurrence_source_path.lower().endswith(".csv")
        else f"read_parquet('{occurrence_source_path}')"
    )
    source_species = "species" if source_is_db or not occurrence_source_path.lower().endswith(".csv") else "scientificname"

    raw_total_rows, raw_total_species = con.execute(
        f"""
        SELECT COUNT(*)::BIGINT, COUNT(DISTINCT {source_species})::BIGINT
        FROM {source_expr}
        WHERE countrycode = '{clean_country_code}'
        """
    ).fetchone()

    kingdom_rows = con.execute(
        f"""
        SELECT kingdom, COUNT(*)::BIGINT AS occurrence_count, COUNT(DISTINCT {source_species})::BIGINT AS species_count
        FROM {source_expr}
        WHERE countrycode = '{clean_country_code}'
        GROUP BY kingdom
        ORDER BY occurrence_count DESC, kingdom
        """
    ).fetchall()

    clean_occurrences, clean_species = con.execute(
        f"""
        SELECT COALESCE(SUM(country_count), 0)::BIGINT, COUNT(DISTINCT canonical_lower)::BIGINT
        FROM read_parquet('{country_index_path}')
        WHERE country_code = '{clean_country_code}'
        """
    ).fetchone()

    highly_distinctive_species = con.execute(
        f"""
        SELECT COUNT(*)::BIGINT
        FROM (
            SELECT canonical_lower, MAX(localisation_score) AS max_localisation_score
            FROM read_parquet('{country_index_path}')
            WHERE country_code = '{clean_country_code}'
            GROUP BY canonical_lower
        )
        WHERE max_localisation_score > 0.8
        """
    ).fetchone()[0]

    distinctive_share = (highly_distinctive_species / clean_species) if clean_species else 0

    endemic_rows = con.execute(
        f"""
        SELECT
            e.sanbi_endemic_status,
            COUNT(DISTINCT b.canonical_lower)::BIGINT AS species_count
        FROM read_parquet('{country_index_path}') b
        JOIN read_parquet('{endemic_path}') e
          ON b.canonical_lower = e.canonical_lower
        WHERE b.country_code = '{clean_country_code}'
        GROUP BY 1
        ORDER BY species_count DESC, sanbi_endemic_status
        """
    ).fetchall()

    endemic_total = sum(row[1] for row in endemic_rows)
    source_data_files = source_data_files or infer_source_data_files(
        occurrence_source_path,
        country_index_path,
        endemic_path,
    )

    lines = [
        f"# Report: {country_adjective} Biodiversity Insights ({snapshot_label} GBIF Snapshot)",
        "",
        "## 1. Overview",
        f"This report provides a summary of the {country_adjective} biodiversity data available in the {snapshot_description} GBIF (Global Biodiversity Information Facility) snapshot. It reflects a major effort to clean, categorize, and prioritize species based on their biological significance and geographic distinctiveness.",
        "",
        "## 2. The Data Foundation",
        f"In the {snapshot_label} snapshot, {country_name} is represented by over **{_fmt_int(raw_total_rows)} records**, making it one of the most comprehensively mapped regions in the world. After applying high-quality filters (removing birds, fossils, and records with GPS errors), we focused on a core set of **{_fmt_int(clean_occurrences)} occurrences** covering **{_fmt_int(clean_species)} distinct species**.",
        "",
        f"### Occurrences by Kingdom ({country_code.upper()})",
        "The majority of observations are concentrated in the Animal and Plant kingdoms, reflecting both biodiversity richness and sampling effort.",
        "",
        _render_markdown_table(["Kingdom", "Occurrences", "Species"], kingdom_rows),
        "",
        "## 3. Measuring \"Distinctiveness\"",
        f"A major goal of this project was to move beyond simple record counts and identify species that are **truly characteristic of {country_name}**. We developed a **Localisation Score** that measures how unique a species is to the {country_adjective} landscape.",
        "",
        f"*   **The Problem:** Common global species can have millions of records in {country_name}, which can overshadow unique native species in raw statistics.",
        f"*   **The Solution:** We applied a \"Commonness Penalty\" to species found in many countries. This allows us to highlight species that are biologically concentrated in {country_name}.",
        f"*   **Highly Distinctive Species:** We identified **{_fmt_int(highly_distinctive_species)} species** ({distinctive_share:.0%} of the total) with a **Localisation Score > 0.8**.",
        "",
        "## 4. Biological Origin and Endemic Status",
        f"By integrating SANBI endemic data, we can distinguish between a species' current distribution and its conservation status in {country_name}.",
        "",
        f"*   **SANBI Endemic Records:** **{_fmt_int(endemic_total)} species** are flagged in the SANBI endemic reference.",
    ]

    if endemic_rows:
        lines.append("")
        lines.append(_render_markdown_table(["SANBI Status", "Species"], endemic_rows))

    lines.extend(
        [
            "",
            "## 5. Why This Matters",
            f"This dataset provides a refined lens for understanding the biodiversity footprint of {country_adjective} biology. By filtering out common global species, we can more effectively track how unique species are being used in scientific research and conservation analysis.",
            "",
            "## 6. Source Data Files",
        ]
    )
    lines.extend([f"- `{path}`" for path in source_data_files if path])
    lines.extend(
        [
            "",
            "---",
            f"**Data Source:** {data_source_citation}",
            f"**Processing Script:** `{processing_script}`",
            f"**Generated:** {date.today().isoformat()}",
        ]
    )

    ensure_parent_dir(output_path)
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")

    print(f"South Africa report written to {output_path}")


def main():
    build_country_index()


if __name__ == "__main__":
    main()
