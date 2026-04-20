"""Generate the CBD Publisher Country Share Report (no-Aves primary, All Taxa annex).

This report presents the GBIF publisher country share analysis with Class Aves
excluded as the primary dataset, because bird observation data from citizen science
platforms (eBird, iNaturalist) dominate the dataset and are primarily published by
organisations in developed countries. An annex with All Taxa (including Aves) is
provided for reference.
"""

import csv
import os
from datetime import date

PROCESSED_DIR = "data/processed"
OUTPUT_PATH = "CBD_Publisher_Country_Share_Report.md"

DATA_SOURCE_CITATION = "GBIF.org (1 June 2025) GBIF Occurrence Download https://doi.org/10.15468/dl.jsevhc, downloaded 1 January 2026."
SNAPSHOT_LABEL = "2026"

# File registry
NO_AVES_FILES = {
    "un_region": "cbd_parties_no_aves_un_region_summary.csv",
    "un_sub_region": "cbd_parties_no_aves_un_region_un_sub_region_summary.csv",
    "un_intermediate_region": "cbd_parties_no_aves_un_region_un_sub_region_un_intermediate_region_summary.csv",
    "development_status": "cbd_parties_no_aves_development_status_summary.csv",
    "income_group": "cbd_parties_no_aves_income_group_summary.csv",
}

ALL_TAXA_FILES = {
    "un_region": "cbd_parties_all_taxa_un_region_summary.csv",
    "un_sub_region": "cbd_parties_all_taxa_un_region_un_sub_region_summary.csv",
    "un_intermediate_region": "cbd_parties_all_taxa_un_region_un_sub_region_un_intermediate_region_summary.csv",
    "development_status": "cbd_parties_all_taxa_development_status_summary.csv",
    "income_group": "cbd_parties_all_taxa_income_group_summary.csv",
}


def _fmt_int(value):
    return f"{int(value):,}"


def _fmt_pct(value):
    return f"{float(value):.2f}%"


def _read_csv(filename):
    path = os.path.join(PROCESSED_DIR, filename)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _cell(value, is_pct=False):
    try:
        f = float(value)
        if is_pct:
            return _fmt_pct(f)
        if f.is_integer():
            return _fmt_int(f)
        return f"{f:,.2f}"
    except (ValueError, TypeError):
        return str(value) if value else "—"


def _render_table(col_specs, rows):
    if not rows:
        return "_No data found._"
    headers = [spec[0] for spec in col_specs]
    table = ["| " + " | ".join(headers) + " |"]
    table.append("| " + " | ".join(["---" for _ in headers]) + " |")
    for row in rows:
        table.append("| " + " | ".join(
            _cell(row.get(spec[1], ""), is_pct=spec[2])
            for spec in col_specs
        ) + " |")
    return "\n".join(table)


_COUNT_COLS = [
    ("Internal Count", "internal_count", False),
    ("Regional Count", "regional_count", False),
    ("External Count", "external_count", False),
    ("Unknown Count", "unknown_count", False),
    ("Total Count", "total_count", False),
    ("Internal %", "internal_percentage", True),
    ("Regional %", "regional_percentage", True),
    ("External %", "external_percentage", True),
]


def _region_table(rows, label_col="un_region_name"):
    col_specs = [(label_col.replace("_", " ").title(), label_col, False)] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _sub_region_table(rows):
    col_specs = [
        ("UN Region", "un_region_name", False),
        ("UN Sub-region", "un_sub_region_name", False),
    ] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _intermediate_region_table(rows):
    col_specs = [
        ("UN Region", "un_region_name", False),
        ("UN Sub-region", "un_sub_region_name", False),
        ("Intermediate Region", "un_intermediate_region_name", False),
    ] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _dev_status_table(rows):
    col_specs = [("Development Status", "un_developed_or_developing_countries", False)] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _income_table(rows):
    col_specs = [("Income Group", "wb_income_group", False)] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _country_table(rows):
    col_specs = [
        ("Country", "country_name", False),
        ("ISO3", "iso3c", False),
        ("UN Region", "un_region_name", False),
        ("Income Group", "wb_income_group", False),
        ("LDC", "is_ldc", False),
        ("SIDS", "is_sids", False),
        ("Internal", "internal_count", False),
        ("Regional", "regional_count", False),
        ("External", "external_count", False),
        ("Total", "total_count", False),
        ("Int %", "internal_percentage", True),
        ("Reg %", "regional_percentage", True),
        ("Ext %", "external_percentage", True),
        ("Flags", "data_flags", False),
    ]
    return _render_table(col_specs, rows)


def _section(data, key, renderer_fn):
    rows = data.get(key, [])
    if not rows:
        return ["_Data not available._", ""]
    return [renderer_fn(rows), ""]


def generate_report(output_path=OUTPUT_PATH):
    no_aves = {key: _read_csv(fname) for key, fname in NO_AVES_FILES.items()}
    all_taxa = {key: _read_csv(fname) for key, fname in ALL_TAXA_FILES.items()}

    lines = [
        "# CBD Parties Publisher Country Share Report",
        "",
        f"_Generated: {date.today().isoformat()} | Snapshot: {SNAPSHOT_LABEL}_",
        "",
        "## 1. Introduction",
        "",
        "This report examines the geographic origin of data publishing for countries that are Parties to the Convention on Biological Diversity (CBD). "
        "Each occurrence record in the GBIF dataset is classified as:",
        "",
        "- **Internal**: The record was published by an organisation based in the same country as the occurrence.",
        "- **Regional**: The record was published by an organisation in a different country within the same UN region (e.g. a US-published record for Canada is Regional, not External, because both are in the Americas).",
        "- **External**: The record was published by an organisation in a different UN region from the occurrence.",
        "- **Unknown**: The publisher's country could not be resolved from the GBIF registry.",
        "",
        "**Note on coverage:** Approximately 43 million records in the GBIF dataset (roughly 1.2% of 3.7 billion) have no occurrence country code and therefore cannot be assigned to any CBD Party. These records are excluded from the analysis.",
        "",
        f"**Data Source:** {DATA_SOURCE_CITATION}",
        "",
        "### Why Birds (Aves) are Excluded from the Primary Analysis",
        "",
        "Bird observation data (Class Aves) accounts for approximately 2 billion of the 3.7 billion records in the GBIF dataset (64%). "
        "This data is overwhelmingly generated by citizen science platforms — principally **eBird** (Cornell Lab of Ornithology, US) and **iNaturalist** (US) — "
        "which are based in developed countries. Because these platforms are classified as publishers in the country where the organisation is registered, "
        "bird records for any country are counted as externally published, even when the observations are submitted by in-country citizen scientists.",
        "",
        "This creates a systematic bias: the internal publishing percentage for countries with large citizen-science bird datasets is artificially depressed, "
        "while countries with domestic bird organisations (e.g. South Africa's FitzPatrick Institute) appear to have higher internal shares. "
        "The effect is particularly pronounced for biodiverse developing countries that have extensive eBird coverage but few domestic GBIF publishers.",
        "",
        "For this reason, **the primary analysis in this report excludes Class Aves**. An annex with Aves (Birds) data is provided for reference, "
        "but the Excluding Birds data should be used for policy analysis concerning gaps in taxonomic capacity and domestic data publishing infrastructure.",
        "",
        "---",
        "",
        "## 2. Source Distribution (Excluding Birds)",
        "",
        "### 2.1 By UN Region",
        "",
    ]
    lines.extend(_section(no_aves, "un_region", _region_table))
    # Add interpretive text
    lines.extend([
        "Europe dominates internal publishing, while Africa and the Americas have the majority of their biodiversity data published by organisations based elsewhere.",
        "",
    ])

    lines.append("### 2.2 By UN Sub-region")
    lines.append("")
    lines.extend(_section(no_aves, "un_sub_region", _sub_region_table))
    lines.extend([
        "Sub-Saharan Africa shows a markedly different pattern from Northern Africa. "
        "In the Americas, Latin America and the Caribbean has near-parity internal/external publishing, while Northern America is close to even.",
        "",
    ])

    lines.append("### 2.3 By UN Intermediate Region")
    lines.append("")
    lines.extend(_section(no_aves, "un_intermediate_region", _intermediate_region_table))
    lines.extend([
        "At the intermediate region level, Eastern Africa and Middle Africa remain heavily dependent on external publishers. "
        "Southern Africa is the exception within Africa, driven by South African publishing organisations. "
        "In the Americas, the Caribbean stands out for high external dependence.",
        "",
    ])

    lines.append("### 2.4 By Development Status")
    lines.append("")
    lines.extend(_section(no_aves, "development_status", _dev_status_table))
    lines.extend([
        "Developing countries publish a substantially smaller share of their own biodiversity data internally compared to developed countries.",
        "",
    ])

    lines.append("### 2.5 By World Bank Income Group")
    lines.append("")
    lines.extend(_section(no_aves, "income_group", _income_table))
    lines.extend([
        "There is a steep gradient: low and lower-middle income countries are almost entirely dependent on external publishers. "
        "Upper middle income countries show improved internal publishing shares when Aves is excluded.",
        "",
    ])

    # Country table
    import sys, importlib
    sys.path.insert(0, os.path.dirname(__file__))
    from generate_country_tables import generate_country_table
    country_df = generate_country_table()
    country_rows = [row._asdict() if hasattr(row, '_asdict') else row for row in country_df.to_dict('records')]
    lines.append("### 2.6 Per-Country Table (Alphabetical)")
    lines.append("")
    lines.append("LDC = Least Developed Country. SIDS = Small Island Developing State. "
                 "Int % = Internal publishing percentage (same country). "
                 "Reg % = Regional publishing percentage (same UN region, different country). "
                 "Ext % = External publishing percentage (different UN region). "
                 "Flags indicate data quality concerns.")
    lines.append("")
    lines.append(_country_table(country_rows))
    lines.extend([
        "",
        "---",
        "",
        "## 3. Key Findings",
        "",
        "- **Developing countries publish a minority of their own biodiversity data internally.** The majority of records for developing CBD Parties are published by organisations based in other countries.",
        "- **Low and lower-middle income countries are almost entirely dependent on external publishers**, with very low internal shares.",
        "- **Europe is the only region where internal publishing dominates** (excluding Aves). When including regional (same-UN-region) publishing, the picture changes significantly: the Americas has 51% internal + regional publishing combined, meaning only 49% of data is published from outside the region. Africa has 19% internal + regional, with 81% published from outside the region.",
        "- **Southern Africa is an outlier within Africa**, with higher internal publishing compared to Eastern and Middle Africa.",
        "- **Excluding Aves provides a more accurate picture of domestic taxonomic capacity.** Including Aves data systematically depresses internal publishing percentages for countries with extensive citizen-science coverage, as these records are classified under the publisher's country (typically a developed country).",
        "",
        "---",
        "",
        "## 4. Methodology",
        "",
        "For full methodology details, see [README.md](README.md).",
        "",
        "In summary:",
        "",
        "1. The GBIF registry was downloaded and a local lookup table (`data/gbif_registry_lookup.parquet`) was built mapping `publishingorgkey` to `resolved_country`.",
        "2. Each occurrence record was classified as Internal (publisher country = occurrence country) or External (publisher country differs) or Unknown (no registry match).",
        "3. Results were enriched with UN regional, development status, and World Bank income group metadata.",
        "4. CBD party records were filtered and aggregated into the summary tables used in this report.",
        "",
        "**Processing scripts:**",
        "- `src/calculate_source_distribution.py` — Source classification and aggregation",
        "- `src/create_registry_lookup.py` — Registry reconciliation",
        "- `src/enrich_source_distribution.py` — Metadata enrichment",
        "- `src/analyze_cbd_parties.py` — CBD Parties summaries",
        "",
        "---",
        "",
        "## Annex A: Aves (Birds)",
        "",
        "The tables below include all taxa, including Class Aves (birds). As explained in Section 1, "
        "bird data from citizen science platforms (eBird, iNaturalist) is primarily published by organisations in developed countries "
        "and can distort the internal/external ratio, particularly for biodiverse developing countries with extensive citizen-science coverage. "
        "These figures should be interpreted with caution for policy analysis.",
        "",
    ])

    # All Taxa sections
    lines.append("### A.1 By UN Region")
    lines.append("")
    lines.extend(_section(all_taxa, "un_region", _region_table))

    lines.append("### A.2 By UN Sub-region")
    lines.append("")
    lines.extend(_section(all_taxa, "un_sub_region", _sub_region_table))

    lines.append("### A.3 By UN Intermediate Region")
    lines.append("")
    lines.extend(_section(all_taxa, "un_intermediate_region", _intermediate_region_table))

    lines.append("### A.4 By Development Status")
    lines.append("")
    lines.extend(_section(all_taxa, "development_status", _dev_status_table))

    lines.append("### A.5 By World Bank Income Group")
    lines.append("")
    lines.extend(_section(all_taxa, "income_group", _income_table))

    # Source data listing
    lines.extend([
        "",
        "---",
        "",
        "## Source Data",
        "",
    ])
    all_source_files = sorted(set(
        list(NO_AVES_FILES.values()) + list(ALL_TAXA_FILES.values())
    ))
    for fname in all_source_files:
        lines.append(f"- `data/processed/{fname}`")

    lines.extend([
        "",
        f"**Data Source:** {DATA_SOURCE_CITATION}",
        f"**Generated:** {date.today().isoformat()}",
    ])

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(f"CBD Publisher Country Share Report written to {output_path}")


if __name__ == "__main__":
    generate_report()
