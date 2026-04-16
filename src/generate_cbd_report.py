"""Generate a markdown report on CBD Party publisher country share from processed CSVs."""

import csv
import os
from datetime import date

PROCESSED_DIR = "data/processed"
OUTPUT_PATH = "CBD_Publisher_Country_Share_Report.md"

DATA_SOURCE_CITATION = "GBIF.org (1 January 2026) GBIF Occurrence Download https://doi.org/10.15468/dl.vp6jpz"
SNAPSHOT_LABEL = "2026"

ALL_TAXA_FILES = {
    "un_region": "cbd_parties_all_taxa_un_region_summary.csv",
    "un_intermediate_region": "cbd_parties_all_taxa_un_region_un_intermediate_region_summary.csv",
    "development_status": "cbd_parties_all_taxa_development_status_summary.csv",
    "income_group": "cbd_parties_all_taxa_income_group_summary.csv",
}

NO_AVES_FILES = {
    "un_region": "cbd_parties_no_aves_un_region_summary.csv",
    "un_intermediate_region": "cbd_parties_no_aves_un_region_un_intermediate_region_summary.csv",
    "development_status": "cbd_parties_no_aves_development_status_summary.csv",
    "income_group": "cbd_parties_no_aves_income_group_summary.csv",
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
        return str(value)


def _render_table(col_specs, rows):
    """Render a markdown table from column specs and dict rows.

    col_specs: list of (display_header, dict_key, is_percentage) tuples
    """
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
    ("External Count", "external_count", False),
    ("Unknown Count", "unknown_count", False),
    ("Total Count", "total_count", False),
    ("Internal %", "internal_percentage", True),
    ("External %", "external_percentage", True),
]


def _region_table(rows, label_col="un_region_name"):
    col_specs = [
        (label_col.replace("_", " ").title(), label_col, False),
    ] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _dev_status_table(rows):
    col_specs = [
        ("Development Status", "un_developed_or_developing_countries", False),
    ] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _income_table(rows):
    col_specs = [
        ("Income Group", "wb_income_group", False),
    ] + _COUNT_COLS
    return _render_table(col_specs, rows)


def _intermediate_region_table(rows):
    col_specs = [
        ("UN Region", "un_region_name", False),
        ("Intermediate Region", "un_intermediate_region_name", False),
    ] + _COUNT_COLS
    return _render_table(col_specs, rows)


def generate_report(output_path=OUTPUT_PATH):
    all_taxa = {key: _read_csv(fname) for key, fname in ALL_TAXA_FILES.items()}
    no_aves = {key: _read_csv(fname) for key, fname in NO_AVES_FILES.items()}

    lines = [
        "# CBD Party Publisher Country Share Report",
        "",
        f"_Generated: {date.today().isoformat()} | Snapshot: {SNAPSHOT_LABEL}_",
        "",
        "## 1. Overview",
        "",
        "This report examines the geographic origin of data publishing for countries that are parties to the Convention on Biological Diversity (CBD). "
        "Each occurrence record in the GBIF dataset is classified as:",
        "",
        "- **Internal**: The record was published by an organization based in the same country as the occurrence.",
        "- **External**: The record was published by an organization based in a different country.",
        "- **Unknown**: The publisher's country could not be resolved from the GBIF registry or text inference.",
        "",
        f"**Data Source:** {DATA_SOURCE_CITATION}",
        "",
        "Results are presented for all taxa and for a subset excluding Class Aves (birds), which accounts for approximately 64% of all GBIF records and can dominate aggregate statistics.",
        "",
        "---",
        "",
        "## 2. All Taxa",
        "",
        "### 2.1 By UN Region",
        "",
        _region_table(all_taxa["un_region"]),
        "",
        "Europe dominates internal publishing at 82.29%, while the Americas (14.76%) and Asia (18.14%) have the majority of their biodiversity data published by organizations based elsewhere. "
        "Africa sits at 43.51% internal, meaning over half of African occurrence data is externally published. Oceania is near parity at 54.11% internal.",
        "",
        "### 2.2 By UN Intermediate Region",
        "",
        _intermediate_region_table(all_taxa["un_intermediate_region"]),
        "",
        "At the intermediate region level, the Caribbean stands out with 98.10% external publishing. "
        "Eastern Africa (94.31%) and Middle Africa (94.60%) are similarly dependent on external publishers. "
        "Southern Africa is a notable exception in Africa at 68.72% internal, driven by South African publishing organisations. "
        "In the Americas, Central America is 87.26% external while South America is 75.53% external.",
        "",
        "### 2.3 By Development Status",
        "",
        _dev_status_table(all_taxa["development_status"]),
        "",
        "Developed countries publish 70.80% of their own data internally. Developing countries have only 19.98% internal publishing, with 80.02% of their biodiversity records published by organisations in other countries.",
        "",
        "### 2.4 By World Bank Income Group",
        "",
        _income_table(all_taxa["income_group"]),
        "",
        "The income group breakdown reveals a steep gradient: Low income countries have only 4.00% internal publishing (95.96% external), and Lower middle income countries are nearly as dependent at 3.95% internal (96.05% external). "
        "Upper middle income countries improve to 29.37% internal, while High income countries reach 68.42% internal.",
        "",
        "---",
        "",
        "## 3. Excluding Aves (Birds)",
        "",
        "Aves records account for approximately 2 billion of the 3.7 billion records in the full dataset. "
        "Excluding them provides a clearer picture of data publishing patterns for the remaining taxa.",
        "",
        "### 3.1 By UN Region",
        "",
    ]

    if no_aves.get("un_region"):
        lines.append(_region_table(no_aves["un_region"]))
    else:
        lines.append("_Data not available._")

    lines.extend([
        "",
        "### 3.2 By Development Status",
        "",
    ])
    if no_aves.get("development_status"):
        lines.append(_dev_status_table(no_aves["development_status"]))
    else:
        lines.append("_Data not available._")

    lines.extend([
        "",
        "### 3.3 By World Bank Income Group",
        "",
    ])
    if no_aves.get("income_group"):
        lines.append(_income_table(no_aves["income_group"]))
    else:
        lines.append("_Data not available._")

    lines.extend([
        "",
        "### 3.4 By UN Intermediate Region",
        "",
    ])
    if no_aves.get("un_intermediate_region"):
        lines.append(_intermediate_region_table(no_aves["un_intermediate_region"]))
    else:
        lines.append("_Data not available._")

    lines.extend([
        "",
        "---",
        "",
        "## 4. Key Findings",
        "",
        "- **Developing countries publish only ~20% of their own biodiversity data internally.** 80% of records for developing CBD parties are published by organisations based in other countries.",
        "- **Low and lower-middle income countries are almost entirely dependent on external publishers**, with internal shares of 4% and 4% respectively (all taxa).",
        "- **Europe is the only region where internal publishing dominates** (82%), while the Americas (15%) and Asia (18%) rely overwhelmingly on external data publishers.",
        "- **Southern Africa is an outlier within Africa**, with 69% internal publishing compared to under 6% for Eastern and Middle Africa.",
        "- **Excluding Aves shifts the picture for some regions.** Upper middle income countries improve to 54% internal (from 29% with Aves), and the Developing group rises to 44% internal (from 20%), reflecting the concentration of bird data in external repositories.",
        "",
        "---",
        "",
        "## 5. Methodology",
        "",
        "For full methodology details, see [`METHODS_SOURCE_ANALYSIS.md`](METHODS_SOURCE_ANALYSIS.md).",
        "",
        "In summary:",
        "",
        "1. The GBIF registry was downloaded and a local lookup table (`data/gbif_registry_lookup.parquet`) was built mapping `publishingorgkey` to country.",
        "2. Each occurrence record was classified as Internal (publisher country = occurrence country) or External (publisher country differs).",
        "3. Results were enriched with UN regional, development status, and World Bank income group metadata.",
        "4. CBD party records were filtered and aggregated into the summary tables used in this report.",
        "",
        "**Processing scripts:**",
        "- `src/calculate_source_distribution.py`",
        "- `src/create_registry_lookup.py`",
        "- `src/enrich_source_distribution.py`",
        "- `src/analyze_cbd_parties.py`",
        "",
        "---",
        "",
        "## 6. Source Data",
        "",
    ])

    all_source_files = sorted(set(
        list(ALL_TAXA_FILES.values()) + list(NO_AVES_FILES.values())
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
