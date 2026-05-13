"""Generate the CBD Publisher Country Share Report (no-Aves primary, All Taxa annex).

This report presents the GBIF publisher country share analysis with Class Aves
excluded as the primary dataset, because bird observation data from citizen science
platforms (eBird, iNaturalist) dominate the dataset and are primarily published by
organisations in developed countries. An annex with All Taxa (including Aves) is
provided for reference.

The opening section of the report (title through ### Bird Data to ---) is read
from the existing report.md template and preserved. Only {var} placeholders are
filled. The data sections and annexes are generated from CSV summaries.
Methods are read from methods.md and appended as an annex.
"""

import csv
import os
import re
from datetime import date

PROCESSED_DIR = "data/processed"
OUTPUT_PATH = "CBD_Publisher_Country_Share_Report.md"
TEMPLATE_PATH = "CBD_Publisher_Country_Share_Report_template.md"
METHODS_PATH = "methods.md"

DATA_SOURCE_CITATION = "GBIF.org (1 April 2026) GBIF Occurrence Download https://doi.org/10.15468/dl.9z6p8m"
SNAPSHOT_LABEL = "April 2026"

# Template variable values — update when running against a new snapshot
VARS = {
    "Sys Data": "22 April 2026",
    "occurrence citations": "[DOI 10.15468/dl.9z6p8m](https://doi.org/10.15468/dl.9z6p8m)",
    # Snapshot counts (April 2026 S3 open data: 3.58B; DOI full download: 3.77B)
    "aves_total_billion": "2.2",
    "aves_percentage": "61.9",
    "current_snapshot_total_billion": "3.6",
    "ref_snapshot_total_billion": "3.17",
}

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
    # Add total row
    total_row = {}
    for spec in col_specs:
        key = spec[1]
        is_pct = spec[2]
        if is_pct:
            # Calculate percentage from totals
            if key == "internal_percentage" and "internal_count" in total_row and "total_count" in total_row:
                total_row[key] = round(100.0 * total_row["internal_count"] / total_row["total_count"], 2) if total_row["total_count"] > 0 else 0
            elif key == "sub_regional_percentage" and "sub_regional_count" in total_row and "total_count" in total_row:
                total_row[key] = round(100.0 * total_row["sub_regional_count"] / total_row["total_count"], 2) if total_row["total_count"] > 0 else 0
            elif key == "regional_percentage" and "regional_count" in total_row and "total_count" in total_row:
                total_row[key] = round(100.0 * total_row["regional_count"] / total_row["total_count"], 2) if total_row["total_count"] > 0 else 0
            elif key == "external_percentage" and "external_count" in total_row and "total_count" in total_row:
                total_row[key] = round(100.0 * total_row["external_count"] / total_row["total_count"], 2) if total_row["total_count"] > 0 else 0
        else:
            # Sum numeric columns, leave label columns blank for "Total" row
            if key in ("internal_count", "sub_regional_count", "regional_count", "external_count", "unknown_count", "total_count"):
                total_row[key] = sum(int(row.get(key, 0) or 0) for row in rows)
    total_cells = []
    for spec in col_specs:
        key = spec[1]
        is_pct = spec[2]
        if key in total_row:
            total_cells.append(_cell(total_row[key], is_pct=is_pct))
        elif spec == col_specs[0]:
            total_cells.append("**Total**")
        else:
            total_cells.append("")
    table.append("| " + " | ".join(total_cells) + " |")
    return "\n".join(table)


_COUNT_COLS = [
    ("Internal Count", "internal_count", False),
    ("Sub-regional Count", "sub_regional_count", False),
    ("Regional Count", "regional_count", False),
    ("External Count", "external_count", False),
    ("Unknown Count", "unknown_count", False),
    ("Total Count", "total_count", False),
    ("Internal %", "internal_percentage", True),
    ("Sub-regional %", "sub_regional_percentage", True),
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
        ("UN Region", "un_region_name", False),
        ("Income Group", "wb_income_group", False),
        ("LDC", "is_ldc", False),
        ("SIDS", "is_sids", False),
        ("Internal", "internal_count", False),
        ("Sub-regional", "sub_regional_count", False),
        ("Regional", "regional_count", False),
        ("External", "external_count", False),
        ("Total", "total_count", False),
        ("Int %", "internal_percentage", True),
        ("Sub-reg %", "sub_regional_percentage", True),
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


def _read_template(path):
    """Read the opening section of the report (everything up to and including the
    first '---' after a heading containing 'Bird'). Fills {var} placeholders."""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # Find the --- that separates the opening section from the data sections.
    # The opening section runs from the start through the '---' after ### Bird Data.
    # Strategy: find the LAST '---' that appears before Section 2.
    # We look for the pattern: a line that is just '---' followed by a blank line
    # and then '## 2.' — this is the separator we want.
    lines = content.split("\n")

    # Find all '---' lines
    hr_indices = [i for i, line in enumerate(lines) if line.strip() == "---"]

    # The opening section ends at the --- that comes AFTER the Bird Data heading
    # and BEFORE the data sections. We find the --- after the ### Bird heading
    # and before the data tables.
    bird_heading_idx = None
    for i, line in enumerate(lines):
        if "Bird Data" in line and line.strip().startswith("###"):
            bird_heading_idx = i
            break

    if bird_heading_idx is None:
        raise ValueError("Could not find '### Bird Data' heading in template")

    # Find the first --- after the Bird Data heading
    split_idx = None
    for idx in hr_indices:
        if idx > bird_heading_idx:
            split_idx = idx
            break

    if split_idx is None:
        raise ValueError("Could not find '---' separator after Bird Data section")

    opening_lines = lines[:split_idx + 1]  # Include the --- line

    # Fill {var} placeholders
    opening_text = "\n".join(opening_lines)
    for key, value in VARS.items():
        opening_text = opening_text.replace("{" + key + "}", value)

    return opening_text


def _read_methods(path=METHODS_PATH):
    """Read methods.md and fill {var} placeholders."""
    methods_vars = {
        "insert date of current snapshot": "1 April 2026",
    }
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    # Replace numbered {var} placeholders in order of appearance
    # Pattern: {var} followed by optional unit like % or billion
    # We need context-dependent replacement, so we do it carefully:
    # "roughly {var}% of {var} billion" for the reference snapshot
    # "roughly {var}% of {var} billion" for the current snapshot
    content = content.replace("{insert date of current snapshot}", "1 April 2026")

    # Replace remaining {var} placeholders with snapshot-specific values
    # The methods.md has specific patterns like "roughly {var}% of {var} billion"
    # Reference snapshot: ~43M of 3.17B → 1.36% of 3.17 billion
    content = content.replace(
        "roughly {var}% of {var} billion) had no occurrence country code",
        "roughly 1.36% of 3.17 billion) had no occurrence country code",
    )
    # Current snapshot: ~45.6M of 3.58B → 1.27%
    content = content.replace(
        "roughly {var}% of {var} billion had no occurrence country code",
        "roughly 1.27% of 3.58 billion had no occurrence country code",
    )

    return content


def generate_report(output_path=OUTPUT_PATH, template_path=TEMPLATE_PATH):
    no_aves = {key: _read_csv(fname) for key, fname in NO_AVES_FILES.items()}
    all_taxa = {key: _read_csv(fname) for key, fname in ALL_TAXA_FILES.items()}

    # Step 1: Read the opening section from the existing report (preserving hand-edits)
    opening = _read_template(template_path)

    # Step 2: Generate data sections
    lines = []
    lines.append("")  # blank line after the --- separator

    lines.append("## 2. Source Distribution (Excluding Birds)")
    lines.append("")

    lines.append("### 2.1 By UN Region")
    lines.append("")
    lines.extend(_section(no_aves, "un_region", _region_table))
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
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from generate_country_tables import generate_country_table
    country_df = generate_country_table()
    country_rows = [row._asdict() if hasattr(row, '_asdict') else row for row in country_df.to_dict('records')]
    lines.append("### 2.6 Per-Country Table (Alphabetical)")
    lines.append("")
    lines.append("LDC = Least Developed Country. SIDS = Small Island Developing State. "
                 "Int % = Internal (same country). "
                 "Sub-reg % = Sub-regional (same UN sub-region, different country). "
                 "Reg % = Regional (same UN region, different sub-region). "
                 "Ext % = External (different UN region). "
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
        "- **Europe is the only region where internal publishing dominates** (excluding Aves). When including sub-regional and regional publishing, the picture changes: the Americas has 51% internal + sub-regional + regional combined, meaning only 49% of data is published from outside the region. However, much of this is cross-sub-regional (e.g. US-published records for LAC countries). At the tighter sub-regional level, internal + sub-regional within LAC is lower. Africa has 19% internal + sub-regional + regional, with 81% published from outside the region.",
        "- **Southern Africa is an outlier within Africa**, with higher internal publishing compared to Eastern and Middle Africa.",
        "- **Excluding Aves provides a more accurate picture of domestic taxonomic capacity.** Including Aves data systematically depresses internal publishing percentages for countries with extensive citizen-science coverage, as these records are classified under the publisher's country (typically a developed country).",
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

    # Step 3: Source data listing
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

    # Step 4: Append methods.md as Annex B
    methods_content = _read_methods()
    lines.extend([
        "",
        "",
        methods_content.strip(),
        "",
        f"**Data Source:** {DATA_SOURCE_CITATION}",
        f"**Generated:** {date.today().isoformat()}",
    ])

    # Step 5: Combine opening + data sections
    full_report = opening + "\n".join(lines) + "\n"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_report)

    print(f"CBD Publisher Country Share Report written to {output_path}")


if __name__ == "__main__":
    generate_report()
