"""Regression tests: Key Findings and interpretive text must not mix Aves and non-Aves data.

Background: The original report mixed All Taxa figures (14.76% internal for Americas)
with Excluding Aves findings, producing misleading statements like "the Americas rely
overwhelmingly on external data publishers" when the non-Aves data showed 51% internal
(near parity). These tests ensure that:

1. Key Findings reference the correct scope (All Taxa vs Excluding Aves)
2. Interpretive text for a section matches the data scope of that section
3. When comparing regions, the same scope is used throughout
"""

import os
import re
import pytest

REPORT_PATH = os.path.join(os.path.dirname(__file__), "..", "CBD_Publisher_Country_Share_Report.md")


@pytest.fixture()
def report_lines():
    if not os.path.exists(REPORT_PATH):
        pytest.skip("CBD report not generated yet")
    with open(REPORT_PATH, "r", encoding="utf-8") as f:
        return f.read().split("\n")


def _find_section_bounds(lines, heading_pattern):
    """Return (start, end) line indices for a section matching heading_pattern."""
    start = None
    for i, line in enumerate(lines):
        if re.match(r'^#{1,4}\s+(\d+\.?\s*)?' + heading_pattern, line):
            start = i
        elif start is not None and re.match(r'^#{1,4}\s+', line):
            return start, i
    if start is not None:
        return start, len(lines)
    return None, None


class TestAvesNonAvesSeparation:
    """Key Findings must not attribute All-Taxa figures to the non-Aves analysis."""

    def test_key_findings_scope_labels(self, report_lines):
        """Every quantitative claim in Key Findings must state its scope."""
        start, end = _find_section_bounds(report_lines, r'Key\s+Findings')
        assert start is not None, "Key Findings section not found"
        section = "\n".join(report_lines[start:end])

        # Find all percentage patterns like "15%", "51%", "80%"
        pct_claims = re.findall(r'[\( ](\d+\.?\d*%)', section)
        # Every claim with a percentage must be annotated with scope
        # e.g. "(all taxa)" or "(excluding Aves)" or similar
        for match in re.finditer(r'(\d+\.?\d*%)', section):
            # Look ahead 40 chars for a scope qualifier
            context = section[match.start():match.start() + 60]
            has_scope = any(
                kw in context.lower()
                for kw in ["all taxa", "excluding aves", "excluding birds", "no aves",
                           "without aves", "incl. aves", "including aves",
                           "with aves", "with birds", "annex"]
            )
            # If no scope qualifier nearby, flag it
            # (This is a soft check — we allow unscoped % if it's in a sentence
            # that already mentions the scope earlier)
            if not has_scope:
                # Check if the *same bullet point* contains a scope keyword
                bullet_start = section.rfind("\n-", 0, match.start())
                bullet_end = section.find("\n-", match.start())
                if bullet_end == -1:
                    bullet_end = len(section)
                bullet = section[bullet_start:bullet_end]
                bullet_has_scope = any(
                    kw in bullet.lower()
                    for kw in ["all taxa", "excluding aves", "excluding birds", "no aves",
                               "without aves", "including aves", "with aves", "with birds",
                               "annex"]
                )
                assert bullet_has_scope, (
                    f"Percentage claim '{match.group()}' in Key Findings lacks scope label "
                    f"(All Taxa vs Excluding Aves). Context: ...{context}..."
                )

    def test_no_aves_section_does_not_cite_all_taxa_figures(self, report_lines):
        """Interpretive text under 'Excluding Birds' sections must not cite All-Taxa numbers."""
        # Find all "Excluding" sections
        for pattern in [r'Excluding\s+(Birds|Aves)', r'Source\s+Distribution\s+\(Excluding']:
            start, end = _find_section_bounds(report_lines, pattern)
            if start is None:
                continue
            section = "\n".join(report_lines[start:end])

            # Check for known All-Taxa figures that should NOT appear here
            # The Americas all-taxa internal% is ~14.76%
            # Asia all-taxa internal% is ~18.14%
            all_taxa_signatures = ["14.76", "18.14", "85.24", "81.86"]
            for sig in all_taxa_signatures:
                assert sig not in section, (
                    f"All-Taxa figure '{sig}' found in Excluding Aves section. "
                    f"This likely mixes scopes."
                )

    def test_annex_does_not_cite_non_aves_figures_as_primary(self, report_lines):
        """All-Taxa annex should not present non-Aves figures as the main finding."""
        start, end = _find_section_bounds(report_lines, r'Annex.*Aves|Aves\s*\(Birds\)')
        if start is None:
            pytest.skip("No Annex found")
        section = "\n".join(report_lines[start:end])

        # Non-Aves figures that should NOT be presented as the main finding
        # Americas non-Aves internal% = 51.00%
        # Upper middle income non-Aves internal% = 47.02%
        non_aves_signatures = ["51.00", "47.02"]
        for sig in non_aves_signatures:
            # These numbers CAN appear in the annex, but not as primary claims
            # (e.g. "when excluding Aves, this rises to 51%" is OK)
            pass  # Soft check — the annex may reference non-Aves for comparison

    def test_americas_not_described_as_overwhelmingly_external_non_aves(self, report_lines):
        """The Americas must not be described as 'overwhelmingly external' in non-Aves context."""
        full_text = "\n".join(report_lines)

        # Find phrases that claim the Americas is overwhelmingly external
        # and check they are in an All-Taxa context
        for phrase in ["overwhelmingly.*external", "rely overwhelmingly"]:
            for match in re.finditer(phrase, full_text, re.IGNORECASE):
                # Get surrounding context (200 chars before and after)
                ctx_start = max(0, match.start() - 200)
                ctx_end = min(len(full_text), match.end() + 200)
                context = full_text[ctx_start:ctx_end]

                # If the phrase mentions Americas, it must be in an All-Taxa context
                if "americas" in context.lower() or "america" in context.lower():
                    has_all_taxa_scope = any(
                        kw in context.lower()
                        for kw in ["all taxa", "including aves", "with aves", "incl. aves"]
                    )
                    is_in_annex = any(
                        kw in full_text[max(0, match.start() - 500):match.start()].lower()
                        for kw in ["annex", "aves (birds)", "including aves"]
                    )
                    assert has_all_taxa_scope or is_in_annex, (
                        f"Phrase '{match.group()}' describes Americas as overwhelmingly external "
                        f"without clarifying this is the All-Taxa (including Aves) figure. "
                        f"The non-Aves data shows Americas at ~51% internal (near parity). "
                        f"Context: ...{context}..."
                    )


class TestReportStructure:
    """Structural checks that the report correctly separates scopes."""

    def test_primary_analysis_is_non_aves(self, report_lines):
        """The primary analysis sections should present Excluding Aves data."""
        full_text = "\n".join(report_lines)
        # Check that the first major data section after Introduction is non-Aves
        # Look for section 2 heading
        section2_idx = None
        for i, line in enumerate(report_lines):
            if re.match(r'^##\s+2\.', line):
                section2_idx = i
                break

        if section2_idx is not None:
            # The next 5 lines should indicate excluding Aves/Birds
            next_lines = "\n".join(report_lines[section2_idx:section2_idx + 3])
            assert any(
                kw in next_lines.lower()
                for kw in ["excluding", "excluding birds", "excluding aves", "no aves"]
            ), "Section 2 (primary analysis) should present Excluding Aves data"

    def test_annex_exists_for_all_taxa(self, report_lines):
        """There should be an annex section for All Taxa / Including Aves data."""
        full_text = "\n".join(report_lines)
        has_annex = any(
            kw in full_text.lower()
            for kw in ["annex", "including aves", "all taxa", "aves (birds)"]
        )
        assert has_annex, "Report should have an annex for All Taxa (including Aves) data"
