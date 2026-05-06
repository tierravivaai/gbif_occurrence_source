# GBIF Source Distribution — Mapping Methodology

## 1. Dataset

**Source**: GBIF.org (1 June 2025) GBIF Occurrence Download  
**DOI**: https://doi.org/10.15468/dl.jsevhc  
**Downloaded**: 1 January 2026  
**Total records**: ~3.7 billion occurrence records  
**Format**: Parquet files at `/Volumes/Mybook18/TAXONOMY_ARCHIVE/gbifdump_20260101/occurrence.parquet/`

---

## 2. Record Filtering

Only records meeting all of the following criteria are included in the analysis:

| Filter | Value | Rationale |
|--------|-------|-----------|
| `taxonrank` | `SPECIES` | Excludes higher-order identifications that may inflate counts without adding species information |
| `occurrencestatus` | `PRESENT` | Excludes absence records |
| `basisofrecord` | `OBSERVATION`, `HUMAN_OBSERVATION`, `MACHINE_OBSERVATION`, `LIVING_SPECIMEN`, `OCCURRENCE`, `MATERIAL_SAMPLE` | Excludes literature-based and preserved specimen records that may have different geographic attribution patterns |
| `class` | `!= 'Aves'` | **Aves exclusion** — bird records constitute ~2 billion records (>50% of the dataset) and are dominated by a small number of very large citizen-science publishers (particularly iNaturalist and eBird). Including Aves would heavily skew the Internal/External analysis towards countries with strong birding communities, masking patterns in the broader biodiversity data. A separate "all taxa" analysis is available from `source_by_country.csv`. |
| `decimallatitude` | Not null, between -90 and 90 | Removes records with invalid or missing coordinates |
| `decimallongitude` | Not null | Removes records with missing coordinates |

---

## 3. Source Classification

Each occurrence record is classified by comparing the **occurrence country** (where the species was observed) with the **publisher country** (where the publishing organisation is registered).

### Publisher Country Resolution

Publisher country is resolved from the GBIF Registry via `data/gbif_registry_lookup.parquet`, which is built from the GBIF Organizations API. Each record is joined on `publishingorgkey` → `resolved_country` (ISO2 code).

### Classification Categories

| Category | Definition |
|----------|------------|
| **INTERNAL** | `occurrence.countrycode == publisher.resolved_country` — the data was published by an organisation in the same country where the occurrence was recorded. Also called "Self-Published". |
| **REGIONAL** | `occurrence.countrycode != publisher.resolved_country` AND the occurrence country and publisher country share the same **UN macro-region** (Africa, Americas, Asia, Europe, Oceania) |
| **EXTERNAL** | `occurrence.countrycode != publisher.resolved_country` AND occurrence and publisher countries are in **different UN macro-regions** |
| **UNKNOWN** | Publisher country could not be resolved from the registry (e.g., publishingorgkey not in registry, or organisation has no country) |

### SQL Implementation

```sql
CASE
    WHEN reg.resolved_country IS NULL                            THEN 'UNKNOWN'
    WHEN occ.countrycode = reg.resolved_country                  THEN 'INTERNAL'
    WHEN occ_region.un_region_name = pub_region.un_region_name   THEN 'REGIONAL'
    ELSE 'EXTERNAL'
END AS source_type
```

---

## 4. Hexbin Aggregation

### Purpose

The 3.7 billion occurrence records cannot be rendered directly in a browser. Coordinates are rounded to a grid and counts are summed, producing a manageable number of weighted cells for visualisation.

### Precision Levels

| Precision | Rounding | Approximate cell size | Use case |
|-----------|----------|----------------------|----------|
| `p0` | 1 decimal place | ~111 km | Global toggle map (kepler.gl), lightweight HTML |
| `p1` | 0.1 decimal places | ~11 km | High-resolution global map (deck.gl), country drilldowns |
| `p2` | 0.01 decimal places | ~1 km | City/reserve level (not pre-computed by default) |

### Aggregation Logic

```sql
SELECT
    ROUND(decimallatitude::DOUBLE,  {precision}) AS lat,
    ROUND(decimallongitude::DOUBLE, {precision}) AS lon,
    source_type,
    COUNT(*) AS record_count
FROM ...
GROUP BY lat, lon, source_type
```

Each row in the output represents a unique (lat, lon, source_type) combination. The `record_count` column carries the weight used for 3D extrusion and colour intensity in the visualisations.

---

## 5. Country Metadata Enrichment

After aggregation, each hexbin cell (lat/lon) is enriched with country, income group, and UN region via two steps:

### 5.1 Point-in-Polygon Join

Each cell centroid is tested against Natural Earth 110m country boundaries (`ne_110m_admin_0_countries.geojson`) using DuckDB Spatial `ST_Contains`. This assigns:
- `countrycode` (ISO2)
- `country_name`

Cells over open ocean, polar regions, or small territories not in the 110m dataset receive `countrycode = 'XX'`.

**Limitation**: Natural Earth 110m resolution is approximate (~1:110,000,000 scale). Small island territories and narrow coastal areas may be misclassified. Higher-resolution boundaries would improve accuracy but substantially increase processing time.

### 5.2 WB Income Group and UN Region Join

The assigned ISO2 code is then joined to `source_by_country.csv`, which carries:
- `wb_income_group` — World Bank income classification (High income, Upper middle income, Lower middle income, Low income)
- `un_region_name` — UN M.49 macro-region
- `un_sub_region_name` — UN M.49 sub-region

Cells with `countrycode = 'XX'` receive `wb_income_group = 'Not classified'` and `un_region_name = 'Unknown'`.

---

## 6. Colour Scheme

All visualisations use a consistent 6-stop colour ramp derived from the GBIF brand palette. The ramp runs from low-density teal to high-density red:

| Stop | Hex | RGB | Meaning |
|------|-----|-----|---------|
| 1 | `#0198BD` | (1, 152, 189) | Lowest density / teal |
| 2 | `#49E3CE` | (73, 227, 206) | Low-mid / mint |
| 3 | `#D8FEB5` | (216, 254, 181) | Mid / lime |
| 4 | `#FEEDB1` | (254, 237, 177) | Mid-high / yellow |
| 5 | `#FEAD54` | (254, 173, 84) | High / orange |
| 6 | `#D1374E` | (209, 55, 78) | Highest density / red |

### Income Group Colour Assignments

Used when layers are split by WB income group:

| Income group | Colour |
|---|---|
| High income | Teal `#0198BD` |
| Upper middle income | Mint `#49E3CE` |
| Lower middle income | Orange `#FEAD54` |
| Low income | Red `#D1374E` |

---

## 7. Visualisation Scripts

### 7.1 `aggregate_hexbin_pipeline.py` — Data Rebuild

Single entry-point to rebuild all hexbin data from the raw GBIF parquet. Run this first whenever the source data changes or output files are missing.

```bash
python src/aggregate_hexbin_pipeline.py
# Options:
#   --skip-global          skip global (p0 and p1) builds
#   --skip-countries       skip per-country builds
#   --countries BR,US,ZA   comma-separated ISO2 codes
#   --precision 0          override precision for all builds
```

Expected runtime: 20–60 minutes depending on hardware (queries 3.7B records via DuckDB).

### 7.2 `visualise_global_choropleth.py` — Country Polygon Choropleth

Fills each country polygon with a colour proportional to `internal_percentage` (self-published share) or `total_count` (log-scaled density). Best for policy audiences and print-ready outputs.

```bash
python src/visualise_global_choropleth.py --mode both
python src/visualise_global_choropleth.py --mode internal --income-group "Low income"
```

**Output**: `output/gbif_choropleth_internal.html`, `output/gbif_choropleth_total.html`

### 7.3 `visualise_global_deckgl.py` — 3D Hexbin (deck.gl HexagonLayer)

Replicates the deck.gl UK Road Safety example at global scale. Raw weighted cells are fed to pydeck `HexagonLayer`; deck.gl aggregates them client-side. Height and colour represent record density. Hold Shift + drag to rotate the 3D view.

```bash
python src/visualise_global_deckgl.py --mode both
python src/visualise_global_deckgl.py --mode internal --income-group "Low income"
python src/visualise_global_deckgl.py --un-region "Africa"
```

**Key parameters**:
- `radius = 50000` m (50 km hexagons at global scale)
- `elevation_scale = 50`
- `upper_percentile = 100` (full range preserved)

**Output**: `output/gbif_deckgl_global_all.html`, `output/gbif_deckgl_global_internal.html`

### 7.4 `visualise_global_toggle_kepler.py` — Interactive Layer Toggle (kepler.gl)

Single HTML file with kepler.gl's interactive sidebar. Users click the eye icon to toggle between layers — no coding required. Suitable for live presentations and public-facing interactive maps.

```bash
python src/visualise_global_toggle_kepler.py               # precision=0, ~18MB
python src/visualise_global_toggle_kepler.py --precision 1 # high-res, larger file
```

**Output**: `output/gbif_kepler_global_toggle_p{N}.html`

**Layers available in sidebar**:
- All Records (default visible)
- Self-Published / Internal (default visible)
- High / Upper-middle / Lower-middle / Low income — Internal (hidden by default)
- Africa / Americas / Asia / Europe / Oceania — Internal (hidden by default)

### 7.5 `visualise_country_drilldown.py` — Country Deep Dive

High-resolution country-level view (11km hexbins) for any country with pre-computed data. Default: Brazil.

```bash
python src/visualise_country_drilldown.py --country BR
python src/visualise_country_drilldown.py --country ZA --filter internal
python src/visualise_country_drilldown.py --country IN --income-group "Lower middle income"
```

**Output**: `output/gbif_drilldown_{cc}_all.html`, `output/gbif_drilldown_{cc}_internal.html`

Countries with pre-built data by default: BR, US, ZA, IN, CO, MX, AU, GB, FR, DE, ID, JP, CA.

---

## 8. Known Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| Natural Earth 110m boundaries | Small territories may be misclassified to wrong country or ocean | Accept as approximation at global scale; use higher-res boundaries for country-level if needed |
| Ocean/polar cells | Cells over ocean are assigned `countrycode='XX'` and excluded from income/region filters | These cells still appear in "All Records" layers |
| Registry gaps | ~5–15% of records have no resolvable publisher country (`UNKNOWN`) | Reported separately; not attributed to INTERNAL or EXTERNAL |
| Aves exclusion | Results do not represent total GBIF holdings | Stated explicitly in titles and tooltips; all-taxa versions available from `source_by_country.csv` |
| WB income group | Some territories (e.g. Taiwan, Kosovo, small islands) lack WB classification | Shown as "Not classified" |
| Precision rounding | p1 (~11km) cells may straddle country borders | Cells are attributed to whichever country polygon contains the cell centroid |
| deck.gl v9.2 regression | HexagonLayer may not render in some browsers with pydeck 0.9.2 | Use `render_static_maps.py` for guaranteed static PNG output |

---

## 8. Static PNG Output

If the interactive HTML maps do not render in your browser due to a deck.gl 9.2 regression, use `render_static_maps.py` to generate publication-quality static PNGs or SVGs using matplotlib.

```bash
# Render all configured maps as static PNGs
python src/render_static_maps.py --all

# Render a specific map
python src/render_static_maps.py --map global_all --method hexbin

# Scatter plot (faster, less precise)
python src/render_static_maps.py --map br_internal --method scatter

# SVG output for print
python src/render_static_maps.py --map global_all --format svg
```

**Output**: `output/static_maps/`

Available maps: `global_all`, `global_internal`, `global_external`, `br_all`, `br_internal`, `za_all`, `za_internal`, `us_all`, `us_internal`.

---

## 9. Reproducing the Analysis

### Prerequisites

- Python 3.11+
- Packages: `pydeck`, `keplergl`, `duckdb`, `pandas`, `geopandas`, `shapely`, `pyarrow`, `matplotlib`, `playwright`
- GBIF occurrence parquet at the path in `OCC_PATH` (external drive)
- `MAPBOX_API_KEY` in `~/hermes-secure-runner/hermes-data/.env` (optional, for dark basemap)

### Full Pipeline

```bash
# Step 1: Rebuild all hexbin data from raw GBIF parquet
python src/aggregate_hexbin_pipeline.py

# Step 2: Country choropleth (uses source_by_country.csv — no hexbin data needed)
python src/visualise_global_choropleth.py --mode both

# Step 3: deck.gl global 3D hexbin
python src/visualise_global_deckgl.py --mode both

# Step 4: kepler.gl interactive toggle map
python src/visualise_global_toggle_kepler.py

# Step 5: Brazil drilldown (or any other country)
python src/visualise_country_drilldown.py --country BR

# Optional: build additional country drilldowns
python src/aggregate_hexbin_pipeline.py --countries ZA,IN,CO --skip-global
python src/visualise_country_drilldown.py --country ZA
```

All output HTML files are written to `output/`. Open them directly in any modern web browser — no server required.
