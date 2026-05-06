"""
Render static PNG/SVG maps from GBIF hexbin data using matplotlib.

Reads enriched hexbin parquet files and renders them as static images with
a proper map projection. This avoids WebGL entirely and is useful for
publication-quality figures, print, and quick assessment.

Two approaches:
  1. hexbin_scatter -- fast scatter plot with alpha/record_count weighted
  2. voronoi_map    -- hexagonal bins with colour = record density

Usage:
    python src/render_static_maps.py
    python src/render_static_maps.py --map br_internal
    python src/render_static_maps.py --map global_all --max-records 500000
    python src/render_static_maps.py --map global_internal --format svg
    python src/render_static_maps.py --all

Output:
    output/static_maps/<map_name>.png  (or .svg)
"""

import argparse
import os
import sys

import pandas as pd
import numpy as np

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR    = os.path.join(PROJECT_DIR, "data", "processed")
OUTPUT_DIR  = os.path.join(PROJECT_DIR, "output")
STATIC_DIR  = os.path.join(OUTPUT_DIR, "static_maps")

# GBIF colour ramp as matplotlib normalised RGB tuples
GBIF_COLORS = [
    (0.004, 0.596, 0.741),   # teal
    (0.286, 0.890, 0.808),   # mint
    (0.847, 0.996, 0.710),   # lime
    (0.996, 0.929, 0.690),   # yellow
    (0.996, 0.678, 0.329),   # orange
    (0.820, 0.212, 0.294),   # red
]

MAP_CONFIGS = {
    "global_all": {
        "file": "hexbin_all_no_aves_p0_enriched.parquet",
        "title": "GBIF Species Occurrences — All Records (No Aves)",
        "subtitle": "680M records | p0 (111km cells)",
        "filter": None,
    },
    "global_internal": {
        "file": "hexbin_all_no_aves_p0_enriched.parquet",
        "title": "GBIF Species Occurrences — Self-Published (Internal)",
        "subtitle": "590M records | p0 (111km cells)",
        "filter": {"source_type": "INTERNAL"},
    },
    "global_external": {
        "file": "hexbin_all_no_aves_p0_enriched.parquet",
        "title": "GBIF Species Occurrences — External Publishers",
        "subtitle": "50M records | p0 (111km cells)",
        "filter": {"source_type": "EXTERNAL"},
    },
    "br_all": {
        "file": "hexbin_br_no_aves_p1_enriched.parquet",
        "title": "Brazil — All GBIF Records (No Aves)",
        "subtitle": "1.4M records | p1 (11km cells)",
        "filter": None,
        "extent": (-74, -34, -34, 6),   # lon_min, lon_max, lat_min, lat_max
    },
    "br_internal": {
        "file": "hexbin_br_no_aves_p1_enriched.parquet",
        "title": "Brazil — Self-Published Records",
        "subtitle": "577K records | p1 (11km cells)",
        "filter": {"source_type": "INTERNAL"},
        "extent": (-74, -34, -34, 6),
    },
    "za_all": {
        "file": "hexbin_za_no_aves_p1_enriched.parquet",
        "title": "South Africa — All GBIF Records (No Aves)",
        "subtitle": "2.3M records | p1 (11km cells)",
        "filter": None,
        "extent": (16, 33, -35, -22),
    },
    "za_internal": {
        "file": "hexbin_za_no_aves_p1_enriched.parquet",
        "title": "South Africa — Self-Published Records",
        "subtitle": "163K records | p1 (11km cells)",
        "filter": {"source_type": "INTERNAL"},
        "extent": (16, 33, -35, -22),
    },
    "us_all": {
        "file": "hexbin_us_no_aves_p1_enriched.parquet",
        "title": "United States — All GBIF Records (No Aves)",
        "subtitle": "52.9M records | p1 (11km cells)",
        "filter": None,
        "extent": (-125, -66, 24, 50),
    },
    "us_internal": {
        "file": "hexbin_us_no_aves_p1_enriched.parquet",
        "title": "United States — Self-Published Records",
        "subtitle": "51.0M records | p1 (11km cells)",
        "filter": {"source_type": "INTERNAL"},
        "extent": (-125, -66, 24, 50),
    },
}


def load_data(file_name: str, filters: dict | None = None, max_records: int | None = None) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(path):
        print(f"ERROR: {path} not found")
        sys.exit(1)

    df = pd.read_parquet(path)
    print(f"Loaded {len(df):,} cells, {df['record_count'].sum():,} records")

    if filters:
        for col, val in filters.items():
            df = df[df[col] == val].copy()
            print(f"  Filter {col}={val}: {len(df):,} cells")

    if max_records and len(df) > max_records:
        df = df.nlargest(max_records, "record_count").reset_index(drop=True)
        print(f"  Downsampled to top {max_records:,} cells by record_count")

    return df


def _gbif_colour(val: float) -> tuple:
    """Map 0..1 to the GBIF colour ramp."""
    ramp = GBIF_COLORS
    idx = val * (len(ramp) - 1)
    lo = int(np.floor(idx))
    hi = min(lo + 1, len(ramp) - 1)
    frac = idx - lo
    return tuple(ramp[lo][i] + frac * (ramp[hi][i] - ramp[lo][i]) for i in range(3))


def render_hexbin_scatter(
    df: pd.DataFrame,
    title: str,
    subtitle: str = "",
    extent: tuple | None = None,
    figsize: tuple = (16, 10),
    dpi: int = 150,
    output_path: str = "",
    format: str = "png",
) -> str:
    """Fast scatter plot weighted by log(record_count), alpha by density."""
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    cmap = LinearSegmentedColormap.from_list("gbif", GBIF_COLORS)

    fig, ax = plt.subplots(figsize=figsize)

    # Log-scale for size and colour
    sizes = np.log10(df["record_count"].clip(lower=1))
    sizes = (sizes - sizes.min()) / (sizes.max() - sizes.min())
    point_sizes = 5 + sizes * 100

    # Colour by log density
    colours = np.log10(df["record_count"].clip(lower=1))
    norm = plt.Normalize(colours.min(), colours.max())

    scatter = ax.scatter(
        df["lon"], df["lat"],
        c=colours,
        s=point_sizes,
        cmap=cmap,
        alpha=0.7,
        edgecolors="none",
        norm=norm,
    )

    # Add subtle world landmass outline (optional, from Natural Earth if available)
    # For now, just grid
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_xlabel("Longitude", fontsize=11)
    ax.set_ylabel("Latitude", fontsize=11)

    if extent:
        ax.set_xlim(extent[0], extent[1])
        ax.set_ylim(extent[2], extent[3])

    ax.set_title(title, fontsize=14, fontweight="bold", pad=10)
    if subtitle:
        ax.text(0.5, 1.02, subtitle, transform=ax.transAxes,
                fontsize=10, ha="center", color="#666")

    cbar = plt.colorbar(scatter, ax=ax, shrink=0.6, aspect=30, pad=0.02)
    cbar.set_label("log10(record count)", fontsize=9)

    plt.tight_layout()

    if not output_path:
        os.makedirs(STATIC_DIR, exist_ok=True)
        stem = title.lower().replace(" ", "_").replace("—", "_").replace("|", "_")
        output_path = os.path.join(STATIC_DIR, f"{stem}.{format}")

    if format == "svg":
        fig.savefig(output_path, format="svg", bbox_inches="tight")
    else:
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    print(f"  Saved: {output_path}")
    return output_path


def render_hexbin_map(
    df: pd.DataFrame,
    title: str,
    subtitle: str = "",
    extent: tuple | None = None,
    gridsize: int = 60,
    figsize: tuple = (16, 10),
    dpi: int = 150,
    output_path: str = "",
    format: str = "png",
) -> str:
    """Matplotlib hexbin — proper hexagonal bins with GBIF colour map."""
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    cmap = LinearSegmentedColormap.from_list("gbif", GBIF_COLORS)

    fig, ax = plt.subplots(figsize=figsize)

    # Weighted hexbin
    hb = ax.hexbin(
        df["lon"], df["lat"],
        C=df["record_count"],
        gridsize=gridsize,
        reduce_C_function=np.sum,
        cmap=cmap,
        mincnt=1,
        edgecolors="none",
        linewidths=0,
    )

    ax.grid(True, alpha=0.3, linestyle="--")
    ax.set_xlabel("Longitude", fontsize=11)
    ax.set_ylabel("Latitude", fontsize=11)

    if extent:
        ax.set_xlim(extent[0], extent[1])
        ax.set_ylim(extent[2], extent[3])

    ax.set_title(title, fontsize=14, fontweight="bold", pad=10)
    if subtitle:
        ax.text(0.5, 1.02, subtitle, transform=ax.transAxes,
                fontsize=10, ha="center", color="#666")

    cbar = plt.colorbar(hb, ax=ax, shrink=0.6, aspect=30, pad=0.02)
    cbar.set_label("Sum of records per hex bin", fontsize=9)

    plt.tight_layout()

    if not output_path:
        os.makedirs(STATIC_DIR, exist_ok=True)
        stem = title.lower().replace(" ", "_").replace("—", "_").replace("|", "_")
        output_path = os.path.join(STATIC_DIR, f"{stem}.{format}")

    if format == "svg":
        fig.savefig(output_path, format="svg", bbox_inches="tight")
    else:
        fig.savefig(output_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)

    print(f"  Saved: {output_path}")
    return output_path


def render_map(name: str, method: str = "hexbin", max_records: int | None = None,
               format: str = "png", figsize: tuple = (16, 10), dpi: int = 150) -> str:
    cfg = MAP_CONFIGS[name]
    df = load_data(cfg["file"], filters=cfg.get("filter"), max_records=max_records)

    print(f"\nRendering {name} ({method})...")

    if method == "scatter":
        return render_hexbin_scatter(
            df, cfg["title"], cfg.get("subtitle", ""),
            extent=cfg.get("extent"),
            figsize=figsize, dpi=dpi,
            format=format,
        )
    else:
        return render_hexbin_map(
            df, cfg["title"], cfg.get("subtitle", ""),
            extent=cfg.get("extent"),
            figsize=figsize, dpi=dpi,
            format=format,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render static PNG/SVG maps from GBIF hexbin data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--map",        default="br_all", help="Map key to render")
    parser.add_argument("--all",        action="store_true", help="Render all configured maps")
    parser.add_argument("--method",     choices=["scatter", "hexbin"], default="hexbin",
                        help="Rendering method: scatter (fast) or hexbin (proper bins)")
    parser.add_argument("--max-records", type=int, default=None,
                        help="Downsample to N top cells (useful for global maps)")
    parser.add_argument("--format",     choices=["png", "svg"], default="png")
    parser.add_argument("--dpi",        type=int, default=150)
    parser.add_argument("--width",      type=int, default=16, help="Figure width in inches")
    parser.add_argument("--height",     type=int, default=10, help="Figure height in inches")
    args = parser.parse_args()

    targets = list(MAP_CONFIGS.keys()) if args.all else [args.map]
    figsize = (args.width, args.height)

    for name in targets:
        if name not in MAP_CONFIGS:
            print(f"ERROR: unknown map '{name}'. Available: {', '.join(MAP_CONFIGS)}")
            continue
        try:
            render_map(name, method=args.method, max_records=args.max_records,
                       format=args.format, figsize=figsize, dpi=args.dpi)
        except Exception as e:
            print(f"  ERROR rendering {name}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\nDone. Static maps saved to {STATIC_DIR}/")


if __name__ == "__main__":
    main()
