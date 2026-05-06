"""
Screenshot HTML map files using Playwright for static PNG/SVG output.

Since the deck.gl / kepler.gl maps are large interactive HTML files,
this script renders them in a headless browser and captures a full-viewport
screenshot as PNG. Useful for quick assessment when the HTML cannot be
opened directly in a browser due to file:// protocol restrictions.

Usage:
    python src/screenshot_maps.py
    python src/screenshot_maps.py --map gbif_deckgl_global_internal
    python src/screenshot_maps.py --map gbif_drilldown_br_all --wait 5000
    python src/screenshot_maps.py --all

Output:
    output/screenshots/<map_name>.png
"""

import argparse
import os
import sys

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR  = os.path.join(PROJECT_DIR, "output")
SCREEN_DIR  = os.path.join(OUTPUT_DIR, "screenshots")

DEFAULT_MAPS = [
    "gbif_choropleth_internal",
    "gbif_choropleth_total",
    "gbif_deckgl_global_all",
    "gbif_deckgl_global_internal",
    "gbif_kepler_global_toggle_p0",
    "gbif_drilldown_br_all",
    "gbif_drilldown_br_internal",
]


def take_screenshot(html_path: str, png_path: str, wait_ms: int = 8000) -> str:
    """Render HTML in headless Chromium and capture full-page PNG."""
    os.makedirs(os.path.dirname(png_path), exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--allow-file-access-from-files",
                "--disable-web-security",
                "--no-sandbox",
            ],
        )
        page = browser.new_page(viewport={"width": 1920, "height": 1080})

        # Allow file:// access
        page.set_default_timeout(max(wait_ms + 2000, 30000))

        print(f"  Navigating: {html_path}")
        page.goto(f"file://{html_path}", wait_until="domcontentloaded", timeout=30000)

        # Wait for deck.gl or kepler.gl to finish rendering
        print(f"  Waiting {wait_ms}ms for WebGL to initialise...")
        page.wait_for_timeout(wait_ms)

        # Screenshot
        page.screenshot(path=png_path, full_page=False)
        print(f"  Screenshot saved: {png_path}")

        browser.close()

    return png_path


def screenshot_map(name: str, wait_ms: int = 8000) -> str:
    html_path = os.path.join(OUTPUT_DIR, f"{name}.html")
    if not os.path.exists(html_path):
        print(f"ERROR: {html_path} not found")
        sys.exit(1)

    png_path = os.path.join(SCREEN_DIR, f"{name}.png")
    print(f"\nScreenshot: {name}")
    return take_screenshot(html_path, png_path, wait_ms)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Screenshot HTML map files with Playwright",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--map",     default="", help="Map file stem to screenshot")
    parser.add_argument("--all",     action="store_true", help="Screenshot all default maps")
    parser.add_argument("--wait",    type=int, default=8000, help="Wait time in ms for WebGL init")
    parser.add_argument("--width",   type=int, default=1920, help="Viewport width")
    parser.add_argument("--height",  type=int, default=1080, help="Viewport height")
    args = parser.parse_args()

    targets = []
    if args.all:
        targets = DEFAULT_MAPS
    elif args.map:
        targets = [args.map]
    else:
        # Default: screenshot the smallest/fastest map for quick check
        targets = ["gbif_drilldown_br_all"]

    for name in targets:
        try:
            screenshot_map(name, wait_ms=args.wait)
        except PWTimeoutError as e:
            print(f"  TIMEOUT screenshotting {name}: {e}")
        except Exception as e:
            print(f"  ERROR screenshotting {name}: {e}")

    print(f"\nDone. Screenshots saved to {SCREEN_DIR}/")


if __name__ == "__main__":
    main()
