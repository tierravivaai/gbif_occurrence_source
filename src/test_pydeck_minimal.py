"""
Minimal test: pydeck HexagonLayer with identical config to the official example.
This tests whether pydeck 0.9.2 + deck.gl 9.2 can render at all with our data.

If this works, we can gradually add parameters back to find the breaking change.
"""

import pydeck as pdk
import pandas as pd

# Load Brazil p0 data (small, fast)
df = pd.read_parquet("data/processed/hexbin_all_no_aves_p0_enriched.parquet")
df = df[df["source_type"] == "INTERNAL"].nlargest(1000, "record_count")

print(f"Test data: {len(df)} cells, {df['record_count'].sum()} records")

# EXACT config from pydeck docs example
layer = pdk.Layer(
    "HexagonLayer",
    df,
    get_position=["lon", "lat"],
    auto_highlight=True,
    elevation_scale=50,
    pickable=True,
    elevation_range=[0, 3000],
    extruded=True,
    coverage=1,
)

view_state = pdk.ViewState(
    longitude=10,
    latitude=20,
    zoom=1.5,
    min_zoom=1,
    max_zoom=15,
    pitch=40,
    bearing=-20,
)

r = pdk.Deck(layers=[layer], initial_view_state=view_state)
r.to_html("output/test_minimal.html")
print("Saved: output/test_minimal.html")
