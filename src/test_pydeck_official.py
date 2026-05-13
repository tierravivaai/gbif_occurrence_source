"""
Test pydeck 0.9.2 with the OFFICIAL deck.gl example (UK road safety CSV).
This will tell us if pydeck itself is broken or if our data format is incompatible.
"""

import pydeck as pdk

HEXAGON_LAYER_DATA = "https://raw.githubusercontent.com/visgl/deck.gl-data/master/examples/3d-heatmap/heatmap-data.csv"

layer = pdk.Layer(
    "HexagonLayer",
    HEXAGON_LAYER_DATA,
    get_position=["lng", "lat"],
    auto_highlight=True,
    elevation_scale=50,
    pickable=True,
    elevation_range=[0, 3000],
    extruded=True,
    coverage=1,
)

view_state = pdk.ViewState(
    longitude=-1.415,
    latitude=52.2323,
    zoom=6,
    min_zoom=5,
    max_zoom=15,
    pitch=40.5,
    bearing=-27.36,
)

r = pdk.Deck(layers=[layer], initial_view_state=view_state)
r.to_html("output/test_official.html")
print("Saved: output/test_official.html")
