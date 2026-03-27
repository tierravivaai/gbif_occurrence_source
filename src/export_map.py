import pydeck as pdk
import pandas as pd
import os

# Tableau Palette Mapping
KINGDOM_COLORS = {
    'Animalia': [225, 87, 89],   # #E15759 (Red)
    'Plantae': [89, 161, 79],    # #59A14F (Green)
    'Bacteria': [78, 121, 167],  # #4E79A7 (Blue)
    'Viruses': [186, 176, 172],  # #BAB0AC (Grey)
    'Fungi': [176, 122, 161],    # #B07AA1 (Purple)
    'Protozoa': [255, 157, 167], # #FF9DA7 (Pink)
    'Archaea': [242, 142, 43],   # #F28E2B (Orange)
    'Chromista': [118, 183, 178],# #76B7B2 (Teal)
    'incertae sedis': [156, 117, 95] # #9C755F (Brown)
}
DEFAULT_COLOR = [200, 200, 200]

def export_static_map():
    print("Generating static HTML map...")
    path = "map-data/au_endemics_250k.parquet"
    if not os.path.exists(path):
        path = "map-data/au_sample_100k.parquet"
    
    df = pd.read_parquet(path)
    df['color'] = df['kingdom'].map(KINGDOM_COLORS).fillna(pd.Series([DEFAULT_COLOR] * len(df)))

    view_state = pdk.ViewState(
        latitude=-25.2744,
        longitude=133.7751,
        zoom=3.5,
        pitch=0,
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_radius=5000,
        pickable=True,
        opacity=0.6,
    )

    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="light", # Basic style without Mapbox token requirement
        tooltip={"text": "{species}\nKingdom: {kingdom}"}
    )

    # Save to HTML
    output_html = "map-data/au_endemics_map.html"
    r.to_html(output_html)
    print(f"Static HTML map saved to {output_html}")

if __name__ == "__main__":
    export_static_map()
