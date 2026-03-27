import streamlit as st
import pandas as pd
import pydeck as pdk
import os

# Set page config
st.set_page_config(page_title="Australia Species Occurrence Viz", layout="wide")

# Tableau Palette Mapping (from tableau-palette skill)
# RGB values for deck.gl
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

@st.cache_data
def load_data():
    # Load endemics sample (now 250k)
    path = "map-data/au_endemics_250k.parquet"
    if not os.path.exists(path):
        # Fallback to general sample if needed
        path = "map-data/au_sample_100k.parquet"
    
    df = pd.read_parquet(path)
    # Map colors
    df['color'] = df['kingdom'].map(KINGDOM_COLORS).fillna(pd.Series([DEFAULT_COLOR] * len(df)))
    return df

st.title("Australia Endemic Species Occurrence Visualization")
st.markdown("""
Visualizing **250,000 occurrence records** of species identified as **Australian Endemics** (via Wikidata).
Colors are assigned by **Kingdom** using the **Tableau Palette** standard.
""")

try:
    df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")
    selected_kingdoms = st.sidebar.multiselect(
        "Select Kingdoms",
        options=sorted(df['kingdom'].unique()),
        default=sorted(df['kingdom'].unique())
    )
    
    search_species = st.sidebar.text_input("Search Species (e.g. Melaleuca alternifolia)")

    # Filtering
    filtered_df = df[df['kingdom'].isin(selected_kingdoms)]
    if search_species:
        filtered_df = filtered_df[filtered_df['species'].str.contains(search_species, case=False, na=False)]

    st.write(f"Displaying {len(filtered_df):,} records.")

    # Pydeck View
    view_state = pdk.ViewState(
        latitude=-25.2744,
        longitude=133.7751,
        zoom=3.5,
        pitch=0,
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=filtered_df,
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_radius=5000,
        pickable=True,
        opacity=0.6,
        auto_highlight=True,
    )

    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="light", # Changed from Mapbox URL to basic deck.gl style
        tooltip={"text": "{species}\nKingdom: {kingdom}"}
    )

    st.pydeck_chart(r)

    # Legend
    st.markdown("### Legend (Kingdoms)")
    cols = st.columns(len(selected_kingdoms))
    for i, k in enumerate(selected_kingdoms):
        color = KINGDOM_COLORS.get(k, DEFAULT_COLOR)
        hex_color = '#%02x%02x%02x' % tuple(color)
        cols[i].markdown(f'<div style="display: flex; align-items: center;"><div style="width: 20px; height: 20px; background-color: {hex_color}; margin-right: 10px; border-radius: 50%;"></div>{k}</div>', unsafe_allow_html=True)

    # Export Info
    st.sidebar.markdown("---")
    st.sidebar.info("To save as PNG: Use the 'browser print' or a screenshot tool. Pydeck also supports `r.to_html('viz.html')` for sharing.")

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Ensure 'data/au_sample_100k.parquet' exists.")
