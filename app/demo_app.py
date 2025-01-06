import streamlit as st
import pydeck as pdk
import pandas as pd
from entsoe import EntsoePandasClient
from utils.settings import ENV_VARS
from app_utils import BZN2CITY, get_city_coordinates

client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])

start = pd.Timestamp('20240523', tz='Europe/Stockholm')
end = pd.Timestamp('20240524', tz='Europe/Stockholm')


FI_import = list(client.query_physical_crossborder_allborders('FI', start, end, export=False).columns)[:-1]
FI_export = list(client.query_physical_crossborder_allborders('FI', start, end, export=True).columns)[:-1]

st.title("Nordic Cross-border Electricity Flow Prediction")

# Mapping of cities to coordinates
GREEN_RGB = [0, 255, 0, 150]
RED_RGB = [240, 100, 0, 150]

arc_data = pd.DataFrame({
    "start_city": [*['FI']*len(FI_export), *FI_import],
    "end_city": [*FI_export, *['FI']*len(FI_import)]
})


# Add coordinates based on city names
arc_data["start_coords"] = arc_data["start_city"].map(get_city_coordinates)
arc_data["end_coords"] = arc_data["end_city"].map(get_city_coordinates)

# PathLayer data: Line splitting Sweden into two regions
path_data = pd.DataFrame({
    "path": [[
        [14.5, 69.0],  # Top of Sweden near the center
        [14.5, 55.0]   # Bottom of Sweden near the center
    ]]
})

# TextLayer data: Labels for the two regions
text_data = pd.DataFrame({
    "text": ["West Sweden", "East Sweden"],
    "lat": [62.0, 62.0],
    "lng": [12.0, 17.0]
})

# Define the ArcLayer
arc_layer = pdk.Layer(
    "ArcLayer",
    data=arc_data,
    get_source_position="start_coords",
    get_target_position="end_coords",
    get_width=3,
    get_source_color=RED_RGB,
    get_target_color=GREEN_RGB,
    pickable=True,
    auto_highlight=True
)

# Define the PathLayer
path_layer = pdk.Layer(
    "PathLayer",
    data=path_data,
    get_path="path",
    get_width=5,
    get_color=[0, 255, 0],
    width_scale=10
)

# Define the TextLayer
text_layer = pdk.Layer(
    "TextLayer",
    data=text_data,
    get_position="[lng, lat]",
    get_text="text",
    get_size=20,
    get_color=[0, 0, 0],
    get_alignment_baseline="bottom"
)

# Set the initial view of the map
view_state = pdk.ViewState(
    latitude=62.0,
    longitude=15.0,
    zoom=4,
    pitch=30
)


# First selectbox for Energy flow type
option1 = st.selectbox(
    "Energy flow type:",
    ("Direction", "Export", "Import"),
)



nordic_zones = tuple(BZN2CITY.keys())
option2 = st.selectbox(
    "Bidding zone:",
    ("All", *nordic_zones),
)

hour = st.slider("Hour of the day:", -1, 23, -1, format="%.2f")
st.write(f"Energy flows at {hour:.2f} today.") if hour != -1 else st.write("Total Energy flows today.")


# Render the map
st.pydeck_chart(
    pdk.Deck(
        layers=[arc_layer, path_layer, text_layer],
        initial_view_state=view_state,
        tooltip={"text": "Flow from {start_city} to {end_city}"}
    )
)
