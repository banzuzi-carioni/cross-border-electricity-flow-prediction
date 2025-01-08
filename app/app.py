import streamlit as st
import pydeck as pdk
import pandas as pd
import plotly.express as px
from app_utils import get_country_center_coordinates


# Set page configuration
st.set_page_config(
    page_title="Netherlands Energy Flow Prediction",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Title and Description
st.title("Netherlands Cross-border Electricity Flow Prediction 🇳🇱 ")
st.markdown("""
Monitor and analyze the predicted electricity flows **to and from** the Netherlands.
Use the interactive maps and charts to gain insights into energy transfers, prices, and generation related to the Netherlands.
""")

# Sidebar for Filters
st.sidebar.header("Filters")

# Load predictions data
@st.cache_data
def load_predictions(csv_path: str = 'inference_pipeline/predictions/predictions.csv'):
    df = pd.read_csv(csv_path, index_col=0, parse_dates=['datetime'])
    df.loc[df['energy_sent'] < 0, 'energy_sent'] = 0
    return df[['datetime', 'country_from', 'country_to', 'energy_sent', 'energy_price_nl', 'total_generation_nl']]

# Load Monitoring Metrics
@st.cache_data
def load_maes(csv_path: str = 'inference_pipeline/monitoring/mae_metrics.csv'):
    df = pd.read_csv(csv_path)
    mae_import = df['mae_import'].iloc[-1]
    mae_export = df['mae_export'].iloc[-1]
    return mae_import, mae_export


predictions_df = load_predictions()
mae_import, mae_export = load_maes()


# Filter the DataFrame to include only flows to/from the Netherlands
filtered_df = predictions_df[
    (predictions_df['country_from'] == 'NL') | (predictions_df['country_to'] == 'NL')
].copy()

# Create a new column to indicate the direction of flow
filtered_df['flow_direction'] = filtered_df.apply(
    lambda row: 'Export' if row['country_from'] == 'NL' else 'Import',
    axis=1
)

# Sidebar Filters
with st.sidebar:
    # Date Range Filter
    min_date = filtered_df['datetime'].min().date()
    max_date = filtered_df['datetime'].max().date()
    selected_date = st.date_input(
        "Select Date",
        min_value=min_date,
        max_value=max_date,
        value=min_date
    )
    
    # Hour Dropdown
    time_options = [f"{hour:02d}:00" for hour in range(24)]  # Generate 'HH:00' options
    selected_time = st.selectbox(
        "Select Hour",
        options=["Total Day"] + time_options,
        index=0
    )
    
    # Energy Flow Type
    flow_type = st.radio(
        "Energy Flow Type",
        options=["All", "Export", "Import"],
        index=0
    )
    
    # Apply Filters Button
    apply_filters = st.button("Apply Filters")

# Apply filters to the DataFrame
def filter_data(df, date, time, flow):
    df_filtered = df.copy()
    
    # Filter by date
    df_filtered = df_filtered[df_filtered['datetime'].dt.date == date]
    
    # Filter by hour if not "All"
    if time != "Total Day":
        selected_hour = int(time.split(":")[0])  # Extract hour as integer
        df_filtered = df_filtered[df_filtered['datetime'].dt.hour == selected_hour]
    
    # Filter by flow type
    if flow == "Export":
        df_filtered = df_filtered[df_filtered['flow_direction'] == "Export"]
    elif flow == "Import":
        df_filtered = df_filtered[df_filtered['flow_direction'] == "Import"]
    
    return df_filtered

if apply_filters:
    filtered_df = filter_data(
        filtered_df,
        selected_date,
        selected_time,
        flow_type
    )

# Display Key Metrics
st.header("Key Metrics")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_energy_sent = filtered_df[filtered_df['flow_direction'] == 'Export']['energy_sent'].sum()
    st.metric(
        label="Total Energy Exported (MWh)",
        value=f"{total_energy_sent:,.2f}"
    )

with col2:
    total_energy_received = filtered_df[filtered_df['flow_direction'] == 'Import']['energy_sent'].abs().sum()
    st.metric(
        label="Total Energy Imported (MWh)",
        value=f"{total_energy_received:,.2f}"
    )

with col3:
    avg_energy_price_nl = filtered_df['energy_price_nl'].mean()
    st.metric(
        label="Average Energy Price (EUR/MWh)",
        value=f"{avg_energy_price_nl:.2f}"
    )

with col4:
    st.metric(
        label="MAE Import Predictions yesterday (MWh)",
        value=f"{mae_import:.2f}"
    )

with col5:
    st.metric(
        label="MAE Export Predictions yesterday (MWh)",
        value=f"{mae_export:.2f}"
    )

# Tabs for Organized Content
tab1, tab2, tab3 = st.tabs(["🌏 Energy Flow Map", "📈 Time-Series Analysis", "👩‍💻 Tabular Format"])

with tab1:
    st.subheader("Energy Flow Map")
    
    # Prepare Arc Data for Mapping
    arc_data = pd.DataFrame({
        "start_city": filtered_df['country_from'],
        "end_city": filtered_df['country_to'],
        "energy_sent": filtered_df['energy_sent'],
        "flow_direction": filtered_df['flow_direction']
    })
    
    # Add coordinates based on city names
    arc_data["start_coords"] = arc_data["start_city"].map(get_country_center_coordinates)
    arc_data["end_coords"] = arc_data["end_city"].map(get_country_center_coordinates)
    
    # Remove entries with missing coordinates after mapping
    arc_data = arc_data.dropna(subset=["start_coords", "end_coords"])
    
    # Define color based on energy sent
    def get_color(direction):
        if direction == "Export":
            return [0, 255, 0, 150]  # Green for exports
        else:
            return [255, 0, 0, 150]  # Red for imports
    
    arc_data['color'] = arc_data['flow_direction'].apply(get_color)
    
    # Define the ArcLayer
    arc_layer = pdk.Layer(
        "ArcLayer",
        data=arc_data,
        get_source_position="start_coords",
        get_target_position="end_coords",
        get_source_color="color",
        get_target_color="color",
        get_width=2,
        auto_highlight=True,
        pickable=True
    )
    
    # Set the initial view of the map centered on the Netherlands
    view_state = pdk.ViewState(
        latitude=52.25,  # Latitude of the Netherlands
        longitude=5.54,  # Longitude of the Netherlands
        zoom=5,
        pitch=30
    )
    
    # Define the tooltip
    tooltip = {
        "html": "<b>Flow Direction:</b> {flow_direction} <br/> <b>From:</b> {start_city} <br/> <b>To:</b> {end_city} <br/> <b>Energy Sent:</b> {energy_sent} MWh",
        "style": {
            "backgroundColor": "steelblue",
            "color": "white"
        }
    }
    
    # Render the map
    st.pydeck_chart(
        pdk.Deck(
            layers=[arc_layer],
            initial_view_state=view_state,
            tooltip=tooltip
        )
    )

with tab2:
    st.subheader("Time-Series Analysis")
    
    # Time-Series Plot for Energy Sent (Export and Import)
    hourly_aggregated = filtered_df.copy()
    hourly_aggregated['hour'] = hourly_aggregated['datetime'].dt.floor('h')  # Group by hour
    aggregated_data = hourly_aggregated.groupby(['hour', 'flow_direction'])['energy_sent'].sum().reset_index()

    fig1 = px.line(
        aggregated_data,
        x='hour',
        y='energy_sent',
        color='flow_direction',
        title='Energy Sent Over Time',
        labels={'hour': 'Datetime', 'energy_sent': 'Energy Sent (MWh)', 'flow_direction': 'Flow Direction'},
        markers=True
    )
    st.plotly_chart(fig1, use_container_width=True)
    
    # Time-Series Plot for Energy Price
    fig2 = px.line(
        filtered_df,
        x='datetime',
        y='energy_price_nl',
        title='Energy Price Over Time',
        labels={'datetime': 'Datetime', 'energy_price_nl': 'Energy Price (EUR/MWh)'},
        markers=True
    )
    st.plotly_chart(fig2, use_container_width=True)
    
    # Time-Series Plot for Total Generation
    fig3 = px.line(
        filtered_df,
        x='datetime',
        y='total_generation_nl',
        title='Total Energy Generation Over Time',
        labels={'datetime': 'Datetime', 'total_generation_nl': 'Total Generation (MWh)'},
        markers=True
    )
    st.plotly_chart(fig3, use_container_width=True)

with tab3:
    st.subheader("Detailed Predictions Data")
    
    # Display DataFrame
    st.dataframe(filtered_df)
    
    # Option to download the filtered data
    def convert_df(df):
        return df.to_csv(index=False).encode('utf-8')
    
    csv = convert_df(filtered_df)
    
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name='filtered_predictions_nl.csv',
        mime='text/csv',
    )

# Footer
st.markdown("""
---
*Developed with ❤️ by [Eric Banzuzi](https://www.linkedin.com/in/eric-banzuzi/) and [Rosamelia Carioni](https://www.linkedin.com/in/rosamelia-carioni/)*
""")
