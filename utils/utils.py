import hopsworks
import hsfs
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple
from utils.settings import ENV_VARS
from feature_pipeline.ETL import load, transform
from matplotlib.ticker import MultipleLocator


COUTRY_TO_COORDS = {
    'NL': (52.25, 5.54),
    'GB': (54.7, -3.28),
    'NO_2': (61.15, 8.79),
    'DK_1': (55.67, 10.33),
    'DE_LU': (50.93, 6.94),
    'BE': (50.64, 4.67)
}


def get_country_center_coordinates(country_code: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    latitude, longitude = COUTRY_TO_COORDS.get(country_code, (0, 0))
    return longitude, latitude


def get_model_registry(model_name: str = 'model', version: int = 1) -> str:
    '''
    Returns the model from the model registry.
    '''
    project = hopsworks.login(api_key_value=ENV_VARS["HOPSWORKS"], project='cross_border_electricity')
    mr = project.get_model_registry()
    model_dir = mr.get_model(model_name, version=version)
    return model_dir.download()


def create_feature_view(version: int = 1) -> hsfs.feature_view.FeatureView:
    '''
    Creates a feature view with the pivoted multi-country weather, generation, and energy price features for each timestamp, along with country_from and country_to.
    '''
    feature_store = load.get_feature_store()
    
    weather_fg, prices_generation_fg, physical_flow_fg = _retrieve_feature_groups(version=version)
    restructured_fg = transform.transform_data_for_feature_view(weather_fg, prices_generation_fg, physical_flow_fg, feature_store, version)
    selected_features = restructured_fg.select_all()

    feature_view = feature_store.create_feature_view(
        name='cross_border_electricity_fv',
        description='Cross-border electricity dataset with energy_sent as the target, combining pivoted multi-country weather, generation, and energy price features for each timestamp, along with country_from and country_to.',
        version=version,
        labels=['energy_sent'],
        query=selected_features
    )
    return feature_view


def _retrieve_feature_groups(version: int = 1) -> Tuple[hsfs.feature_group.FeatureGroup, hsfs.feature_group.FeatureGroup, hsfs.feature_group.FeatureGroup]:
    '''
    Retrieves the feature groups from the feature store.
    '''
    weather_fg = load.retrieve_feature_group(name='weather_open_meteo', version=version)
    prices_generation_fg = load.retrieve_feature_group(name='prices_generation', version=version)
    physical_flow_fg = load.retrieve_feature_group(name='physical_flow', version=version)
    return weather_fg, prices_generation_fg, physical_flow_fg


def get_feature_view(feature_view_name: str = 'cross_border_electricity_fv', version: int = 1) -> hsfs.feature_view.FeatureView:
    '''
    Returns the feature view from the feature store.
    '''
    feature_store = load.get_feature_store()
    feature_view = feature_store.get_feature_view(feature_view_name, version=version)
    return feature_view


def get_feature_group(name: str, version: int = 1) -> hsfs.feature_group.FeatureGroup:
    '''
    Returns the feature group from the feature store.
    '''
    return load.retrieve_feature_group(name=name, version=version)


def plot_hourly_data(df: pd.DataFrame, df2: pd.DataFrame, file_path: str = None) -> None:
    """
    Generates monitoring plots for the hourly energy data.
    """
    # Ensure datetime is parsed correctly
    hourly_data = df.copy()
    hourly_data['hour'] = hourly_data['datetime'].dt.hour  # Extract hour from datetime
    # Group data by hour
    hourly_data.set_index('hour', inplace=True)
    
    hourly_data2 = df2.copy()
    hourly_data2['hour'] = hourly_data2['datetime'].dt.hour  # Extract hour from datetime
    # Group data by hour
    hourly_data2.set_index('hour', inplace=True)

    # Plotting
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot data
    ax.plot(hourly_data.index, hourly_data['energy_sent'], label='predictions', color='red', linewidth=2, marker='o')
    ax.plot(hourly_data2.index, hourly_data2['energy_sent'], label='real', color='black', linewidth=2, marker='^')
    
    # Customize the plot
    ax.set_xlabel('Hour of Day', fontsize=12)
    ax.set_ylabel('Values', fontsize=12)
    ax.set_title('Hourly Energy Data', fontsize=14)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.xaxis.set_major_locator(MultipleLocator(1))  # Ensure x-axis shows every hour
    ax.legend(fontsize=10)
    
    # Save and return the plot
    plt.tight_layout()
    plt.xlim(-0.25, 23)
    if file_path:
        plt.savefig(file_path)
    plt.show()
    