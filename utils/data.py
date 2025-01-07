import os
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from xgboost import XGBRegressor
from xgboost import plot_importance
from sklearn.metrics import mean_squared_error, r2_score
from feature_pipeline.ETL import load, transform
import hopsworks


BZN2COUNTRY = {
    'NL': 'Netherlands',
    'BE': 'Belgium',
    'NO_2': 'Norway',
    'DK_1': 'Denmark',
    'DE_LU': 'Germany/Luxembourg',
    'GB': 'United Kingdom'
}

NEIGHBOUR_ZONES = {
    'BE', 'DE_LU', 'GB', 'NO_2', 'DK_1'
}

ZONES = {
    'NL', 'BE', 'DE_LU', 'GB', 'NO_2', 'DK_1'
}


def create_feature_view(version: int = 1):
    # Connect to Hopsworks
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


def _retrieve_feature_groups(version: int = 1):
    weather_fg = load.retrieve_feature_group(name='weather_open_meteo', version=version)
    prices_generation_fg = load.retrieve_feature_group(name='prices_generation', version=version)
    physical_flow_fg = load.retrieve_feature_group(name='physical_flow', version=version)
    return weather_fg, prices_generation_fg, physical_flow_fg


# load dataset: either for training or inference 


# prepare data (train test split): training_pipeline 
