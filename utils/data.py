import pandas as pd
import hsfs
from feature_pipeline.ETL import load
from typing import Tuple
from utils.utils import create_feature_view


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

COLUMNS_MODEL_TOTAL_PRODUCTION = [
    'datetime',
    'country_from',
    'country_to',
    'cloudcover_be',
    'cloudcover_de_lu',
    'cloudcover_dk_1',
    'cloudcover_gb',
    'cloudcover_nl',
    'cloudcover_no_2',
    'diffuse_radiation_be',
    'diffuse_radiation_de_lu',
    'diffuse_radiation_dk_1',
    'diffuse_radiation_gb',
    'diffuse_radiation_nl',
    'diffuse_radiation_no_2',
    'direct_radiation_be',
    'direct_radiation_de_lu',
    'direct_radiation_dk_1',
    'direct_radiation_gb',
    'direct_radiation_nl',
    'direct_radiation_no_2',
    'precipitation_be',
    'precipitation_de_lu',
    'precipitation_dk_1',
    'precipitation_gb',
    'precipitation_nl',
    'precipitation_no_2',
    'snow_depth_be',
    'snow_depth_de_lu',
    'snow_depth_dk_1',
    'snow_depth_gb',
    'snow_depth_nl',
    'snow_depth_no_2',
    'surface_pressure_be',
    'surface_pressure_de_lu',
    'surface_pressure_dk_1',
    'surface_pressure_gb',
    'surface_pressure_nl',
    'surface_pressure_no_2',
    'temperature_2m_be',
    'temperature_2m_de_lu',
    'temperature_2m_dk_1',
    'temperature_2m_gb',
    'temperature_2m_nl',
    'temperature_2m_no_2',
    'wind_direction_100m_be',
    'wind_direction_100m_de_lu',
    'wind_direction_100m_dk_1',
    'wind_direction_100m_gb',
    'wind_direction_100m_nl',
    'wind_direction_100m_no_2',
    'wind_direction_10m_be',
    'wind_direction_10m_de_lu',
    'wind_direction_10m_dk_1',
    'wind_direction_10m_gb',
    'wind_direction_10m_nl',
    'wind_direction_10m_no_2',
    'wind_speed_100m_be',
    'wind_speed_100m_de_lu',
    'wind_speed_100m_dk_1',
    'wind_speed_100m_gb',
    'wind_speed_100m_nl',
    'wind_speed_100m_no_2',
    'wind_speed_10m_be',
    'wind_speed_10m_de_lu',
    'wind_speed_10m_dk_1',
    'wind_speed_10m_gb',
    'wind_speed_10m_nl',
    'wind_speed_10m_no_2',
    'energy_price_be',
    'energy_price_de_lu',
    'energy_price_dk_1',
    'energy_price_gb',
    'energy_price_nl',
    'energy_price_no_2',
    'total_generation_be',
    'total_generation_de_lu',
    'total_generation_dk_1',
    'total_generation_gb',
    'total_generation_nl',
    'total_generation_no_2'
]


def prepare_data_for_training(X_train: pd.DataFrame, X_test: pd.DataFrame, total_production: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Prepares the data for training by removing unnecessary columns, applying one-hot encoding, and dropping the datetime column.
    '''
    X_train_result = X_train.copy()
    X_test_result = X_test.copy()

    # Remove not needed columns 
    if total_production:
        X_train_result = X_train_result[COLUMNS_MODEL_TOTAL_PRODUCTION]
        X_test_result = X_test_result[COLUMNS_MODEL_TOTAL_PRODUCTION]
    else:
        columns_to_drop = ['total_generation_nl', 'total_generation_be', 'total_generation_de_lu', 'total_generation_dk_1', 'total_generation_gb',  'total_generation_no_2']
        X_train_result = X_train_result.drop(columns = columns_to_drop)
        X_test_result = X_test_result.drop(columns = columns_to_drop)
    
    # Remove datetime column
    X_train_result = X_train_result.drop(columns = ['datetime'])
    X_test_result = X_test_result.drop(columns = ['datetime'])

    # Apply one-hot encoding with prefixes
    X_train_result = one_hot_encoding(X_train_result, 'country_from', prefix='from')
    X_train_result = one_hot_encoding(X_train_result, 'country_to', prefix='to')   
    X_test_result = one_hot_encoding(X_test_result, 'country_from', prefix='from')
    X_test_result = one_hot_encoding(X_test_result, 'country_to', prefix='to')  
 
    return X_train_result, X_test_result


def one_hot_encoding(df, feature, prefix=None) -> pd.DataFrame:
    """
    Performs one-hot encoding on the specified feature and ensures the result is 1 and 0.
    Adds a prefix to the columns to avoid name collisions.
    """
    df_result = df.copy()
    # Generate one-hot encoding
    df_one_hot = pd.get_dummies(df[feature], prefix=prefix or feature)
    # Convert to integers to ensure 1 and 0
    df_one_hot = df_one_hot.astype(int)
    # Drop the original feature column and join one-hot encoding
    df_result = df_result.drop(columns=[feature], axis=1)
    df_result = df_result.join(df_one_hot)
    return df_result


def revert_one_hot_encoding(df: pd.DataFrame, column_prefix: str, original_column: str) -> pd.DataFrame:
    """
    Reverts one-hot encoding back to the original categorical column.
    
    Args:
    - df (pd.DataFrame): The DataFrame with one-hot encoded columns.
    - column_prefix (str): The prefix used for the one-hot encoded columns (e.g., 'from' or 'to').
    - original_column (str): The name of the original column to restore.
    
    Returns:
    - pd.DataFrame: The DataFrame with the original column restored.
    """
    df_result = df.copy()
    
    # Filter out the one-hot encoded columns based on the prefix
    one_hot_columns = [col for col in df.columns if col.startswith(f"{column_prefix}_")]
    
    # Map one-hot encoded columns back to the original categorical column
    df_result[original_column] = df_result[one_hot_columns].idxmax(axis=1).str[len(column_prefix) + 1:]
    
    # Drop the one-hot encoded columns
    df_result = df_result.drop(columns=one_hot_columns)
    
    return df_result


def split_training_data(
    feature_view: hsfs.feature_view.FeatureView,
    test_start: pd.Timestamp = pd.Timestamp('2024-01-01', tz='UTC').normalize()
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    '''
    Splits the data from the feature view into training and testing datasets.
    '''
    X_train, X_test, y_train, y_test = feature_view.train_test_split(test_start=test_start)
    return X_train, X_test, y_train, y_test


def get_training_data(version: int, total_production: bool, create_fv: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    '''
    Loads the feature view from the feature store and splits the data into training and testing sets.
    '''
    feature_store = load.get_feature_store()

    if create_fv:
        feature_view = create_feature_view(version)
    else:
        feature_view = feature_store.get_feature_view(
            name = 'cross_border_electricity_fv',
            version = version
        )
    X_train, X_test, y_train, y_test = split_training_data(feature_view)
    X_train_one_hot, X_test_one_hot = prepare_data_for_training(X_train, X_test, total_production) 
    return X_train_one_hot, X_test_one_hot, y_train, y_test


def prepare_data_for_predictions(X: pd.DataFrame, total_production: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Prepares the data for training by removing unnecessary columns, applying one-hot encoding, and dropping the datetime column.
    '''
    X_result = X.copy()
    # Apply one-hot encoding with prefixes
    X_result = X_result.drop(columns = ['datetime'])
    X_result = one_hot_encoding(X_result, 'country_from', prefix='from')
    X_result = one_hot_encoding(X_result, 'country_to', prefix='to')
    return X_result


def add_country_codes_for_prediction(X: pd.DataFrame) -> pd.DataFrame:
    '''
    Adds the country codes to the DataFrame.
    '''
    df = X.copy()

    # Create combinations of NL to NEIGHBOUR_ZONES and NEIGHBOUR_ZONES to NL
    combinations = [('NL', zone) for zone in NEIGHBOUR_ZONES] + [(zone, 'NL') for zone in NEIGHBOUR_ZONES]

    # Create a DataFrame with every combination
    expanded_rows = []
    for index, row in df.iterrows():
        for country_from, country_to in combinations:
            new_row = row.copy()
            new_row['country_from'] = country_from
            new_row['country_to'] = country_to
            expanded_rows.append(new_row)

    # Create a new DataFrame with the expanded rows
    expanded_df = pd.DataFrame(expanded_rows)
    expanded_df.reset_index(drop=True, inplace=True) 
    return expanded_df
