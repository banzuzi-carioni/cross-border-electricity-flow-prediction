import pandas as pd
from great_expectations.core import ExpectationSuite, ExpectationConfiguration


# clean and validation
def merge_export_import(export_data: pd.DataFrame, import_data: pd.DataFrame) -> pd.DataFrame:
    export_data = reformat_flow(export_data, export=True)
    import_data = reformat_flow(import_data, export=False)
    combined_data = pd.concat([export_data, import_data], ignore_index=True)
    combined_data = combined_data.sort_values(by='datetime')
    combined_data = combined_data.reset_index(drop=True)
    return combined_data


def reformat_flow(data: pd.DataFrame, export: bool = True, from_api: bool = False) -> pd.DataFrame:
    data = _clean_flow_columns(data, from_api=from_api)
    if export:
        data = data.melt(
            id_vars=['datetime'],  # Keep 'datetime' as is
            var_name='country_to',  # New column for countries
            value_name='energy_sent'  # New column for energy values
            )
        data['country_from'] = 'NL'
    else:
        data = data.melt(
            id_vars=['datetime'],  # Keep 'datetime' as is
            var_name='country_from',  # New column for countries
            value_name='energy_sent'  # New column for energy values
            )
        data['country_to'] = 'NL'
    
    data = data[['datetime', 'country_from', 'country_to', 'energy_sent']]
    data.reset_index(drop=True, inplace=True)
    return data


def _clean_flow_columns(data: pd.DataFrame, from_api: bool = False) -> pd.DataFrame:
    if from_api:
        data = data.reset_index()
        data = data.rename(columns={'index': 'datetime'})
    else:
        data = data.rename(columns={'Unnamed: 0': 'datetime'})
    data = data.drop(columns=['sum'])
    data['datetime'] = pd.to_datetime(data['datetime'], utc=True).dt.tz_convert('Europe/Amsterdam')
    data = data.fillna(0)
    return data


def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    df_cleaned = df.copy()

    # 1. Rename 'time' to 'datetime'
    df_cleaned = df_cleaned.rename(columns={'time': 'datetime'})

    # 2. Cast columns: 'datetime' column to UTC and all other columns to float64
    df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], utc=True)
    for col in df_cleaned.columns:
        if col != 'datetime':
            df_cleaned[col] = df_cleaned[col].astype('float64')

    # 3. Drop rows with any NaN values
    df_cleaned.dropna(axis=0, how='any', inplace=True)

    return df_cleaned


def transform_day_ahead_prices(df: pd.DataFrame) -> pd.DataFrame:
    df_cleaned = df.copy()

    # 1. Drop the first unnamed column if it exists
    if df_cleaned.columns[0].startswith('Unnamed'):
        df_cleaned.drop(df_cleaned.columns[0], axis=1, inplace=True)

    # 2. Rename 'Timestamp' and 'Price'
    df_cleaned.rename(columns={'Timestamp': 'datetime', 'Price': 'energy_price'}, inplace=True)

    # 3. Cast columns: convert 'datetime' to UTC and 'price' to float64
    df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], utc=True)
    df_cleaned['energy_price'] = df_cleaned['energy_price'].astype('float64')

    # 4. Drop rows with nans
    df_cleaned.dropna(axis=0, how='any', inplace=True)

    return df_cleaned