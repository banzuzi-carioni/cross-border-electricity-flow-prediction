import pandas as pd

# clean and validation
def merge_export_import(export_data: pd.DataFrame, import_data: pd.DataFrame) -> pd.DataFrame:
    export_data = transform_flow(export_data, export=True)
    import_data = transform_flow(import_data, export=False)
    combined_data = pd.concat([export_data, import_data], ignore_index=True)
    combined_data = combined_data.sort_values(by='datetime')
    combined_data = combined_data.reset_index(drop=True)
    return combined_data


def transform_flow(df: pd.DataFrame, export: bool = True, from_api: bool = False) -> pd.DataFrame:
    df_cleaned = df.copy()

    df_cleaned = _clean_flow_columns(df_cleaned, from_api=from_api)
    if export:
        df_cleaned = df_cleaned.melt(
            id_vars=['datetime'],  # Keep 'datetime' as is
            var_name='country_to',  # New column for countries
            value_name='energy_sent'  # New column for energy values
            )
        df_cleaned['country_from'] = 'NL'
    else:
        df_cleaned = df_cleaned.melt(
            id_vars=['datetime'],  # Keep 'datetime' as is
            var_name='country_from',  # New column for countries
            value_name='energy_sent'  # New column for energy values
            )
        df_cleaned['country_to'] = 'NL'
    
    df_cleaned = df_cleaned[['datetime', 'country_from', 'country_to', 'energy_sent']]
    df_cleaned.reset_index(drop=True, inplace=True)
    return df_cleaned


def _clean_flow_columns(df: pd.DataFrame, from_api: bool = False) -> pd.DataFrame:
    df_cleaned = df.copy()

    if from_api:
        df_cleaned = df_cleaned.reset_index()
        df_cleaned = df_cleaned.rename(columns={'index': 'datetime'}, inplace=True)
    else:
        df_cleaned = df_cleaned.rename(columns={'Unnamed: 0': 'datetime'}, inplace=True)
    df_cleaned = df_cleaned.drop(columns=['sum'])
    df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], utc=True)
    # TODO: check this
    df_cleaned = df_cleaned.fillna(0)
    # df_cleaned.dropna(axis=0, how='any', inplace=True)
    return df_cleaned


def transform_weather_data(df_NL: pd.DataFrame, 
                           df_BE: pd.DataFrame,
                           df_DE_LU: pd.DataFrame, 
                           df_DK_1: pd.DataFrame,
                           df_GB: pd.DataFrame,
                           df_NO_2: pd.DataFrame,
                           ) -> pd.DataFrame:
    
    # Dictionary to map DataFrames to their country codes
    dfs = {
        "NL": df_NL.copy(),
        "BE": df_BE.copy(),
        "DE_LU": df_DE_LU.copy(),
        "DK_1": df_DK_1.copy(),
        "GB": df_GB.copy(),
        "NO_2": df_NO_2.copy(),
    }

    # 1. Drop the first unnamed column if it exists
    for country_code, df in dfs.items():
        if df.columns[0].startswith('Unnamed'):
            df.drop(df.columns[0], axis=1, inplace=True)

    # 2. Add country code to each dataframe 
    for country_code, df in dfs.items():
        df['country_code'] = country_code
    
    # 3. Concatenate the dataframes 
    df_combined = pd.concat(dfs.values(), axis=0, ignore_index=True)

    # 4. Rename 'time' to 'datetime'
    df_combined = df_combined.rename(columns={'time': 'datetime'})
    
    # 5. Cast columns: 'datetime' column to UTC and all other columns to float64
    df_combined['datetime'] = pd.to_datetime(df_combined['datetime'], utc=True)
    for col in df_combined.columns:
        if col not in ['datetime', 'country_code']:
            df_combined[col] = df_combined[col].astype('float64')
    
    # 6. Drop rows with any NaN values
    df_combined.dropna(axis=0, how='any', inplace=True)
    
    return df_combined


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

