import pandas as pd
import hsfs


def merge_export_import(export_data: pd.DataFrame, import_data: pd.DataFrame, from_api: bool = False) -> pd.DataFrame:
    '''
    Merges the export and import dataframes into a single dataframe from raw data.
    '''
    export_data = transform_flow(export_data, export=True, from_api=from_api)
    import_data = transform_flow(import_data, export=False, from_api=from_api)
    combined_data = pd.concat([export_data, import_data], ignore_index=True)
    combined_data = combined_data.sort_values(by='datetime')
    combined_data = combined_data.reset_index(drop=True)
    return combined_data


def transform_flow(df: pd.DataFrame, export: bool = True, from_api: bool = False) -> pd.DataFrame:
    '''
    Transforms the flow data into a clean dataframe format.
    '''
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
    '''
    Cleans the flow data columns.
    '''
    df_cleaned = df.copy()

    if from_api:
        df_cleaned = df_cleaned.reset_index()
        df_cleaned = df_cleaned.rename(columns={'index': 'datetime'})
    else:
        df_cleaned = df_cleaned.rename(columns={'Unnamed: 0': 'datetime'})
    df_cleaned = df_cleaned.drop(columns=['sum'])
    df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], utc=True)
    df_cleaned = df_cleaned.infer_objects(copy=False)
    df_cleaned = df_cleaned.fillna(0)
    return df_cleaned


def transform_weather_data(
    df_NL: pd.DataFrame, 
    df_BE: pd.DataFrame,
    df_DE_LU: pd.DataFrame, 
    df_DK_1: pd.DataFrame,
    df_GB: pd.DataFrame,
    df_NO_2: pd.DataFrame,
    from_api: bool = False
) -> pd.DataFrame:
    '''
    Transforms the weather data into a clean dataframe format.
    '''
    # Dictionary to map DataFrames to their country codes
    dfs = {
        "NL": df_NL.copy(),
        "BE": df_BE.copy(),
        "DE_LU": df_DE_LU.copy(),
        "DK_1": df_DK_1.copy(),
        "GB": df_GB.copy(),
        "NO_2": df_NO_2.copy(),
    }
    if from_api:
        for country_code, df in dfs.items():
            df = df.reset_index()
            dfs[country_code] = df

    for country_code, df in dfs.items():
        # 1. Drop the first unnamed column if it exists
        if df.columns[0].startswith('Unnamed'):
            df.drop(df.columns[0], axis=1, inplace=True)
        # 2. Add country code to each dataframe 
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


def transform_day_ahead_prices(
    df_NL: pd.DataFrame,
    df_BE: pd.DataFrame,
    df_DE_LU: pd.DataFrame,
    df_DK_1: pd.DataFrame,
    df_GB: pd.DataFrame,
    df_NO_2: pd.DataFrame,
) -> pd.DataFrame:
    '''
    Transforms the day-ahead prices data into a clean dataframe format.
    '''
    # Dictionary that maps the DataFrames to their country codes
    dfs = {
        "NL": df_NL.copy(),
        "BE": df_BE.copy(),
        "DE_LU": df_DE_LU.copy(),
        "DK_1": df_DK_1.copy(),
        "GB": df_GB.copy(),
        "NO_2": df_NO_2.copy(),
    }

    # Step 1: Drop the first unnamed column if it exists
    for country_code, df in dfs.items():
        if df.columns[0].startswith("Unnamed"):
            df.drop(df.columns[0], axis=1, inplace=True)

    # Step 2: Rename 'Timestamp' -> 'datetime' and 'Price' -> 'energy_price'
    for country_code, df in dfs.items():
        df.rename(columns={"Timestamp": "datetime", "Price": "energy_price"}, inplace=True)

    # Step 3: Add 'country_code' column
    for country_code, df in dfs.items():
        df["country_code"] = country_code

    # Step 4: Concatenate all dataframes
    df_combined = pd.concat(dfs.values(), axis=0, ignore_index=True)

    # Step 5: Convert 'datetime' to UTC and 'energy_price' to float64
    df_combined["datetime"] = pd.to_datetime(df_combined["datetime"], utc=True)
    df_combined["energy_price"] = df_combined["energy_price"].astype("float64")

    # Step 6: Drop rows with NaN values
    df_combined.dropna(axis=0, how="any", inplace=True)

    return df_combined


def transform_generation_data(
    df_NL: pd.DataFrame,
    df_BE: pd.DataFrame,
    df_DE_LU: pd.DataFrame,
    df_DK_1: pd.DataFrame,
    df_GB: pd.DataFrame,
    df_NO_2: pd.DataFrame, 
    from_api: bool = False
) -> pd.DataFrame:
    '''
    Transforms the generation data into a clean dataframe format.
    '''
    # Dictionary that maps the DataFrames to their country codes
    dfs = {
        "NL": df_NL.copy(),
        "BE": df_BE.copy(),
        "DE_LU": df_DE_LU.copy(),
        "DK_1": df_DK_1.copy(),
        "GB": df_GB.copy(),
        "NO_2": df_NO_2.copy(),
    }
    
    for country_code, df in dfs.items():
        df = _clean_generation_columns(df, from_api=from_api)    
        # Resample the data to hourly frequency
        df =  df.resample('h').sum()
        df['total_generation'] = df.sum(axis=1).astype('float64')
        df['country_code'] = country_code
        df.reset_index(inplace=True)
        
        # 3. Cast columns: convert 'datetime' to UTC
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        df.dropna(axis=0, how="any", inplace=True)
        dfs[country_code] = df

    # Step 4: Concatenate all dataframes
    df_combined = pd.concat(dfs.values(), axis=0, ignore_index=True)
    
    # Step 5: Convert 'datetime' to UTC and 'energy_price' to float64
    df_combined = df_combined.sort_values(by='datetime')

    # Step 6: Drop rows with NaN values
    df_combined = df_combined.infer_objects(copy=False)
    df_combined = df_combined.fillna(0)
    df_combined = df_combined.reset_index(drop=True)
    return df_combined


def _clean_generation_columns(df: pd.DataFrame, from_api: bool = False) -> pd.DataFrame:
    '''
    Cleans the generation data columns.
    '''
    df_cleaned = df.copy()

    if from_api:
        try:
            df_cleaned = df_cleaned.xs(key='Actual Aggregated', axis=1, level=1)
        except Exception:
            pass
        
        df_cleaned = df_cleaned.rename(columns=lambda x: '_'.join(str(x).lower().split()))
        df_cleaned = df_cleaned.rename(columns=lambda x: x.replace('-', '_') if '-' in x else x.replace('/', '_'))
        df_cleaned.reset_index(inplace=True)
        df_cleaned = df_cleaned.rename(columns={'index': 'datetime'})
        df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], utc=True)
        df_cleaned.set_index('datetime', inplace=True)
    else:
        try:
            datetimes = df.xs(key='Unnamed: 0_level_1', axis=1, level=1)
            actual_aggregated = df_cleaned.xs(key='Actual Aggregated', axis=1, level=1)
            df_cleaned = pd.merge(datetimes, actual_aggregated, left_index=True, right_index=True)
            # Ensure the timestamp column is in datetime format
            df_cleaned['datetime'] = pd.to_datetime(df_cleaned['Unnamed: 0_level_0'], utc=True)  # Replace 'Unnamed: 0' with the actual column name
            # Set the timestamp column as the index
            df_cleaned.set_index('datetime', inplace=True)
            # Drop the 'Unnamed: 0' column
            df_cleaned.drop(columns='Unnamed: 0_level_0', inplace=True)
            # reformat columns
            df_cleaned = df_cleaned.rename(columns=lambda x: '_'.join(str(x).lower().split()))
            df_cleaned = df_cleaned.rename(columns=lambda x: x.replace('-', '_') if '-' in x else x.replace('/', '_'))
        except Exception:  # DK_1, GB, NO_2
            # Drop columns containing "Consumption"
            df_cleaned = df_cleaned.drop(columns=[col for col in df_cleaned.columns if "Consumption" in str(col)])
            # Rename columns like "('Fossil Gas', 'Actual Aggregated')" to "Fossil Gas"
            df_cleaned = df_cleaned.rename(columns=lambda x: x.split(",")[0].strip("()'") if "Actual Aggregated" in str(x) else x)

            df_cleaned = df_cleaned.T.groupby(df_cleaned.columns).sum()
            df_cleaned = df_cleaned.T.rename(columns={'Unnamed: 0': 'datetime'})
            df_cleaned = df_cleaned.rename(columns=lambda x: '_'.join(str(x).lower().split()))
            df_cleaned = df_cleaned.rename(columns=lambda x: x.replace('-', '_') if '-' in x else x.replace('/', '_'))
            df_cleaned['datetime'] = pd.to_datetime(df_cleaned['datetime'], utc=True)
            df_cleaned.set_index('datetime', inplace=True)

    return df_cleaned


def transform_prices_generation(df_prices: pd.DataFrame, df_generation: pd.DataFrame) -> pd.DataFrame:
    '''
    Merges the prices and generation dataframes into a single dataframe.
    '''
    df_merged = pd.merge(
        df_prices, 
        df_generation, 
        on=["datetime", "country_code"], 
        how="inner"
    )
    return df_merged


def transform_data_for_feature_view(
    fg_weather: hsfs.feature_group.FeatureGroup, 
    fg_prices_generation: hsfs.feature_group.FeatureGroup, 
    fg_flow: hsfs.feature_group.FeatureGroup,
    fs: hsfs.feature_store.FeatureStore, 
    version: int = 1
) -> hsfs.feature_group.FeatureGroup:
    '''
    Transforms the data from feature groups into a format suitable for a feature view to a ML model.
    '''
    df_weather = fg_weather.read()
    df_prices_generation = fg_prices_generation.read()
    df_flow = fg_flow.read()

    weather_columns = list(df_weather.columns)[1:]
    prices_generation_columns = list(df_prices_generation.columns)[1:]

    weather_pivot = _pivot_transform(df_weather, weather_columns)
    prices_generation_pivot = _pivot_transform(df_prices_generation, prices_generation_columns)
    prices_generation_pivot = prices_generation_pivot.infer_objects(copy=False)
    prices_generation_pivot = prices_generation_pivot.fillna(0)

    # Combine pivot dfs
    df_combined_pivot = pd.merge(
        weather_pivot, 
        prices_generation_pivot,
        on="datetime", 
        how="inner"
    )
    
    # Make a final df format
    df_combined_full = pd.merge(
        df_flow, 
        df_combined_pivot,
        on="datetime", 
        how="right"
    )

    df_combined_full.dropna(axis=0, how='any', inplace=True)
    df_combined_full = df_combined_full.sort_values(by='datetime', ascending=True)

    # Create the Feature Group
    model_data_fg = fs.get_or_create_feature_group(
        name="model_data",
        version=version,
        description=(
            'Cross-border electricity dataset with energy_sent as the target, combining pivoted multi-country weather, generation, and energy price features for each timestamp, along with country_from and country_to.'
        ),
        primary_key=["datetime", "country_from", "country_to"],
        event_time="datetime"
    )

    model_data_fg.insert(df_combined_full, write_options={"wait_for_job": True})
    return model_data_fg


def _pivot_transform(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    '''
    Transforms the DataFrame into a pivot table format.
    '''
    df_pivot = df.pivot_table(
        index="datetime",
        columns="country_code",
        values=columns
    )
    # df pivot will have a multi-level column index like: ('temperature_2m', 'DK_1'), ('temperature_2m', 'BE'), etc.
    df_pivot.columns = [f"{var}_{country}" for var, country in df_pivot.columns]

    df_pivot.reset_index(inplace=True)
    df_pivot = df_pivot.sort_values(by=['datetime'], ascending=True)
    return df_pivot
