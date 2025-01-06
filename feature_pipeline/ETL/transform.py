import pandas as pd

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