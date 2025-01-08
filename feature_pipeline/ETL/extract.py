import pandas as pd
from entsoe import EntsoePandasClient
from typing import Tuple, Optional
from utils.settings import ENV_VARS
from utils.utils import get_country_center_coordinates
import requests


def extract_day_ahead_price(
    country_code: str,
    start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='UTC').normalize(), 
    end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='UTC').normalize(),
    to_CSV: bool = True
) -> Optional[pd.DataFrame]:
    '''
    Extracts day-ahead electricity prices from ENTSO-E API for a given country code.
    '''
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    day_ahead_prices = client.query_day_ahead_prices('NL', start_time, end_time)
    
    # convert the series to a DataFrame
    day_ahead_prices_df = day_ahead_prices.reset_index()
    day_ahead_prices_df.columns = ['Timestamp', 'Price']
    
    if to_CSV:
        day_ahead_prices_df.to_csv(f'./feature_pipeline/data/{country_code}_day_ahead_prices.csv')
        print(f'Day ahead prices successfully saved for {country_code}.')
        return 
    
    return day_ahead_prices_df


def extract_physical_flows(
    country_code: str = 'NL', 
    start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='UTC').normalize(), 
    end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='UTC').normalize(),
    to_CSV: bool = True
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    '''
    Extracts physical electricity flows from ENTSO-E API for a given country code
    '''
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    import_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=False, per_hour=True)
    export_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=True, per_hour=True)
    
    if to_CSV:
        import_data.to_csv(f'./feature_pipeline/data/{country_code}_import_flow.csv')
        export_data.to_csv(f'./feature_pipeline/data/{country_code}_export_flow.csv')
        print(f'Import and export physcial flows successfully saved for {country_code}.')
        return 
    
    return import_data, export_data


def extract_energy_generation(
    country_code: str, 
    start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='UTC').normalize(), 
    end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='UTC').normalize(),
    to_CSV: bool = True,
    forecast: bool = False
) -> Optional[pd.DataFrame]:
    '''
    Extracts energy generation data from ENTSO-E API for a given country code
    '''
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    if forecast:
        generation_data = client.query_generation_forecast(country_code, start=start_time, end=end_time).to_frame()
    else: 
        generation_data = client.query_generation(country_code, start=start_time, end=end_time, psr_type=None)

    if to_CSV:
        generation_data.to_csv(f'./feature_pipeline/data/{country_code}_energy_generation.csv')
        print(f'Energy generation successfully saved for {country_code}.')
        return 
    return generation_data


def extract_historical_weather_data(
    country_code: str,
    start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='UTC').normalize(), 
    end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='UTC').normalize(),
    to_CSV: bool = True
) -> pd.DataFrame:
    """
    Extracts hourly weather data from Open-Meteo's ERA5 reanalysis archive. 
        Solar energy: temperature_2m, cloudcover, direct_radiation, diffuse_radiation, 
        Wind energy: surface_pressure, wind_speed_10m, wind_direction_10m, wind_speed_100m, wind_direction_100m 
        Hydro energy: precipitation, snow_depth
    """
    
    # Convert the Timestamps to YYYY-MM-DD (format Open-Meteo needs)
    start_date_str = start_time.strftime('%Y-%m-%d')
    end_date_str = end_time.strftime('%Y-%m-%d')
    latitude, longitude = get_country_center_coordinates(country_code)

    base_url = "https://archive-api.open-meteo.com/v1/era5"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date_str,
        "end_date": end_date_str,
        "hourly": [
            "temperature_2m",
            "surface_pressure",
            "cloudcover",
            "direct_radiation",
            "diffuse_radiation",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_speed_100m",
            "wind_direction_100m",
            "precipitation",
            "snow_depth"
        ],
        "timezone": "Europe/Amsterdam"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()  # will raise an error if the request failed
    data = response.json()
    hourly_data = data.get("hourly", {})
    if not hourly_data:
        print("No weather data returned for the specified parameters.")
        return None

    # Convert to DataFrame
    df = pd.DataFrame(hourly_data)
    
    # 'time' is given as an ISO string, so convert to datetime
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

    # Optionally save to CSV
    if to_CSV:
        csv_path = f"./feature_pipeline/data/{country_code}_weather_data.csv"
        df.to_csv(csv_path)
        print(f"Weather data successfully saved to {csv_path}")
        return None

    return df


def extract_weather_forecast(
    country_code: str,
    start_time: pd.Timestamp = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1), 
    end_time: pd.Timestamp = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1)
)-> Optional[pd.DataFrame]:
    """
    Extracts forecast data from Open-Meteo's forecast endpoint. Up to 7–16 days max in the future. 
        Solar energy: temperature_2m, cloudcover, direct_radiation, diffuse_radiation, 
        Wind energy: surface_pressure, wind_speed_10m, wind_direction_10m, wind_speed_100m, wind_direction_100m 
        Hydro energy: precipitation, snow_depth
    """

    # Convert the Timestamps to YYYY-MM-DD (the format Open-Meteo needs)
    start_date_str = start_time.strftime('%Y-%m-%d')
    end_date_str = end_time.strftime('%Y-%m-%d')
    
    latitude, longitude = get_country_center_coordinates(country_code)
    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date_str,      
        "end_date": end_date_str,          
        "hourly": [
            "temperature_2m",
            "surface_pressure",
            "cloudcover",
            "direct_radiation",
            "diffuse_radiation",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_speed_100m",
            "wind_direction_100m",
            "precipitation",
            "snow_depth"
        ],
        "timezone": "Europe/Amsterdam"
    }

    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    # Parse the 'hourly' forecast data
    hourly_data = data.get("hourly", {})
    if not hourly_data:
        print("No forecast data returned for the specified parameters/timeframe.")
        return None

    # Convert to a DataFrame
    df = pd.DataFrame(hourly_data)

    # 'time' is given as an ISO string, so convert to datetime
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

    return df


def extract_weather_data(load_locally:bool = True, daily:bool = False, forecast:bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:     
    """
    Extracts weather data for multiple countries, supporting both historical data and recent daily forecasts.
    """
    if load_locally and not daily: 
        return pre_load_df('weather_data')
    if daily: 
        if forecast: 
            # today's data: 24 hours ahead 
            start_time = pd.Timestamp.today(tz='UTC').normalize()
            end_time = pd.Timestamp.today(tz='UTC').normalize() 
        else:
            # yesterday's data 
            start_time = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1)
            end_time = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1)
        df_NL = extract_weather_forecast('NL', start_time=start_time, end_time=end_time)
        df_BE = extract_weather_forecast('BE', start_time=start_time, end_time=end_time)
        df_DE_LU = extract_weather_forecast('DE_LU', start_time=start_time, end_time=end_time)
        df_DK_1 = extract_weather_forecast('DK_1', start_time=start_time, end_time=end_time)
        df_GB = extract_weather_forecast('GB', start_time=start_time, end_time=end_time)
        df_NO_2 = extract_weather_forecast('NO_2', start_time=start_time, end_time=end_time)
        return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2
    else:
        df_NL = extract_historical_weather_data('NL')
        df_BE = extract_historical_weather_data('BE')
        df_DE_LU = extract_historical_weather_data('DE_LU')
        df_DK_1 = extract_historical_weather_data('DK_1')
        df_GB = extract_historical_weather_data('GB')
        df_NO_2 = extract_historical_weather_data('NO_2')
        return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_price_data(load_locally:bool = True, daily:bool= False, forecast:bool = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    """
    Retrieves day-ahead electricity prices for multiple countries, either from saved local data or API queries.
    """
    if load_locally and not daily: 
        return pre_load_df('day_ahead_prices')
    if daily:
        if forecast: 
            start_time = pd.Timestamp.today(tz='UTC').normalize()
            end_time = pd.Timestamp.today(tz='UTC').normalize() + pd.Timedelta(days=1)
        else: 
            start_time = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1)
            end_time = pd.Timestamp.today(tz='UTC').normalize()
        to_CSV = False
    else: 
        start_time = pd.Timestamp('2019-01-01', tz='UTC').normalize()
        end_time = pd.Timestamp('2025-01-05', tz='UTC').normalize()
        to_CSV = True

    df_NL = extract_day_ahead_price('NL', start_time=start_time, end_time=end_time,to_CSV=to_CSV)
    df_BE = extract_day_ahead_price('BE', start_time=start_time, end_time=end_time, to_CSV=to_CSV)
    df_DE_LU = extract_day_ahead_price('DE_LU', start_time=start_time, end_time=end_time, to_CSV=to_CSV)
    df_DK_1 = extract_day_ahead_price('DK_1', start_time=start_time, end_time=end_time, to_CSV=to_CSV)
    df_GB = extract_day_ahead_price('GB', start_time=start_time, end_time=end_time, to_CSV=to_CSV)
    df_NO_2 = extract_day_ahead_price('NO_2', start_time=start_time, end_time=end_time, to_CSV=to_CSV)
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_energy_generation_data(load_locally = True, daily = False, forecast = False) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    """
    Collects historical energy generation data for multiple countries, with options for daily updates or local file loading.
    """
    if load_locally and not daily: 
        return pre_load_df('energy_generation')
    if daily: 
        if forecast: 
            start_time = pd.Timestamp.today(tz='UTC').normalize() 
            end_time = pd.Timestamp.today(tz='UTC').normalize() + pd.Timedelta(days=1)
        else: 
            start_time = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1)
            end_time = pd.Timestamp.today(tz='UTC').normalize()
        to_CSV = False
    else: 
        start_time = pd.Timestamp('2019-01-01', tz='UTC').normalize()
        end_time = pd.Timestamp('2025-01-05', tz='UTC').normalize()
        to_CSV = True

    df_NL = extract_energy_generation('NL', start_time=start_time, end_time=end_time, to_CSV=to_CSV, forecast=forecast)
    df_BE = extract_energy_generation('BE', start_time=start_time, end_time=end_time, to_CSV=to_CSV, forecast=forecast)
    df_DE_LU = extract_energy_generation('DE_LU', start_time=start_time, end_time=end_time, to_CSV=to_CSV, forecast=forecast)
    df_DK_1 = extract_energy_generation('DK_1', start_time=start_time, end_time=end_time, to_CSV=to_CSV, forecast=forecast)
    df_GB = extract_energy_generation('UK', start_time=start_time, end_time=end_time, to_CSV=to_CSV, forecast=forecast) if forecast else None
    df_NO_2 = extract_energy_generation('NO_2', start_time=start_time, end_time=end_time, to_CSV=to_CSV, forecast=forecast)
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_flow_data(load_locally: bool = True, country_code: str = 'NL', daily: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts physical electricity flow data (import/export) for a specific country, supporting daily updates or historical backfills.
    """
    if load_locally and not daily:
        return pd.read_csv(f'./feature_pipeline/data/{country_code}_import_flow.csv'), pd.read_csv(f'./feature_pipeline/data/{country_code}_export_flow.csv')
    if daily: 
        start_time = pd.Timestamp.today(tz='UTC').normalize() - pd.Timedelta(days=1)
        end_time = pd.Timestamp.today(tz='UTC').normalize()
        to_CSV = False
    else: 
        start_time = pd.Timestamp('2019-01-01', tz='UTC').normalize()
        end_time = pd.Timestamp('2025-01-05', tz='UTC').normalize()
        to_CSV = True

    return extract_physical_flows(country_code=country_code, start_time=start_time, end_time=end_time, to_CSV=to_CSV)


def pre_load_df(path_specific) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Loads pre-existing data from CSV files for multiple countries, supporting both energy generation and general data pipelines.
    """
    if path_specific == 'energy_generation':
        df_NL = pd.read_csv(f'./feature_pipeline/data/NL_{path_specific}.csv', header=[0, 1])
        df_BE = pd.read_csv(f'./feature_pipeline/data/BE_{path_specific}.csv', header=[0, 1])
        df_DE_LU = pd.read_csv(f'./feature_pipeline/data/DE_LU_{path_specific}.csv', header=[0, 1])
    else:
        df_NL = pd.read_csv(f'./feature_pipeline/data/NL_{path_specific}.csv')
        df_BE = pd.read_csv(f'./feature_pipeline/data/BE_{path_specific}.csv')
        df_DE_LU = pd.read_csv(f'./feature_pipeline/data/DE_LU_{path_specific}.csv')
    df_DK_1 = pd.read_csv(f'./feature_pipeline/data/DK_1_{path_specific}.csv')
    df_GB = pd.read_csv(f'./feature_pipeline/data/GB_{path_specific}.csv')
    df_NO_2 = pd.read_csv(f'./feature_pipeline/data/NO_2_{path_specific}.csv')
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_backfill_data():
    """
    Collects all necessary historical data (weather, prices, generation, flows) for multiple countries to backfill a feature pipeline.
    """
    weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2 =  extract_weather_data()
    energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2 = extract_price_data()
    generation_NL, generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2 = extract_energy_generation_data()
    import_flow, export_flow = extract_flow_data()
    return weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2, generation_NL,\
          generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, import_flow, export_flow


def extract_daily_data(forecast = False):
    """
    Fetches daily updates for weather, electricity prices, generation, and flow data, integrating them into a daily feature pipeline.
    """
    weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2 = extract_weather_data(load_locally=False, daily=True, forecast=forecast)
    energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2 = extract_price_data(load_locally=False, daily=True, forecast=forecast)
    generation_NL, generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2 = extract_energy_generation_data(load_locally=False, daily=True, forecast=forecast)
    import_flow, export_flow = extract_flow_data(daily=True) # target, so not possible to forecast 
     
    return weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2, generation_NL,\
          generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, import_flow, export_flow

