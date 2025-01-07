import pandas as pd
from entsoe import EntsoePandasClient
from typing import Tuple, Optional
from utils.settings import ENV_VARS
from utils.utils import get_country_center_coordinates
import requests


def extract_day_ahead_price(country_code: str,
                           start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Amsterdam'), 
                            end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Amsterdam'),
                            to_CSV: bool = True) -> Optional[pd.DataFrame]:
    '''
    Extracts day-ahead electricity prices from ENTSO-E API for a given country code.
    '''
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    day_ahead_prices = client.query_day_ahead_prices('NL', start_time, end_time)
    
    # convert the series to a DataFrame
    day_ahead_prices_df = day_ahead_prices.reset_index()
    day_ahead_prices_df.columns = ['Timestamp', 'Price']
    
    if to_CSV:
        day_ahead_prices_df.to_csv(f'../data/{country_code}_day_ahead_prices.csv')
        print(f'Day ahead prices successfully saved for {country_code}.')
        return 
    
    return day_ahead_prices_df


# Historical 
def extract_physical_flows(country_code: str = 'NL', 
                           start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Amsterdam'), 
                           end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Amsterdam'),
                           to_CSV: bool = True) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    '''
    Extracts physical electricity flows from ENTSO-E API for a given country code
    '''
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    import_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=False, per_hour=True)
    export_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=True, per_hour=True)
    
    if to_CSV:
        import_data.to_csv(f'../data/{country_code}_import_flow.csv')
        export_data.to_csv(f'../data/{country_code}_export_flow.csv')
        print(f'Import and export physcial flows successfully saved for {country_code}.')
        return 
    
    return import_data, export_data


def extract_energy_generation(country_code: str, 
                              start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Amsterdam'), 
                              end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Amsterdam'),
                              to_CSV: bool = True) -> Optional[pd.DataFrame]:
    '''
    Extracts energy generation data from ENTSO-E API for a given country code
    '''
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    generation_data = client.query_generation(country_code, start_time, end_time, psr_type=None)

    if to_CSV:
        generation_data.to_csv(f'../data/{country_code}_energy_generation.csv')
        print(f'Energy generation successfully saved for {country_code}.')
        return 
    return generation_data


def extract_historical_weather_data(country_code: str,
                         start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Amsterdam'), 
                         end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Amsterdam'),
                         to_CSV: bool = True) -> pd.DataFrame:
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
        csv_path = f"../data/{country_code}_weather_data.csv"
        df.to_csv(csv_path)
        print(f"Weather data successfully saved to {csv_path}")
        return None

    return df


# Forecasts 
def extract_energy_generation_forecasts(country_code: str, 
                                        start_time: pd.Timestamp = pd.Timestamp.today(tz='Europe/Amsterdam').normalize(), 
                                        end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Amsterdam')) -> pd.DataFrame:
    '''
    Extracts future forecast data from ENTSO-E API for a given country code 
    '''
    # seems to be only available for the next day
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    generation_forecasts = client.query_generation_forecast(country_code, start_time, end_time)
    return generation_forecasts


def extract_weather_forecast(country_code: str,
                             start_time: pd.Timestamp = pd.Timestamp.today(tz='Europe/Amsterdam').normalize(),
                             end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Amsterdam')) -> Optional[pd.DataFrame]:
    """
    Extracts future forecast data (hourly) from Open-Meteo's forecast endpoint. Up to 7–16 days max in the future. 
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


# Multiple countries 
def extract_weather_data(load_locally = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    if load_locally: 
        return pre_load_df('energy_generation')
    df_NL = extract_historical_weather_data('NL')
    df_BE = extract_historical_weather_data('BE')
    df_DE_LU = extract_historical_weather_data('DE_LU')
    df_DK_1 = extract_historical_weather_data('DK_1')
    df_GB = extract_historical_weather_data('GB')
    df_NO_2 = extract_historical_weather_data('NO_2')
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_price_data(load_locally = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    if load_locally: 
        # TODO: Belgium doesnt exist 
        return pre_load_df('day_ahead_prices')
    df_NL = extract_day_ahead_price('NL')
    df_BE = extract_day_ahead_price('BE') # TODO: this gives error timeout! 
    df_DE_LU = extract_day_ahead_price('DE_LU')
    df_DK_1 = extract_day_ahead_price('DK_1')
    df_GB = extract_day_ahead_price('GB')
    df_NO_2 = extract_day_ahead_price('NO_2')
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_energy_generation_data(load_locally = True) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: 
    if load_locally: 
        return pre_load_df('energy_generation')
    df_NL = extract_energy_generation('NL')
    df_BE = extract_energy_generation('BE')
    df_DE_LU = extract_energy_generation('DE_LU')
    df_DK_1 = extract_energy_generation('DK_1')
    df_GB = extract_energy_generation('GB')
    df_NO_2 = extract_energy_generation('NO_2')
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2


def extract_flow_data(load_locally: bool = True, country_code: str = 'NL') -> Tuple[pd.DataFrame, pd.DataFrame]:
    if load_locally:
        return pd.read_csvf('../feature_pipeline/data/{country_code}_import_flow.csv'), pd.read_csv('../feature_pipeline/data/{country_code}_export_flow.csv')
    else:
        return extract_physical_flows(country_code=country_code)


def pre_load_df(path_specific) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if path_specific == 'energy_generation':
        df_NL = pd.read_csv(f'../feature_pipeline/data/NL_{path_specific}.csv', header=[0, 1]),
        df_BE = pd.read_csv(f'../feature_pipeline/data/BE_{path_specific}.csv', header=[0, 1]),
        df_DE_LU = pd.read_csv(f'../feature_pipeline/data/DE_LU_{path_specific}.csv', header=[0, 1])
    else:
        df_NL = pd.read_csv(f'../feature_pipeline/data/NL_{path_specific}.csv')
        df_BE = pd.read_csv(f'../feature_pipeline/data/BE_{path_specific}.csv')
        df_DE_LU = pd.read_csv(f'../feature_pipeline/data/DE_LU_{path_specific}.csv')
    df_DK_1 = pd.read_csv(f'../feature_pipeline/data/DK_1_{path_specific}.csv')
    df_GB = pd.read_csv(f'../feature_pipeline/data/GB_{path_specific}.csv')
    df_NO_2 = pd.read_csv(f'../feature_pipeline/data/NO_2_{path_specific}.csv')
    return df_NL, df_BE, df_DE_LU, df_DK_1, df_GB, df_NO_2
