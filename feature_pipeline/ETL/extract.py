import pandas as pd
from entsoe import EntsoePandasClient
from typing import Tuple
from utils.settings import ENV_VARS


# ENTSOE: Physical Flows and energy generation
def extract_physical_flows_to_csv(country_code: str, 
                                  start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Stockholm'), 
                                  end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Stockholm')) -> None:
    
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    import_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=False, per_hour=True)
    export_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=True, per_hour=True)

    import_data.to_csv(f'../data/{country_code}_import_flow.csv')
    export_data.to_csv(f'../data/{country_code}_export_flow.csv')
    return


def extract_energy_generation_to_csv(country_code: str, 
                                     start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Stockholm'), 
                                     end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Stockholm')) -> None:
    
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    generation_data = client.query_generation(country_code, start_time, end_time, psr_type=None)
    generation_data.to_csv(f'../data/{country_code}_energy_generation.csv')
    return


def extract_physical_flows(country_code: str, 
                           start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Stockholm'), 
                           end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Stockholm')) -> Tuple[pd.DataFrame, pd.DataFrame]:
    
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    import_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=False, per_hour=True)
    export_data = client.query_physical_crossborder_allborders(country_code, start_time, end_time, export=True, per_hour=True)
    return import_data, export_data


def extract_energy_generation(country_code: str, 
                              start_time: pd.Timestamp = pd.Timestamp('2019-01-01', tz='Europe/Stockholm'), 
                              end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Stockholm')) -> pd.DataFrame:
    
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    generation_data = client.query_generation(country_code, start_time, end_time, psr_type=None)
    return generation_data


def extract_energy_generation_forecasts(country_code: str, 
                                        start_time: pd.Timestamp = pd.Timestamp.today(tz='Europe/Stockholm').normalize(), 
                                        end_time: pd.Timestamp = pd.Timestamp('2025-01-05', tz='Europe/Stockholm')) -> pd.DataFrame:
    # seems to be only available for the next day
    client = EntsoePandasClient(api_key=ENV_VARS['EntsoePandasClient'])
    generation_forecasts = client.query_generation_forecast(country_code, start_time, end_time)
    return generation_forecasts


# Weather data 
