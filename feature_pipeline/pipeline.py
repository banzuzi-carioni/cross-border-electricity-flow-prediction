from ETL import extract, load, transform 
import pandas as pd

# BACKFILL 
"""
A ETL pipeline for weather data from multiple countries.
1) Extracts historical weather data from Open-Meteo for each country.
2) Transforms/cleans the DataFrames into a single DataFrame.
3) Loads the combined DataFrame into the Hopsworks Feature Store.
"""

def run(version):
    # -------------------- EXTRACT --------------------
    weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2, generation_NL,\
        generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, import_flow, export_flow = extract_data()    

    # -------------------- TRANSFORM --------------------
    df_weather = transform.transform_weather_data(weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2)
    df_prices = transform.transform_day_ahead_prices(energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2)
    df_generation = transform.transform_generation_data(generation_NL, generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2)
    df_prices_generation = transform.transform_prices_generation(df_prices, df_generation)
    df_flow = transform.merge_export_import(import_flow, export_flow)

    # -------------------- LOAD --------------------
    weather_expectation_suite = load.create_weather_validation_suite()
    generation_prices_expectation_suite = load.create_prices_generation_validation_suite()
    flow_expectation_suite = load.create_physical_flow_validation_suite()
    
    # Insert data into the feature store
    load.to_feature_store_weather(df_weather, weather_expectation_suite, version)
    load.to_feature_store_prices_generation(df_prices_generation, generation_prices_expectation_suite, version) 
    load.to_feature_store_physical_flow(df_flow, flow_expectation_suite, version)


def extract_data():
    weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2 =  extract.extract_weather_data()
    energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2 = extract.extract_price_data()
    generation_NL, generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2 = extract.extract_energy_generation_data()
    import_flow, export_flow = extract.extract_flow_data()
    return weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2, generation_NL,\
          generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, import_flow, export_flow

if __name__ == "__main__":
    version = 1
    run(version)
