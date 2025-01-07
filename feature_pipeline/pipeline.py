import argparse
from ETL import extract, load, transform 


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-v', type=int, default=1, help='Version for the feature groups.')
    # Mutually exclusive group: backfill (-b) or daily (-d) is required
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--backfill', '-b', action='store_true', help='Run backfill feature pipeline.')
    group.add_argument('--daily', '-d', action='store_true', help='Run daily feature pipeline.')
    return parser


def backfill_run(version: int = 1) -> None:
    """
    A ETL pipeline for weather data from multiple countries.
    1) Extracts historical weather data from Open-Meteo for each country.
    2) Transforms/cleans the DataFrames into a single DataFrame.
    3) Loads the combined DataFrame into the Hopsworks Feature Store.
    """
    print("Starting backfill feature pipeline...")

    # -------------------- EXTRACT --------------------
    weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2, generation_NL,\
        generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, import_flow, export_flow = extract.extract_backfill_data()    

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

    print("Backfill feature pipeline run complete.")


def daily_run(version: int = 1) -> None:
    """
    A smaller-scale ETL pipeline for daily updates:
    1) Extracts the most recent day's weather, prices, and generation data.
    2) Transforms them into consistent DataFrames.
    3) Loads/appends them into the same feature store groups as the backfill (same version).
    """
    print("Starting daily feature pipeline...")

    # -------------------- EXTRACT --------------------
    # Example: these could be partial or near real-time extracts for the "current" day
    weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, \
        energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2, \
        generation_NL, generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, \
        import_flow, export_flow = extract.extract_daily_data()

    # -------------------- TRANSFORM --------------------
    df_weather = transform.transform_weather_data(
        weather_NL, weather_BE, weather_DE_LU, weather_DK_1, weather_GB, weather_NO_2, from_api= True
    )
    df_prices = transform.transform_day_ahead_prices(
        energy_price_NL, energy_price_BE, energy_price_DE_LU, energy_price_DK_1, energy_price_GB, energy_price_NO_2
    )
    df_generation = transform.transform_generation_data(
        generation_NL, generation_BE, generation_DE_LU, generation_DK_1, generation_GB, generation_NO_2, True
    )
    df_prices_generation = transform.transform_prices_generation(df_prices, df_generation)
    df_flow = transform.merge_export_import(import_flow, export_flow, True)

    # -------------------- LOAD --------------------
    # Retrieve feature group
    weather_fg = load.retrieve_feature_group(name='weather_open_meteo', version=version)
    prices_generation_fg = load.retrieve_feature_group(name='prices_generation', version=version)
    physical_flow_fg = load.retrieve_feature_group(name='physical_flow', version=version)
   
    # Insert data into the feature store
    load.insert_data_to_fg(df_weather, weather_fg)
    load.insert_data_to_fg(df_prices_generation, prices_generation_fg)
    load.insert_data_to_fg(df_flow, physical_flow_fg)

    print("Daily feature pipeline run complete.")


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    version = args.version
    if args.backfill:
        backfill_run(args.version)
    else:
        daily_run(args.version)
