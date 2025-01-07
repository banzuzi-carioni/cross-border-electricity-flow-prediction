import hopsworks
import pandas as pd
from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from hsfs.feature_group import FeatureGroup
from utils.settings import ENV_VARS
import great_expectations as ge


def to_feature_store_weather(
    data: pd.DataFrame,
    validation_expectation_suite: ExpectationSuite,
    feature_group_version: int
) -> FeatureGroup:
    # 1. Connect to Hopsworks
    feature_store = get_feature_store()

    # 2. Create (or retrieve) the Feature Group using 'datetime' as primary_key and event_time.
    weather_feature_group = feature_store.get_or_create_feature_group(
        name="weather_open_meteo",
        version=feature_group_version,
        description="Hourly weather data from Open-Meteo. Covers solar, wind, and hydro-related variables.",
        primary_key=['datetime', 'country_code'],
        event_time="datetime",
        expectation_suite=validation_expectation_suite,
    )

    # 3. Upload data
    insert_data_to_fg(data, weather_feature_group)

    # 4. Add feature descriptions
    feature_descriptions = [
        {
            "name": "datetime",
            "description": (
                "UTC timestamp for the hour at which the weather data applies."
            )
        },
        {
            "name": "country_code",
            "description": (
                "Country code where NL:Netherlands, BE:Belgium, NO_2:Norway, DK_1:Denmark, DE_LU:Germany/Luxembourg, GB:United Kingdom. "
            )
        },
       
        # -------------------- Solar variables --------------------
        {
            "name": "direct_radiation",
            "description": (
                "Direct (beam) solar radiation in W/m² reaching the surface."
            )
        },
        {
            "name": "diffuse_radiation",
            "description": (
                "Diffuse solar radiation in W/m² (light scattered by the atmosphere)."
            )
        },
        {
            "name": "cloudcover",
            "description": (
                "Cloud cover in %, indicating fraction of sky covered by clouds."
            )
        },
        {
            "name": "temperature_2m",
            "description": (
                "Air temperature at 2m above ground in °C."
            )
        },
        # -------------------- Wind variables --------------------
        {
            "name": "wind_speed_10m",
            "description": (
                "Wind speed at 10m height in m/s."
            )
        },
        {
            "name": "wind_speed_100m",
            "description": (
                "Wind speed at 100m height in m/s."
            )
        },
        {
            "name": "wind_direction_10m",
            "description": (
                "Wind direction at 10m in degrees (0 = North, 90 = East, etc.)."
            )
        },
        {
            "name": "wind_direction_100m",
            "description": (
                "Wind direction at 100m in degrees (0 = North, 90 = East, etc.)."
            )
        },
        {
            "name": "surface_pressure",
            "description": (
                "Surface air pressure in hPa (hectopascals)."
            )
        },
        # -------------------- Hydro variables --------------------
        {
            "name": "precipitation",
            "description": (
                "Hourly precipitation in mm (includes rainfall and equivalent snowfall)."
            )
        },
        {
            "name": "snow_depth",
            "description": (
                "Snow depth on the ground in cm."
            )
        },
    ]

    # 5. Update feature descriptions
    for desc in feature_descriptions:
        # Only update if the column actually exists in the DataFrame
        if desc["name"] in data.columns:
            weather_feature_group.update_feature_description(
                desc["name"], desc["description"]
            )

    return weather_feature_group


def create_weather_validation_suite():
    weather_expectation_suite = ge.core.ExpectationSuite(expectation_suite_name="weather_expectation_suite")

    def expect_within_value_set(col, value_set):
        weather_expectation_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": col,
                    "value_set": value_set
                }
            )
        )

    def expect_greater_than_zero(col):
        weather_expectation_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_min_to_be_between",
                kwargs={
                    "column": col,
                    "min_value": -0.1,
                    "max_value": None,
                    "strict_min": True
                }
            )
        )

    def expect_within_range(col, min_value, max_value):
        weather_expectation_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": col,
                    "min_value": min_value,
                    "max_value": max_value
                }
            )
        )

    # Add expectations for the weather data
    expect_within_value_set("country_code", ["NL", "BE", "DE_LU", "DK_1", "GB", "NO_2"])

    expect_greater_than_zero("precipitation")
    expect_greater_than_zero("wind_speed_10m")
    expect_greater_than_zero("wind_speed_100m")
    expect_greater_than_zero("surface_pressure")
    expect_greater_than_zero("snow_depth")
    expect_greater_than_zero("cloudcover")
    expect_greater_than_zero("wind_direction_10m")
    expect_greater_than_zero("wind_direction_100m")

    expect_within_range("temperature_2m", -50, 50)

    return weather_expectation_suite


def create_prices_generation_validation_suite() -> ExpectationSuite:
    """
    Creates a Great Expectations validation suite for day-ahead prices 
    plus energy generation data.
    
    Ensures:
    1) country_code is one of the allowed codes
    2) Generation columns (e.g., biomass, fossil_gas, solar, etc.) >= 0
    3) Note that energy prices can be negative, so a rule for the variable is not enforeced. 
    """
    prices_generation_suite = ge.core.ExpectationSuite(expectation_suite_name="prices_generation_suite")

    def expect_within_value_set(col, value_set):
        prices_generation_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": col,
                    "value_set": value_set
                }
            )
        )

    def expect_at_least_zero(col):
        prices_generation_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_min_to_be_between",
                kwargs={
                    "column": col,
                    "min_value": -0.1,
                    "max_value": None,
                    "strict_min": False
                }
            )
        )

    # 1. country_code must be one of these
    expect_within_value_set("country_code", ["NL", "BE", "DE_LU", "DK_1", "GB", "NO_2"])

    # 2. generation columns >= 0
    generation_columns = [
        "biomass",
        "fossil_gas",
        "fossil_hard_coal",
        "hydro_run_of_river_and_poundage",
        "nuclear",
        "other",
        "solar",
        "wind_offshore",
        "wind_onshore",
        "total_generation",
        "fossil_oil",
        "hydro_pumped_storage",
        "fossil_brown_coal_lignite",
        "fossil_coal_derived_gas",
        "geothermal",
        "hydro_water_reservoir",
        "other_renewable"
    ]

    for col in generation_columns:
        expect_at_least_zero(col)

    return prices_generation_suite


def to_feature_store_prices_generation(
    data: pd.DataFrame,
    validation_expectation_suite: ExpectationSuite,
    feature_group_version: int
) -> FeatureGroup:

    # 1. Connect to Hopsworks
    feature_store = get_feature_store()

    # 2. Create (or retrieve) the Feature Group
    prices_generation_fg = feature_store.get_or_create_feature_group(
        name="prices_generation",
        version=feature_group_version,
        description=(
            "Combined day-ahead electricity prices and energy generation data. Includes multiple fuel types like biomass, gas, coal, wind, solar, etc."
        ),
        primary_key=["datetime", "country_code"],
        event_time="datetime",
        expectation_suite=validation_expectation_suite,
    )

    # 3. Insert the data
    insert_data_to_fg(data, prices_generation_fg)
    

    # 4. Define feature descriptions
    feature_descriptions = [
        {
            "name": "datetime",
            "description": (
                "UTC timestamp for the hour at which the price/generation data applies."
            )
        },
        {
            "name": "country_code",
            "description": (
                "Country code. BE:Belgium, NO_2:Norway, DK_1:Denmark, DE_LU:Germany/Luxembourg, GB:United Kingdom."
            )
        },
        {
            "name": "energy_price",
            "description": (
                "Day-ahead electricity price in EUR/MWh."
            )
        },
        {
            "name": "biomass",
            "description": (
                "Biomass generation in MWh."
            )
        },
        {
            "name": "fossil_gas",
            "description": (
                "Fossil gas generation in MWh."
            )  
        },
        {
            "name": "solar",
            "description": (
                "Solar generation in MWh."
            )
        },
        {
            "name": "wind_onshore",
            "description": (
                "Onshore wind generation in MWh."
            )
        },
        {
            "name": "wind_offshore",
            "description": (
                "Offshore wind generation in MWh."
            )
        },
        {
            "name": "total_generation",
            "description": (
                "Total power generation from all sources in MWh."
            )
        },
        {
        "name": "fossil_oil",
        "description": (
            "Electricity generation from fossil oil in MWh."
        )
        },
        {
            "name": "hydro_pumped_storage",
            "description": (
                "Electricity generation from pumped-storage hydropower in MWh "
                "(when in generating mode)."
            )
        },
        {
            "name": "fossil_brown_coal_lignite",
            "description": "Electricity generation from brown coal / lignite in MWh."
        },
        {
            "name": "fossil_coal_derived_gas",
            "description": "Electricity generation from coal-derived gas in MWh."
        },
        {
            "name": "geothermal",
            "description": "Electricity generation from geothermal sources in MWh."
        },
        {
            "name": "hydro_water_reservoir",
            "description": "Electricity generation from water reservoir hydropower in MWh."
        },
        {
            "name": "other_renewable",
            "description": "Electricity generation from other renewable sources in MWh."
        },
    ]

    # 5. Update feature descriptions in Hopsworks
    for desc in feature_descriptions:
        # Only update if the column actually exists in the DataFrame
        if desc["name"] in data.columns:
            prices_generation_fg.update_feature_description(
                desc["name"], desc["description"]
            )
    return prices_generation_fg


def create_physical_flow_validation_suite() -> ExpectationSuite:

    physical_flow_suite = ge.core.ExpectationSuite(expectation_suite_name="import_export_suite")

    def expect_within_value_set(col, value_set):
        physical_flow_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": col,
                    "value_set": value_set
                }
            )
        )

    def expect_at_least_zero(col):
        physical_flow_suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_min_to_be_between",
                kwargs={
                    "column": col,
                    "min_value": -0.1,
                    "max_value": None,
                    "strict_min": False
                }
            )
        )

    # 1. countries must be one of these
    expect_within_value_set("country_from", ["NL", "BE", "DE_LU", "DK_1", "GB", "NO_2"])
    expect_within_value_set("country_to", ["NL", "BE", "DE_LU", "DK_1", "GB", "NO_2"])


    # 2. energy sent >= 0
    expect_at_least_zero('energy_sent')

    return physical_flow_suite


def to_feature_store_physical_flow(
    data: pd.DataFrame,
    validation_expectation_suite: ExpectationSuite,
    feature_group_version: int
) -> FeatureGroup:

    # 1. Connect to Hopsworks
    feature_store = get_feature_store()

    # 2. Create (or retrieve) the Feature Group
    physical_flow_fg = feature_store.get_or_create_feature_group(
        name="physical_flow",
        version=feature_group_version,
        description=(
            "Hourly cross-border physical electricity flows. Indicates how much electricity (in MW) was physically sent from 'country_from' to 'country_to' at a given time."
      
        ),
        primary_key=["datetime", "country_from", "country_to"],
        event_time="datetime",
        expectation_suite=validation_expectation_suite,
    )

    # 3. Insert the data
    insert_data_to_fg(data, physical_flow_fg)

    # 4. Define feature descriptions
    feature_descriptions = [
        {
            "name": "datetime",
            "description": (
                "UTC timestamp for the hour at which the price/generation data applies."
            )
        },
        {
            "name": "country_from",
            "description": (
                "The country code from which electricity is physically flowing, where NL:Netherlands, BE:Belgium, NO_2:Norway, DK_1:Denmark, DE_LU:Germany/Luxembourg, GB:United Kingdom."
            )
        },
        {
            "name": "country_to",
            "description": (
                "The country code to which electricity is physically flowing, where NL:Netherlands, BE:Belgium, NO_2:Norway, DK_1:Denmark, DE_LU:Germany/Luxembourg, GB:United Kingdom."
            )
        },
        {
            "name": "energy_sent",
            "description": (
                "Volume of electricity physically sent (in MW) for that hour."
            )
        },
    ]

    # 5. Update feature descriptions in Hopsworks
    for desc in feature_descriptions:
        # Only update if the column actually exists in the DataFrame
        if desc["name"] in data.columns:
            physical_flow_fg.update_feature_description(
                desc["name"], desc["description"]
            )
    return physical_flow_fg


def to_feature_store_model_data(
    data: pd.DataFrame,
    feature_group_version: int
) -> FeatureGroup:
    # 1. Connect to Hopsworks
    feature_store = get_feature_store()

    # 2. Create (or retrieve) the Feature Group
    model_data_fg = feature_store.get_or_create_feature_group(
        name="model_data",
        version=feature_group_version,
        description=(
            'Cross-border electricity dataset with energy_sent as the target, combining pivoted multi-country weather, generation, and energy price features for each timestamp, along with country_from and country_to.'
      
        ),
        primary_key=["datetime", "country_from", "country_to"],
        event_time="datetime",
    )

    # 3. Insert the data
    insert_data_to_fg(data, model_data_fg)
    return model_data_fg


def retrieve_feature_group(name: str, version: int):
    # 1. Connect to Hopsworks
    feature_store = get_feature_store()

    # 2. Retrieve the Feature Group
    feature_group = feature_store.get_feature_group(
        name=name,
        version=version
    )
    return feature_group


def get_feature_store(): 
    project = hopsworks.login(api_key_value=ENV_VARS["HOPSWORKS"], project='cross_border_electricity')
    feature_store = project.get_feature_store()
    return feature_store


def insert_data_to_fg(data, fg):
    fg.insert(data, write_options={"wait_for_job": True})