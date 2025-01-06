import hopsworks
import pandas as pd
from great_expectations.core import ExpectationSuite
from hsfs.feature_group import FeatureGroup
from utils.settings import ENV_VARS
import great_expectations as ge


def to_feature_store_weather(data: pd.DataFrame,
                             validation_expectation_suite: ExpectationSuite,
                             feature_group_version: int) -> FeatureGroup:
    
    # 1. Connect to Hopsworks
    project = hopsworks.login(api_key_value=ENV_VARS["HOPSWORKS"])
    feature_store = project.get_feature_store()

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
    weather_feature_group.insert(data)

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
            ge.core.ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": col,
                    "value_set": value_set
                }
            )
        )

    def expect_greater_than_zero(col):
        weather_expectation_suite.add_expectation(
            ge.core.ExpectationConfiguration(
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
            ge.core.ExpectationConfiguration(
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
    expect_greater_than_zero("direct_radiation")
    expect_greater_than_zero("diffuse_radiation")
    expect_greater_than_zero("wind_speed_10m")
    expect_greater_than_zero("wind_speed_100m")
    expect_greater_than_zero("surface_pressure")
    expect_greater_than_zero("snow_depth")
    expect_greater_than_zero("cloudcover")
    expect_greater_than_zero("wind_direction_10m")
    expect_greater_than_zero("wind_direction_100m")

    expect_within_range("temperature_2m", -50, 50)

    return weather_expectation_suite
