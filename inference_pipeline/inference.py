import argparse
from feature_pipeline.ETL import load
from feature_pipeline.pipeline import daily_forecast_run
from utils import utils
import pandas as pd
from xgboost import XGBRegressor
from utils.data import prepare_data_for_predictions, add_country_codes_for_prediction
from inference_pipeline.monitoring import get_monitoring_metrics
from utils.settings import PREDICTIONS_PATH



def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--version', '-v', type=int, default=1, help='Version for the feature groups.')
    return parser


def daily_inference(version: int = 1) -> None:
    """
    A daily inference pipeline that, given the most recent day's data, predicts the energy_sent for each country pair.
    """
    print("Starting daily inference pipeline...")
    feature_store = load.get_feature_store()
    model_dir = utils.get_model_registry()

    model = XGBRegressor()
    model.load_model(model_dir + '/model.json')

    batch_data = daily_forecast_run(version=version)
    batch_data_datetime = add_country_codes_for_prediction(batch_data)
    batch_data = prepare_data_for_predictions(batch_data_datetime)

    predictions = model.predict(batch_data)

    mae_import, mae_export = get_monitoring_metrics(version=version)
    print(f"Mean Absolute Error for import flows yesterday: {mae_import}")
    print(f"Mean Absolute Error for export flows yesterday: {mae_export}")

    batch_data_datetime['energy_sent'] = predictions
    batch_data_datetime.to_csv(PREDICTIONS_PATH)
    
    # save predictions to monitoring system
    monitor_fg = feature_store.get_or_create_feature_group(
        name='predictions',
        description='Predictions for cross-border electricity dataset with energy_sent as the target, combining pivoted multi-country weather, generation, and energy price features for each timestamp, along with country_from and country_to.',
        version=version,
        primary_key=['datetime', 'country_from', 'country_to'],
        event_time="datetime"
    )

    load.insert_data_to_fg(batch_data_datetime, monitor_fg)
    print("Daily inference pipeline run complete.")


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    daily_inference(args.version)
