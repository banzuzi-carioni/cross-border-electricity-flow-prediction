import pandas as pd
from feature_pipeline.ETL import load
from sklearn.metrics import mean_absolute_error
import os
from typing import Tuple
from utils.settings import PREDICTIONS_PATH, MAE_PATH
    

def _load_predictions(csv_path: str = PREDICTIONS_PATH) -> pd.DataFrame:
    '''
    Load the predictions from the predictions.csv file.
    '''
    df = pd.read_csv(csv_path, index_col=0, parse_dates=['datetime'])
    df.loc[df['energy_sent'] < 0, 'energy_sent'] = 0
    return df[['datetime', 'country_from', 'country_to', 'energy_sent', 'energy_price_nl', 'total_generation_nl']]


def _load_model_data(fg_name: str, version: int, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    '''
    Load the model data from the feature store.
    '''
    model_fg = load.retrieve_feature_group(name=fg_name, version=version)
    model_data_df = model_fg.filter(
        (model_fg.datetime > start) & 
        (model_fg.datetime < end)
    ).read()
    return model_data_df[['datetime', 'country_from', 'country_to', 'energy_sent', 'energy_price_nl', 'total_generation_nl']]


def _filter_and_process_data(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Filter the DataFrame to include only flows to/from the Netherlands and create a new column to indicate the direction of flow.
    '''
    filtered_df = df[(df['country_from'] == 'NL') | (df['country_to'] == 'NL')].copy()
    filtered_df['flow_direction'] = filtered_df.apply(
        lambda row: 'Export' if row['country_from'] == 'NL' else 'Import',
        axis=1
    )
    return filtered_df.sort_values(by='datetime', ascending=True)


def _compute_mae(merged_df_import: pd.DataFrame, merged_df_export: pd.DataFrame) -> Tuple[float, float]:
    '''
    Compute the Mean Absolute Error for import and export flows.
    '''
    mae_import = mean_absolute_error(merged_df_import['energy_sent_x'], merged_df_import['energy_sent_y'])
    mae_export = mean_absolute_error(merged_df_export['energy_sent_x'], merged_df_export['energy_sent_y'])
    return mae_import, mae_export


def save_metrics_to_csv(date: pd.Timestamp, mae_import: float, mae_export: float, csv_file: str) -> None:
    '''
    Save the MAE results to a CSV file.
    '''
    result = {
        'date': date,
        'mae_import': mae_import,
        'mae_export': mae_export
    }
    result_df = pd.DataFrame([result])
    if os.path.exists(csv_file):
        result_df.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        result_df.to_csv(csv_file, mode='w', header=True, index=False)
    print(f"MAE results saved to {csv_file}")


def get_monitoring_metrics(fg_name: str = 'model_data', version: int = 1, csv_file: str = MAE_PATH) -> Tuple[float, float]:
    '''
    Get the monitoring metrics for the daily inference pipeline.
    '''
    yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    start = yesterday.replace(hour=23, minute=0, second=0, microsecond=0) - pd.Timedelta(days=1)
    end = pd.Timestamp.today().normalize()

    # Load and process model data
    model_data_df = _load_model_data(fg_name, version, start, end)
    model_filtered_df = _filter_and_process_data(model_data_df)

    model_filtered_df_import = model_filtered_df[model_filtered_df['flow_direction'] == 'Import']
    model_filtered_df_export = model_filtered_df[model_filtered_df['flow_direction'] == 'Export']
    
    # Load and process predictions
    predictions_df = _load_predictions()
    filtered_df = _filter_and_process_data(predictions_df)

    filtered_df_import = filtered_df[filtered_df['flow_direction'] == 'Import']
    filtered_df_export = filtered_df[filtered_df['flow_direction'] == 'Export']

    # Merge dataframes
    merged_df_import = pd.merge(filtered_df_import, model_filtered_df_import, on='datetime', how='inner')
    merged_df_export = pd.merge(filtered_df_export, model_filtered_df_export, on='datetime', how='inner')

    # Compute and save metrics to CSV
    mae_import, mae_export = _compute_mae(merged_df_import, merged_df_export)
    first_date = filtered_df['datetime'].iloc[0]
    save_metrics_to_csv(first_date, mae_import, mae_export, csv_file)
    return mae_import, mae_export
