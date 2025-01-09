import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
from feature_pipeline.ETL import load
from sklearn.metrics import mean_absolute_error
import os
from typing import Tuple
from utils.settings import PREDICTIONS_PATH, MAE_PATH


def plot_hourly_data(df: pd.DataFrame, df2: pd.DataFrame, file_path: str = None) -> None:
    
    # Ensure datetime is parsed correctly
    df['hour'] = df['datetime'].dt.hour  # Extract hour from datetime
    
    # Group data by hour
    hourly_data = df  # Use mean; can adjust as needed
    hourly_data.set_index('hour', inplace=True)
    
    df2['hour'] = df2['datetime'].dt.hour  # Extract hour from datetime

    # Group data by hour
    hourly_data2 = df2  # Adjust if needed
    hourly_data2.set_index('hour', inplace=True)

    # Plotting
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot data
    ax.plot(hourly_data.index, hourly_data['energy_sent'], label='predictions', color='red', linewidth=2, marker='o')
    ax.plot(hourly_data2.index, hourly_data2['energy_sent'], label='real', color='black', linewidth=2, marker='^')
    
    # Customize the plot
    ax.set_xlabel('Hour of Day', fontsize=12)
    ax.set_ylabel('Values', fontsize=12)
    ax.set_title('Hourly Energy Data', fontsize=14)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.7)
    ax.xaxis.set_major_locator(MultipleLocator(1))  # Ensure x-axis shows every hour
    ax.legend(fontsize=10)
    
    # Save and return the plot
    plt.tight_layout()
    plt.xlim(-0.25, 23)
    if file_path:
        plt.savefig(file_path)
    plt.show()
    

def load_predictions(csv_path: str = PREDICTIONS_PATH) -> pd.DataFrame:
    df = pd.read_csv(csv_path, index_col=0, parse_dates=['datetime'])
    df.loc[df['energy_sent'] < 0, 'energy_sent'] = 0
    return df[['datetime', 'country_from', 'country_to', 'energy_sent', 'energy_price_nl', 'total_generation_nl']]


def get_monitoring_metrics(fg_name: str = 'model_data', version: int = 1, csv_file: str = MAE_PATH) -> Tuple[float, float]:
    """
    Generates monitoring plots for the hourly energy data.
    """
    yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    start = yesterday.replace(hour=23, minute=0, second=0, microsecond=0) - pd.Timedelta(days=1)

    model_fg = load.retrieve_feature_group(name=fg_name, version=version)
    model_data_df = model_fg.filter(
        (model_fg.datetime > start) & 
        (model_fg.datetime < pd.Timestamp.today().normalize())
    ).read()
    model_data_df = model_data_df[['datetime', 'country_from', 'country_to', 'energy_sent', 'energy_price_nl', 'total_generation_nl']]

    # Filter the DataFrame to include only flows to/from the Netherlands
    model_filtered_df = model_data_df[
        (model_data_df['country_from'] == 'NL') | (model_data_df['country_to'] == 'NL')
    ].copy()

    # Create a new column to indicate the direction of flow
    model_filtered_df['flow_direction'] = model_filtered_df.apply(
        lambda row: 'Export' if row['country_from'] == 'NL' else 'Import',
        axis=1
    )
    model_filtered_df = model_filtered_df.sort_values(by='datetime', ascending=True)

    model_filtered_df_import = model_filtered_df[model_filtered_df['flow_direction'] == 'Import']
    model_filtered_df_export = model_filtered_df[model_filtered_df['flow_direction'] == 'Export']

    # Filter the DataFrame to include only flows to/from the Netherlands
    predictions_df = load_predictions()
    filtered_df = predictions_df[
        (predictions_df['country_from'] == 'NL') | (predictions_df['country_to'] == 'NL')
    ].copy()

    filtered_df['flow_direction'] = filtered_df.apply(
        lambda row: 'Export' if row['country_from'] == 'NL' else 'Import',
        axis=1
    )

    filtered_df_import = filtered_df[filtered_df['flow_direction'] == 'Import']
    filtered_df_export = filtered_df[filtered_df['flow_direction'] == 'Export']

    merged_df_import = pd.merge(filtered_df_import, model_filtered_df_import, on='datetime', how='inner')
    merged_df_export = pd.merge(filtered_df_export, model_filtered_df_export, on='datetime', how='inner')

    mae_import = mean_absolute_error(merged_df_import['energy_sent_x'], merged_df_import['energy_sent_y'])
    mae_export = mean_absolute_error(merged_df_export['energy_sent_x'], merged_df_export['energy_sent_y'])
    
    # Create a new row with the date and MAE values
    first_date = filtered_df['datetime'].iloc[0]
    result = {
        'date': first_date,
        'mae_import': mae_import,
        'mae_export': mae_export
    }

    result_df = pd.DataFrame([result])
    # Check if the CSV file exists
    if os.path.exists(csv_file):
        # If file exists, append to the file without writing header
        result_df.to_csv(csv_file, mode='a', header=False, index=False)
    else:
        # If file doesn't exist, create the file and write the header
        result_df.to_csv(csv_file, mode='w', header=True, index=False)

    print(f"MAE results saved to {csv_file}")
    return mae_import, mae_export
