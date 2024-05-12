from datetime import date, timedelta
from pathlib import Path
import os

import pandas as pd
import pendulum

from airflow.decorators import dag, task
from typing import TypeAlias

Table: TypeAlias = pd.DataFrame

@task(task_id="load", execution_timeout=timedelta(minutes=5))
def load_activity_table(profit_table: Table, csv_target: Path = Path("data/flags_activity.csv")) -> None:
    """Loads the DataFrame with activity flags into a CSV file.

    Args:
        profit_table (Table): DataFrame with activity flags.
        csv_target (Path): Path to the target CSV file.
    """
    if os.stat(csv_target).st_size == 0:
        profit_table.to_csv(csv_target, index=False)
    else:
        profit_table.to_csv(csv_target, mode='a', header=False, index=False)

@task(task_id="extract", execution_timeout=timedelta(minutes=5))
def extract_profit_table(csv_source: Path = Path("data/profit_table.csv")) -> Table:
    """Extracts data from a CSV file into a DataFrame.

    Args:
        csv_source (Path): Path to the CSV file containing the data.

    Returns:
        Table: DataFrame loaded with the CSV data.
    """
    return pd.read_csv(csv_source)

@task(task_id="transform", execution_timeout=timedelta(minutes=5), retries=5)
def transform_profit_table(profit_table: Table, current_date: date) -> Table:
    """Transforms the profit table to add activity flags for products.

    Args:
        profit_table (Table): DataFrame containing transaction sums and counts.
        current_date (date): Date for which the activity flags are being calculated.

    Returns:
        Table: DataFrame with activity flags for the products.
    """
    current_date_pd = pd.to_datetime(current_date)
    start_date = current_date_pd - pd.DateOffset(months=2)
    end_date = current_date_pd + pd.DateOffset(months=1)

    date_list = pd.date_range(start=start_date, end=end_date, freq='M').strftime('%Y-%m-01')
    filtered_df = profit_table[profit_table['date'].isin(date_list)]
    grouped_df = filtered_df.drop('date', axis=1).groupby('id').sum()

    for product in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']:
        grouped_df[f'flag_{product}'] = grouped_df.apply(
            lambda x: int(x[f'sum_{product}'] > 0 and x[f'count_{product}'] > 0), axis=1
        )

    result_df = grouped_df.filter(regex='flag').reset_index()
    return result_df

@dag(dag_display_name="Pipeline", start_date=pendulum.now(),
     schedule="0 0 5 * *", dag_id="etl")
def etl():
    """Defines the pipeline for processing and loading data."""
    extract = extract_profit_table()
    transform = transform_profit_table(extract, date.today())
    load = load_activity_table(transform)
    extract >> transform >> load

dag_etl = etl()

if __name__ == "__main__":
    dag_etl.test()
