import os
from datetime import date, timedelta
from functools import reduce

from pathlib import Path
from typing import TypeAlias

import pandas as pd
import pendulum
from airflow.decorators import dag, task

now = pendulum.now()
Table: TypeAlias = pd.DataFrame

@task(task_id="load", execution_timeout=timedelta(minutes=5))
def load_activity_table(profit_tables: tuple[Table, ...], csv_target: Path = Path("data/flags_activity.csv")) -> None:
    """
    Loads a merged DataFrame of activity flags into a CSV file.

    Args:
        profit_tables (tuple[Table, ...]): Tuple of DataFrames with activity flags.
        csv_target (Path): Path to the target CSV file.
    """
    merged_df = reduce(
        lambda left, right: pd.merge(left, right, on="id", how="outer"),
        profit_tables
    )

    if os.stat(csv_target).st_size == 0:
        merged_df.to_csv(csv_target, index=False)
    else:
        merged_df.to_csv(csv_target, mode='a', header=False, index=False)

@task(task_id="extract", execution_timeout=timedelta(minutes=5))
def extract_profit_table(csv_source: Path = Path("data/profit_table.csv")) -> Table:
    """
    Extracts profit data from a CSV file.

    Args:
        csv_source (Path): Path to the CSV file.

    Returns:
        Table: DataFrame with profit data.
    """
    return pd.read_csv(csv_source)

@task(task_id="prepare", execution_timeout=timedelta(minutes=5))
def prepare_profit_table(profit_table: Table, current_date: date) -> Table:
    """
    Prepares the profit table by filtering and summing up data based on date range.

    Args:
        profit_table (Table): DataFrame with profit data.
        current_date (date): Date for calculating the date range.

    Returns:
        Table: DataFrame with summed data.
    """
    current_date = pd.to_datetime(current_date)
    start_date = current_date - pd.DateOffset(months=2)
    end_date = current_date + pd.DateOffset(months=1)

    date_list = pd.date_range(start=start_date, end=end_date, freq="M").strftime("%Y-%m-01")
    prepared_table = profit_table[profit_table["date"].isin(date_list)].drop("date", axis=1).groupby("id").sum()

    return prepared_table

@task(task_id="transform", retries=5)
def transform_profit_table(profit_table: Table, product_name: str) -> Table:
    """
    Transforms profit table by adding activity flags for a specified product.

    Args:
        profit_table (Table): DataFrame with summed profit data.
        product_name (str): Product name for which to calculate flags.

    Returns:
        Table: DataFrame with activity flags.
    """
    flag_column = f"flag_{product_name}"
    profit_table[flag_column] = profit_table.apply(
        lambda row: int(row[f"sum_{product_name}"] > 0 and row[f"count_{product_name}"] > 0),
        axis=1
    )

    return profit_table.filter(regex='flag').reset_index()

@dag(dag_display_name="Additional Pipeline", catchup=False,
     start_date=now, schedule="0 0 5 * *")
def etl_additional():
    """
    Defines the pipeline for processing and storing profit data with activity flags.
    """
    profit_table = extract_profit_table()
    prepared_table = prepare_profit_table(profit_table, date.today())

    transform_tasks = tuple(
        transform_profit_table.override(task_id=f"transform_product_{product}")(
            profit_table=prepared_table, product_name=product
        )
        for product in ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j')
    )

    load_activity_table(profit_tables=transform_tasks)

dag_etl = etl_additional()

if __name__ == "__main__":
    dag_etl.test()
