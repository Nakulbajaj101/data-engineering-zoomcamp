import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

HOURS_EXPIRATION = 2
DATA_DIR = f"data"

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def fetch_data(dataset_url: str=None) -> pd.DataFrame:
    """Read the data from the web into pandas dataframe"""

    df = pd.read_parquet(dataset_url)
    return df

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans the taxi dataset"""

    # Convering to datetime
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def write_local(df: pd.DataFrame, color: str, dataset_file: str):
    """Stores the clean data locally"""

    data_dir = f"{DATA_DIR}/{color}"
    if not os.path.isdir(f"{data_dir}"):
        os.makedirs(f"{data_dir}")

    path = Path(f"{data_dir}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path
    
@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def upload_data_gcs(file_path: str, bucket_path: str, bucket_block: str) -> None:
    """Function to upload data to gcs"""

    gcp_cloud_storage_bucket_block = GcsBucket.load(f"{bucket_block}")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{file_path}",
        to_path=f"{bucket_path}"
    )

@flow()
def etl_web_to_gcs(color: str, year: int, month: int, bucket_block: str) -> None:
    """The main ETL Function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet"

    df = fetch_data(dataset_url=dataset_url)
    df_clean = clean_data(df)

    local_path = write_local(df=df_clean,
                             color=color,
                             dataset_file=dataset_file)

    upload_data_gcs(file_path=local_path,
                    bucket_path=local_path,
                    bucket_block=bucket_block)

if __name__ == '__main__':
    color = "yellow"
    year = 2021
    month = 1
    bucket_block="my-taxi-bucket"
    etl_web_to_gcs(color=color, year=year, month=month, bucket_block=bucket_block)
