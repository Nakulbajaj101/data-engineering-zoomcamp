from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials

HOURS_EXPIRATION = 2
DATA_DIR = f"data"

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def get_path(file_path: str) -> Path:
    """Function to get bucket path"""

    data_path = Path(f"./{DATA_DIR}/{file_path}")
    return data_path

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def transform(path: str) -> pd.DataFrame:
    """Function to transform the data before loading into bq"""

    df = pd.read_parquet(path)
    
    num_missing_records = df['passenger_count'].isna().sum()
    print(f"Missing records for passenger count were {num_missing_records}")

    df['passenger_count'].fillna(value=0, inplace=True)
    
    pre_filter_passenger_count = df['passenger_count'].isin([0]).sum()
    df_transformed = df[df["passenger_count"] != 0].reset_index(drop=True)
    post_filter_passenger_count = df_transformed['passenger_count'].isin([0]).sum()

    print(f"Before filtering zero passenger count records were {pre_filter_passenger_count} and after filtering were {post_filter_passenger_count}")

    return df_transformed



@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(seconds=3600*HOURS_EXPIRATION))
def write_bq(df: pd.DataFrame, destination_table: str, project_id: str, credential_block: str, load_strategy: str="append") -> None:
    """Function to write data to BQ"""

    gcp_credentials = GcpCredentials.load(f"{credential_block}")
    df["load_time_utc"] = datetime.now()
    
    df.to_gbq(
        destination_table=destination_table,
        project_id=project_id,
        chunksize=500000,
        progress_bar=True,
        if_exists=load_strategy,
        credentials=gcp_credentials.get_credentials_from_service_account())

@flow(name="GCS to BQ ETL flow")
def etl_gcs_to_bq(credential_block: str, file_path: str, table_id: str, project_id: str):
    """Main ETL flow to load data from gcs to BQ"""

    data_path = get_path(file_path=file_path)
    transform_data = transform(path=data_path)
    write_bq(df=transform_data, destination_table=table_id, credential_block=credential_block, project_id=project_id)


if __name__ == "__main__":
    file_path = f"yellow/yellow_tripdata_2021-01.parquet"
    project_id = "dataengineeringzoomcamp-2023"
    credential_block = "prefect-zoomcamp"
    dataset_id = "trips_data_all"
    table_name = "yellow_taxi_trips"
    table_id = f"{dataset_id}.{table_name}"
    etl_gcs_to_bq(credential_block=credential_block, file_path=file_path, table_id=table_id, project_id=project_id, transform=transform)
