from prefect import flow
from etl_gcs_to_bq import etl_gcs_to_bq
from etl_web_to_gcs import etl_web_to_gcs

@flow(name="Parent flow for GCS to BQ ETL flow")
def etl_parent_flow(months: list[int] = [1,2,3], year: int = 2021, color: str = "yellow"):
    """Parent flow that downloads data and then uploads data to bq"""

    project_id = "dataengineeringzoomcamp-2023"
    credential_block = "prefect-zoomcamp"
    dataset_id = "trips_data_all"
    table_name = f"{color}_taxi_trips"
    bucket_block = "my-taxi-bucket"
    table_id = f"{dataset_id}.{table_name}"

    for month in months:
        etl_web_to_gcs(color=color, month=month, year=year, bucket_block=bucket_block)
        file_path = f"yellow/yellow_tripdata_{year}-{month:02}.parquet"
        
        etl_gcs_to_bq(credential_block=credential_block, file_path=file_path, table_id=table_id, project_id=project_id)

if __name__ == "__main__":
    etl_parent_flow()
