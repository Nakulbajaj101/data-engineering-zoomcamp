import pandas as pd
import requests
from sqlalchemy import create_engine
from tqdm import tqdm
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(year: str="", month: str="", url: str=None) -> str:
    """Function to ingest data"""

    stream = False
    if not url:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"
        stream = True
    req = requests.get(url, stream=stream)
    filename = f"yellow_tripdata_{year}-{month}.parquet"
    save_path = f"./{filename}"
    with open(save_path, "wb") as handle:
        for data in tqdm(req.iter_content(),
                         desc=f"{filename}",
                         postfix=f"save to {save_path}",
                         total=int(req.headers["Content-Length"])):
            handle.write(data)

    return filename

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def read_data(filepath: str) -> pd.DataFrame:
    """Function to read data"""

    df = pd.read_parquet(path=filepath)

    # Convering to datetime
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@task(retries=2, log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """Function to transform data"""

    df = data.copy()
    pre_filter_passenger_count = df['passenger_count'].isin([0]).sum()
    df_filter = df[df["passenger_count"] != 0].reset_index(drop=True)
    post_filter_passenger_count = df_filter['passenger_count'].isin([0]).sum()

    print(f"Before filtering zero passenger count records were {pre_filter_passenger_count} and after filtering were {post_filter_passenger_count}")

    return df_filter


@task(retries=2, log_prints=True)
def ingest_data(df, table_name):
    """Function to ingest data into the postgres"""

    with SqlAlchemyConnector.load("taxi-postgres-connector") as database_block:
        with database_block.get_connection(begin=False) as engine:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
            df.to_sql(name=table_name, con=engine, if_exists="append", chunksize=100000, index=False)

@flow(name="Subflow", log_prints=True)
def subflow(table_name: str):
    print(f"This is the subflow test with table name {table_name}")

@flow(name="Ingest taxi data Flow")
def main(table_name: str):
    table_name = table_name
    url = "http://10.132.0.2:8000/yellow_tripdata_2021-01.parquet"

    # Ingest the data

    filename = download_data(year="2021", month="01", url=url)
    
    # Read the data
    df = read_data(filepath=filename)

    # Transform and filter data
    df = transform_data(data=df)
    
    # Ingest data
    ingest_data(df=df,
                table_name=table_name)

    subflow(table_name=table_name)

if __name__ == "__main__":
    main(table_name="yellow_taxi_trips")
