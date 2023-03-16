
import argparse

import pandas as pd
import requests
from sqlalchemy import create_engine
from tqdm import tqdm
from prefect import flow, task

@task(retries=2, log_prints=True)
def ingest_data(year: str="", month: str="", url: str=None) -> str:
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

@task(retries=2, log_prints=True)
def read_data(filepath: str) -> pd.DataFrame:
    """Function to read data"""

    df = pd.read_parquet(path=filepath)

    # Convering to datetime
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df

@flow(name="Ingest taxi data Flow")
def main(params):
    user = params.user
    password = params.password
    database = params.db
    port = params.port
    host = params.host
    url = params.url
    db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Ingest the data

    filename = ingest_data(year=params.year, month=params.month, url=url)
    
    # Read the data
    df = read_data(filepath=filename)
    
    # Create engine
    engine = create_engine(url=db_url)
    df.head(n=0).to_sql(name=params.table_name, con=engine, if_exists="replace")
    df.to_sql(name=params.table_name, con=engine, if_exists="append", chunksize=100000, index=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ingest Taxi parquet data")
    parser.add_argument('--user',)
    parser.add_argument('--password')
    parser.add_argument('--port')
    parser.add_argument('--table_name')
    parser.add_argument('--db')
    parser.add_argument('--host')
    parser.add_argument('--year')
    parser.add_argument('--month')
    parser.add_argument('--url', default=None)

    args = parser.parse_args()
    main(params=args)
