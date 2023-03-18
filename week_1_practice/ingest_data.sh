#!/bin/bash
URL="http://192.168.2.37:8000/yellow_tripdata_2021-01.parquet"

docker run -it \
--network=pgnetwork \
taxi_ingest:latest \
    --user=root \
    --password=root \
    --host=de-zoomcamp \
    --port=5432 \
    --db=my_taxi \
    --table_name=yellow_taxi_trips \
    --year=2021 \
    --month=01 \
    --url=${URL}
