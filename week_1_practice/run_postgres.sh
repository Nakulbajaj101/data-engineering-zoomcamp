#!/bin/bash

docker rm $(docker ps -a -q)

docker network create pgnetwork


docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="my_taxi" \
    -v $(pwd)/my_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pgnetwork \
    --name mytaxidb \
    postgres:13

