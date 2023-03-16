#!/bin/bash
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -v $(pwd)/my_pg_admin_data:/var/lib/pgadmin \
    -p 8080:80\
    --network=pgnetwork \
    --name mypgadmin \
    dpage/pgadmin4