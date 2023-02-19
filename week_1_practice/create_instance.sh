#!/bin/bash

gcloud compute instances create de-zoomcamp-2023 \
    --project=dataengineeringzoomcamp-2023 \
    --zone=europe-west1-b \
    --machine-type=e2-standard-4 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=553934010274-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --create-disk=auto-delete=yes,boot=yes,device-name=de-zoomcamp-2023,image=projects/ubuntu-os-cloud/global/images/ubuntu-2004-focal-v20230213,mode=rw,size=30,type=projects/dataengineeringzoomcamp-2023/zones/europe-west1-b/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any