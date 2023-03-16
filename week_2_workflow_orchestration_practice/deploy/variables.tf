locals {
  my_taxi_bucket = "my_taxi_bucket"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "dataengineeringzoomcamp-2023"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "australia-southeast1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bucket_age" {
  description = "Age of the bucket in days"
  default = 30
}

variable "bq_taxi_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}
