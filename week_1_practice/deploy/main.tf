terraform {
  backend "gcs" {
     bucket  = "de-zoomcamp-terraform"
     prefix  = "terraform/state"
  }
  required_version = "1.3.9"
  
  required_providers {
    google = {
        source = "hashicorp/google"
        version = "4.53.1"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
}

resource "google_storage_bucket" "my_taxi_bucket" {
    name = "${local.my_taxi_bucket}_${var.project}"
    public_access_prevention = "enforced"
    storage_class = var.storage_class
    uniform_bucket_level_access = true
    location = var.region

    versioning {
      enabled = true
    }

    lifecycle_rule {
      action {
        type = "Delete"
      }
      condition {
        age = var.bucket_age
      }
    }

    force_destroy = true
}

resource "google_bigquery_dataset" "my_taxi_dataset" {
  dataset_id = var.bq_taxi_dataset
  description = "This is the raw taxi dataset"
  location = var.region
  project = var.project
}
