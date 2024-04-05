terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.23.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "imdb_datalake" {
  name     = "${local.IMDB_DATALAKE}_${var.project}"
  location = var.region

  storage_class = var.storage_class

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 10 //days
    }
  }

  force_destroy = true
}


resource "google_dataproc_cluster" "imdb_spark_cluster" {
  name    = "${local.IMDB_DATAPROC}-${var.project}"
  project = var.project
  region  = var.region

  cluster_config {
    master_config {
      num_instances = 1
    }

    worker_config {
      num_instances = 3
    }

    software_config {
      optional_components = ["DOCKER", "JUPYTER"]
    }
  }
}

resource "google_bigquery_dataset" "imdb_analytics_dataset" {
  project                    = var.project
  location                   = var.region
  dataset_id                 = var.imdb_analytics_datasets
  delete_contents_on_destroy = true
}
