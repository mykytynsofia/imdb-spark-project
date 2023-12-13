data "archive_file" "default" {
  type        = "zip"
  output_path = "/tmp/cloud_function.zip"
  source_dir  = "../cloud_function/"
}

resource "google_storage_bucket_object" "default" {
  name   = "function-source.zip"
  bucket = "functions-code-bigdata"
  source = data.archive_file.default.output_path
}

resource "google_project_iam_member" "gcs_pubsub_publishing" {
  project = "mlops-398308"
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:139168890807-compute@developer.gserviceaccount.com"
}

resource "google_project_iam_member" "invoking" {
  project    = "mlops-398308"
  role       = "roles/run.invoker"
  member     = "serviceAccount:139168890807-compute@developer.gserviceaccount.com"
  depends_on = [google_project_iam_member.gcs_pubsub_publishing]
}

resource "google_project_iam_member" "event_receiving" {
  project    = "mlops-398308"
  role       = "roles/eventarc.eventReceiver"
  member     = "serviceAccount:139168890807-compute@developer.gserviceaccount.com"
  depends_on = [google_project_iam_member.invoking]
}

resource "google_project_iam_member" "artifactregistry_reader" {
  project    = "mlops-398308"
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:139168890807-compute@developer.gserviceaccount.com"
  depends_on = [google_project_iam_member.event_receiving]
}

resource "google_cloudfunctions2_function" "default" {
  depends_on = [
    google_project_iam_member.event_receiving,
    google_project_iam_member.artifactregistry_reader,
  ]
  name     = "rename-dataframe-results"
  location = "europe-west1"
  project  = "mlops-398308"

  build_config {
    runtime     = "python312"
    entry_point = "hello_gcs"
    source {
      storage_source {
        bucket = "functions-code-bigdata"
        object = google_storage_bucket_object.default.name
      }
    }
  }

  service_config {
    max_instance_count             = 3
    min_instance_count             = 1
    available_memory               = "256M"
    timeout_seconds                = 60
    ingress_settings               = "ALLOW_INTERNAL_ONLY"
    all_traffic_on_latest_revision = true
    service_account_email          = "139168890807-compute@developer.gserviceaccount.com"
  }

  event_trigger {
    trigger_region        = "europe-west1"
    event_type            = "google.cloud.storage.object.v1.finalized"
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = "139168890807-compute@developer.gserviceaccount.com"
    event_filters {
      attribute = "bucket"
      value     = "imdb-dataframes-results"
    }
  }
}
