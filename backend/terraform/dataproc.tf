resource "google_dataproc_job" "title_ratings_job" {
  region       = "europe-west1"
  project      = "mlops-398308"
  force_delete = true
  placement {
    cluster_name = "cluster-93b2"
  }

  pyspark_config {
    main_python_file_uri = "gs://jobs-codebase/title_ratings_job.py"
    properties = {
      "spark.logConf" = "true"
    }
  }
}

resource "google_dataproc_job" "title_ratings_preprocessing" {
  region       = "europe-west1"
  project      = "mlops-398308"
  force_delete = true
  placement {
    cluster_name = "cluster-93b2"
  }

  pyspark_config {
    main_python_file_uri = "gs://jobs-codebase/title_ratings_preprocessing.py"
    properties = {
      "spark.logConf" = "true"
    }
  }
}
