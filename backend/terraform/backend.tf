terraform {
  backend "gcs" {
    bucket = "terraform-state-bigdata"
    prefix = "dataproc"
  }
}
