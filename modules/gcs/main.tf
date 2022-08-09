resource "google_storage_bucket" "default" {

  name = var.bucket_name
  location = var.location
  region = var.region
  project_id = var.project_id
}