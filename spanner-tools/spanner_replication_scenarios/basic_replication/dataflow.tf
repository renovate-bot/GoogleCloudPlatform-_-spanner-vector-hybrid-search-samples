resource "random_id" "bucket_suffix" {
  byte_length = 4
}

locals {
  temp_bucket_name = var.dataflow_bucket_name != "" ? var.dataflow_bucket_name : "${var.project_id}-dataflow-temp-${random_id.bucket_suffix.hex}"
}

resource "google_storage_bucket" "dataflow_temp" {
  name          = local.temp_bucket_name
  location      = var.dataflow_region
  force_destroy = true
  
  uniform_bucket_level_access = true
}

output "dataflow_temp_bucket" {
  value = google_storage_bucket.dataflow_temp.url
}
