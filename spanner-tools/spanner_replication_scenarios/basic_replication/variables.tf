variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "source_region" {
  description = "The region for the source Spanner instance"
  type        = string
  default     = "us-central1"
}

variable "dest_region" {
  description = "The region for the destination Spanner instance"
  type        = string
  default     = "us-east1"
}

variable "dataflow_region" {
  description = "The region where Dataflow jobs will run"
  type        = string
  default     = "us-central1"
}

variable "dataflow_bucket_name" {
  description = "Name of the GCS bucket for Dataflow temp files. If not provided, one will be generated."
  type        = string
  default     = ""
}
