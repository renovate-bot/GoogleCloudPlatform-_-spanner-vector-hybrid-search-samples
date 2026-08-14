# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
