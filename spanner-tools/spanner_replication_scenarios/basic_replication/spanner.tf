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

locals {
  ecommerce_ddl = [
    "CREATE TABLE Users ( UserId STRING(36) NOT NULL, FullName STRING(MAX), Email STRING(MAX), CreatedAt TIMESTAMP OPTIONS (allow_commit_timestamp = true) ) PRIMARY KEY (UserId)",
    "CREATE TABLE Products ( ProductId STRING(36) NOT NULL, Name STRING(MAX), Description STRING(MAX), Price FLOAT64, InventoryCount INT64 ) PRIMARY KEY (ProductId)",
    "CREATE TABLE Orders ( OrderId STRING(36) NOT NULL, UserId STRING(36) NOT NULL, OrderDate TIMESTAMP OPTIONS (allow_commit_timestamp = true), TotalAmount FLOAT64, Status STRING(50) ) PRIMARY KEY (OrderId)",
    "CREATE TABLE OrderItems ( OrderId STRING(36) NOT NULL, ProductId STRING(36) NOT NULL, Quantity INT64, UnitPrice FLOAT64 ) PRIMARY KEY (OrderId, ProductId)"
  ]
}

resource "google_spanner_instance" "source" {
  name         = "source-instance"
  config       = "regional-${var.source_region}"
  display_name = "Source Instance"
  num_nodes    = 1
}

resource "google_spanner_database" "source_db" {
  instance = google_spanner_instance.source.name
  name     = "source-db"
  ddl = concat(local.ecommerce_ddl, ["CREATE CHANGE STREAM streamall FOR ALL"])
  deletion_protection = false
}

resource "google_spanner_instance" "destination" {
  name         = "dest-instance"
  config       = "regional-${var.dest_region}"
  display_name = "Destination Instance"
  num_nodes    = 1
}

resource "google_spanner_database" "dest_db" {
  instance = google_spanner_instance.destination.name
  name     = "dest-db"
  ddl = local.ecommerce_ddl
  deletion_protection = false
}
