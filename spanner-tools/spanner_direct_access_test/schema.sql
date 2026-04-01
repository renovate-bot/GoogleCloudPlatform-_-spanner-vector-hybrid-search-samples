/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

CREATE TABLE customer_insights (
  cust_id STRING(36) NOT NULL,
  acct_no STRING(36) NOT NULL,
  phone_number STRING(20) NOT NULL,
  insight_category STRING(50) NOT NULL,
  insight_name STRING(100) NOT NULL,
  insight_values STRING(MAX),
  updated_by STRING(100),
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(cust_id, acct_no, phone_number, insight_category, insight_name);

CREATE TABLE customer_insights_phone (
  cust_id STRING(36) NOT NULL,
  acct_no STRING(36) NOT NULL,
  phone_number STRING(20) NOT NULL,
  insight_category STRING(50) NOT NULL,
  insight_name STRING(100) NOT NULL,
  insight_values STRING(MAX),
  updated_by STRING(100),
  updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(phone_number, insight_category, insight_name);
