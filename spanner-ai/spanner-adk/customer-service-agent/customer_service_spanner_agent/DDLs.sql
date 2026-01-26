-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE
  Customers ( CustomerId STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
    Email STRING(255) NOT NULL,
    CreatedDate TIMESTAMP NOT NULL OPTIONS ( allow_commit_timestamp = TRUE ),
    )
PRIMARY KEY
  (CustomerId);

CREATE TABLE
  Orders ( OrderId STRING(36) NOT NULL,
    CustomerId STRING(36) NOT NULL,
    OrderDate DATE NOT NULL,
    OrderStatus STRING(50) NOT NULL,
    TrackingNumber STRING(255),
    EstimatedDelivery DATE,
    ReturnEligible BOOL NOT NULL,
    ReturnWindowDays INT64 NOT NULL,
    LastUpdated TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
    )
PRIMARY KEY
  (CustomerId,
    OrderId),
  INTERLEAVE IN PARENT Customers
ON
DELETE
  CASCADE;

CREATE TABLE
  OrderItems ( CustomerId STRING(36) NOT NULL,
    OrderId STRING(36) NOT NULL,
    ProductId STRING(36) NOT NULL,
    Quantity INT64 NOT NULL,
    UnitPrice FLOAT64 NOT NULL,
    LineItemTotal FLOAT64 NOT NULL,
    )
PRIMARY KEY
  (CustomerId,
    OrderId,
    ProductId),
  INTERLEAVE IN PARENT Orders
ON
DELETE
  CASCADE;

CREATE TABLE
  Products ( ProductId STRING(36) NOT NULL,
    Name STRING(255) NOT NULL,
    Description STRING(MAX),
    Warranty STRING(255),
    Price FLOAT64 NOT NULL,
    )
PRIMARY KEY
  (ProductId);

CREATE TABLE
  RETURNS ( ReturnId STRING(36) NOT NULL,
    OrderId STRING(36) NOT NULL,
    ProductId STRING(36) NOT NULL,
    CustomerId STRING(36) NOT NULL,
    ReturnStatus STRING(50) NOT NULL,
    RequestDate DATE NOT NULL,
    ApprovalDate DATE,
    RefundAmount FLOAT64,
    LastUpdated TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
    )
PRIMARY KEY
  (ReturnId);