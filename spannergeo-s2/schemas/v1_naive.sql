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

-- Schema v1: Naive lat/lng columns
--
-- This is the simplest approach: store raw coordinates and index them.
-- Problem: a composite index on (Latitude, Longitude) can only efficiently
-- filter on the leading key. A radius query degrades to scanning an entire
-- latitude band, then post-filtering on longitude.

CREATE TABLE PointOfInterest (
    PoiId     STRING(36) NOT NULL,
    Name      STRING(MAX),
    Category  STRING(256),
    Latitude  FLOAT64 NOT NULL,
    Longitude FLOAT64 NOT NULL,
) PRIMARY KEY (PoiId);

-- Composite index on (Latitude, Longitude).
-- Spanner can range-scan on Latitude efficiently, but the Longitude filter
-- becomes a post-filter — you end up reading every row in the latitude band.
CREATE INDEX PointOfInterestByLatLng
    ON PointOfInterest(Latitude, Longitude);
