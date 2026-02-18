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

-- Schema v2: Single S2 Cell ID column
--
-- Improvement: encode each location's lat/lng into an S2 Cell ID at a fixed
-- level (e.g., level 16, ~150m cells). The Cell ID is a single INT64 that
-- captures both dimensions via a space-filling curve (Hilbert curve).
--
-- Query pattern: compute the S2 covering for a search region, then issue
-- range scans on the Cell ID index.
--
-- Tradeoff: a fixed cell level means either too coarse (large cells = many
-- false positives) or too fine (small cells = many covering ranges to query).
--
-- Note: S2 Cell IDs are unsigned 64-bit integers. Spanner's INT64 is signed.
-- We store the raw bit pattern (reinterpreted as signed) and handle the
-- sign bit in application code.

CREATE TABLE PointOfInterest (
    PoiId     STRING(36) NOT NULL,
    Name      STRING(MAX),
    Category  STRING(256),
    Latitude  FLOAT64 NOT NULL,
    Longitude FLOAT64 NOT NULL,
    -- S2 Cell ID at level 16 (~150m cells), stored as signed INT64
    S2CellId  INT64 NOT NULL,
) PRIMARY KEY (PoiId);

-- Index on S2CellId enables range scans over cell ID ranges.
-- STORING clause avoids index join-backs for common columns.
CREATE INDEX PointOfInterestByS2Cell
    ON PointOfInterest(S2CellId)
    STORING (Name, Latitude, Longitude);
