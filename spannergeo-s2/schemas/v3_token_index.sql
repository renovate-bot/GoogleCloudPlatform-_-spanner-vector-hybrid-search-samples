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

-- Schema v3: S2 Token interleaved index table (recommended)
--
-- This is the canonical pattern for geo-spatial indexing on Spanner.
-- Instead of a single Cell ID per location, we store multiple S2 "tokens"
-- at varying cell levels. Each token is a (CellId, PoiId) pair in a
-- separate interleaved table.
--
-- Benefits:
--   - Multi-level tokens balance precision vs. number of index terms
--   - Interleaving with the parent table provides data locality
--   - The covering + post-filter pattern works at any search radius
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
) PRIMARY KEY (PoiId);

-- Interleaved child table: one row per S2 token per location.
-- Each location has tokens at multiple cell levels (e.g., 12, 14, 16)
-- to support efficient queries at different zoom levels / radii.
CREATE TABLE PointOfInterestLocationIndex (
    PoiId      STRING(36) NOT NULL,
    S2CellId   INT64 NOT NULL,
    -- Cell level (0-30) for debugging/filtering; not strictly required
    CellLevel  INT64 NOT NULL,
) PRIMARY KEY (PoiId, S2CellId),
  INTERLEAVE IN PARENT PointOfInterest ON DELETE CASCADE;

-- Index on S2CellId enables the covering query pattern:
-- compute covering cells for a search region, then look up matching tokens.
-- STORING the parent's lat/lng avoids a join-back for post-filtering.
CREATE INDEX LocationIndexByS2Cell
    ON PointOfInterestLocationIndex(S2CellId)
    STORING (CellLevel);

-- Optionally, a secondary index on the parent table for name lookups
CREATE INDEX PointOfInterestByName
    ON PointOfInterest(Name);
