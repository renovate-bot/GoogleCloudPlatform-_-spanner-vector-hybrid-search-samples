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

-- Schema v4: Range scans on a single leaf-level S2 Cell ID
--
-- Key insight: S2 Cell IDs are hierarchically encoded. A leaf cell (level 30)
-- contains all parent cell IDs as bit prefixes. Instead of pre-materializing
-- tokens at multiple levels in an interleaved table (v3), we store a single
-- leaf-level Cell ID on the main table and use range scans (BETWEEN) to match
-- at any level.
--
-- For any covering cell C at level L:
--   rangeMin = C - (lowestSetBit(C) - 1)
--   rangeMax = C + (lowestSetBit(C) - 1)
-- All leaf descendants of C fall within [rangeMin, rangeMax].
-- In SQL: lowestSetBit(x) = x & (-x)
--
-- Benefits over v3:
--   - No interleaved table: 1 row per POI instead of 4 (1 parent + 3 tokens)
--   - 1 write mutation per insert instead of 4
--   - No JOIN or DISTINCT needed in queries
--   - Flexible: can query at any cell level, not just pre-stored levels
--   - Covering index avoids back-join to base table
--
-- Tradeoffs:
--   - Range scans instead of point lookups (slightly different I/O pattern)
--   - Covering index duplicates columns (storage overhead)

CREATE TABLE PointOfInterest (
    PoiId     STRING(36) NOT NULL,
    Name      STRING(MAX),
    Category  STRING(256),
    Latitude  FLOAT64 NOT NULL,
    Longitude FLOAT64 NOT NULL,
    S2CellId  INT64 NOT NULL,  -- Leaf cell (level 30)
) PRIMARY KEY (PoiId);

-- Covering index: STORING avoids back-join to the base table for post-filtering.
-- Queries on S2CellId use BETWEEN with ranges derived from covering cells.
CREATE INDEX PointOfInterestByS2Cell
    ON PointOfInterest(S2CellId)
    STORING (Name, Category, Latitude, Longitude);
