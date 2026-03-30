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

-- v4 Radius Search with Remote UDFs and bitwise range computation
--
-- This query combines Remote UDFs (geo.s2_covering_v4, geo.s2_distance) with
-- the v4 range-scan pattern. The key innovation: covering cell IDs are
-- converted to leaf-cell ranges using bitwise arithmetic directly in SQL.
--
-- For a covering cell C:
--   lowestSetBit = C & (-C)       -- the sentinel bit marking the cell's level
--   rangeMin     = C - (lowestSetBit - 1)  -- first leaf descendant
--   rangeMax     = C + (lowestSetBit - 1)  -- last leaf descendant
--
-- geo.s2_covering_v4 differs from geo.s2_covering (v3): the backing Cloud
-- Function lets the S2RegionCoverer choose optimal cell levels freely instead
-- of constraining to levels 12/14/16. This produces tighter coverings with
-- fewer false positives, since the coverer is not forced to snap to 3 specific
-- levels. The SQL then converts each cell to a leaf-cell range inline.
--
-- Key differences from the v3 UDF query:
--   - Uses geo.s2_covering_v4 (unconstrained levels) instead of geo.s2_covering
--   - No interleaved table JOIN
--   - DISTINCT still needed (overlapping covering cells produce duplicate matches)
--   - Range scans (BETWEEN) instead of point lookups (=)
--   - Bitwise range computation in SQL
--
-- Parameters:
--   @centerLat, @centerLng : search center in degrees
--   @radiusMeters : search radius in meters

WITH covering_ranges AS (
    -- Compute covering cells via the Remote UDF, then derive leaf-cell ranges
    -- using bitwise arithmetic. Each covering cell becomes a [min, max] range
    -- that contains all leaf descendants.
    SELECT
        covering_cell - ((covering_cell & (-covering_cell)) - 1) AS range_min,
        covering_cell + ((covering_cell & (-covering_cell)) - 1) AS range_max
    FROM (SELECT geo.s2_covering_v4(@centerLat, @centerLng, @radiusMeters) AS cells),
         UNNEST(cells) AS covering_cell
),
candidates AS (
    -- Join covering ranges against the leaf-cell index on PointOfInterest.
    -- Each range scan hits the PointOfInterestByS2Cell covering index.
    -- DISTINCT: covering cells at different levels may have overlapping
    -- leaf-cell ranges, so the same POI can match through multiple ranges.
    SELECT DISTINCT poi.PoiId, poi.Name, poi.Category, poi.Latitude, poi.Longitude
    FROM covering_ranges cr
    JOIN PointOfInterest@{FORCE_INDEX=PointOfInterestByS2Cell} poi
        ON poi.S2CellId BETWEEN cr.range_min AND cr.range_max
),
with_distance AS (
    -- Compute exact distance for post-filtering via the distance UDF.
    SELECT c.PoiId, c.Name, c.Category, c.Latitude, c.Longitude,
           geo.s2_distance(c.Latitude, c.Longitude, @centerLat, @centerLng) AS distance_meters
    FROM candidates c
)
-- Post-filter to exact radius and sort by proximity.
SELECT * FROM with_distance
WHERE distance_meters <= @radiusMeters
ORDER BY distance_meters;
