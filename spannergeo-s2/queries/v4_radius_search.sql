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

-- v4 Radius Search: Client-side covering with range scans on leaf cell ID
--
-- This query uses the v4 schema where each POI stores a single leaf-level
-- S2 Cell ID. The client computes covering cells using the S2 library, then
-- converts each covering cell into a [rangeMin, rangeMax] range. All leaf
-- descendants of a covering cell fall within that range.
--
-- Key differences from v3:
--   - No JOIN to an interleaved index table
--   - No DISTINCT needed (one row per POI, not one per token)
--   - Queries PointOfInterest directly via a covering index
--   - FORCE_INDEX hint ensures the covering index is used
--
-- Parameters (bound by the application):
--   @min_0, @max_0, @min_1, @max_1, ... : S2 cell ID ranges from covering
--   @centerLat, @centerLng : search center (for Haversine post-filter)
--   @radiusMeters : search radius in meters

WITH candidates AS (
    SELECT PoiId, Name, Category, Latitude, Longitude
    FROM PointOfInterest@{FORCE_INDEX=PointOfInterestByS2Cell}
    WHERE
        -- Each OR branch is a range scan on the covering index.
        -- The application generates one (min, max) pair per covering cell.
        S2CellId BETWEEN @min_0 AND @max_0
        OR S2CellId BETWEEN @min_1 AND @max_1
        -- ... additional ranges as needed ...
)
SELECT
    PoiId,
    Name,
    Category,
    Latitude,
    Longitude,
    -- Haversine distance in meters (ACOS(-1)/180 converts degrees to radians)
    6371000 * ACOS(
        LEAST(1.0,
            COS(Latitude * ACOS(-1) / 180) * COS(@centerLat * ACOS(-1) / 180) *
            COS((Longitude - @centerLng) * ACOS(-1) / 180) +
            SIN(Latitude * ACOS(-1) / 180) * SIN(@centerLat * ACOS(-1) / 180)
        )
    ) AS distance_meters
FROM candidates
-- Post-filter: discard false positives outside the actual radius
WHERE 6371000 * ACOS(
    LEAST(1.0,
        COS(Latitude * ACOS(-1) / 180) * COS(@centerLat * ACOS(-1) / 180) *
        COS((Longitude - @centerLng) * ACOS(-1) / 180) +
        SIN(Latitude * ACOS(-1) / 180) * SIN(@centerLat * ACOS(-1) / 180)
    )
) <= @radiusMeters
ORDER BY distance_meters;
