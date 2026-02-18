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

-- Radius Search Query
--
-- Pattern: Compute S2 covering cells for a circle (center + radius),
-- then query the token index for matching cells, join back to parent,
-- and post-filter with Haversine distance.
--
-- The @cellId_N parameters are the S2 Cell ID ranges from the covering.
-- In practice, you generate these in application code using S2RegionCoverer
-- and bind them as query parameters.

-- Step 1: Find candidate POIs via the token index
-- Step 2: Post-filter with Haversine to eliminate false positives from
--         the covering approximation

SELECT
    poi.PoiId,
    poi.Name,
    poi.Category,
    poi.Latitude,
    poi.Longitude,
    -- Haversine distance in meters
    6371000 * ACOS(
        LEAST(1.0,
            COS(poi.Latitude * ACOS(-1) / 180) * COS(@searchLat * ACOS(-1) / 180) *
            COS((poi.Longitude - @searchLng) * ACOS(-1) / 180) +
            SIN(poi.Latitude * ACOS(-1) / 180) * SIN(@searchLat * ACOS(-1) / 180)
        )
    ) AS distance_meters
FROM PointOfInterest poi
WHERE poi.PoiId IN (
    -- Covering cell lookup: each range corresponds to one cell in the covering
    SELECT idx.PoiId
    FROM PointOfInterestLocationIndex idx
    WHERE idx.S2CellId BETWEEN @cellRangeMin1 AND @cellRangeMax1
       OR idx.S2CellId BETWEEN @cellRangeMin2 AND @cellRangeMax2
       OR idx.S2CellId BETWEEN @cellRangeMin3 AND @cellRangeMax3
)
-- Post-filter: only keep results within the actual radius
AND 6371000 * ACOS(
    LEAST(1.0,
        COS(poi.Latitude * ACOS(-1) / 180) * COS(@searchLat * ACOS(-1) / 180) *
        COS((poi.Longitude - @searchLng) * ACOS(-1) / 180) +
        SIN(poi.Latitude * ACOS(-1) / 180) * SIN(@searchLat * ACOS(-1) / 180)
    )
) <= @radiusMeters
ORDER BY distance_meters ASC;
