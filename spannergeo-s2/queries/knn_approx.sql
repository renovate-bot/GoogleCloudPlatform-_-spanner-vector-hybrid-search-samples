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

-- Approximate k-Nearest Neighbors Query
--
-- Pattern: Start with a small covering around the search point,
-- query for candidates, then expand the covering if fewer than k
-- results are found. This query shows a single iteration.
--
-- The application logic handles the iterative expansion:
-- 1. Start with a small radius (e.g., 500m)
-- 2. Compute covering, run this query
-- 3. If fewer than @k results, double the radius and repeat
-- 4. Return the closest @k results from the final iteration

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
    SELECT idx.PoiId
    FROM PointOfInterestLocationIndex idx
    WHERE idx.S2CellId BETWEEN @cellRangeMin1 AND @cellRangeMax1
       OR idx.S2CellId BETWEEN @cellRangeMin2 AND @cellRangeMax2
       OR idx.S2CellId BETWEEN @cellRangeMin3 AND @cellRangeMax3
)
ORDER BY distance_meters ASC
LIMIT @k;
