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

-- Bounding Box Search Query
--
-- Pattern: Compute S2 covering cells for a rectangle defined by
-- (minLat, minLng) to (maxLat, maxLng), query the token index,
-- then post-filter to the exact bounding box.

SELECT
    poi.PoiId,
    poi.Name,
    poi.Category,
    poi.Latitude,
    poi.Longitude
FROM PointOfInterest poi
WHERE poi.PoiId IN (
    -- Covering cell lookup
    SELECT idx.PoiId
    FROM PointOfInterestLocationIndex idx
    WHERE idx.S2CellId BETWEEN @cellRangeMin1 AND @cellRangeMax1
       OR idx.S2CellId BETWEEN @cellRangeMin2 AND @cellRangeMax2
       OR idx.S2CellId BETWEEN @cellRangeMin3 AND @cellRangeMax3
       OR idx.S2CellId BETWEEN @cellRangeMin4 AND @cellRangeMax4
)
-- Post-filter: exact bounding box check
AND poi.Latitude  BETWEEN @minLat AND @maxLat
AND poi.Longitude BETWEEN @minLng AND @maxLng
ORDER BY poi.Name;
