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

-- =============================================================================
-- Bounding Box Search Using Remote UDFs (s2_covering_rect)
-- =============================================================================
-- This query demonstrates the covering + post-filter pattern for rectangular
-- region searches, using the geo.s2_covering_rect() Remote UDF to compute
-- S2 covering cells server-side. The client only needs to supply the bounding
-- box corners — no S2 library required.
--
-- Compared to bbox_search.sql (which requires the client to pre-compute S2
-- cell ranges and pass them as parameters), this query delegates covering
-- computation to a Cloud Run function via the Remote UDF. The tradeoff is
-- one additional network round-trip to the Cloud Function, but the client
-- code is dramatically simpler.
--
-- NOTE: Remote UDFs must be created in a named schema (e.g., "geo"), so the
-- function call is qualified as geo.s2_covering_rect(). The UNNEST of a
-- Remote UDF result must be done via a subquery — Spanner does not allow
-- UNNEST(remote_udf(...)) directly in FROM.
--
-- Prerequisites:
--   - Remote UDFs deployed (see infra/udf_definition.sql)
--   - Schema v3 deployed (see infra/schema.sql)
--   - Sample data seeded (see App.java)
--
-- Spanner dialect: GoogleSQL
--
-- Query parameters:
--   @minLat FLOAT64  -- South-west corner latitude  (e.g., 37.770)
--   @minLng FLOAT64  -- South-west corner longitude (e.g., -122.420)
--   @maxLat FLOAT64  -- North-east corner latitude  (e.g., 37.810)
--   @maxLng FLOAT64  -- North-east corner longitude (e.g., -122.390)
-- =============================================================================

-- Query plan overview:
--
-- 1. geo.s2_covering_rect() is called ONCE with the bounding box corners.
--    It returns an ARRAY<INT64> of S2 Cell IDs covering the rectangle at
--    levels 12/14/16. This replaces client-side S2RegionCoverer computation.
--    The result is materialized in a subquery so UNNEST can consume it.
--
-- 2. UNNEST flattens the array into rows, which are JOINed against the
--    LocationIndexByS2Cell secondary index on PointOfInterestLocationIndex.
--    This is the "index scan" phase — fast, seeks into the B-tree per cell.
--
-- 3. DISTINCT eliminates duplicate PoiIds (a single POI has tokens at multiple
--    cell levels, and the covering may match the same POI through different
--    level tokens).
--
-- 4. The final WHERE clause post-filters to the exact bounding box bounds,
--    eliminating false positives from the S2 covering approximation. No
--    distance computation is needed — a simple lat/lng range check suffices.

SELECT DISTINCT
    poi.PoiId,
    poi.Name,
    poi.Category,
    poi.Latitude,
    poi.Longitude
FROM (SELECT geo.s2_covering_rect(@minLat, @minLng, @maxLat, @maxLng) AS cells),
     UNNEST(cells) AS covering_cell
JOIN PointOfInterestLocationIndex idx
    ON idx.S2CellId = covering_cell
JOIN PointOfInterest poi
    ON poi.PoiId = idx.PoiId
-- Post-filter: exact bounding box check eliminates false positives from the
-- S2 covering approximation. Covering cells may extend slightly beyond the
-- requested rectangle, so we verify each candidate falls within the bounds.
WHERE poi.Latitude  BETWEEN @minLat AND @maxLat
  AND poi.Longitude BETWEEN @minLng AND @maxLng
ORDER BY poi.Name;
