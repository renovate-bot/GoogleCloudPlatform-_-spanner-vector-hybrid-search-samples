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
-- Approximate k-Nearest Neighbors Using Remote UDFs (s2_covering + s2_distance)
-- =============================================================================
-- This query demonstrates a single iteration of the k-NN pattern using Remote
-- UDFs. It finds the @k closest points of interest to a center point within
-- a given search radius, using geo.s2_covering() for the index scan and
-- geo.s2_distance() for exact distance computation.
--
-- Unlike udf_query.sql (radius search), this query does NOT post-filter by
-- distance. Instead, it returns the top @k results sorted by proximity,
-- regardless of whether they are within @radiusMeters. The radius parameter
-- controls only the size of the S2 covering (i.e., how many candidates to
-- consider), not the final result set.
--
-- ITERATIVE EXPANSION (handled in application code, NOT in this query):
--   Spanner has no looping construct, so the "expand until we have k results"
--   logic lives in the application layer:
--     1. Start with a small radius (e.g., 500m)
--     2. Execute this query
--     3. If fewer than @k results are returned, double @radiusMeters and repeat
--     4. Return the closest @k from the final iteration
--   Each iteration is a separate Spanner read — keep @radiusMeters small
--   initially to minimize the covering size and index scan cost.
--
-- NOTE: Remote UDFs must be created in a named schema (e.g., "geo"), so all
-- function calls are qualified as geo.s2_covering() and geo.s2_distance().
-- The UNNEST of a Remote UDF result must be done via a subquery — Spanner
-- does not allow UNNEST(remote_udf(...)) directly in FROM.
--
-- Prerequisites:
--   - Remote UDFs deployed (see infra/udf_definition.sql)
--   - Schema v3 deployed (see infra/schema.sql)
--   - Sample data seeded (see App.java)
--
-- Spanner dialect: GoogleSQL
--
-- Query parameters:
--   @centerLat    FLOAT64  -- Search center latitude (e.g., 37.7749 for SF)
--   @centerLng    FLOAT64  -- Search center longitude (e.g., -122.4194 for SF)
--   @radiusMeters FLOAT64  -- Search radius in meters — controls covering size
--   @k            INT64    -- Number of nearest neighbors to return
-- =============================================================================

-- Query plan overview:
--
-- 1. geo.s2_covering() is called ONCE with the center point and radius. It
--    returns an ARRAY<INT64> of S2 Cell IDs covering the search circle at
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
-- 4. geo.s2_distance() is called for each candidate POI to compute the exact
--    great-circle distance. Spanner batches these calls to the Cloud Run
--    function (up to max_batching_rows = 100 at a time).
--
-- 5. Results are sorted by distance and limited to @k rows. There is no
--    distance post-filter — we want the closest @k regardless of radius.

WITH candidates AS (
    -- Phase 1: Index scan using S2 covering cells from the Remote UDF.
    -- geo.s2_covering() is called once in a subquery, then UNNEST flattens
    -- the resulting array into rows for joining against the token index.
    SELECT DISTINCT
        poi.PoiId,
        poi.Name,
        poi.Category,
        poi.Latitude,
        poi.Longitude
    FROM (SELECT geo.s2_covering(@centerLat, @centerLng, @radiusMeters) AS cells),
         UNNEST(cells) AS covering_cell
    JOIN PointOfInterestLocationIndex idx
        ON idx.S2CellId = covering_cell
    JOIN PointOfInterest poi
        ON poi.PoiId = idx.PoiId
),
with_distance AS (
    -- Phase 2: Compute exact distance for each candidate using the Remote UDF.
    -- geo.s2_distance() uses the S2 library's earth-surface model (more accurate
    -- than the Haversine approximation in knn_approx.sql).
    SELECT
        c.PoiId,
        c.Name,
        c.Category,
        c.Latitude,
        c.Longitude,
        geo.s2_distance(c.Latitude, c.Longitude, @centerLat, @centerLng) AS distance_meters
    FROM candidates c
)
-- Phase 3: Sort by proximity and return the k closest results.
-- No distance post-filter — the radius only controls the covering size.
-- If fewer than @k candidates are found, the application should expand
-- @radiusMeters and re-execute (see iterative expansion note above).
SELECT *
FROM with_distance
ORDER BY distance_meters ASC
LIMIT @k;
