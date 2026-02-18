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
-- Radius Search Using Remote UDFs (s2_covering + s2_distance)
-- =============================================================================
-- This query demonstrates the same covering + post-filter pattern as
-- radius_search.sql, but uses Remote UDFs to push all S2 logic into the
-- database layer. The client no longer needs to:
--   1. Bundle the S2 library to compute covering cells
--   2. Inline the Haversine formula for post-filtering
--
-- Instead, the query calls geo.s2_covering() to get covering cells and
-- geo.s2_distance() to compute exact distances — both backed by Cloud Run
-- functions defined in infra/udf_definition.sql.
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
--   @radiusMeters FLOAT64  -- Search radius in meters (e.g., 2000.0)
-- =============================================================================

-- Query plan overview:
--
-- 1. geo.s2_covering() is called ONCE with the search parameters. It returns
--    an ARRAY<INT64> of S2 Cell IDs covering the search circle at levels
--    12/14/16. This replaces the client-side S2RegionCoverer computation.
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
--    great-circle distance. This replaces the inline Haversine formula.
--    Because this is in the with_distance CTE, Spanner batches these calls
--    to the Cloud Run function (up to max_batching_rows = 100 at a time).
--
-- 5. The final WHERE clause post-filters to the exact radius, eliminating
--    false positives from the S2 covering approximation.

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
    -- than the Haversine approximation in radius_search.sql).
    SELECT
        c.PoiId,
        c.Name,
        c.Category,
        c.Latitude,
        c.Longitude,
        geo.s2_distance(c.Latitude, c.Longitude, @centerLat, @centerLng) AS distance_meters
    FROM candidates c
)
-- Phase 3: Post-filter to exact radius and sort by proximity.
SELECT *
FROM with_distance
WHERE distance_meters <= @radiusMeters
ORDER BY distance_meters;
