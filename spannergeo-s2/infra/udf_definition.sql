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
-- Remote UDF Definitions for S2 Geo-Spatial Functions
-- =============================================================================
-- These UDFs push S2 geometry logic into Cloud Run functions, allowing Spanner
-- queries to compute S2 coverings and great-circle distances server-side
-- without requiring the client to bundle the S2 library.
--
-- Prerequisites:
--   1. Deploy the backing Cloud Run function(s) (see deploy/deploy-function.sh)
--   2. Grant the Spanner service agent the Spanner API Service Agent role
--      at the project level (see deploy/grant-permissions.sh)
--   3. Replace PLACEHOLDER_URL with the actual deployed function URL(s)
--
-- Spanner dialect: GoogleSQL
-- =============================================================================


-- Remote UDFs must live in a named schema (not the default schema).
-- Create the "geo" schema if it doesn't already exist.
CREATE SCHEMA IF NOT EXISTS geo;


-- -----------------------------------------------------------------------------
-- geo.s2_covering: Compute S2 covering cells for a circular search region
-- -----------------------------------------------------------------------------
-- Given a center point (lat, lng) and radius in meters, returns an array of
-- S2 Cell IDs (as signed INT64) that cover the search region. The backing
-- Cloud Run function computes coverings at levels 12, 14, and 16 to balance
-- precision against the number of index terms.
--
-- Usage in a query (note: UNNEST of a Remote UDF must use a subquery):
--   FROM (SELECT geo.s2_covering(@lat, @lng, @radius) AS cells),
--        UNNEST(cells) AS cell_id
--   JOIN PointOfInterestLocationIndex idx ON idx.S2CellId = cell_id
--
-- The returned Cell IDs are signed INT64 values — the raw bit pattern of the
-- unsigned 64-bit S2 Cell ID reinterpreted as signed, consistent with how
-- Cell IDs are stored in the PointOfInterestLocationIndex table.
--
-- max_batching_rows = 10: Covering computation is CPU-intensive (S2RegionCoverer),
-- so we keep batches small to avoid Cloud Function timeouts.
--
-- Request format sent by Spanner:
--   {"requestId": "...", "calls": [[lat, lng, radius], ...]}
-- Expected response:
--   {"replies": [[-1234567890, 9876543210, ...], ...]}
--   Each reply is a JSON array of signed INT64 cell IDs.
-- -----------------------------------------------------------------------------
CREATE FUNCTION geo.s2_covering(
    centerLat    FLOAT64,
    centerLng    FLOAT64,
    radiusMeters FLOAT64
)
RETURNS ARRAY<INT64>
NOT DETERMINISTIC
LANGUAGE REMOTE
OPTIONS (
    endpoint = 'PLACEHOLDER_URL',
    max_batching_rows = 10
);


-- -----------------------------------------------------------------------------
-- geo.s2_distance: Compute great-circle distance between two points
-- -----------------------------------------------------------------------------
-- Returns the distance in meters between (lat1, lng1) and (lat2, lng2) using
-- the S2 library's earth-surface distance calculation (based on the WGS84
-- ellipsoid model, more accurate than the Haversine approximation used in
-- the non-UDF queries).
--
-- Usage in a query:
--   geo.s2_distance(poi.Latitude, poi.Longitude, @centerLat, @centerLng) AS dist
--
-- This replaces the inline Haversine formula used in radius_search.sql and
-- knn_approx.sql, making queries much more readable.
--
-- max_batching_rows = 100: Distance calculation is lightweight (no S2 covering
-- computation), so larger batches are fine and reduce round-trip overhead.
--
-- Request format sent by Spanner:
--   {"requestId": "...", "calls": [[lat1, lng1, lat2, lng2], ...]}
-- Expected response:
--   {"replies": [1234.56, 789.01, ...]}
--   Each reply is a FLOAT64 distance in meters.
-- -----------------------------------------------------------------------------
CREATE FUNCTION geo.s2_distance(
    lat1 FLOAT64,
    lng1 FLOAT64,
    lat2 FLOAT64,
    lng2 FLOAT64
)
RETURNS FLOAT64
NOT DETERMINISTIC
LANGUAGE REMOTE
OPTIONS (
    endpoint = 'PLACEHOLDER_URL',
    max_batching_rows = 100
);


-- -----------------------------------------------------------------------------
-- geo.s2_covering_rect: Compute S2 covering cells for a rectangular region
-- -----------------------------------------------------------------------------
-- Given a bounding box defined by its south-west (minLat, minLng) and
-- north-east (maxLat, maxLng) corners, returns an array of S2 Cell IDs
-- (as signed INT64) that cover the rectangle. The backing Cloud Run function
-- computes coverings at levels 12, 14, and 16 to balance precision against
-- the number of index terms.
--
-- Usage in a query (note: UNNEST of a Remote UDF must use a subquery):
--   FROM (SELECT geo.s2_covering_rect(@minLat, @minLng, @maxLat, @maxLng) AS cells),
--        UNNEST(cells) AS cell_id
--   JOIN PointOfInterestLocationIndex idx ON idx.S2CellId = cell_id
--
-- The returned Cell IDs are signed INT64 values — the raw bit pattern of the
-- unsigned 64-bit S2 Cell ID reinterpreted as signed, consistent with how
-- Cell IDs are stored in the PointOfInterestLocationIndex table.
--
-- max_batching_rows = 10: Covering computation is CPU-intensive (S2RegionCoverer),
-- so we keep batches small to avoid Cloud Function timeouts.
--
-- Request format sent by Spanner:
--   {"requestId": "...", "calls": [[minLat, minLng, maxLat, maxLng], ...]}
-- Expected response:
--   {"replies": [[-1234567890, 9876543210, ...], ...]}
--   Each reply is a JSON array of signed INT64 cell IDs (as strings, since
--   S2 Cell IDs exceed JavaScript's Number.MAX_SAFE_INTEGER).
-- -----------------------------------------------------------------------------
CREATE FUNCTION geo.s2_covering_rect(
    minLat FLOAT64,
    minLng FLOAT64,
    maxLat FLOAT64,
    maxLng FLOAT64
)
RETURNS ARRAY<INT64>
NOT DETERMINISTIC
LANGUAGE REMOTE
OPTIONS (
    endpoint = 'PLACEHOLDER_URL',
    max_batching_rows = 10
);


-- =============================================================================
-- v4 Remote UDFs: Coverings for the range-scan schema
-- =============================================================================
-- The v4 schema stores a single leaf-level S2 CellId per POI and uses range
-- scans (BETWEEN rangeMin AND rangeMax) instead of exact token matches. The
-- backing Cloud Functions for these UDFs return covering cells at ANY level
-- chosen by the S2RegionCoverer — they are NOT filtered to levels 12/14/16.
--
-- Why separate UDFs?
--   - v3 UDFs (geo.s2_covering, geo.s2_covering_rect) constrain the coverer
--     to levels 12, 14, and 16, matching the discrete tokens stored in the
--     interleaved PointOfInterestLocationIndex table.
--   - v4 UDFs let the coverer pick optimal levels freely (e.g., 12-20), which
--     produces tighter coverings with fewer false positives. The SQL query
--     then converts each covering cell to a leaf-cell range using bitwise
--     arithmetic:
--       lowestSetBit = cell & (-cell)
--       rangeMin     = cell - (lowestSetBit - 1)
--       rangeMax     = cell + (lowestSetBit - 1)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- geo.s2_covering_v4: Compute S2 covering cells for a circular region (v4)
-- -----------------------------------------------------------------------------
-- Given a center point (lat, lng) and radius in meters, returns an array of
-- S2 Cell IDs (as signed INT64) that cover the search region. Unlike
-- geo.s2_covering, the backing Cloud Function does NOT restrict output to
-- levels 12/14/16 — the S2RegionCoverer chooses optimal cell levels freely.
--
-- Usage in a v4 query:
--   WITH covering_ranges AS (
--       SELECT
--           cell - ((cell & (-cell)) - 1) AS range_min,
--           cell + ((cell & (-cell)) - 1) AS range_max
--       FROM (SELECT geo.s2_covering_v4(@lat, @lng, @radius) AS cells),
--            UNNEST(cells) AS cell
--   )
--   SELECT ... FROM covering_ranges cr
--   JOIN PointOfInterest@{FORCE_INDEX=PointOfInterestByS2Cell} poi
--       ON poi.S2CellId BETWEEN cr.range_min AND cr.range_max
-- -----------------------------------------------------------------------------
CREATE FUNCTION geo.s2_covering_v4(
    centerLat    FLOAT64,
    centerLng    FLOAT64,
    radiusMeters FLOAT64
)
RETURNS ARRAY<INT64>
NOT DETERMINISTIC
LANGUAGE REMOTE
OPTIONS (
    endpoint = 'PLACEHOLDER_URL',
    max_batching_rows = 10
);


-- -----------------------------------------------------------------------------
-- geo.s2_covering_rect_v4: Compute S2 covering cells for a rectangle (v4)
-- -----------------------------------------------------------------------------
-- Given a bounding box defined by its south-west (minLat, minLng) and
-- north-east (maxLat, maxLng) corners, returns an array of S2 Cell IDs
-- (as signed INT64) that cover the rectangle. Unlike geo.s2_covering_rect,
-- the backing Cloud Function does NOT restrict output to levels 12/14/16.
-- -----------------------------------------------------------------------------
CREATE FUNCTION geo.s2_covering_rect_v4(
    minLat FLOAT64,
    minLng FLOAT64,
    maxLat FLOAT64,
    maxLng FLOAT64
)
RETURNS ARRAY<INT64>
NOT DETERMINISTIC
LANGUAGE REMOTE
OPTIONS (
    endpoint = 'PLACEHOLDER_URL',
    max_batching_rows = 10
);
