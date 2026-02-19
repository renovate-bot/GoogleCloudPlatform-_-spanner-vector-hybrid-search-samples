# Geo-Spatial Indexing on Google Cloud Spanner with S2

**_NOTE:_** This sample uses the term 'UDF' throughout, which is just a synonym for Spanner Remote Functions.

This sample demonstrates how to perform geo-spatial indexing and querying on [Google Cloud Spanner](https://cloud.google.com/spanner) using the [S2 Geometry Library](https://s2geometry.io/). Spanner does not natively support spatial data types or spatial indexes, so we use S2 to encode geographic coordinates into indexable 64-bit cell IDs.

The sample includes two approaches to querying:

- **Client-side S2**: The application computes S2 coverings and binds cell ID ranges as query parameters.
- **Remote UDFs**: Spanner calls Cloud Functions server-side to compute coverings and distances, so the query is self-contained SQL with no client-side S2 dependency.

## Prerequisites

- Java 17+
- Maven 3.8+
- A Google Cloud project with a Spanner instance and database
- `gcloud` CLI installed and authenticated (`gcloud auth application-default login`)
- For Remote UDFs: permissions to deploy Cloud Functions (Gen 2)

## Quick Start

1. **Create the Spanner schema:**

   ```bash
   gcloud spanner databases ddl update YOUR_DATABASE \
     --instance=YOUR_INSTANCE \
     --ddl-file=infra/schema.sql
   ```

2. **Build the project:**

   ```bash
   mvn compile
   ```

3. **Run the demo:**

   ```bash
   mvn exec:java -Dexec.args="YOUR_PROJECT YOUR_INSTANCE YOUR_DATABASE"
   ```

4. **(Optional) Deploy Remote UDFs** for the server-side S2 query demos:

   ```bash
   ./deploy/setup.sh --project YOUR_PROJECT
   ./deploy/deploy-function.sh --project YOUR_PROJECT
   ./deploy/grant-permissions.sh --project YOUR_PROJECT
   ```

   After deployment, `deploy-function.sh` prints the function URLs. Update [`infra/udf_definition.sql`](infra/udf_definition.sql) with the actual URLs (replacing `PLACEHOLDER_URL`), then apply the DDL:

   ```bash
   gcloud spanner databases ddl update YOUR_DATABASE \
     --instance=YOUR_INSTANCE \
     --ddl-file=infra/udf_definition.sql
   ```

## Schema Design

We walk through three progressively refined schema designs. The recommended pattern is **v3** (token index table).

### v1: Naive Lat/Lng Columns ([`schemas/v1_naive.sql`](schemas/v1_naive.sql))

Store raw coordinates with a composite index. Simple but inefficient for radius queries -- scans an entire latitude band.

### v2: Single S2 Cell ID ([`schemas/v2_single_cell.sql`](schemas/v2_single_cell.sql))

Add an S2 Cell ID column at a fixed level (e.g., level 16, ~150m cells). Better, but a fixed level means either too coarse or too fine for different query radii.

### v3: Interleaved Token Index (Recommended) ([`schemas/v3_token_index.sql`](schemas/v3_token_index.sql))

The canonical pattern. Store multiple S2 tokens per location at varying cell levels in an interleaved child table. This balances precision vs. index size and supports queries at any radius. The production schema ([`infra/schema.sql`](infra/schema.sql)) uses this design.

```sql
CREATE TABLE PointOfInterest (
    PoiId     STRING(36) NOT NULL,
    Name      STRING(MAX),
    Category  STRING(256),
    Latitude  FLOAT64 NOT NULL,
    Longitude FLOAT64 NOT NULL,
) PRIMARY KEY (PoiId);

-- One row per S2 token per location, at levels 12, 14, and 16
CREATE TABLE PointOfInterestLocationIndex (
    PoiId      STRING(36) NOT NULL,
    S2CellId   INT64 NOT NULL,
    CellLevel  INT64 NOT NULL,
) PRIMARY KEY (PoiId, S2CellId),
  INTERLEAVE IN PARENT PointOfInterest ON DELETE CASCADE;

CREATE INDEX LocationIndexByS2Cell
    ON PointOfInterestLocationIndex(S2CellId)
    STORING (CellLevel);
```

**Why interleaving?** The child table rows are physically co-located with their parent row in Spanner's storage. This minimizes the number of splits traversed during a join-back from the index to the parent.

**Why multiple levels?** Level 12 cells (~3.3km) are good for coarse regional queries; level 16 cells (~150m) provide fine-grained precision. The covering algorithm picks the right level automatically.

> **Gotcha -- Signed vs. Unsigned:** S2 Cell IDs are unsigned 64-bit integers, but Spanner's `INT64` is signed. We store the raw bit pattern reinterpreted as a signed long. This preserves sort order for range scans within the same face of the S2 cube. The application code handles the sign bit transparently (Java's `long` is signed, and `S2CellId.id()` returns the raw bits).

## Query Patterns

All queries follow the **covering + post-filter** pattern:

1. **Compute a covering:** Find the set of S2 cells that cover the search region.
2. **Query the index:** Match covering cells against `PointOfInterestLocationIndex.S2CellId` via the `LocationIndexByS2Cell` secondary index.
3. **Post-filter:** The covering is an approximation -- some cells extend beyond the search region. Filter candidates with exact distance or bounds.

### Client-Side Queries

These queries require the application to compute S2 coverings and bind cell ID ranges as parameters.

- **Radius search** ([`queries/radius_search.sql`](queries/radius_search.sql)) -- Find all POIs within a given distance from a point.
- **Bounding box search** ([`queries/bbox_search.sql`](queries/bbox_search.sql)) -- Find all POIs within a rectangle.
- **Approximate k-NN** ([`queries/knn_approx.sql`](queries/knn_approx.sql)) -- Find the k closest POIs using iterative radius expansion.

### Remote UDF Queries

With Remote UDFs deployed, queries become self-contained SQL -- no client-side S2 library needed. The client only provides coordinates and a radius or bounding box.

- **Radius search** ([`queries/udf_query.sql`](queries/udf_query.sql)) -- Uses `geo.s2_covering()` for covering cells and `geo.s2_distance()` for post-filtering.
- **Bounding box search** ([`queries/udf_bbox_query.sql`](queries/udf_bbox_query.sql)) -- Uses `geo.s2_covering_rect()` for covering cells with exact lat/lng post-filter.
- **Approximate k-NN** ([`queries/udf_knn_query.sql`](queries/udf_knn_query.sql)) -- Uses `geo.s2_covering()` and `geo.s2_distance()` with `LIMIT @k`. Iterative expansion is handled in application code.

Here is the UDF radius search query as an example. The client only provides `(lat, lng, radius)`:

```sql
WITH candidates AS (
    SELECT DISTINCT poi.PoiId, poi.Name, poi.Category, poi.Latitude, poi.Longitude
    FROM (SELECT geo.s2_covering(@centerLat, @centerLng, @radiusMeters) AS cells),
         UNNEST(cells) AS covering_cell
    JOIN PointOfInterestLocationIndex idx ON idx.S2CellId = covering_cell
    JOIN PointOfInterest poi ON poi.PoiId = idx.PoiId
),
with_distance AS (
    SELECT c.PoiId, c.Name, c.Category, c.Latitude, c.Longitude,
           geo.s2_distance(c.Latitude, c.Longitude, @centerLat, @centerLng) AS distance_meters
    FROM candidates c
)
SELECT * FROM with_distance
WHERE distance_meters <= @radiusMeters
ORDER BY distance_meters;
```

Three Remote UDFs power these queries:

| UDF | Purpose |
|-----|---------|
| `geo.s2_covering(lat, lng, radius)` | Returns `ARRAY<INT64>` of S2 cell IDs covering a search circle |
| `geo.s2_covering_rect(minLat, minLng, maxLat, maxLng)` | Returns `ARRAY<INT64>` of S2 cell IDs covering a bounding box |
| `geo.s2_distance(lat1, lng1, lat2, lng2)` | Returns great-circle distance in meters between two points |

> **Note:** Remote UDFs must live in a named schema (Spanner does not allow them in the default schema). This sample uses the `geo` schema. Additionally, `UNNEST` of a Remote UDF result requires materializing the array in a subquery first -- `UNNEST(geo.s2_covering(...))` directly in `FROM` is not supported.

## Remote UDFs

Remote UDFs push S2 logic into Spanner so queries don't require a client-side S2 library. Three Cloud Functions back the UDFs:

| Cloud Function | Entry Point | Spanner UDF |
|----------------|-------------|-------------|
| `s2-covering` | `S2CoveringFunction` | `geo.s2_covering()` |
| `s2-covering-rect` | `S2CoveringRectFunction` | `geo.s2_covering_rect()` |
| `s2-distance` | `S2DistanceFunction` | `geo.s2_distance()` |

### Cloud Function Implementation

The Cloud Functions live in [`cloud-function/`](cloud-function/) as a separate Maven project:

- [`S2CoveringFunction.java`](cloud-function/src/main/java/com/example/spannergeo/functions/S2CoveringFunction.java) -- Computes S2 coverings for a circular region at levels 12, 14, 16. Returns cell IDs as **JSON strings** (not numbers) because S2 cell IDs exceed JSON's safe integer limit of 2^53.
- [`S2CoveringRectFunction.java`](cloud-function/src/main/java/com/example/spannergeo/functions/S2CoveringRectFunction.java) -- Computes S2 coverings for a rectangular region (bounding box) at levels 12, 14, 16. Same wire protocol and cell ID encoding as `S2CoveringFunction`.
- [`S2DistanceFunction.java`](cloud-function/src/main/java/com/example/spannergeo/functions/S2DistanceFunction.java) -- Computes great-circle distance using `S2LatLng.getDistance()`.

All three implement the [Spanner Remote UDF wire protocol](https://cloud.google.com/spanner/docs/remote-functions):
- **Request:** `{"requestId": "...", "calls": [[args_row1], [args_row2], ...]}`
- **Response:** `{"replies": [result1, result2, ...]}` (array length must match `calls`)

### Deploying Remote UDFs

Deployment scripts are in [`deploy/`](deploy/). Run them in order:

```bash
# 1. Enable required GCP APIs (Cloud Functions, Cloud Build, Cloud Run, Spanner, Artifact Registry)
./deploy/setup.sh --project YOUR_PROJECT

# 2. Deploy all three Cloud Functions (builds with Maven, then deploys)
./deploy/deploy-function.sh --project YOUR_PROJECT

# 3. Grant Spanner's service agent permission to invoke the functions
./deploy/grant-permissions.sh --project YOUR_PROJECT
```

After deployment, `deploy-function.sh` prints the function URLs. Update [`infra/udf_definition.sql`](infra/udf_definition.sql) with the actual URLs (replacing `PLACEHOLDER_URL`), then apply the DDL:

```bash
gcloud spanner databases ddl update YOUR_DATABASE \
  --instance=YOUR_INSTANCE \
  --ddl-file=infra/udf_definition.sql
```

### Tearing Down

```bash
./deploy/teardown.sh --project YOUR_PROJECT
```

This deletes the Cloud Functions and removes the project-level `roles/spanner.serviceAgent` IAM binding that was granted to the Spanner service agent. To also remove the UDF definitions from Spanner:

```sql
DROP FUNCTION IF EXISTS geo.s2_covering;
DROP FUNCTION IF EXISTS geo.s2_covering_rect;
DROP FUNCTION IF EXISTS geo.s2_distance;
DROP SCHEMA IF EXISTS geo;
```

## Java Application

The Java application demonstrates the full workflow: seeding data, indexing with S2, and running all six query types (three client-side, three Remote UDF).

### Key Classes

| Class | Purpose |
|-------|---------|
| [`App.java`](src/main/java/com/example/spannergeo/App.java) | Entry point -- seeds data and runs all demo queries |
| [`S2Util.java`](src/main/java/com/example/spannergeo/S2Util.java) | S2 helper: cell ID encoding at levels 12/14/16, covering computation for circles and rectangles, Haversine distance |
| [`SpannerGeoDao.java`](src/main/java/com/example/spannergeo/SpannerGeoDao.java) | Spanner data access with parameterized queries -- both client-side and UDF variants |
| [`Location.java`](src/main/java/com/example/spannergeo/model/Location.java) | POJO for a geo-tagged point of interest |

### How Ingestion Works

When saving a location, the app:

1. Computes S2 Cell IDs at levels 12, 14, and 16 using `S2Util.encodeCellIds()`
2. Writes the parent `PointOfInterest` row and one `PointOfInterestLocationIndex` row per cell level
3. Uses `INSERT_OR_UPDATE` mutations so re-running the demo is idempotent (no duplicate rows)

All mutations happen in a single Spanner transaction.

### How Querying Works

**Client-side approach** (e.g., `SpannerGeoDao.radiusSearch()`):
1. `S2Util.computeCovering()` returns a list of S2 cell ID ranges
2. The DAO builds a parameterized query with `BETWEEN` clauses per range
3. Results are post-filtered by exact Haversine distance and sorted

**Remote UDF approach** (e.g., `SpannerGeoDao.radiusSearchWithUdf()`):
1. The DAO sends a single parameterized query with `@centerLat`, `@centerLng`, `@radiusMeters`
2. `geo.s2_covering()` computes covering cells server-side
3. `geo.s2_distance()` computes exact distances server-side
4. No S2 library dependency in the DAO method -- pure SQL

**Remote UDF bounding box** (`SpannerGeoDao.bboxSearchWithUdf()`):
1. Sends a single query with `@minLat`, `@minLng`, `@maxLat`, `@maxLng`
2. `geo.s2_covering_rect()` computes covering cells server-side
3. Post-filters with exact lat/lng bounds -- no distance computation needed

**Remote UDF k-NN** (`SpannerGeoDao.knnSearchWithUdf()`):
1. Wraps `radiusSearchWithUdf()` with iterative radius doubling (starts at 500m, up to 8 attempts)
2. Returns the top k results by distance

The UDF demos in `App.java` are wrapped in try-catch, so the app runs cleanly whether or not UDFs are deployed.

## Sample Output

```
=== Seeding sample data (San Francisco landmarks) ===

Inserted 15 locations.

============================================================
  CLIENT-SIDE QUERIES (S2 computed on the application)
============================================================

--- Radius Search ---
Center: (37.7880, -122.4075), Radius: 2000m

Found 8 location(s):
  0m - Union Square (shopping) [37.788000, -122.407500]
  345m - Chinatown Gate (landmark) [37.790800, -122.405800]
  724m - Moscone Center (convention) [37.784000, -122.401000]
  901m - Transamerica Pyramid (landmark) [37.795200, -122.402800]
  1418m - City Hall (government) [37.779300, -122.419300]
  1492m - Ferry Building (landmark) [37.795600, -122.393500]
  1608m - Coit Tower (landmark) [37.802400, -122.405800]
  1911m - AT&T Park (Oracle Park) (sports) [37.778600, -122.389300]

--- Bounding Box Search ---
Box: (37.775, -122.420) to (37.795, -122.400)

Found 4 location(s):
  ...

--- Approximate k-NN Search (k=3) ---
Center: (37.7880, -122.4075)

Closest 3 location(s):
  0m - Union Square (shopping) [37.788000, -122.407500]
  345m - Chinatown Gate (landmark) [37.790800, -122.405800]
  724m - Moscone Center (convention) [37.784000, -122.401000]

============================================================
  REMOTE UDF QUERIES (S2 computed server-side)
============================================================

--- Radius Search with Remote UDFs ---
Center: (37.7880, -122.4075), Radius: 2000m

Found 8 location(s):
  0m - Union Square (shopping) [37.788000, -122.407500]
  345m - Chinatown Gate (landmark) [37.790800, -122.405800]
  ...

--- Bounding Box Search with Remote UDFs ---
Box: (37.775, -122.420) to (37.795, -122.400)

Found 4 location(s):
  ...

--- Approximate k-NN Search with Remote UDFs (k=3) ---
Center: (37.7880, -122.4075)

Closest 3 location(s):
  ...

Done.
```

If Remote UDFs are not deployed, the UDF section prints a message and continues:

```
--- Radius Search with Remote UDFs ---
Center: (37.7880, -122.4075), Radius: 2000m

Remote UDFs not deployed, skipping radius search.
  Deploy with: ./deploy/deploy-function.sh
  Error: ...
```

## Project Structure

```
sample/
├── README.md                 # This file
├── pom.xml                   # Maven project (main application)
│
├── infra/
│   ├── schema.sql            # Production schema (v3 token index)
│   └── udf_definition.sql    # Remote UDF DDL (geo.s2_covering, geo.s2_covering_rect, geo.s2_distance)
│
├── schemas/                  # All schema iterations (for reference)
│   ├── v1_naive.sql
│   ├── v2_single_cell.sql
│   └── v3_token_index.sql
│
├── queries/
│   ├── radius_search.sql     # Client-side radius search
│   ├── bbox_search.sql       # Client-side bounding box query
│   ├── knn_approx.sql        # Client-side approximate k-NN query
│   ├── udf_query.sql         # UDF radius search
│   ├── udf_bbox_query.sql    # UDF bounding box search
│   └── udf_knn_query.sql     # UDF approximate k-NN search
│
├── cloud-function/           # Cloud Functions backing Remote UDFs
│   ├── pom.xml               # Separate Maven project
│   └── src/main/java/.../functions/
│       ├── S2CoveringFunction.java
│       ├── S2CoveringRectFunction.java
│       └── S2DistanceFunction.java
│
├── deploy/                   # Deployment & IAM scripts
│   ├── setup.sh              # Enable GCP APIs
│   ├── deploy-function.sh    # Deploy Cloud Functions
│   ├── grant-permissions.sh  # IAM: Spanner service agent -> project-level serviceAgent role
│   └── teardown.sh           # Clean up Cloud Functions
│
└── src/main/java/.../
    ├── App.java              # Entry point -- seeds data, runs all queries
    ├── S2Util.java           # S2 geometry utilities
    ├── SpannerGeoDao.java    # Spanner data access (client-side + UDF queries)
    └── model/
        └── Location.java     # Location POJO
```

## S2 Cell Level Reference

| Level | Approx. Cell Size | Good For |
|-------|-------------------|----------|
| 12    | ~3.3 km           | City-wide queries, coarse regional filtering |
| 14    | ~800 m            | Neighborhood-level queries |
| 16    | ~150 m            | Street-level queries, fine-grained precision |
| 20    | ~10 m             | Building-level (not used in this sample) |
| 30    | ~1 cm             | Maximum precision (leaf cell) |


## Some Gotchas to Keep in Mind

**Remote Functions must live in a named schema**. Spanner does not allow Remote Functions in the default schema. We use `CREATE SCHEMA IF NOT EXISTS geo` and qualify all calls as `geo.s2_covering()`, `geo.s2_covering_rect()`, and `geo.s2_distance()`.

**`UNNEST` of a Remote Function result requires a subquery**. Writing `FROM UNNEST(geo.s2_covering(...))` directly in the `FROM` clause does not work. You must materialize the array in a subquery first: `FROM (SELECT geo.s2_covering(...) AS cells), UNNEST(cells)`. This is a peculiarity of how `UNNEST` works with remote functions in Spanner today.

**S2 Cell IDs must be returned as JSON strings from the Cloud Function**. S2 Cell IDs are 64-bit integers that exceed JavaScript/JSON’s safe integer limit of 2⁵³. The Cloud Function returns them as strings (`"3860680815790637056"` instead of `3860680815790637056`), and Spanner parses them into `INT64` automatically.

**Signed vs Unsigned integers. S2 Cell IDs are unsigned 64-bit integers**. Spanner’s `INT64` is signed. We store the raw bit pattern, which means some Cell IDs appear as negative numbers in Spanner. Java's `long` is signed too, and `S2CellId.id()` returns the same raw bits. Range scans within the same S2 cube face (which is the common case for geographically bounded queries) sort correctly regardless of the sign interpretation.

**Cold start**. You may see increased latency in your queries due to cold start time associated with Java based Cloud functions. You can address this by setting a minimum number of instances for use by Cloud Run functions to `1` (it's `0` by default) as recommended [here](https://docs.cloud.google.com/run/docs/tips/functions-best-practices#min).

## References

- [S2 Geometry Library](https://s2geometry.io/)
- [S2 Cell Hierarchy](https://s2geometry.io/devguide/s2cell_hierarchy.html)
- [Google Cloud Spanner DDL Reference](https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language)
- [Spanner Remote Functions](https://docs.cloud.google.com/spanner/docs/cloud-run-remote-function)
- [Spanner Interleaved Tables](https://cloud.google.com/spanner/docs/schema-and-data-model#creating-interleaved-tables)
