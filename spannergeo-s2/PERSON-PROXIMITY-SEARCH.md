# Person Proximity Search: Why Cell-Join Doesn't Work for Distance Sorting

## The Proposed Approach

A social network use case where:
1. A user searches for people by name (separate query)
2. Results are sorted by distance from the searcher

The proposed solution: assign S2 cell IDs at 3 levels to every individual, then join on cell IDs after name filtering to pick up matches with the same cell IDs, and sort by cell level as a proxy for distance.

## Why This Doesn't Work

### Problem 1: Cell co-location != proximity

Cell matching is **binary** (same cell or not) and suffers from the **cell boundary problem**:

```
Cell A          | Cell B
                |
    You *---1m--|--* Friend X  (different cell -- no match!)
                |
    * Friend Y  |
    140m away   |
  (same cell -- |
   "closer"!)   |
```

Two people 1 meter apart straddling a cell boundary won't match, while someone 140m away in the same cell will. This isn't an edge case -- roughly half of all "near the edge" pairs get split.

### Problem 2: 3 levels gives you 4 buckets, not a distance sort

With levels 12, 14, 16 (~3.3km, ~800m, ~150m):

| Bucket | Meaning | Actual distance range |
|--------|---------|----------------------|
| Match at L16 | Same ~150m cell | 0-212m (cell diagonal) |
| Match at L14 only | Same ~800m cell but different L16 | 0-1.1km |
| Match at L12 only | Same ~3.3km cell but different L14 | 0-4.7km |
| No match | Not in same L12 cell | Could be 1km or 10,000km |

Three issues:
- **The buckets overlap** -- a L14-only match could be closer than a L16 match near the cell edge
- **Most results land in "no match"** -- in a social network, people are typically spread across cities/countries. The coarsest level at ~3.3km captures almost nobody
- **Within each bucket, ordering is arbitrary** -- you can't distinguish 50m from 140m within the same L16 cell

### The deeper issue: S2 cells are a containment structure, not a distance metric

The cell-join idea tries to use a spatial index as a sort key. But S2 cells encode *containment* ("is this point inside this cell?"), not *proximity* ("how far apart are these two points?"). For "nearest first" ordering, you always need to compute actual distances at some point. The question is just how aggressively you prune before computing them.

## What To Do Instead

### Small result sets (friends list, <1000 results): Just compute the distance

If a name search against a user's friends returns 5-50 results, no spatial index is needed. Compute actual distance for each result:

```sql
SELECT f.UserId, f.Name, f.Latitude, f.Longitude,
       geo.s2_distance(f.Latitude, f.Longitude, @myLat, @myLng) AS distance_meters
FROM Friends f
WHERE f.UserId IN UNNEST(@matchedFriendIds)
ORDER BY distance_meters
```

This is O(R) where R is the name-filtered count -- trivially fast. No spatial index, no cell matching, no boundary artifacts, and exact distances for sorting.

### Large result sets ("Show me Johns near me"): Radius search with a name filter

When searching the entire network and expecting many matches, use the covering + post-filter pipeline with the name filter pushed into the scan:

```sql
WITH covering_ranges AS (
    SELECT covering_cell - ((covering_cell & (-covering_cell)) - 1) AS range_min,
           covering_cell + ((covering_cell & (-covering_cell)) - 1) AS range_max
    FROM (SELECT geo.s2_covering(@myLat, @myLng, @radiusMeters) AS cells),
         UNNEST(cells) AS covering_cell
),
candidates AS (
    SELECT DISTINCT u.UserId, u.Name, u.Latitude, u.Longitude
    FROM covering_ranges cr
    JOIN Users@{FORCE_INDEX=UsersByS2Cell} u
        ON u.S2CellId BETWEEN cr.range_min AND cr.range_max
    WHERE u.Name LIKE @namePattern   -- push name filter into the scan (NOTE: just a naive illustration, use Spanner FTS instead for optimal performance)
)
SELECT *, geo.s2_distance(Latitude, Longitude, @myLat, @myLng) AS distance_meters
FROM candidates
WHERE distance_meters <= @radiusMeters
ORDER BY distance_meters
```

K is bounded by spatial density x search area, regardless of how many Johns exist globally. The name filter only reduces K further.

### Large result sets, no radius bound ("Show me all Johns, nearest first"): Expanding radius k-NN

When the user wants global results sorted by distance with no explicit radius, use an expanding radius search:

```
k = 20 (page size)
radius = 5km
for attempt in 1..8:
    results = radius_search("John%", radius)
    if |results| >= k: return top k
    radius *= 2
```

This starts local and expands until there are enough results. Worst case scans a 640km radius, but stops early. First page loads fast (small radius, small K).

### Comparison

| | Cell-join (3 levels) | Covering + post-filter |
|---|---|---|
| Boundary artifacts | Yes -- misses nearby matches | No -- uses actual distance |
| Distance accuracy | 4 coarse buckets | Exact meters |
| Global search cost | Must check all matches at all 3 levels | Starts small, expands only if needed |
| Results beyond 3.3km | Unsorted/unknown | Found via radius doubling |
| Pagination | Difficult -- how to page within a bucket? | Natural -- increase radius or offset |

