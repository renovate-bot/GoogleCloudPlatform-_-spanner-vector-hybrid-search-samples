# Faceting and Aggregations Sample

[Back to Elasticsearch to Spanner samples](../README.md)

This sample compares Elasticsearch search-time aggregations with Spanner SQL aggregations over a Full Text Search result set. It also includes a category and brand rollup query that represents the kind of broader analytical aggregation where Spanner columnar execution can help when columnar is enabled for the database.

## Elasticsearch

Elasticsearch facets are usually implemented with aggregations attached to the same `_search` request that returns hits. Search terms define the candidate document set, exact filters constrain the request, and aggregations compute buckets or metrics over the matching documents.

### Index Setup

Before adding facets or metrics, decide how each field should be used:

- Search text, such as `title` and `description`, should be mapped as `text`.
- Values used for facet buckets, such as `category`, `brand`, `colors`, `sizes`, and `status`, should be mapped as `keyword`. Elasticsearch then counts complete values like `footwear` or `Summit`, not the tokens inside those values.
- Values used for ranges or calculations, such as `price_cents`, `rating`, and `inventory_count`, should be mapped as numeric fields.

```json
{
  "mappings": {
    "properties": {
      "tenant_id": {"type": "keyword"},
      "product_id": {"type": "keyword"},
      "title": {"type": "text"},
      "description": {"type": "text"},
      "category": {"type": "keyword"},
      "brand": {"type": "keyword"},
      "colors": {"type": "keyword"},
      "sizes": {"type": "keyword"},
      "price_cents": {"type": "integer"},
      "rating": {"type": "float"},
      "inventory_count": {"type": "integer"},
      "status": {"type": "keyword"}
    }
  }
}
```

Create the index with curl:

```bash
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  http://localhost:9200/sample_faceting_aggregations \
  -d @samples/faceting-aggregations/elasticsearch/index.json
```

Load the sample data:

```bash
curl -fsS -X POST \
  -H 'Content-Type: application/x-ndjson' \
  'http://localhost:9200/sample_faceting_aggregations/_bulk?refresh=true' \
  --data-binary @samples/faceting-aggregations/elasticsearch/documents.ndjson
```

### Query Shape

The representative query searches `waterproof trail`, returns the top product hits, and computes category, brand, color, size, price-range, average-price, and inventory aggregations over the same matching document set.

```json
{
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "query": "waterproof trail",
            "fields": ["title^3", "description"],
            "operator": "and"
          }
        }
      ],
      "filter": [
        {"term": {"tenant_id": "tenant-a"}},
        {"term": {"status": "active"}}
      ]
    }
  },
  "size": 5,
  "aggs": {
    "categories": {"terms": {"field": "category", "size": 10}},
    "brands": {"terms": {"field": "brand", "size": 10}},
    "colors": {"terms": {"field": "colors", "size": 10}},
    "sizes": {"terms": {"field": "sizes", "size": 10}},
    "price_ranges": {
      "range": {
        "field": "price_cents",
        "ranges": [{"to": 10000}, {"from": 10000, "to": 15000}, {"from": 15000}]
      }
    },
    "avg_price_cents": {"avg": {"field": "price_cents"}},
    "total_inventory": {"sum": {"field": "inventory_count"}}
  }
}
```

Run it with curl:

```bash
curl -fsS -X GET \
  -H 'Content-Type: application/json' \
  'http://localhost:9200/sample_faceting_aggregations/_search?pretty' \
  -d @samples/faceting-aggregations/elasticsearch/queries/search-waterproof-trail-facets.json
```

The filtered query uses `post_filter` to filter hits by selected facet values while leaving the aggregation buckets based on the broader search result set.

```bash
curl -fsS -X GET \
  -H 'Content-Type: application/json' \
  'http://localhost:9200/sample_faceting_aggregations/_search?pretty' \
  -d @samples/faceting-aggregations/elasticsearch/queries/search-waterproof-trail-filtered.json
```

The convenience scripts wrap the same curl calls:

```bash
samples/scripts/start_elasticsearch.sh
samples/scripts/load_elasticsearch_sample.sh faceting-aggregations
samples/scripts/run_elasticsearch_query.sh faceting-aggregations search-waterproof-trail-facets
samples/scripts/run_elasticsearch_query.sh faceting-aggregations search-waterproof-trail-filtered
```

## Spanner

In Spanner, facets are SQL aggregations over rows that match the search predicate. The model separates the responsibilities: `SEARCH(...)` identifies matching rows, ordinary SQL predicates apply structured filters, `GROUP BY` computes scalar facets, `UNNEST` computes array facets, `CASE` builds range buckets, and aggregate functions compute metrics.

### Schema and Search Index

```sql
CREATE TABLE FacetedProducts (
  TenantId STRING(36) NOT NULL,
  ProductId STRING(64) NOT NULL,
  Title STRING(MAX) NOT NULL,
  Description STRING(MAX),
  Category STRING(64),
  Brand STRING(64),
  Colors ARRAY<STRING(32)>,
  Sizes ARRAY<STRING(32)>,
  PriceCents INT64,
  Rating FLOAT64,
  InventoryCount INT64,
  Status STRING(32),
  UpdatedAt TIMESTAMP,
  TitleTokens TOKENLIST AS (
    TOKENIZE_FULLTEXT(Title, token_category=>"title")
  ) HIDDEN,
  DescriptionTokens TOKENLIST AS (
    TOKENIZE_FULLTEXT(Description)
  ) HIDDEN,
  SearchTokens TOKENLIST AS (
    TOKENLIST_CONCAT([TitleTokens, DescriptionTokens])
  ) HIDDEN
) PRIMARY KEY (TenantId, ProductId);

CREATE SEARCH INDEX FacetedProductsByText
ON FacetedProducts(SearchTokens)
STORING (
  Title,
  Category,
  Brand,
  Colors,
  Sizes,
  PriceCents,
  Rating,
  InventoryCount,
  Status,
  UpdatedAt
)
PARTITION BY TenantId;
```

### Query Shape

The main query uses a `SearchResults` CTE as the shared candidate set. The final result returns one row with a result page, facet buckets, price-range buckets, and numeric metrics.

```sql
WITH SearchResults AS (
  SELECT ProductId, Title, Category, Brand, Colors, Sizes, PriceCents, Rating, InventoryCount
  FROM FacetedProducts
  WHERE TenantId = @tenant_id
    AND Status = 'active'
    AND SEARCH(SearchTokens, 'waterproof trail')
),
CategoryFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Category AS Value, ProductCount
    FROM (
      SELECT Category, COUNT(*) AS ProductCount
      FROM SearchResults
      GROUP BY Category
    )
    ORDER BY ProductCount DESC, Category ASC
  ) AS Buckets
),
ColorFacets AS (
  SELECT ARRAY(
    SELECT AS STRUCT Color AS Value, ProductCount
    FROM (
      SELECT Color, COUNT(*) AS ProductCount
      FROM SearchResults, UNNEST(Colors) AS Color
      GROUP BY Color
    )
    ORDER BY ProductCount DESC, Color ASC
  ) AS Buckets
),
Metrics AS (
  SELECT COUNT(*) AS ResultCount, AVG(PriceCents) AS AvgPriceCents, SUM(InventoryCount) AS TotalInventory
  FROM SearchResults
)
SELECT CategoryFacets.Buckets AS CategoryFacets, ColorFacets.Buckets AS ColorFacets, Metrics.*
FROM CategoryFacets, ColorFacets, Metrics;
```

The checked-in `search-waterproof-trail-facets.sql` file includes the full version with result rows, category, brand, color, size, price-range, average-price, and inventory output. The `search-waterproof-trail-filtered.sql` query shows how to model Elasticsearch-style `post_filter`: compute facet buckets from the broader search result set, then apply selected facet filters to the result page and metrics.

Deploy and run against the Spanner database from `.env`:

```bash
samples/scripts/apply_spanner_sample_schema.sh faceting-aggregations
samples/scripts/seed_spanner_sample.sh faceting-aggregations
samples/scripts/run_spanner_query.sh faceting-aggregations search-waterproof-trail-facets
samples/scripts/run_spanner_query.sh faceting-aggregations search-waterproof-trail-filtered
```

### Columnar Rollup

Not every aggregation belongs on the interactive search request path. Facets such as category or brand counts are usually computed from the current search result set because the UI needs them alongside the hits. Broader analytics, such as category and brand summaries across the entire product catalog, may scan many more rows, touch fewer columns, and run on a reporting or dashboard path rather than on every search request. Those queries can use the same Spanner table and the same SQL aggregation model, but they are better candidates for Spanner columnar execution when the columnar engine is enabled. The sample includes `spanner/queries/columnar-category-rollup.sql`:

```sql
SELECT
  Category,
  Brand,
  COUNT(*) AS ProductCount,
  AVG(PriceCents) AS AvgPriceCents,
  SUM(InventoryCount) AS TotalInventory,
  AVG(Rating) AS AvgRating
FROM FacetedProducts
WHERE TenantId = @tenant_id
  AND Status = 'active'
GROUP BY Category, Brand
ORDER BY Category, ProductCount DESC, Brand;
```

Columnar is not a separate query language and it is not a replacement for the search index. The application still issues SQL against the same Spanner tables. When the columnar engine is enabled, Spanner can execute suitable scan-heavy aggregation queries by reading selected columns from the columnar store instead of using only the row-oriented execution path. Use this for broad analytics such as catalog rollups, reporting, and dashboards.

## Behavior Notes

Elasticsearch aggregations are embedded in the search request and returned in the same JSON response as hits. Spanner expresses the same behavior with composable SQL: a search CTE, relational filters, `GROUP BY`, `UNNEST`, range-bucket `CASE` expressions, and aggregate functions.

For migration, preserve the user-facing contract rather than the Elasticsearch response shape. Decide which facets are computed before selected filters, which are computed after selected filters, how array-valued facets should count one product that has multiple values, and whether each aggregation must be transactionally current or can come from a cached or analytical path.
