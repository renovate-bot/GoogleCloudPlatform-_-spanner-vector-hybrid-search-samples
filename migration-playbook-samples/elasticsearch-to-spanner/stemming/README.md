# Stemming Sample

[Back to Elasticsearch to Spanner samples](../README.md)

This sample compares an Elasticsearch index that stems English product text with a Spanner Full Text Search schema that uses `TOKENIZE_FULLTEXT` and `SEARCH(..., enhance_query=>true)`.

## Elasticsearch

The Elastic index definition in `elasticsearch/index.json` defines a custom `english_stemmed` analyzer. It uses a standard tokenizer plus lowercase, English stopword, keyword marker, and English stemmer filters. The `title` and `description` fields use that analyzer for both indexing and search.

### Analyzer and Index Setup

The checked-in index body is the source schema plus analyzer setup:

```json
{
  "settings": {
    "analysis": {
      "filter": {
        "english_stop": {
          "type": "stop",
          "stopwords": "_english_"
        },
        "english_keywords": {
          "type": "keyword_marker",
          "keywords": ["skyline"]
        },
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        },
        "english_possessive_stemmer": {
          "type": "stemmer",
          "language": "possessive_english"
        }
      },
      "analyzer": {
        "english_stemmed": {
          "tokenizer": "standard",
          "filter": [
            "english_possessive_stemmer",
            "lowercase",
            "english_stop",
            "english_keywords",
            "english_stemmer"
          ]
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "tenant_id": {"type": "keyword"},
      "product_id": {"type": "keyword"},
      "title": {
        "type": "text",
        "analyzer": "english_stemmed",
        "search_analyzer": "english_stemmed",
        "fields": {
          "keyword": {"type": "keyword"}
        }
      },
      "description": {
        "type": "text",
        "analyzer": "english_stemmed",
        "search_analyzer": "english_stemmed"
      },
      "category": {"type": "keyword"},
      "status": {"type": "keyword"}
    }
  }
}
```

Create the index with curl:

```bash
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  http://localhost:9200/sample_stemming \
  -d @samples/stemming/elasticsearch/index.json
```

Load the sample data:

```bash
curl -fsS -X POST \
  -H 'Content-Type: application/x-ndjson' \
  'http://localhost:9200/sample_stemming/_bulk?refresh=true' \
  --data-binary @samples/stemming/elasticsearch/documents.ndjson
```

### Query Shape

The representative Elasticsearch query uses `multi_match` for the stemmed text fields and `term` filters for exact tenant and status constraints:

```json
{
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "query": "running",
            "fields": ["title^2", "description"]
          }
        }
      ],
      "filter": [
        {"term": {"tenant_id": "tenant-a"}},
        {"term": {"status": "active"}}
      ]
    }
  },
  "_source": ["product_id", "title", "description"]
}
```

Run it with curl:

```bash
curl -fsS -X GET \
  -H 'Content-Type: application/json' \
  'http://localhost:9200/sample_stemming/_search?pretty' \
  -d @samples/stemming/elasticsearch/queries/search-running.json
```

The `search-hiking.json` and `search-walking.json` files use the same query shape with different search text.

The convenience scripts wrap the same curl calls:

```bash
samples/scripts/start_elasticsearch.sh
samples/scripts/load_elasticsearch_sample.sh stemming
samples/scripts/run_elasticsearch_query.sh stemming search-running
samples/scripts/run_elasticsearch_query.sh stemming search-hiking
samples/scripts/run_elasticsearch_query.sh stemming search-walking
```

## Spanner

The Spanner DDL in `spanner/schema.sql` keeps the product text as normal table columns, adds hidden generated `TOKENLIST` columns, combines them into one search surface, and creates a partitioned search index.

### Schema and Search Index

```sql
CREATE TABLE SearchProducts (
  TenantId STRING(36) NOT NULL,
  ProductId STRING(64) NOT NULL,
  Title STRING(MAX) NOT NULL,
  Description STRING(MAX),
  Category STRING(64),
  Status STRING(32),
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

CREATE SEARCH INDEX SearchProductsByText
ON SearchProducts(SearchTokens)
STORING (Title, Description, Category, Status)
PARTITION BY TenantId;
```

The sample rows in `spanner/seed-data.sql` mirror the Elasticsearch fixture documents so the two systems can be compared against the same tiny corpus.

The query examples live under `spanner/queries`. Spanner does not configure a per-index English stemmer the way Elasticsearch does. The default examples add `enhance_query=>true` to `SEARCH(...)` so Spanner broadens the search term to related word forms at query time. The `*-strict.sql` variants omit `enhance_query` and are useful when enhanced expansion is too broad for a particular application query.

### Query Shape

The enhanced query version is the closest starting point when the application expects word-form expansion:

```sql
SELECT
  ProductId,
  Title,
  Description
FROM SearchProducts
WHERE TenantId = @tenant_id
  AND Status = 'active'
  AND SEARCH(SearchTokens, 'running', enhance_query=>true)
LIMIT 25;
```

The strict version omits `enhance_query` when broader expansion is not desired:

```sql
SELECT
  ProductId,
  Title,
  Description
FROM SearchProducts
WHERE TenantId = @tenant_id
  AND Status = 'active'
  AND SEARCH(SearchTokens, 'running')
LIMIT 25;
```

Deploy and run against the Spanner database from `.env`:

```bash
samples/scripts/apply_spanner_sample_schema.sh stemming
samples/scripts/seed_spanner_sample.sh stemming
samples/scripts/run_spanner_query.sh stemming search-running
samples/scripts/run_spanner_query.sh stemming search-hiking
samples/scripts/run_spanner_query.sh stemming search-walking
```

## Behavior Notes

Elasticsearch stemming is analyzer-driven and deterministic for the configured language analyzer. Spanner's enhanced query mode is a query-time expansion feature. It is often the right migration target for English word-form matching, but it is not a byte-for-byte equivalent to a specific Elasticsearch stemmer. Validate high-value production queries before treating the behavior as acceptable.

With this fixture, the observed result counts are:

| Query | Elasticsearch English stemmer | Spanner enhanced query | Spanner strict search |
| :---- | :---- | :---- | :---- |
| `running` | 2 | 2 | 1 |
| `hiking` | 1 | 2 | 1 |
| `walking` | 1 | 2 | 1 |

That difference is the point of the sample: enhanced query can recover useful word-form matches, but it can also expand semantically related terms more broadly than an Elasticsearch stemmer.
