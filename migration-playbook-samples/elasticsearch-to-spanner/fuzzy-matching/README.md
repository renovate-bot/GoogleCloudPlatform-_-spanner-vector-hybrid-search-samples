# Fuzzy Matching Sample

[Back to Elasticsearch to Spanner samples](../README.md)

This sample compares Elasticsearch typo-tolerant text search with a Spanner Full Text Search schema that uses n-gram substring tokenization, `SEARCH_NGRAMS`, and `SCORE_NGRAMS`.

## Elasticsearch

Elasticsearch fuzzy matching is query-driven in this sample. The indexed field is a normal analyzed `text` field, and the query asks Elasticsearch to expand query terms with `fuzziness: "AUTO"`.

### Index Setup

The checked-in index body keeps the mapping intentionally small:

```json
{
  "mappings": {
    "properties": {
      "tenant_id": {"type": "keyword"},
      "product_id": {"type": "keyword"},
      "title": {"type": "text"},
      "brand": {"type": "keyword"},
      "status": {"type": "keyword"}
    }
  }
}
```

Create the index with curl:

```bash
curl -fsS -X PUT \
  -H 'Content-Type: application/json' \
  http://localhost:9200/sample_fuzzy_matching \
  -d @samples/fuzzy-matching/elasticsearch/index.json
```

Load the sample data:

```bash
curl -fsS -X POST \
  -H 'Content-Type: application/x-ndjson' \
  'http://localhost:9200/sample_fuzzy_matching/_bulk?refresh=true' \
  --data-binary @samples/fuzzy-matching/elasticsearch/documents.ndjson
```

### Query Shape

The representative query searches a misspelled product title. The `match` query analyzes the input, applies fuzzy expansion per term, and combines that text condition with exact filters for tenant and status.

```json
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "title": {
              "query": "trail runing shoo",
              "fuzziness": "AUTO",
              "operator": "and",
              "prefix_length": 1,
              "max_expansions": 50
            }
          }
        }
      ],
      "filter": [
        {"term": {"tenant_id": "tenant-a"}},
        {"term": {"status": "active"}}
      ]
    }
  },
  "_source": ["product_id", "title", "brand"]
}
```

Run it with curl:

```bash
curl -fsS -X GET \
  -H 'Content-Type: application/json' \
  'http://localhost:9200/sample_fuzzy_matching/_search?pretty' \
  -d @samples/fuzzy-matching/elasticsearch/queries/search-trail-runing-shoo.json
```

The `search-waterproff-hicking-boot.json` and `search-city-walkng-sneker.json` files use the same query shape with different misspelled input.

The convenience scripts wrap the same curl calls:

```bash
samples/scripts/start_elasticsearch.sh
samples/scripts/load_elasticsearch_sample.sh fuzzy-matching
samples/scripts/run_elasticsearch_query.sh fuzzy-matching search-trail-runing-shoo
samples/scripts/run_elasticsearch_query.sh fuzzy-matching search-waterproff-hicking-boot
samples/scripts/run_elasticsearch_query.sh fuzzy-matching search-city-walkng-sneker
```

## Spanner

Spanner does not expose an Elasticsearch-style edit-distance `fuzziness` switch on `SEARCH(...)`. For typo-tolerant matching on short product names, use substring n-gram tokenization and rank candidates with n-gram similarity.

### Schema and Search Index

```sql
CREATE TABLE FuzzyProducts (
  TenantId STRING(36) NOT NULL,
  ProductId STRING(64) NOT NULL,
  Title STRING(MAX) NOT NULL,
  Brand STRING(64),
  Status STRING(32),
  TitleNgrams TOKENLIST AS (
    TOKENIZE_SUBSTRING(
      Title,
      ngram_size_min=>2,
      ngram_size_max=>3,
      relative_search_types=>["word_prefix", "word_suffix"]
    )
  ) HIDDEN
) PRIMARY KEY (TenantId, ProductId);

CREATE SEARCH INDEX FuzzyProductsByTitleNgrams
ON FuzzyProducts(TitleNgrams)
STORING (Title, Brand, Status)
PARTITION BY TenantId;
```

The sample rows in `spanner/seed-data.sql` mirror the Elasticsearch fixture documents.

### Query Shape

`SEARCH_NGRAMS` finds candidate rows that share enough n-grams with the misspelled input. `SCORE_NGRAMS` scores those candidates by similarity so the closest title can be ranked first, and an outer `Score >= 0.5` predicate removes low-similarity candidates for this fixture.

```sql
SELECT
  ProductId,
  Title,
  Brand,
  Score
FROM (
  SELECT
    ProductId,
    Title,
    Brand,
    SCORE_NGRAMS(TitleNgrams, 'trail runing shoo') AS Score
  FROM FuzzyProducts
  WHERE TenantId = @tenant_id
    AND Status = 'active'
    AND SEARCH_NGRAMS(TitleNgrams, 'trail runing shoo')
  LIMIT 10000
)
WHERE Score >= 0.5
ORDER BY Score DESC
LIMIT 10;
```

Deploy and run against the Spanner database from `.env`:

```bash
samples/scripts/apply_spanner_sample_schema.sh fuzzy-matching
samples/scripts/seed_spanner_sample.sh fuzzy-matching
samples/scripts/run_spanner_query.sh fuzzy-matching search-trail-runing-shoo
samples/scripts/run_spanner_query.sh fuzzy-matching search-waterproff-hicking-boot
samples/scripts/run_spanner_query.sh fuzzy-matching search-city-walkng-sneker
```

## Behavior Notes

Elasticsearch `fuzziness` expands analyzed query terms by edit distance. Spanner n-gram fuzzy search uses indexed substrings to find candidates and `SCORE_NGRAMS` to rank approximate matches. This is a useful target pattern for short fields such as product names, person names, city names, and other compact labels, but it is not a direct semantic equivalent to Elasticsearch fuzziness.

Tune n-gram size, `SEARCH_NGRAMS` candidate behavior, and the `SCORE_NGRAMS` cutoff with real query logs. Lower score cutoffs usually increase recall and noise; higher score cutoffs usually improve precision but can miss harder typos.
