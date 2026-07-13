# Custom Dictionaries Sample

[Back to Elasticsearch to Spanner samples](../README.md)

This sample compares an Elasticsearch search-time custom dictionary with a Spanner Full Text Search schema that uses `TOKENIZE_FULLTEXT` and `SEARCH(..., enhance_query=>true)`.

## Elasticsearch

The Elasticsearch index defines a `synonym_graph` token filter named `product_dictionary`. It represents domain vocabulary that the application expects search to understand, such as `raincoat` matching `waterproof jacket`, `trainer` matching `running shoe`, and `daypack` matching `hydration pack`.

### Analyzer and Index Setup

```json
{
  "settings": {
    "analysis": {
      "filter": {
        "product_dictionary": {
          "type": "synonym_graph",
          "synonyms": [
            "raincoat, rain jacket, waterproof jacket",
            "sneaker, trainer, running shoe",
            "daypack, hydration pack, running vest"
          ]
        }
      },
      "analyzer": {
        "product_index_analyzer": {
          "tokenizer": "standard",
          "filter": ["lowercase"]
        },
        "product_search_analyzer": {
          "tokenizer": "standard",
          "filter": ["lowercase", "product_dictionary"]
        }
      }
    }
  }
}
```

Create the index and load data:

```bash
samples/scripts/start_elasticsearch.sh
samples/scripts/load_elasticsearch_sample.sh custom-dictionaries
```

### Query Shape

The representative query uses the search analyzer to expand domain vocabulary and keeps tenant and status as exact filters:

```json
{
  "query": {
    "bool": {
      "must": [
        {
          "multi_match": {
            "query": "raincoat",
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

Run the sample queries:

```bash
samples/scripts/run_elasticsearch_query.sh custom-dictionaries search-raincoat
samples/scripts/run_elasticsearch_query.sh custom-dictionaries search-trainer
samples/scripts/run_elasticsearch_query.sh custom-dictionaries search-daypack
```

## Spanner

Spanner does not load an Elasticsearch synonym file or custom dictionary into the search index. The closest target behavior is enhanced query mode, enabled with `enhance_query=>true` on the `SEARCH(...)` predicate. Use it when the application needs query-time expansion for related terms, then validate the expansion against the source dictionary behavior.

### Schema and Search Index

```sql
CREATE TABLE DictionaryProducts (
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

CREATE SEARCH INDEX DictionaryProductsByText
ON DictionaryProducts(SearchTokens)
STORING (Title, Description, Category, Status)
PARTITION BY TenantId;
```

### Query Shape

```sql
SELECT
  ProductId,
  Title,
  Description
FROM DictionaryProducts
WHERE TenantId = @tenant_id
  AND Status = 'active'
  AND SEARCH(SearchTokens, 'raincoat', enhance_query=>true)
ORDER BY ProductId
LIMIT 25;
```

The `search-raincoat`, `search-trainer`, and `search-daypack` files intentionally use the source dictionary terms. In this fixture they are useful gap checks: they execute successfully, but Spanner enhanced query does not reproduce those explicit Elasticsearch synonym entries. The canonical phrase queries prove that the Spanner search index and enhanced-query form work against the indexed product text. Strict variants omit `enhance_query=>true` so the target behavior can be compared with and without query expansion:

```bash
samples/scripts/apply_spanner_sample_schema.sh custom-dictionaries
samples/scripts/seed_spanner_sample.sh custom-dictionaries
samples/scripts/run_spanner_query.sh custom-dictionaries search-waterproof-jacket
samples/scripts/run_spanner_query.sh custom-dictionaries search-running-shoe
samples/scripts/run_spanner_query.sh custom-dictionaries search-hydration-pack
samples/scripts/run_spanner_query.sh custom-dictionaries search-raincoat
samples/scripts/run_spanner_query.sh custom-dictionaries search-raincoat-strict
samples/scripts/run_spanner_query.sh custom-dictionaries search-trainer
samples/scripts/run_spanner_query.sh custom-dictionaries search-trainer-strict
samples/scripts/run_spanner_query.sh custom-dictionaries search-daypack
samples/scripts/run_spanner_query.sh custom-dictionaries search-daypack-strict
```

## Behavior Notes

Elasticsearch custom dictionaries are explicit and workload-specific. The source analyzer can encode a curated list of product terms, acronyms, abbreviations, brand language, and phrase-level synonyms. Spanner enhanced query mode is not the same artifact. It is a query-time expansion option that may recover related-term matches, but it does not guarantee exact parity with a custom Elasticsearch synonym or dictionary file.

For migration validation, run the source dictionary queries and the Spanner enhanced and strict variants side by side. Treat missing high-value source matches as recall gaps, and treat additional enhanced-query matches as possible precision changes that product owners should approve. In the checked-in fixture, the Elasticsearch source dictionary maps `raincoat`, `trainer`, and `daypack` to matching products, while Spanner enhanced query matches the canonical indexed phrases `waterproof jacket`, `running shoe`, and `hydration pack` but not those curated source dictionary aliases.
