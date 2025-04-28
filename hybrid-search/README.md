# Spanner hybrid search

<<TODO: Overview of hybrid search>>

## Schema

### Products table and FTS search index (TODO: link to docs)

```
CREATE TABLE products (
id INT64,
description STRING(MAX),
description_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(description)) HIDDEN,
embedding ARRAY<FLOAT32>(vector_length=>768),
) PRIMARY KEY(id);

CREATE SEARCH INDEX si_desc ON products(description_tokens);
```

### Model to generate embeddings for vector search (TODO: link to docs)

```
CREATE OR REPLACE MODEL embedding_gen
INPUT(content STRING(MAX))
OUTPUT(embeddings STRUCT<values ARRAY<FLOAT32>>)
REMOTE
OPTIONS ( endpoint="//aiplatform.googleapis.com/projects/362376585789/locations/us-central1/publishers/google/models/text-embedding-004"
);
```

## Data generation

We'll use Gemini to generate product descriptions that we can then use to index and search for our demo.

<<TODO: Gemini prompt to generate product descriptions>>

## Example 1: FTS performs better compared to vector search

### FTS query (TODO: add context/explanation)

```
SELECT description
FROM products
WHERE SEARCH(description_tokens, 'LEGO 43203')
ORDER BY SCORE(description_tokens, 'LEGO 43203') DESC
LIMIT 200;
```

Results:
```
LEGO Disney Princess Aurora, Merida and Tiana's Enchanted Creations (43203) - Creative playset with 3 princesses and their animal friends
```

### Vector search

```
WITH vector AS (
 SELECT embeddings.values FROM ML.PREDICT(
   MODEL embedding_gen, (
     SELECT "LEGO 43203" AS content)
))
SELECT description
FROM products, vector
ORDER BY COSINE_DISTANCE(embedding, vector.values)
LIMIT 200;
```

Results:
```
LEGO Disney Frozen II Elsa and Anna's Frozen Wonderland (43194) - Ice castle playset with Elsa, Anna and Olaf figures
LEGO Technic Catamaran (42105) - Build a realistic model of a racing catamaran
LEGO Friends Horse-Riding Camp (41683) - Campsite playset with horses, a stable and 3 mini-dolls
LEGO Disney Princess Cinderella's Royal Carriage (43192) - Fairy tale carriage playset with Cinderella and a horse figure
LEGO Technic Jeep Wrangler (42122) - Build a replica of the iconic off-road vehicle
LEGO Disney Princess Ariel and the Magical Spell (43211) - Underwater adventure playset with Ariel, Flounder and Sebastian figures
LEGO Disney Encanto The Madrigal House (43202) - Magical house playset inspired by Disney's Encanto
...
```

## Example 2: Vector search performs better compared to FTS

### FTS

```
SELECT description
FROM products
WHERE SEARCH(description_tokens, 'Space themed LEGO for 8+')
ORDER BY SCORE(description_tokens, 'Space themed LEGO for 8+') DESC
LIMIT 200;
```

Results:
```
```

### Vector search

```
WITH vector AS (
 SELECT embeddings.values FROM ML.PREDICT(
   MODEL embedding_gen, (
     SELECT "Space themed LEGO for 8+" AS content)
))
SELECT description
FROM products, vector
ORDER BY COSINE_DISTANCE(embedding, vector.values)
LIMIT 200;
```

Results:
```
```

## Example 3: Use RRF (TODO: Add links to docs)

```
@{optimizer_version=7}
WITH vector AS (
 SELECT embeddings.values FROM ML.PREDICT(
   MODEL embedding_gen, (
     SELECT "Space themed LEGO for 8+" AS content)
)),
knn AS (
 SELECT rank, x.id, x.description
 FROM UNNEST(ARRAY(
   SELECT AS STRUCT id, description
   FROM products, vector
   ORDER BY COSINE_DISTANCE(vector.values, embedding)
   LIMIT 200)) AS x WITH OFFSET AS rank
),
fts AS (
 SELECT rank, x.id, x.description
 FROM UNNEST(ARRAY(
   SELECT AS STRUCT id, description
   FROM products
   WHERE SEARCH(description_tokens, 'Space themed LEGO for 8+')
   ORDER BY SCORE(description_tokens, 'Space themed LEGO for 8+') DESC
   LIMIT 200)) AS x WITH OFFSET AS rank
)
-- https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf
SELECT SUM(1 / (60 + rank)) AS rrf_score, ANY_VALUE(description)
FROM ((
 SELECT rank, id, description
 FROM knn
)
UNION ALL (
 SELECT rank, id, description
 FROM fts
))
GROUP BY id
ORDER BY rrf_score DESC
LIMIT 50;
```

Results:
```
```

## Example 4: Simple re-ranking

### Let's create our re-ranker model first

```
CREATE OR REPLACE MODEL reranker
INPUT (text string(max), text_pair string(max))
OUTPUT (score FLOAT32)
REMOTE
OPTIONS (
endpoint = '//aiplatform.googleapis.com/projects/362376585789/locations/us-central1/endpoints/3831807918404009984'
);
```

### Query

```
@{optimizer_version=7}
WITH vector AS (
 SELECT embeddings.values FROM ML.PREDICT(
   MODEL embedding_gen, (
     SELECT "Space themed LEGO for 8+" AS content)
)),
knn AS (
 SELECT rank, x.id, x.description
 FROM UNNEST(ARRAY(
   SELECT AS STRUCT id, description
   FROM products, vector
   ORDER BY COSINE_DISTANCE(vector.values, embedding)
   LIMIT 200)) AS x WITH OFFSET AS rank
),
fts AS (
 SELECT rank, x.id, x.description
 FROM UNNEST(ARRAY(
   SELECT AS STRUCT id, description
   FROM products
   WHERE SEARCH(description_tokens, 'Space themed LEGO for 8+')
   ORDER BY SCORE(description_tokens, 'Space themed LEGO for 8+') DESC
   LIMIT 200)) AS x WITH OFFSET AS rank
),
rrf AS (
 SELECT SUM(1 / (60 + rank)) AS rrf_score, ANY_VALUE(description) as description
 FROM ((
   SELECT rank, id, description
   FROM knn
 )
 UNION ALL (
   SELECT rank, id, description
   FROM fts
 ))
 GROUP BY id
 ORDER BY rrf_score DESC
 LIMIT 50
)
SELECT score, text AS description
FROM ML.PREDICT(MODEL reranker, (
 SELECT description AS text, "Space themed LEGO for 8+" AS text_pair
 FROM rrf
))
ORDER BY score DESC
LIMIT 10;
```

Results:
```
```