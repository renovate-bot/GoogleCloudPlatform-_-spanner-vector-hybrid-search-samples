# Spanner hybrid search

Spanner supports both [vector search](https://cloud.google.com/spanner/docs/find-k-nearest-neighbors) and [full-text search](https://cloud.google.com/spanner/docs/full-text-search) (FTS). In some scenarios, it may be beneficial to combine FTS and vector search. In this sample, we'll walk through an approach for performing hybrid searches using Spanner.

## Getting started

In order to run the queries in this sample, you first need to go through the following steps:

1. [Select or create a Cloud Platform project](https://console.cloud.google.com/project)
2. [Enable billing for your project](https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project)
3. [Enable the Google Cloud Spanner API](https://console.cloud.google.com/flows/enableapi?apiid=spanner.googleapis.com)
4. [Setup Authentication](https://googleapis.dev/python/google-api-core/latest/auth.html)
5. [Familiarize yourself with Spanner Studio](https://cloud.google.com/spanner/docs/manage-data-using-console)

NOTE: You can run the queries below using Spanner Studio.

## Schema

Let's first create a basic schema for a table called `products` which we'll use to store a toy catalog along with a description of each toy.

### Products table and FTS search index

```
CREATE TABLE products (
id INT64,
description STRING(MAX),
description_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(description)) HIDDEN,
embedding ARRAY<FLOAT32>(vector_length=>768),
) PRIMARY KEY(id);

CREATE SEARCH INDEX si_desc ON products(description_tokens);
```

### Model to generate embeddings for vector search

Documentation [here](https://cloud.google.com/spanner/docs/ml-tutorial-embeddings#generate_and_store_text_embeddings)

```
CREATE OR REPLACE MODEL embedding_gen
INPUT(content STRING(MAX))
OUTPUT(embeddings STRUCT<values ARRAY<FLOAT32>>)
REMOTE
OPTIONS ( endpoint="//aiplatform.googleapis.com/projects/362376585789/locations/us-central1/publishers/google/models/text-embedding-004"
);
```

## Data generation

We'll use [Gemini](https://gemini.google.com/app) 2.5 Pro to generate product descriptions that we can then use to index and search for our demo.

```
Given the following schema, could you please generate 10 insert statements for sample data into this table? The items being stored are children's toys. Feel free to generate made up values for the description column. Use the embeddings model below to generate the vector embeddings.

CREATE TABLE products (
id INT64,
description STRING(MAX),
description_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(description)) HIDDEN,
embedding ARRAY<FLOAT32>(vector_length=>768),
) PRIMARY KEY(id);

CREATE OR REPLACE MODEL embedding_gen
INPUT(content STRING(MAX))
OUTPUT(embeddings STRUCT<values ARRAY<FLOAT32>>)
REMOTE
OPTIONS ( endpoint="//aiplatform.googleapis.com/projects/362376585789/locations/us-central1/publishers/google/models/text-embedding-004"
);
```

Result:

```
INSERT INTO products (id, description, embedding) VALUES
(1, 'Ultra-soft and huggable plush teddy bear, a perfect companion for children. Classic brown color with a cheerful red ribbon.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Ultra-soft and huggable plush teddy bear, a perfect companion for children. Classic brown color with a cheerful red ribbon.'))));

INSERT INTO products (id, description, embedding) VALUES
(2, 'Set of 100 natural wooden building blocks in various shapes and sizes. Ideal for fostering creativity and developing fine motor skills in young builders.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Set of 100 natural wooden building blocks in various shapes and sizes. Ideal for fostering creativity and developing fine motor skills in young builders.'))));

INSERT INTO products (id, description, embedding) VALUES
(3, 'Sleek remote control sports car with full function controls (forward, backward, left, right). Bright yellow finish, designed for speed and fun.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Sleek remote control sports car with full function controls (forward, backward, left, right). Bright yellow finish, designed for speed and fun.'))));

INSERT INTO products (id, description, embedding) VALUES
(4, 'Charming three-story wooden dollhouse complete with 5 rooms of detailed miniature furniture. Encourages imaginative role-play.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Charming three-story wooden dollhouse complete with 5 rooms of detailed miniature furniture. Encourages imaginative role-play.'))));

INSERT INTO products (id, description, embedding) VALUES
(5, 'Double-sided children''s art easel featuring a chalkboard on one side and a magnetic dry-erase board on the other. Includes paper roll and clips.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Double-sided children''s art easel featuring a chalkboard on one side and a magnetic dry-erase board on the other. Includes paper roll and clips.'))));

INSERT INTO products (id, description, embedding) VALUES
(6, 'Classic battery-operated toy train set with steam locomotive, coal tender, passenger car, and 12 pieces of track to form a circular layout.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Classic battery-operated toy train set with steam locomotive, coal tender, passenger car, and 12 pieces of track to form a circular layout.'))));

INSERT INTO products (id, description, embedding) VALUES
(7, 'Detailed Tyrannosaurus Rex dinosaur action figure with articulated limbs and jaw. Made from durable, non-toxic plastic for long-lasting play.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Detailed Tyrannosaurus Rex dinosaur action figure with articulated limbs and jaw. Made from durable, non-toxic plastic for long-lasting play.'))));

INSERT INTO products (id, description, embedding) VALUES
(8, 'Educational learning tablet for kids featuring interactive games for letters, numbers, animals, and music. Touch-sensitive screen and colorful graphics.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Educational learning tablet for kids featuring interactive games for letters, numbers, animals, and music. Touch-sensitive screen and colorful graphics.'))));

INSERT INTO products (id, description, embedding) VALUES
(9, 'Inflatable bouncy castle with slide for backyard fun. Made from puncture-resistant material and includes an electric air blower for quick setup.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Inflatable bouncy castle with slide for backyard fun. Made from puncture-resistant material and includes an electric air blower for quick setup.'))));

INSERT INTO products (id, description, embedding) VALUES
(10, 'Beginner''s science experiment kit with 30 fun and safe activities. Includes lab coat, goggles, and all necessary chemicals and apparatus to explore basic chemistry and physics.', (SELECT embeddings.values FROM embedding_gen(INPUT(content => 'Beginner''s science experiment kit with 30 fun and safe activities. Includes lab coat, goggles, and all necessary chemicals and apparatus to explore basic chemistry and physics.'))));
```

Note that this is just an example. Feel free to generate more or less data for your needs. The data samples shown below are from inserts that were generated by us. Your results will look different based on the data you generate.

## Example 1: FTS performs better compared to vector search

### FTS query

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

## Example 3: Simple re-ranking (no ML model involved)

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

## Example 4: RRF (w/ ML model from Hugging Face)

### Let's create our re-ranker model first

The re-ranker model we're using is from Hugging Face. The name of the model is 'bge-reranker-v2-m3-1748472166957'. Please see [Use Hugging Face Models](https://cloud.google.com/vertex-ai/generative-ai/docs/open-models/use-hugging-face-models) for more information on how to deploy and use it on VertexAI in your GCP project. Once deployed, you can reference it via the endpoint attribute of `CREATE MODEL` DDL statement as shown below.

```
CREATE OR REPLACE MODEL reranker
INPUT (text string(max), text_pair string(max))
OUTPUT (score FLOAT32)
REMOTE
OPTIONS (
endpoint = '//aiplatform.googleapis.com/projects/<your project id>/locations/<your region>/endpoints/<your endpoint id>'
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