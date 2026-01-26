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
"LEGO Friends Olivia's Mission Vehicle - A space-themed mission vehicle with a mini-robot and telescope, great for kids ages 8+."
"LEGO Ninjago Galaxy Battle Set - A space-themed battle scene with ninja characters, recommended for fans ages 8+."
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

Results (200):
```
"LEGO Creator Space Shuttle Adventure - A classic space shuttle build with astronauts and satellite, great for ages 8+."
"LEGO Creator Spaceship Explorer - A 3-in-1 spaceship with options to build a space mech or shuttle, ideal for ages 8+."
"LEGO Juniors Space Mission - A beginner space-themed LEGO set featuring astronauts, rocket ships, and planets, ideal for ages 4-7."
"LEGO City Mars Space Station - A modular space station with multiple rooms, mini astronauts, and a shuttle, recommended for ages 8+."
"LEGO Creator Space Robot - A buildable robot that can transform into space vehicles, ideal for ages 7+."
"LEGO Classic Space Bricks Set - A classic brick box with simple instructions to create space objects, designed for ages 4+."
"LEGO Space Team Adventure - Includes mini astronauts, rovers, and a small rocket, designed for ages 6+."
"LEGO Junior Space Adventure Set - A beginner-friendly LEGO set with mini astronauts and space vehicles, ideal for ages 4+."
"LEGO City Satellite Service Mission - A mini shuttle and satellite repair mission set, designed for young astronauts aged 8+."
"LEGO City Satellite Service Mission - A small, easy-to-build space shuttle and satellite set, ideal for ages 5+."
"LEGO Duplo Space Explorers - A set with an astronaut, moon, and small spaceship, perfect for ages 4+."
"LEGO Creator Star Explorer - A mini spaceship with astronaut figure, suitable for ages 5+."
...
```

## Example 3: Simple ranking (no ML model involved)

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

Results (50):
```
0.027031740495291244,"LEGO Ninjago Galaxy Battle Set - A space-themed battle scene with ninja characters, recommended for fans ages 8+."
0.026767676767676767,"LEGO Friends Olivia's Mission Vehicle - A space-themed mission vehicle with a mini-robot and telescope, great for kids ages 8+."
0.016666666666666666,"LEGO Creator Space Shuttle Adventure - A classic space shuttle build with astronauts and satellite, great for ages 8+."
0.01639344262295082,"LEGO Creator Spaceship Explorer - A 3-in-1 spaceship with options to build a space mech or shuttle, ideal for ages 8+."
0.016129032258064516,"LEGO Juniors Space Mission - A beginner space-themed LEGO set featuring astronauts, rocket ships, and planets, ideal for ages 4-7."
0.015873015873015872,"LEGO City Mars Space Station - A modular space station with multiple rooms, mini astronauts, and a shuttle, recommended for ages 8+."
0.015625,"LEGO Creator Space Robot - A buildable robot that can transform into space vehicles, ideal for ages 7+."
0.015384615384615385,"LEGO Classic Space Bricks Set - A classic brick box with simple instructions to create space objects, designed for ages 4+."
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
0.9999020099639893,"LEGO Friends Olivia's Mission Vehicle - A space-themed mission vehicle with a mini-robot and telescope, great for kids ages 8+."
0.9998645782470703,"LEGO Ninjago Galaxy Battle Set - A space-themed battle scene with ninja characters, recommended for fans ages 8+."
0.9998202919960022,"LEGO Creator Space Shuttle Adventure - A classic space shuttle build with astronauts and satellite, great for ages 8+."
0.9997809529304504,"LEGO Friends Space Research Lab - A research lab with experiments, space rover, and astronaut figures, perfect for ages 8+."
0.9997766613960266,"LEGO City Mars Space Station - A modular space station with multiple rooms, mini astronauts, and a shuttle, recommended for ages 8+."
0.9997122883796692,"LEGO City Deep Space Rocket and Launch Control - A detailed rocket and launch station set with mini-figures, ideal for space exploration fans ages 8+."
0.9996609687805176,"LEGO City Lunar Roving Vehicle - A lunar rover with functional wheels and space tools, recommended for kids ages 8+."
0.9995812773704529,"LEGO Creator Space Mining Mech - A 3-in-1 set featuring a mining mech, space explorer, and alien landscape, suited for ages 8+."
0.9994454979896545,"LEGO Technic Mini Space Shuttle - A compact and functional space shuttle with moving parts, designed for ages 8+."
0.9994049072265625,"LEGO Creator Spaceship Explorer - A 3-in-1 spaceship with options to build a space mech or shuttle, ideal for ages 8+."
```