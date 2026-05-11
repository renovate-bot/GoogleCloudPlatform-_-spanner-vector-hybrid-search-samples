# Skill: Google Cloud Spanner Multi-Model Architecture

This reference guide outlines key architectural concepts, official documentation links, design best practices, and generalized syntax templates for Spanner's multi-model features (Graph, Vector Search, Full-Text Search, and Columnar store).

---

## 1. Spanner Graph (Property Graphs & openCypher)
* **Concept**: Spanner Graph natively stores property graphs co-located with relational tables. It uses the openCypher query dialect embedded directly inside SQL.
* **Official Docs**: [Spanner Graph Overview](https://cloud.google.com/spanner/docs/graph/overview)
* **Architectural Best Practices**:
  * **Co-location**: Node and Edge tables are standard Spanner tables. Design them with solid primary keys.
  * **Key Mapping**: Edge tables represent relationships between nodes. The edge table's primary key must uniquely identify the edge, and it must reference the source and destination node primary keys.
  * **Property Mapping**: Map node and edge properties directly to columns in their backing tables.
* **Generalized DDL Template**:
  ```sql
  -- 1. Define Node Backing Table
  CREATE TABLE <NodeTableName> (
    <NodeIdColumn> <DataType> NOT NULL,
    <PropertyColumn1> <DataType>,
    ...
  ) PRIMARY KEY (<NodeIdColumn>);

  -- 2. Define Edge Backing Table
  CREATE TABLE <EdgeTableName> (
    <EdgeIdColumn> <DataType> NOT NULL,
    <SourceIdColumn> <DataType> NOT NULL,
    <DestinationIdColumn> <DataType> NOT NULL,
    <PropertyColumn1> <DataType>,
    ...
  ) PRIMARY KEY (<EdgeIdColumn>);

  -- 3. Define Property Graph
  CREATE PROPERTY GRAPH <GraphName>
    NODE TABLES (
      <NodeTableName> [LABEL <NodeLabel>]
    )
    EDGE TABLES (
      <EdgeTableName>
      SOURCE KEY (<SourceIdColumn>) REFERENCES <NodeTableName> (<NodeIdColumn>)
      DESTINATION KEY (<DestinationIdColumn>) REFERENCES <NodeTableName> (<NodeIdColumn>)
      [LABEL <EdgeLabel>]
    );
  ```
* **Generalized Query Template**:
  ```sql
  SELECT <ColumnsToProject>
  FROM GRAPH_TABLE(<GraphName>
    MATCH (<source_alias>:<NodeLabel>)-[<edge_alias>:<EdgeLabel>]->(<dest_alias>:<NodeLabel>)
    WHERE <edge_alias>.<PropertyColumn> = <Value>
    RETURN <source_alias>.<PropertyColumn> AS <Alias1>, <dest_alias>.<PropertyColumn> AS <Alias2>
  );
  ```

---

## 2. Vector Search (AI & Retrieval-Augmented Generation)
* **Concept**: Spanner natively stores high-dimensional vector embeddings and searches them in real-time for semantic matching and GenAI applications.
* **Official Docs**: [Spanner Vector Search Guide](https://cloud.google.com/spanner/docs/find-k-nearest-neighbors)
* **Architectural Best Practices**:
  * **Storage Type**: Store vector embeddings as `ARRAY<FLOAT64>` or `ARRAY<FLOAT32>`.
  * **Vector Indexing**: To speed up K-Nearest Neighbors (KNN) search over millions of rows, build a secondary Vector Index on the embedding column.
  * **Distance Metrics**: Choose the right metric for your model: `COSINE` (recommended for typical text models), `EUCLIDEAN`, or `DOT_PRODUCT`.
* **Generalized DDL Template**:
  ```sql
  -- Add embedding column to an existing table
  ALTER TABLE <TableName> ADD COLUMN <EmbeddingColumnName> ARRAY<FLOAT64>;

  -- Create Vector Index
  CREATE VECTOR INDEX <VectorIndexName> ON <TableName>(<EmbeddingColumnName>)
    OPTIONS (distance_type='<COSINE|EUCLIDEAN|DOT_PRODUCT>');
  ```
* **Generalized Query Template**:
  ```sql
  -- Find top K nearest neighbors
  SELECT <ColumnName>, COSINE_DISTANCE(<EmbeddingColumnName>, <QueryVectorEmbedding>) AS distance
  FROM <TableName>
  ORDER BY distance
  LIMIT <K_Value>;
  ```

---

## 3. Full-Text Search (FTS)
* **Concept**: Spanner provides native, highly customizable full-text indexing for high-performance tokenized searches across text columns.
* **Official Docs**: [Spanner Full-Text Search Overview](https://cloud.google.com/spanner/docs/fts-overview)
* **Architectural Best Practices**:
  * **FTS Indexes**: Use `CREATE SEARCH INDEX` to build indexes on string columns you want tokenized.
  * **Storing Extra Columns**: Use the `STORING` clause to co-locate non-indexed columns with the search index. This prevents the query engine from having to do a row lookup on the primary table.
* **Generalized DDL Template**:
  ```sql
  CREATE SEARCH INDEX <SearchIndexName> ON <TableName>(<ColumnToTokenize1>, <ColumnToTokenize2>)
    [STORING (<ExtraColumnToRetrieve1>, <ExtraColumnToRetrieve2>)];
  ```
* **Generalized Query Template**:
  ```sql
  SELECT <ColumnName>, <ExtraColumnToRetrieve1>
  FROM <TableName>
  WHERE SEARCH(<SearchIndexName>, '<SearchQueryToken>')
  LIMIT <Limit>;
  ```

---

## 4. Columnar Engine (Dual-Engine HTAP)
* **Concept**: Spanner implements a dual-engine architecture where data is projected in both row-oriented format (for transactional OLTP) and columnar format (for analytical OLAP) in real-time.
* **Official Docs**: [Spanner Columnar Indexes Guide](https://cloud.google.com/spanner/docs/columnar-indexes)
* **Architectural Best Practices**:
  * **Zero ETL**: No complex pipelines needed. The engine automatically syncs transactions to the columnar representation in real-time with transactional consistency.
  * **Aggregation Acceleration**: Create Columnar Indexes on columns frequently used for filters, aggregations (`SUM`, `AVG`, `COUNT`), and group-by queries.
* **Generalized DDL Template**:
  ```sql
  -- Create Columnar Index on selected columns
  ALTER TABLE <TableName> ADD COLUMNAR INDEX (<Column1>, <Column2>, <Column3>);
  ```