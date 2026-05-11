/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.karthitect.spannersplits.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

public class LocalSplitResponse {
    private long id;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("split_value")
    private String splitValue;

    @JsonProperty("operation_type")
    private OperationType operationType;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("index_name")
    private String indexName;

    @JsonProperty("index_key")
    private String indexKey;

    public LocalSplitResponse() {}

    public LocalSplitResponse(long id, String tableName, String splitValue,
                              OperationType operationType, Instant createdAt,
                              String indexName, String indexKey) {
        this.id = id;
        this.tableName = tableName;
        this.splitValue = splitValue;
        this.operationType = operationType;
        this.createdAt = createdAt;
        this.indexName = indexName;
        this.indexKey = indexKey;
    }

    public long getId() { return id; }
    public void setId(long id) { this.id = id; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getSplitValue() { return splitValue; }
    public void setSplitValue(String splitValue) { this.splitValue = splitValue; }

    public OperationType getOperationType() { return operationType; }
    public void setOperationType(OperationType operationType) { this.operationType = operationType; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public String getIndexName() { return indexName; }
    public void setIndexName(String indexName) { this.indexName = indexName; }

    public String getIndexKey() { return indexKey; }
    public void setIndexKey(String indexKey) { this.indexKey = indexKey; }
}
