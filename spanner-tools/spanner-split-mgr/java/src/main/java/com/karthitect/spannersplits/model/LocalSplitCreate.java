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
import jakarta.validation.constraints.NotBlank;

public class LocalSplitCreate {
    @NotBlank
    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("split_value")
    private String splitValue = "";

    @JsonProperty("operation_type")
    private OperationType operationType = OperationType.ADD;

    @JsonProperty("index_name")
    private String indexName;

    @JsonProperty("index_key")
    private String indexKey;

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getSplitValue() { return splitValue == null ? "" : splitValue; }
    public void setSplitValue(String splitValue) { this.splitValue = splitValue; }

    public OperationType getOperationType() { return operationType; }
    public void setOperationType(OperationType operationType) { this.operationType = operationType; }

    public String getIndexName() { return indexName; }
    public void setIndexName(String indexName) { this.indexName = indexName; }

    public String getIndexKey() { return indexKey; }
    public void setIndexKey(String indexKey) { this.indexKey = indexKey; }
}
