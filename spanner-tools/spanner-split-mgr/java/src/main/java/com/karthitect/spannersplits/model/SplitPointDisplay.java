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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

@JsonInclude(JsonInclude.Include.ALWAYS)
public class SplitPointDisplay {
    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("split_value")
    private String splitValue;

    private SplitStatus status;

    @JsonProperty("expire_time")
    private Instant expireTime;

    @JsonProperty("local_id")
    private Long localId;

    private String initiator;

    private String index;

    @JsonProperty("index_key")
    private String indexKey;

    @JsonProperty("table_key")
    private String tableKey;

    public SplitPointDisplay() {}

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getSplitValue() { return splitValue; }
    public void setSplitValue(String splitValue) { this.splitValue = splitValue; }

    public SplitStatus getStatus() { return status; }
    public void setStatus(SplitStatus status) { this.status = status; }

    public Instant getExpireTime() { return expireTime; }
    public void setExpireTime(Instant expireTime) { this.expireTime = expireTime; }

    public Long getLocalId() { return localId; }
    public void setLocalId(Long localId) { this.localId = localId; }

    public String getInitiator() { return initiator; }
    public void setInitiator(String initiator) { this.initiator = initiator; }

    public String getIndex() { return index; }
    public void setIndex(String index) { this.index = index; }

    public String getIndexKey() { return indexKey; }
    public void setIndexKey(String indexKey) { this.indexKey = indexKey; }

    public String getTableKey() { return tableKey; }
    public void setTableKey(String tableKey) { this.tableKey = tableKey; }
}
