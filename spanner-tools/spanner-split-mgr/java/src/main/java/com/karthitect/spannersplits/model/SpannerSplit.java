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

public class SpannerSplit {
    private String table;
    private String index;
    private String initiator;

    @JsonProperty("split_key")
    private String splitKey;

    @JsonProperty("expire_time")
    private Instant expireTime;

    public SpannerSplit() {}

    public SpannerSplit(String table, String index, String initiator,
                        String splitKey, Instant expireTime) {
        this.table = table;
        this.index = index;
        this.initiator = initiator;
        this.splitKey = splitKey;
        this.expireTime = expireTime;
    }

    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }

    public String getIndex() { return index; }
    public void setIndex(String index) { this.index = index; }

    public String getInitiator() { return initiator; }
    public void setInitiator(String initiator) { this.initiator = initiator; }

    public String getSplitKey() { return splitKey; }
    public void setSplitKey(String splitKey) { this.splitKey = splitKey; }

    public Instant getExpireTime() { return expireTime; }
    public void setExpireTime(Instant expireTime) { this.expireTime = expireTime; }
}
