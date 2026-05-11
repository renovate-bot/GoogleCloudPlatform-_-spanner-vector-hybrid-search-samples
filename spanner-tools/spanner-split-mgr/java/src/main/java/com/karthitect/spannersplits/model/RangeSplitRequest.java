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
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public class RangeSplitRequest {
    @NotBlank
    @JsonProperty("table_name")
    private String tableName;

    @NotBlank
    @JsonProperty("start_value")
    private String startValue;

    @NotBlank
    @JsonProperty("end_value")
    private String endValue;

    @Min(2)
    @Max(100)
    @JsonProperty("num_splits")
    private int numSplits;

    @JsonProperty("include_boundaries")
    private boolean includeBoundaries = true;

    @JsonProperty("index_name")
    private String indexName;

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getStartValue() { return startValue; }
    public void setStartValue(String startValue) { this.startValue = startValue; }

    public String getEndValue() { return endValue; }
    public void setEndValue(String endValue) { this.endValue = endValue; }

    public int getNumSplits() { return numSplits; }
    public void setNumSplits(int numSplits) { this.numSplits = numSplits; }

    public boolean isIncludeBoundaries() { return includeBoundaries; }
    public void setIncludeBoundaries(boolean includeBoundaries) { this.includeBoundaries = includeBoundaries; }

    public String getIndexName() { return indexName; }
    public void setIndexName(String indexName) { this.indexName = indexName; }
}
