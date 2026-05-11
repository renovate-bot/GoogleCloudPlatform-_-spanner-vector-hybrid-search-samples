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

public class KeyColumnInfo {
    @JsonProperty("column_name")
    private String columnName;

    @JsonProperty("spanner_type")
    private String spannerType;

    @JsonProperty("ordinal_position")
    private int ordinalPosition;

    public KeyColumnInfo() {}

    public KeyColumnInfo(String columnName, String spannerType, int ordinalPosition) {
        this.columnName = columnName;
        this.spannerType = spannerType;
        this.ordinalPosition = ordinalPosition;
    }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public String getSpannerType() { return spannerType; }
    public void setSpannerType(String spannerType) { this.spannerType = spannerType; }

    public int getOrdinalPosition() { return ordinalPosition; }
    public void setOrdinalPosition(int ordinalPosition) { this.ordinalPosition = ordinalPosition; }
}
