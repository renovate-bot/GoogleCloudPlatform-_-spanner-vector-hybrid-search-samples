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

import java.util.List;

public class EntityKeySchema {
    @JsonProperty("entity_name")
    private String entityName;

    @JsonProperty("entity_type")
    private EntityType entityType;

    @JsonProperty("key_columns")
    private List<KeyColumnInfo> keyColumns;

    @JsonProperty("is_composite")
    private boolean composite;

    @JsonProperty("parent_table")
    private String parentTable;

    @JsonProperty("parent_key_columns")
    private List<KeyColumnInfo> parentKeyColumns;

    public EntityKeySchema() {}

    public EntityKeySchema(String entityName, EntityType entityType,
                           List<KeyColumnInfo> keyColumns,
                           String parentTable,
                           List<KeyColumnInfo> parentKeyColumns) {
        this.entityName = entityName;
        this.entityType = entityType;
        this.keyColumns = keyColumns;
        this.composite = keyColumns != null && keyColumns.size() > 1;
        this.parentTable = parentTable;
        this.parentKeyColumns = parentKeyColumns;
    }

    public String getEntityName() { return entityName; }
    public void setEntityName(String entityName) { this.entityName = entityName; }

    public EntityType getEntityType() { return entityType; }
    public void setEntityType(EntityType entityType) { this.entityType = entityType; }

    public List<KeyColumnInfo> getKeyColumns() { return keyColumns; }
    public void setKeyColumns(List<KeyColumnInfo> keyColumns) {
        this.keyColumns = keyColumns;
        this.composite = keyColumns != null && keyColumns.size() > 1;
    }

    public boolean isComposite() { return composite; }
    public void setComposite(boolean composite) { this.composite = composite; }

    public String getParentTable() { return parentTable; }
    public void setParentTable(String parentTable) { this.parentTable = parentTable; }

    public List<KeyColumnInfo> getParentKeyColumns() { return parentKeyColumns; }
    public void setParentKeyColumns(List<KeyColumnInfo> parentKeyColumns) {
        this.parentKeyColumns = parentKeyColumns;
    }
}
