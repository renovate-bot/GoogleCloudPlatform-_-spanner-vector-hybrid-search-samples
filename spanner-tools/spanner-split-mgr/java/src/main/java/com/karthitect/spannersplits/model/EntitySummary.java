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

public class EntitySummary {
    @JsonProperty("entity_name")
    private String entityName;

    @JsonProperty("entity_type")
    private EntityType entityType;

    @JsonProperty("parent_table")
    private String parentTable;

    @JsonProperty("total_splits")
    private int totalSplits;

    @JsonProperty("synced_count")
    private int syncedCount;

    @JsonProperty("pending_add_count")
    private int pendingAddCount;

    @JsonProperty("pending_delete_count")
    private int pendingDeleteCount;

    public EntitySummary() {}

    public EntitySummary(String entityName, EntityType entityType, String parentTable) {
        this.entityName = entityName;
        this.entityType = entityType;
        this.parentTable = parentTable;
    }

    public String getEntityName() { return entityName; }
    public void setEntityName(String entityName) { this.entityName = entityName; }

    public EntityType getEntityType() { return entityType; }
    public void setEntityType(EntityType entityType) { this.entityType = entityType; }

    public String getParentTable() { return parentTable; }
    public void setParentTable(String parentTable) { this.parentTable = parentTable; }

    public int getTotalSplits() { return totalSplits; }
    public void setTotalSplits(int totalSplits) { this.totalSplits = totalSplits; }

    public int getSyncedCount() { return syncedCount; }
    public void setSyncedCount(int syncedCount) { this.syncedCount = syncedCount; }

    public int getPendingAddCount() { return pendingAddCount; }
    public void setPendingAddCount(int pendingAddCount) { this.pendingAddCount = pendingAddCount; }

    public int getPendingDeleteCount() { return pendingDeleteCount; }
    public void setPendingDeleteCount(int pendingDeleteCount) { this.pendingDeleteCount = pendingDeleteCount; }
}
