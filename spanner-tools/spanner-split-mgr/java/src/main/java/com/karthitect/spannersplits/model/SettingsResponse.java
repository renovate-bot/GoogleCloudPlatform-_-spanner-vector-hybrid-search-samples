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

public class SettingsResponse {
    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("instance_id")
    private String instanceId;

    @JsonProperty("database_id")
    private String databaseId;

    public SettingsResponse() {}

    public SettingsResponse(String projectId, String instanceId, String databaseId) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
    }

    public String getProjectId() { return projectId; }
    public void setProjectId(String projectId) { this.projectId = projectId; }

    public String getInstanceId() { return instanceId; }
    public void setInstanceId(String instanceId) { this.instanceId = instanceId; }

    public String getDatabaseId() { return databaseId; }
    public void setDatabaseId(String databaseId) { this.databaseId = databaseId; }
}
