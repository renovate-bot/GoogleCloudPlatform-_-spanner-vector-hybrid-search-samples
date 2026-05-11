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

import java.util.ArrayList;
import java.util.List;

public class SyncResult {
    private boolean success;
    private String message;

    @JsonProperty("added_count")
    private int addedCount;

    @JsonProperty("deleted_count")
    private int deletedCount;

    private List<String> errors = new ArrayList<>();

    public SyncResult() {}

    public SyncResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public static SyncResult ok(String message) {
        return new SyncResult(true, message);
    }

    public static SyncResult fail(String message, List<String> errors) {
        SyncResult r = new SyncResult(false, message);
        if (errors != null) r.errors = errors;
        return r;
    }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public int getAddedCount() { return addedCount; }
    public void setAddedCount(int addedCount) { this.addedCount = addedCount; }

    public int getDeletedCount() { return deletedCount; }
    public void setDeletedCount(int deletedCount) { this.deletedCount = deletedCount; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
}
