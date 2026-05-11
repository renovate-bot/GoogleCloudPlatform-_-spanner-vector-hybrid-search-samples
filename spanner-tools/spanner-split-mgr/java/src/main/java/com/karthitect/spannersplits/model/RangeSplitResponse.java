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

public class RangeSplitResponse {
    private boolean success;
    private String message;

    @JsonProperty("generated_values")
    private List<String> generatedValues = new ArrayList<>();

    @JsonProperty("splits_created")
    private int splitsCreated;

    private List<String> warnings = new ArrayList<>();
    private List<String> errors = new ArrayList<>();

    public RangeSplitResponse() {}

    public RangeSplitResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public List<String> getGeneratedValues() { return generatedValues; }
    public void setGeneratedValues(List<String> generatedValues) { this.generatedValues = generatedValues; }

    public int getSplitsCreated() { return splitsCreated; }
    public void setSplitsCreated(int splitsCreated) { this.splitsCreated = splitsCreated; }

    public List<String> getWarnings() { return warnings; }
    public void setWarnings(List<String> warnings) { this.warnings = warnings; }

    public List<String> getErrors() { return errors; }
    public void setErrors(List<String> errors) { this.errors = errors; }
}
