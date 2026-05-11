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

public class RangeValidationResult {
    @JsonProperty("is_valid")
    private boolean valid;

    @JsonProperty("range_type")
    private SupportedRangeType rangeType;

    @JsonProperty("error_message")
    private String errorMessage;

    public RangeValidationResult() {}

    public RangeValidationResult(boolean valid, SupportedRangeType rangeType, String errorMessage) {
        this.valid = valid;
        this.rangeType = rangeType;
        this.errorMessage = errorMessage;
    }

    public static RangeValidationResult ok(SupportedRangeType type) {
        return new RangeValidationResult(true, type, null);
    }

    public static RangeValidationResult fail(SupportedRangeType type, String message) {
        return new RangeValidationResult(false, type, message);
    }

    public boolean isValid() { return valid; }
    public void setValid(boolean valid) { this.valid = valid; }

    public SupportedRangeType getRangeType() { return rangeType; }
    public void setRangeType(SupportedRangeType rangeType) { this.rangeType = rangeType; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}
