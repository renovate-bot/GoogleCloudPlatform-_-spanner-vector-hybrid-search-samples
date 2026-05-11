/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.karthitect.spannersplits.util;

import com.karthitect.spannersplits.model.EntityKeySchema;
import com.karthitect.spannersplits.model.KeyColumnInfo;
import com.karthitect.spannersplits.model.RangeValidationResult;
import com.karthitect.spannersplits.model.SupportedRangeType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class RangeUtils {

    private static final Pattern UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private static final Pattern STRING_TYPE = Pattern.compile("^STRING\\((\\d+|MAX)\\)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern BYTES_TYPE = Pattern.compile("^BYTES\\((\\d+|MAX)\\)$", Pattern.CASE_INSENSITIVE);

    private RangeUtils() {}

    public static boolean isValidUuid(String value) {
        if (value == null || value.length() != 36) return false;
        return UUID_PATTERN.matcher(value).matches();
    }

    public static BigInteger uuidToInt(String uuidStr) {
        UUID u = UUID.fromString(uuidStr);
        BigInteger hi = BigInteger.valueOf(u.getMostSignificantBits()).and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
        BigInteger lo = BigInteger.valueOf(u.getLeastSignificantBits()).and(new BigInteger("FFFFFFFFFFFFFFFF", 16));
        return hi.shiftLeft(64).or(lo);
    }

    public static String intToUuid(BigInteger value) {
        BigInteger mask = new BigInteger("FFFFFFFFFFFFFFFF", 16);
        long lo = value.and(mask).longValue();
        long hi = value.shiftRight(64).and(mask).longValue();
        return new UUID(hi, lo).toString();
    }

    public static class RangeResult {
        public final List<String> values;
        public final List<String> warnings;
        public RangeResult(List<String> values, List<String> warnings) {
            this.values = values;
            this.warnings = warnings;
        }
    }

    public static RangeResult generateInt64RangeSplits(long start, long end, int numSplits, boolean includeBoundaries) {
        if (start >= end) throw new IllegalArgumentException("Start value must be less than end value");
        if (numSplits < 2) throw new IllegalArgumentException("Number of splits must be at least 2");

        List<String> warnings = new ArrayList<>();
        List<String> values = new ArrayList<>();

        if (includeBoundaries) {
            double step = (double) (end - start) / (double) (numSplits - 1);
            for (int i = 0; i < numSplits; i++) {
                long value;
                if (i == 0) value = start;
                else if (i == numSplits - 1) value = end;
                else value = start + (long) (step * i);
                values.add(Long.toString(value));
            }
            long actualEnd = start + (long) (step * (numSplits - 1));
            if (actualEnd != end) warnings.add("End boundary adjusted due to integer division rounding");
        } else {
            double step = (double) (end - start) / (double) (numSplits + 1);
            for (int i = 1; i <= numSplits; i++) {
                long value = start + (long) (step * i);
                values.add(Long.toString(value));
            }
        }
        return new RangeResult(values, warnings);
    }

    public static RangeResult generateUuidRangeSplits(String startUuid, String endUuid,
                                                      int numSplits, boolean includeBoundaries) {
        if (!isValidUuid(startUuid)) {
            throw new IllegalArgumentException("Value '" + startUuid + "' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
        }
        if (!isValidUuid(endUuid)) {
            throw new IllegalArgumentException("Value '" + endUuid + "' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
        }

        BigInteger startInt = uuidToInt(startUuid);
        BigInteger endInt = uuidToInt(endUuid);

        if (startInt.compareTo(endInt) >= 0) {
            throw new IllegalArgumentException("Start value must be less than end value");
        }
        if (numSplits < 2) throw new IllegalArgumentException("Number of splits must be at least 2");

        List<String> values = new ArrayList<>();

        if (includeBoundaries) {
            BigInteger diff = endInt.subtract(startInt);
            for (int i = 0; i < numSplits; i++) {
                BigInteger v;
                if (i == 0) v = startInt;
                else if (i == numSplits - 1) v = endInt;
                else {
                    // step * i computed as diff * i / (numSplits - 1)
                    v = startInt.add(diff.multiply(BigInteger.valueOf(i))
                            .divide(BigInteger.valueOf(numSplits - 1)));
                }
                values.add(intToUuid(v));
            }
        } else {
            BigInteger diff = endInt.subtract(startInt);
            for (int i = 1; i <= numSplits; i++) {
                BigInteger v = startInt.add(diff.multiply(BigInteger.valueOf(i))
                        .divide(BigInteger.valueOf(numSplits + 1)));
                values.add(intToUuid(v));
            }
        }
        return new RangeResult(values, new ArrayList<>());
    }

    public static DetectResult detectRangeType(String spannerType, String sampleValue) {
        String upper = spannerType == null ? "" : spannerType.toUpperCase();
        if (upper.equals("INT64")) {
            return new DetectResult(SupportedRangeType.INT64, null);
        }
        if (upper.startsWith("STRING")) {
            Matcher m = STRING_TYPE.matcher(spannerType);
            if (m.matches()) {
                String lenStr = m.group(1);
                int length = "MAX".equalsIgnoreCase(lenStr) ? 36 : Integer.parseInt(lenStr);
                if (length <= 35) {
                    return new DetectResult(null, "Column length (" + length + ") too short for UUIDs (need greater than 35)");
                }
                if (sampleValue != null && !isValidUuid(sampleValue)) {
                    return new DetectResult(null, "Value '" + sampleValue + "' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
                }
                return new DetectResult(SupportedRangeType.STRING_UUID, null);
            }
            return new DetectResult(null, "Could not parse STRING type: " + spannerType);
        }
        if (upper.startsWith("BYTES")) {
            Matcher m = BYTES_TYPE.matcher(spannerType);
            if (m.matches()) {
                String lenStr = m.group(1);
                int length = "MAX".equalsIgnoreCase(lenStr) ? 16 : Integer.parseInt(lenStr);
                if (length <= 15) {
                    return new DetectResult(null, "Column length (" + length + ") too short for UUIDs (need greater than 15)");
                }
                if (sampleValue != null && !isValidUuid(sampleValue)) {
                    return new DetectResult(null, "Value '" + sampleValue + "' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
                }
                return new DetectResult(SupportedRangeType.BYTES_UUID, null);
            }
            return new DetectResult(null, "Could not parse BYTES type: " + spannerType);
        }
        return new DetectResult(null, "Column type '" + spannerType + "' not supported. Supported: INT64, STRING(>35) with UUIDs, BYTES(>15) with UUIDs.");
    }

    public static RangeValidationResult validateRangeRequest(EntityKeySchema schema, String startValue, String endValue) {
        if (schema.isComposite()) {
            return RangeValidationResult.fail(null, "Range splits are not supported for composite keys. Please add splits individually.");
        }
        if (schema.getKeyColumns() == null || schema.getKeyColumns().isEmpty()) {
            return RangeValidationResult.fail(null, "No key columns found in schema");
        }

        KeyColumnInfo keyColumn = schema.getKeyColumns().get(0);
        DetectResult detected = detectRangeType(keyColumn.getSpannerType(), startValue);
        if (detected.error != null) {
            return RangeValidationResult.fail(null, detected.error);
        }

        SupportedRangeType rangeType = detected.type;
        if (rangeType == SupportedRangeType.INT64) {
            try {
                long s = Long.parseLong(startValue);
                long e = Long.parseLong(endValue);
                if (s >= e) {
                    return RangeValidationResult.fail(rangeType, "Start value must be less than end value");
                }
            } catch (NumberFormatException ex) {
                return RangeValidationResult.fail(rangeType,
                        "Invalid integer value(s): start='" + startValue + "', end='" + endValue + "'");
            }
        } else {
            if (!isValidUuid(startValue)) {
                return RangeValidationResult.fail(rangeType,
                        "Value '" + startValue + "' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
            }
            if (!isValidUuid(endValue)) {
                return RangeValidationResult.fail(rangeType,
                        "Value '" + endValue + "' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
            }
            if (uuidToInt(startValue).compareTo(uuidToInt(endValue)) >= 0) {
                return RangeValidationResult.fail(rangeType, "Start value must be less than end value");
            }
        }

        return RangeValidationResult.ok(rangeType);
    }

    public static RangeResult generateRangeSplits(SupportedRangeType rangeType,
                                                  String startValue, String endValue,
                                                  int numSplits, boolean includeBoundaries) {
        if (rangeType == SupportedRangeType.INT64) {
            return generateInt64RangeSplits(
                    Long.parseLong(startValue),
                    Long.parseLong(endValue),
                    numSplits, includeBoundaries);
        }
        if (rangeType == SupportedRangeType.STRING_UUID || rangeType == SupportedRangeType.BYTES_UUID) {
            return generateUuidRangeSplits(startValue, endValue, numSplits, includeBoundaries);
        }
        throw new IllegalArgumentException("Unsupported range type: " + rangeType);
    }

    public static class DetectResult {
        public final SupportedRangeType type;
        public final String error;
        public DetectResult(SupportedRangeType type, String error) {
            this.type = type;
            this.error = error;
        }
    }
}
