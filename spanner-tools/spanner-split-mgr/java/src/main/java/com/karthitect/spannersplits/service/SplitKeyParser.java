/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.karthitect.spannersplits.service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SplitKeyParser {

    private static final Pattern INDEX_PATTERN = Pattern.compile(
            "^Index:\\s*(?<index>.+?)\\s+on\\s+(?<table>[^,]+),\\s*Index Key:\\s*\\((?<indexKey>.*?)\\),\\s*Primary Table Key:\\s*\\((?<tableKey>.*?)\\)\\s*$"
    );

    private static final Pattern TABLE_PATTERN = Pattern.compile("^(?<table>[^(]+)\\((?<tableKey>.*)\\)\\s*$");

    private SplitKeyParser() {}

    public static class Parsed {
        public final String indexName;
        public final String indexKey;
        public final String tableKey;
        public Parsed(String indexName, String indexKey, String tableKey) {
            this.indexName = indexName;
            this.indexKey = indexKey;
            this.tableKey = tableKey;
        }
    }

    public static Parsed parse(String splitKey) {
        if (splitKey == null || splitKey.isBlank()) {
            return new Parsed(null, null, "");
        }
        String s = splitKey.trim();
        Matcher idx = INDEX_PATTERN.matcher(s);
        if (idx.matches()) {
            return new Parsed(
                    idx.group("index").trim(),
                    idx.group("indexKey").trim(),
                    idx.group("tableKey").trim()
            );
        }
        Matcher tbl = TABLE_PATTERN.matcher(s);
        if (tbl.matches()) {
            return new Parsed(null, null, tbl.group("tableKey").trim());
        }
        return new Parsed(null, null, s);
    }

    public static String formatSpannerError(String errorStr) {
        if (errorStr == null) return "";
        String tableName = null;
        Matcher tm = Pattern.compile("table:\\s*\\\\?\"([^\"]+)\\\\?\"").matcher(errorStr);
        if (tm.find()) tableName = tm.group(1).trim();

        String reason = null;
        Matcher rm = Pattern.compile("due to\\s+(.+?)(?:\\.\\s*\\[|$)").matcher(errorStr);
        if (rm.find()) {
            reason = rm.group(1).trim();
            if (reason.endsWith(".")) reason = reason.substring(0, reason.length() - 1);
        } else {
            Matcher im = Pattern.compile("is invalid,?\\s*(.+?)(?:\\.\\s*\\[|$)").matcher(errorStr);
            if (im.find()) {
                reason = im.group(1).trim();
                if (reason.endsWith(".")) reason = reason.substring(0, reason.length() - 1);
            }
        }

        String msg;
        if (tableName != null && reason != null) {
            msg = "Table '" + tableName + "': " + reason;
        } else if (reason != null) {
            msg = reason;
        } else if (tableName != null) {
            Matcher sm = Pattern.compile("^\\d+\\s+(.+?)(?:\\s*\\[locale|$)").matcher(errorStr);
            if (sm.find()) {
                String inner = sm.group(1).trim();
                if (inner.length() > 200) inner = inner.substring(0, 200);
                msg = "Table '" + tableName + "': " + inner;
            } else {
                msg = "Table '" + tableName + "': Operation failed";
            }
        } else {
            String cleaned = errorStr.replaceAll("\\s*\\[locale.*$", "")
                    .replaceAll("go/debugproto\\s*\\\\n", "")
                    .replace("\\n", " ")
                    .trim();
            if (cleaned.length() > 200) cleaned = cleaned.substring(0, 200) + "...";
            msg = cleaned;
        }

        return unescape(msg);
    }

    private static String unescape(String s) {
        return s.replace("\\\"", "\"")
                .replace("\\'", "'")
                .replace("\\n", " ")
                .replace("\\t", " ")
                .replace("\\\\", "\\");
    }
}
