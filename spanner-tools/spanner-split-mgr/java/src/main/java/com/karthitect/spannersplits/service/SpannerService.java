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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.protobuf.ListValue;
import com.google.protobuf.Timestamp;
import com.google.spanner.admin.database.v1.AddSplitPointsRequest;
import com.google.spanner.admin.database.v1.DatabaseName;
import com.google.spanner.admin.database.v1.SplitPoints;
import com.karthitect.spannersplits.model.EntityKeySchema;
import com.karthitect.spannersplits.model.EntityType;
import com.karthitect.spannersplits.model.KeyColumnInfo;
import com.karthitect.spannersplits.model.OperationType;
import com.karthitect.spannersplits.model.LocalSplitResponse;
import com.karthitect.spannersplits.model.SpannerSplit;
import com.karthitect.spannersplits.model.SyncResult;
import com.karthitect.spannersplits.repository.LocalSplitsRepository;
import com.karthitect.spannersplits.repository.SettingsRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class SpannerService {
    private static final Logger log = LoggerFactory.getLogger(SpannerService.class);

    public static final int BATCH_LIMIT = 100;
    public static final int DEFAULT_EXPIRATION_DAYS = 10;

    private final SettingsRepository settingsRepo;
    private final LocalSplitsRepository splitsRepo;

    @Value("${spanner.project:}")
    private String envProject;

    @Value("${spanner.instance:}")
    private String envInstance;

    @Value("${spanner.database:}")
    private String envDatabase;

    private Spanner cachedSpanner;
    private String cachedSpannerProject;
    private DatabaseAdminClient cachedAdminClient;

    public SpannerService(SettingsRepository settingsRepo, LocalSplitsRepository splitsRepo) {
        this.settingsRepo = settingsRepo;
        this.splitsRepo = splitsRepo;
    }

    @PreDestroy
    public void close() {
        if (cachedSpanner != null) {
            try { cachedSpanner.close(); } catch (Exception ignored) {}
        }
        if (cachedAdminClient != null) {
            try { cachedAdminClient.close(); } catch (Exception ignored) {}
        }
    }

    /** Reset cached clients when settings change. */
    public synchronized void reset() {
        if (cachedSpanner != null) {
            try { cachedSpanner.close(); } catch (Exception ignored) {}
            cachedSpanner = null;
            cachedSpannerProject = null;
        }
        if (cachedAdminClient != null) {
            try { cachedAdminClient.close(); } catch (Exception ignored) {}
            cachedAdminClient = null;
        }
    }

    public String getProjectId() {
        String fromDb = settingsRepo.getSetting("project_id");
        if (StringUtils.hasText(fromDb)) return fromDb;
        return StringUtils.hasText(envProject) ? envProject : null;
    }

    public String getInstanceId() {
        String fromDb = settingsRepo.getSetting("instance_id");
        if (StringUtils.hasText(fromDb)) return fromDb;
        return StringUtils.hasText(envInstance) ? envInstance : null;
    }

    public String getDatabaseId() {
        String fromDb = settingsRepo.getSetting("database_id");
        if (StringUtils.hasText(fromDb)) return fromDb;
        return StringUtils.hasText(envDatabase) ? envDatabase : null;
    }

    public boolean isConfigured() {
        return StringUtils.hasText(getInstanceId()) && StringUtils.hasText(getDatabaseId());
    }

    private synchronized Spanner getSpanner() {
        String project = getProjectId();
        if (cachedSpanner == null
                || (project != null && !project.equals(cachedSpannerProject))) {
            if (cachedSpanner != null) {
                try { cachedSpanner.close(); } catch (Exception ignored) {}
            }
            SpannerOptions.Builder b = SpannerOptions.newBuilder();
            if (StringUtils.hasText(project)) {
                b.setProjectId(project);
            }
            cachedSpanner = b.build().getService();
            cachedSpannerProject = project;
        }
        return cachedSpanner;
    }

    private synchronized DatabaseAdminClient getAdminClient() throws Exception {
        if (cachedAdminClient == null) {
            cachedAdminClient = DatabaseAdminClient.create();
        }
        return cachedAdminClient;
    }

    private DatabaseClient getDatabaseClient() {
        if (!isConfigured()) {
            throw new IllegalStateException("Spanner instance and database must be configured");
        }
        Spanner spanner = getSpanner();
        String project = StringUtils.hasText(getProjectId()) ? getProjectId() : spanner.getOptions().getProjectId();
        return spanner.getDatabaseClient(DatabaseId.of(project, getInstanceId(), getDatabaseId()));
    }

    public static class ConnectionTestResult {
        public final boolean success;
        public final String errorMessage;
        public ConnectionTestResult(boolean success, String errorMessage) {
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }

    public ConnectionTestResult testConnection() {
        if (!isConfigured()) {
            return new ConnectionTestResult(false, "Instance and database must be configured");
        }
        try {
            DatabaseClient db = getDatabaseClient();
            try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                 ResultSet rs = tx.executeQuery(Statement.of("SELECT 1"))) {
                while (rs.next()) { /* consume */ }
            }
            return new ConnectionTestResult(true, null);
        } catch (Exception e) {
            String msg = e.getMessage() == null ? e.toString() : e.getMessage();
            String lower = msg.toLowerCase();
            if (lower.contains("not_found") || lower.contains("not found")) {
                if (lower.contains("instance")) {
                    return new ConnectionTestResult(false,
                            "Instance '" + getInstanceId() + "' not found. Please check the instance ID.");
                } else if (lower.contains("database")) {
                    return new ConnectionTestResult(false,
                            "Database '" + getDatabaseId() + "' not found in instance '" + getInstanceId() + "'. Please check the database ID.");
                }
                return new ConnectionTestResult(false, "Resource not found: " + truncate(msg, 200));
            }
            if (lower.contains("permission_denied") || lower.contains("permission")) {
                return new ConnectionTestResult(false, "Permission denied. Please check your credentials and IAM permissions.");
            }
            if (lower.contains("unauthenticated") || lower.contains("authentication")) {
                return new ConnectionTestResult(false, "Authentication failed. Please run 'gcloud auth application-default login' and try again.");
            }
            if (lower.contains("invalid_argument")) {
                return new ConnectionTestResult(false, "Invalid configuration: " + truncate(msg, 200));
            }
            return new ConnectionTestResult(false, "Connection failed: " + truncate(msg, 300));
        }
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
    }

    public List<String> listTables() {
        if (!isConfigured()) return List.of();
        List<String> tables = new ArrayList<>();
        try {
            DatabaseClient db = getDatabaseClient();
            try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                 ResultSet rs = tx.executeQuery(Statement.of(
                         "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                                 "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = ''"))) {
                while (rs.next()) {
                    String name = rs.isNull(0) ? null : rs.getString(0);
                    if (StringUtils.hasText(name)) tables.add(name);
                }
            }
        } catch (Exception e) {
            log.error("Error listing tables: {}", e.getMessage());
        }
        return tables;
    }

    /** Returns list of pairs: (index_name, parent_table_name). */
    public List<String[]> listIndexes() {
        if (!isConfigured()) return List.of();
        List<String[]> indexes = new ArrayList<>();
        try {
            DatabaseClient db = getDatabaseClient();
            try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                 ResultSet rs = tx.executeQuery(Statement.of(
                         "SELECT INDEX_NAME, TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES " +
                                 "WHERE INDEX_TYPE != 'PRIMARY_KEY' AND SPANNER_IS_MANAGED = FALSE"))) {
                while (rs.next()) {
                    String idx = rs.isNull(0) ? "" : rs.getString(0);
                    String tbl = rs.isNull(1) ? "" : rs.getString(1);
                    if (StringUtils.hasText(idx)) indexes.add(new String[]{idx, tbl});
                }
            }
        } catch (Exception e) {
            log.error("Error listing indexes: {}", e.getMessage());
        }
        return indexes;
    }

    public EntityKeySchema getTableKeySchema(String tableName) {
        List<KeyColumnInfo> keyColumns = new ArrayList<>();
        if (isConfigured()) {
            try {
                DatabaseClient db = getDatabaseClient();
                Statement stmt = Statement.newBuilder("""
                        SELECT ic.COLUMN_NAME, c.SPANNER_TYPE, ic.ORDINAL_POSITION
                        FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic
                        JOIN INFORMATION_SCHEMA.COLUMNS c
                          ON ic.TABLE_NAME = c.TABLE_NAME AND ic.COLUMN_NAME = c.COLUMN_NAME
                        WHERE ic.TABLE_NAME = @table_name
                          AND ic.INDEX_TYPE = 'PRIMARY_KEY'
                        ORDER BY ic.ORDINAL_POSITION
                        """)
                        .bind("table_name").to(tableName)
                        .build();
                try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                     ResultSet rs = tx.executeQuery(stmt)) {
                    while (rs.next()) {
                        keyColumns.add(new KeyColumnInfo(
                                rs.isNull(0) ? "" : rs.getString(0),
                                rs.isNull(1) ? "" : rs.getString(1),
                                rs.isNull(2) ? 0 : (int) rs.getLong(2)
                        ));
                    }
                }
            } catch (Exception e) {
                log.error("Error getting table key schema: {}", e.getMessage());
            }
        }
        return new EntityKeySchema(tableName, EntityType.TABLE, keyColumns, null, null);
    }

    public EntityKeySchema getIndexKeySchema(String indexName) {
        List<KeyColumnInfo> keyColumns = new ArrayList<>();
        String parentTable = null;
        List<KeyColumnInfo> parentKeyColumns = null;

        if (isConfigured()) {
            try {
                DatabaseClient db = getDatabaseClient();
                Statement parentStmt = Statement.newBuilder(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.INDEXES WHERE INDEX_NAME = @index_name LIMIT 1")
                        .bind("index_name").to(indexName).build();
                try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                     ResultSet rs = tx.executeQuery(parentStmt)) {
                    if (rs.next()) {
                        parentTable = rs.isNull(0) ? null : rs.getString(0);
                    }
                }

                Statement colsStmt = Statement.newBuilder("""
                        SELECT ic.COLUMN_NAME, c.SPANNER_TYPE, ic.ORDINAL_POSITION
                        FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic
                        JOIN INFORMATION_SCHEMA.COLUMNS c
                          ON ic.TABLE_NAME = c.TABLE_NAME AND ic.COLUMN_NAME = c.COLUMN_NAME
                        WHERE ic.INDEX_NAME = @index_name
                        ORDER BY ic.ORDINAL_POSITION
                        """).bind("index_name").to(indexName).build();
                try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                     ResultSet rs = tx.executeQuery(colsStmt)) {
                    while (rs.next()) {
                        keyColumns.add(new KeyColumnInfo(
                                rs.isNull(0) ? "" : rs.getString(0),
                                rs.isNull(1) ? "" : rs.getString(1),
                                rs.isNull(2) ? 0 : (int) rs.getLong(2)
                        ));
                    }
                }
            } catch (Exception e) {
                log.error("Error getting index key schema: {}", e.getMessage());
            }

            if (StringUtils.hasText(parentTable)) {
                EntityKeySchema parent = getTableKeySchema(parentTable);
                parentKeyColumns = parent.getKeyColumns().isEmpty() ? null : parent.getKeyColumns();
            }
        }

        return new EntityKeySchema(indexName, EntityType.INDEX, keyColumns, parentTable, parentKeyColumns);
    }

    public List<SpannerSplit> listSplits() {
        if (!isConfigured()) return List.of();
        List<SpannerSplit> splits = new ArrayList<>();
        try {
            DatabaseClient db = getDatabaseClient();
            try (ReadOnlyTransaction tx = db.singleUseReadOnlyTransaction();
                 ResultSet rs = tx.executeQuery(Statement.of("SELECT * FROM SPANNER_SYS.USER_SPLIT_POINTS"))) {
                while (rs.next()) {
                    int cols = rs.getColumnCount();
                    String table = cols > 0 ? safeString(rs, 0) : "";
                    String index = cols > 1 ? safeString(rs, 1) : null;
                    String initiator = cols > 2 ? safeString(rs, 2) : "";
                    String splitKey = cols > 3 ? safeString(rs, 3) : "";
                    Instant expireTime = null;
                    if (cols > 4 && !rs.isNull(4)) {
                        Type colType = rs.getColumnType(4);
                        if (colType != null && colType.getCode() == Type.Code.TIMESTAMP) {
                            com.google.cloud.Timestamp ts = rs.getTimestamp(4);
                            expireTime = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
                        }
                    }
                    splits.add(new SpannerSplit(
                            table == null ? "" : table,
                            StringUtils.hasText(index) ? index : null,
                            initiator == null ? "" : initiator,
                            splitKey == null ? "" : splitKey,
                            expireTime
                    ));
                }
            }
        } catch (Exception e) {
            log.error("Error listing split points: {}", e.getMessage());
        }
        return splits;
    }

    private static String safeString(ResultSet rs, int col) {
        if (rs.isNull(col)) return "";
        Type colType = rs.getColumnType(col);
        if (colType == null) return "";
        switch (colType.getCode()) {
            case STRING: return rs.getString(col);
            case INT64: return Long.toString(rs.getLong(col));
            case BOOL: return Boolean.toString(rs.getBoolean(col));
            case FLOAT64: return Double.toString(rs.getDouble(col));
            case BYTES: return rs.getBytes(col).toBase64();
            default: return rs.getValue(col).toString();
        }
    }

    private SplitPoints.Key makeKey(String keyValue) {
        ListValue.Builder lvBuilder = ListValue.newBuilder();
        if (StringUtils.hasText(keyValue)) {
            for (String part : keyValue.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    lvBuilder.addValues(com.google.protobuf.Value.newBuilder().setStringValue(trimmed).build());
                }
            }
        }
        return SplitPoints.Key.newBuilder().setKeyParts(lvBuilder.build()).build();
    }

    private SplitPoints makeSplitPoint(String tableName, String splitValue, Instant expireTime,
                                       String indexName, String indexKey) {
        SplitPoints.Builder builder = SplitPoints.newBuilder();
        if (StringUtils.hasText(indexName)) {
            builder.setIndex(indexName);
            if (StringUtils.hasText(indexKey)) {
                builder.addKeys(makeKey(indexKey));
            }
            if (StringUtils.hasText(splitValue) && !splitValue.contains("<begin>")) {
                builder.addKeys(makeKey(splitValue));
            }
        } else {
            builder.setTable(tableName);
            builder.addKeys(makeKey(splitValue));
        }
        if (expireTime != null) {
            builder.setExpireTime(Timestamp.newBuilder()
                    .setSeconds(expireTime.getEpochSecond())
                    .setNanos(expireTime.getNano())
                    .build());
        }
        return builder.build();
    }

    private List<List<SplitPoints>> batch(List<SplitPoints> all) {
        List<List<SplitPoints>> out = new ArrayList<>();
        for (int i = 0; i < all.size(); i += BATCH_LIMIT) {
            out.add(new ArrayList<>(all.subList(i, Math.min(i + BATCH_LIMIT, all.size()))));
        }
        return out;
    }

    public SyncResult syncPendingChanges() {
        if (!isConfigured()) {
            SyncResult r = new SyncResult(false, "Spanner not configured");
            r.getErrors().add("Instance and database must be configured");
            return r;
        }

        List<LocalSplitResponse> pendingAdds = splitsRepo.getLocalSplitsByOperation(OperationType.ADD);
        List<LocalSplitResponse> pendingDeletes = splitsRepo.getLocalSplitsByOperation(OperationType.DELETE);

        int totalAdded = 0;
        int totalDeleted = 0;
        List<String> errors = new ArrayList<>();

        Instant defaultExpire = Instant.now().plus(DEFAULT_EXPIRATION_DAYS, ChronoUnit.DAYS);
        Instant expireNow = Instant.now().minusSeconds(10);

        DatabaseAdminClient admin;
        try {
            admin = getAdminClient();
        } catch (Exception e) {
            SyncResult r = new SyncResult(false, "Failed to initialize admin client");
            r.getErrors().add(e.getMessage());
            return r;
        }

        String dbPath = DatabaseName.of(
                StringUtils.hasText(getProjectId()) ? getProjectId() : getSpanner().getOptions().getProjectId(),
                getInstanceId(),
                getDatabaseId()
        ).toString();

        // Process adds
        if (!pendingAdds.isEmpty()) {
            List<SplitPoints> apiSplits = new ArrayList<>();
            for (LocalSplitResponse split : pendingAdds) {
                apiSplits.add(makeSplitPoint(
                        split.getTableName(),
                        split.getSplitValue(),
                        defaultExpire,
                        split.getIndexName(),
                        split.getIndexKey()
                ));
            }
            List<List<SplitPoints>> batches = batch(apiSplits);
            int batchStart = 0;
            for (List<SplitPoints> b : batches) {
                AddSplitPointsRequest req = AddSplitPointsRequest.newBuilder()
                        .setDatabase(dbPath)
                        .addAllSplitPoints(b)
                        .build();
                try {
                    admin.addSplitPoints(req);
                    totalAdded += b.size();
                    for (int i = batchStart; i < batchStart + b.size(); i++) {
                        LocalSplitResponse s = pendingAdds.get(i);
                        splitsRepo.deleteLocalSplitByValue(s.getTableName(), s.getSplitValue(),
                                s.getIndexName(), s.getIndexKey());
                    }
                } catch (Exception e) {
                    errors.add(SplitKeyParser.formatSpannerError(e.getMessage()));
                    log.error("Error adding split points batch: {}", e.getMessage());
                }
                batchStart += b.size();
            }
        }

        // Process deletes
        if (!pendingDeletes.isEmpty()) {
            List<SplitPoints> apiSplits = new ArrayList<>();
            for (LocalSplitResponse split : pendingDeletes) {
                SplitKeyParser.Parsed parsed = SplitKeyParser.parse(split.getSplitValue());
                apiSplits.add(makeSplitPoint(
                        split.getTableName(),
                        parsed.tableKey,
                        expireNow,
                        parsed.indexName,
                        parsed.indexKey
                ));
            }
            List<List<SplitPoints>> batches = batch(apiSplits);
            int batchStart = 0;
            for (List<SplitPoints> b : batches) {
                AddSplitPointsRequest req = AddSplitPointsRequest.newBuilder()
                        .setDatabase(dbPath)
                        .addAllSplitPoints(b)
                        .build();
                try {
                    admin.addSplitPoints(req);
                    totalDeleted += b.size();
                    for (int i = batchStart; i < batchStart + b.size(); i++) {
                        LocalSplitResponse s = pendingDeletes.get(i);
                        splitsRepo.deleteLocalSplitByValue(s.getTableName(), s.getSplitValue(),
                                s.getIndexName(), s.getIndexKey());
                    }
                } catch (Exception e) {
                    errors.add(SplitKeyParser.formatSpannerError(e.getMessage()));
                    log.error("Error deleting split points batch: {}", e.getMessage());
                }
                batchStart += b.size();
            }
        }

        boolean success = errors.isEmpty();
        int total = totalAdded + totalDeleted;
        String message;
        List<String> parts = new ArrayList<>();
        if (totalAdded > 0) parts.add("Added " + totalAdded);
        if (totalDeleted > 0) parts.add("Deleted " + totalDeleted);
        if (parts.isEmpty()) {
            message = "Nothing synced. Check for *potential* errors below.";
        } else {
            String word = total == 1 ? "split point" : "split points";
            message = String.join(", ", parts) + " " + word;
        }

        SyncResult r = new SyncResult(success, message);
        r.setAddedCount(totalAdded);
        r.setDeletedCount(totalDeleted);
        r.setErrors(errors);
        return r;
    }
}
