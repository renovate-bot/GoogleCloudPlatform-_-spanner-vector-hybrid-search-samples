/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.karthitect.spannersplits.repository;

import com.karthitect.spannersplits.model.LocalSplitResponse;
import com.karthitect.spannersplits.model.OperationType;
import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public class LocalSplitsRepository {

    private final JdbcTemplate jdbc;

    public LocalSplitsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void initSchema() {
        boolean tableExists = !jdbc.queryForList(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='local_splits'"
        ).isEmpty();

        if (!tableExists) {
            createFreshTable();
            return;
        }

        Set<String> columns = jdbc.queryForList(
                "PRAGMA table_info(local_splits)"
        ).stream()
                .map(row -> (String) row.get("name"))
                .collect(java.util.stream.Collectors.toSet());

        if (!columns.contains("index_name")) {
            migrateAddIndexColumns();
            return;
        }

        String createSql = jdbc.queryForObject(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='local_splits'",
                String.class
        );
        if (createSql != null && !createSql.contains("UNIQUE(table_name, split_value, index_name, index_key)")) {
            recreateWithCompositeUnique();
        }
    }

    private void createFreshTable() {
        jdbc.execute("""
            CREATE TABLE local_splits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                split_value TEXT NOT NULL DEFAULT '',
                operation_type TEXT NOT NULL,
                index_name TEXT DEFAULT '',
                index_key TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(table_name, split_value, index_name, index_key)
            )
        """);
    }

    private void migrateAddIndexColumns() {
        jdbc.execute("ALTER TABLE local_splits RENAME TO local_splits_old");
        createFreshTable();
        jdbc.execute("""
            INSERT INTO local_splits (id, table_name, split_value, operation_type, index_name, index_key, created_at)
            SELECT id, table_name, split_value, operation_type, '', '', created_at
            FROM local_splits_old
        """);
        jdbc.execute("DROP TABLE local_splits_old");
    }

    private void recreateWithCompositeUnique() {
        jdbc.execute("ALTER TABLE local_splits RENAME TO local_splits_old");
        createFreshTable();
        jdbc.execute("""
            INSERT INTO local_splits (id, table_name, split_value, operation_type, index_name, index_key, created_at)
            SELECT id, table_name, split_value, operation_type,
                   COALESCE(index_name, ''), COALESCE(index_key, ''), created_at
            FROM local_splits_old
        """);
        jdbc.execute("DROP TABLE local_splits_old");
    }

    private final RowMapper<LocalSplitResponse> mapper = (rs, rowNum) -> {
        Instant createdAt;
        Object raw = rs.getObject("created_at");
        if (raw instanceof String s) {
            createdAt = LocalDateTime.parse(s.replace(" ", "T")).toInstant(ZoneOffset.UTC);
        } else if (raw instanceof java.sql.Timestamp ts) {
            createdAt = ts.toInstant();
        } else {
            createdAt = Instant.now();
        }
        String idxName = rs.getString("index_name");
        String idxKey = rs.getString("index_key");
        return new LocalSplitResponse(
                rs.getLong("id"),
                rs.getString("table_name"),
                rs.getString("split_value") == null ? "" : rs.getString("split_value"),
                OperationType.valueOf(rs.getString("operation_type")),
                createdAt,
                idxName,
                idxKey
        );
    };

    public LocalSplitResponse addLocalSplit(String tableName, String splitValue,
                                            OperationType operationType,
                                            String indexName, String indexKey) {
        String idxName = indexName == null ? "" : indexName;
        String idxKey = indexKey == null ? "" : indexKey;
        String sv = splitValue == null ? "" : splitValue;

        jdbc.update("""
                INSERT INTO local_splits (table_name, split_value, operation_type, index_name, index_key)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(table_name, split_value, index_name, index_key) DO UPDATE SET
                    operation_type = excluded.operation_type,
                    created_at = CURRENT_TIMESTAMP
                """,
                tableName, sv, operationType.name(), idxName, idxKey
        );

        return jdbc.queryForObject(
                """
                SELECT * FROM local_splits
                WHERE table_name = ? AND split_value = ?
                  AND COALESCE(index_name, '') = ? AND COALESCE(index_key, '') = ?
                """,
                mapper, tableName, sv, idxName, idxKey
        );
    }

    public List<LocalSplitResponse> getAllLocalSplits() {
        return jdbc.query("SELECT * FROM local_splits ORDER BY created_at DESC", mapper);
    }

    public List<LocalSplitResponse> getLocalSplitsByOperation(OperationType operationType) {
        return jdbc.query(
                "SELECT * FROM local_splits WHERE operation_type = ? ORDER BY created_at DESC",
                mapper, operationType.name()
        );
    }

    public boolean deleteLocalSplit(long id) {
        return jdbc.update("DELETE FROM local_splits WHERE id = ?", id) > 0;
    }

    public boolean deleteLocalSplitByValue(String tableName, String splitValue,
                                           String indexName, String indexKey) {
        String idxName = indexName == null ? "" : indexName;
        String idxKey = indexKey == null ? "" : indexKey;
        String sv = splitValue == null ? "" : splitValue;
        return jdbc.update(
                """
                DELETE FROM local_splits
                WHERE table_name = ? AND split_value = ?
                  AND COALESCE(index_name, '') = ? AND COALESCE(index_key, '') = ?
                """,
                tableName, sv, idxName, idxKey
        ) > 0;
    }

    public int clearPendingSplits() {
        return jdbc.update("DELETE FROM local_splits");
    }

    public int clearPendingSplits(OperationType operationType) {
        return jdbc.update("DELETE FROM local_splits WHERE operation_type = ?", operationType.name());
    }

    public Optional<LocalSplitResponse> getLocalSplitByTableAndValue(String tableName, String splitValue,
                                                                     String indexName, String indexKey) {
        String idxName = indexName == null ? "" : indexName;
        String idxKey = indexKey == null ? "" : indexKey;
        String sv = splitValue == null ? "" : splitValue;
        List<LocalSplitResponse> rows = jdbc.query(
                """
                SELECT * FROM local_splits
                WHERE table_name = ? AND split_value = ?
                  AND COALESCE(index_name, '') = ? AND COALESCE(index_key, '') = ?
                """,
                mapper, tableName, sv, idxName, idxKey
        );
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }
}
