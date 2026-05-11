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

import com.karthitect.spannersplits.model.SettingsResponse;
import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class SettingsRepository {

    private static final List<String> SETTING_KEYS = List.of("project_id", "instance_id", "database_id");

    private final JdbcTemplate jdbc;

    public SettingsRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    public void initSchema() {
        jdbc.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """);
    }

    public String getSetting(String key) {
        return jdbc.query(
                "SELECT value FROM settings WHERE key = ?",
                rs -> rs.next() ? rs.getString(1) : null,
                key
        );
    }

    public void setSetting(String key, String value) {
        jdbc.update(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                key, value
        );
    }

    public SettingsResponse getAllSettings() {
        return new SettingsResponse(
                getSetting("project_id"),
                getSetting("instance_id"),
                getSetting("database_id")
        );
    }

    public void updateSettings(String projectId, String instanceId, String databaseId) {
        if (projectId != null) setSetting("project_id", projectId);
        if (instanceId != null) setSetting("instance_id", instanceId);
        if (databaseId != null) setSetting("database_id", databaseId);
    }

    public void clearSettings() {
        jdbc.update("DELETE FROM settings WHERE key IN ('project_id', 'instance_id', 'database_id')");
    }
}
