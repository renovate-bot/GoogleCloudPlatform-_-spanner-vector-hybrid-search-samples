/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.karthitect.spannersplits.controller;

import com.karthitect.spannersplits.model.SettingsResponse;
import com.karthitect.spannersplits.repository.SettingsRepository;
import com.karthitect.spannersplits.service.SpannerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.HashMap;
import java.util.Map;

@Controller
public class WebController {

    private final SettingsRepository settingsRepo;
    private final SpannerService spannerService;

    @Value("${spanner.project:}")
    private String envProject;

    @Value("${spanner.instance:}")
    private String envInstance;

    @Value("${spanner.database:}")
    private String envDatabase;

    public WebController(SettingsRepository settingsRepo, SpannerService spannerService) {
        this.settingsRepo = settingsRepo;
        this.spannerService = spannerService;
    }

    private Map<String, Object> connectionInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("isConfigured", spannerService.isConfigured());
        info.put("projectId", spannerService.getProjectId());
        info.put("instanceId", spannerService.getInstanceId());
        info.put("databaseId", spannerService.getDatabaseId());
        return info;
    }

    private Map<String, Object> envVarInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("usingEnvVars", false);
        info.put("projectId", null);
        info.put("instanceId", null);
        info.put("databaseId", null);

        SettingsResponse dbSettings = settingsRepo.getAllSettings();

        if (!StringUtils.hasText(dbSettings.getProjectId()) && StringUtils.hasText(envProject)) {
            info.put("projectId", envProject);
            info.put("usingEnvVars", true);
        }
        if (!StringUtils.hasText(dbSettings.getInstanceId()) && StringUtils.hasText(envInstance)) {
            info.put("instanceId", envInstance);
            info.put("usingEnvVars", true);
        }
        if (!StringUtils.hasText(dbSettings.getDatabaseId()) && StringUtils.hasText(envDatabase)) {
            info.put("databaseId", envDatabase);
            info.put("usingEnvVars", true);
        }
        return info;
    }

    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("settings", settingsRepo.getAllSettings());
        model.addAttribute("isConfigured", spannerService.isConfigured());
        model.addAttribute("connectionInfo", connectionInfo());
        return "index";
    }

    @GetMapping("/settings")
    public String settingsPage(Model model) {
        model.addAttribute("settings", settingsRepo.getAllSettings());
        model.addAttribute("envInfo", envVarInfo());
        model.addAttribute("isConfigured", spannerService.isConfigured());
        model.addAttribute("connectionInfo", connectionInfo());
        return "settings";
    }

    @PostMapping("/settings")
    public String saveSettings(@RequestParam(name = "project_id", defaultValue = "") String projectId,
                               @RequestParam(name = "instance_id", defaultValue = "") String instanceId,
                               @RequestParam(name = "database_id", defaultValue = "") String databaseId,
                               Model model) {
        settingsRepo.updateSettings(
                projectId.isBlank() ? "" : projectId,
                instanceId.isBlank() ? "" : instanceId,
                databaseId.isBlank() ? "" : databaseId
        );
        spannerService.reset();

        Map<String, Object> envInfo = envVarInfo();
        model.addAttribute("settings", settingsRepo.getAllSettings());
        model.addAttribute("envInfo", envInfo);
        model.addAttribute("connectionInfo", connectionInfo());

        if (StringUtils.hasText(instanceId) && StringUtils.hasText(databaseId)) {
            SpannerService.ConnectionTestResult result = spannerService.testConnection();
            if (result.success) {
                model.addAttribute("isConfigured", true);
                model.addAttribute("successMessage", "Settings saved and connection verified successfully.");
            } else {
                model.addAttribute("isConfigured", spannerService.isConfigured());
                model.addAttribute("errorMessage", result.errorMessage);
            }
        } else {
            model.addAttribute("isConfigured", spannerService.isConfigured());
            model.addAttribute("successMessage", "Settings saved.");
        }
        return "settings";
    }

    @PostMapping("/settings/clear")
    public String clearSettings(Model model) {
        settingsRepo.clearSettings();
        spannerService.reset();

        Map<String, Object> envInfo = envVarInfo();
        model.addAttribute("settings", settingsRepo.getAllSettings());
        model.addAttribute("envInfo", envInfo);
        model.addAttribute("connectionInfo", connectionInfo());

        boolean usingEnv = (Boolean) envInfo.get("usingEnvVars");
        if (usingEnv && spannerService.isConfigured()) {
            SpannerService.ConnectionTestResult result = spannerService.testConnection();
            if (result.success) {
                model.addAttribute("isConfigured", true);
                model.addAttribute("successMessage", "Settings cleared. Now using environment variables.");
            } else {
                model.addAttribute("isConfigured", true);
                model.addAttribute("errorMessage", "Settings cleared but connection failed: " + result.errorMessage);
            }
        } else {
            model.addAttribute("isConfigured", spannerService.isConfigured());
            model.addAttribute("successMessage",
                    "Settings cleared." + (usingEnv ? "" : " No environment variables found."));
        }
        return "settings";
    }
}
