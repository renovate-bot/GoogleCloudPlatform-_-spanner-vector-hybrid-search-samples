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

import com.karthitect.spannersplits.model.EntityKeySchema;
import com.karthitect.spannersplits.model.EntitySummary;
import com.karthitect.spannersplits.model.EntityType;
import com.karthitect.spannersplits.model.LocalSplitCreate;
import com.karthitect.spannersplits.model.LocalSplitResponse;
import com.karthitect.spannersplits.model.OperationType;
import com.karthitect.spannersplits.model.RangeSplitRequest;
import com.karthitect.spannersplits.model.RangeSplitResponse;
import com.karthitect.spannersplits.model.RangeValidationResult;
import com.karthitect.spannersplits.model.SettingsResponse;
import com.karthitect.spannersplits.model.SplitPointDisplay;
import com.karthitect.spannersplits.model.SyncResult;
import com.karthitect.spannersplits.repository.LocalSplitsRepository;
import com.karthitect.spannersplits.repository.SettingsRepository;
import com.karthitect.spannersplits.service.SpannerService;
import com.karthitect.spannersplits.service.SplitsAggregator;
import com.karthitect.spannersplits.util.RangeUtils;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ApiController {
    private static final Logger log = LoggerFactory.getLogger(ApiController.class);

    private final SpannerService spannerService;
    private final SplitsAggregator aggregator;
    private final SettingsRepository settingsRepo;
    private final LocalSplitsRepository splitsRepo;

    public ApiController(SpannerService spannerService,
                         SplitsAggregator aggregator,
                         SettingsRepository settingsRepo,
                         LocalSplitsRepository splitsRepo) {
        this.spannerService = spannerService;
        this.aggregator = aggregator;
        this.settingsRepo = settingsRepo;
        this.splitsRepo = splitsRepo;
    }

    @GetMapping("/entities")
    public List<EntitySummary> listEntities() {
        return aggregator.getEntitySummaries();
    }

    @GetMapping("/entity-schema")
    public EntityKeySchema getEntitySchema(@RequestParam("entity_name") String entityName,
                                           @RequestParam("entity_type") EntityType entityType) {
        if (entityType == EntityType.TABLE) {
            return spannerService.getTableKeySchema(entityName);
        }
        return spannerService.getIndexKeySchema(entityName);
    }

    @GetMapping("/splits")
    public List<SplitPointDisplay> listSplits(@RequestParam(value = "entity_name", required = false) String entityName,
                                              @RequestParam(value = "entity_type", required = false) EntityType entityType) {
        return aggregator.getCombinedSplits(entityName, entityType);
    }

    @PostMapping("/splits")
    public LocalSplitResponse addSplit(@Valid @RequestBody LocalSplitCreate split) {
        return splitsRepo.addLocalSplit(
                split.getTableName(),
                split.getSplitValue(),
                split.getOperationType(),
                split.getIndexName(),
                split.getIndexKey()
        );
    }

    @DeleteMapping("/splits/{splitId}")
    public ResponseEntity<Map<String, Object>> deleteSplit(@PathVariable("splitId") long splitId) {
        if (splitsRepo.deleteLocalSplit(splitId)) {
            return ResponseEntity.ok(Map.of("success", true));
        }
        return ResponseEntity.status(404).body(Map.of("detail", "Split not found"));
    }

    @PostMapping("/splits/clear")
    public Map<String, Object> clearPending() {
        int cleared = splitsRepo.clearPendingSplits();
        return Map.of("success", true, "cleared", cleared);
    }

    @PostMapping("/sync")
    public SyncResult sync() {
        if (!spannerService.isConfigured()) {
            return new SyncResult(false, "Spanner not configured. Please set instance and database in settings.");
        }
        return spannerService.syncPendingChanges();
    }

    @GetMapping("/settings")
    public SettingsResponse getSettings() {
        return settingsRepo.getAllSettings();
    }

    @PostMapping("/splits/range")
    public RangeSplitResponse addRangeSplits(@Valid @RequestBody RangeSplitRequest request) {
        String entityName = request.getIndexName() != null ? request.getIndexName() : request.getTableName();
        EntityType entityType = request.getIndexName() != null ? EntityType.INDEX : EntityType.TABLE;

        EntityKeySchema schema;
        try {
            schema = (entityType == EntityType.TABLE)
                    ? spannerService.getTableKeySchema(entityName)
                    : spannerService.getIndexKeySchema(entityName);
        } catch (Exception e) {
            RangeSplitResponse r = new RangeSplitResponse(false, "Failed to get entity schema: " + e.getMessage());
            r.getErrors().add(e.getMessage());
            return r;
        }

        RangeValidationResult validation = RangeUtils.validateRangeRequest(schema, request.getStartValue(), request.getEndValue());
        if (!validation.isValid()) {
            RangeSplitResponse r = new RangeSplitResponse(false,
                    validation.getErrorMessage() != null ? validation.getErrorMessage() : "Validation failed");
            if (validation.getErrorMessage() != null) {
                r.getErrors().add(validation.getErrorMessage());
            }
            return r;
        }

        RangeUtils.RangeResult generated;
        try {
            generated = RangeUtils.generateRangeSplits(
                    validation.getRangeType(),
                    request.getStartValue(),
                    request.getEndValue(),
                    request.getNumSplits(),
                    request.isIncludeBoundaries()
            );
        } catch (IllegalArgumentException e) {
            RangeSplitResponse r = new RangeSplitResponse(false, e.getMessage());
            r.getErrors().add(e.getMessage());
            return r;
        }

        int createdCount = 0;
        RangeSplitResponse response = new RangeSplitResponse();
        response.setGeneratedValues(generated.values);
        response.setWarnings(generated.warnings);

        for (String value : generated.values) {
            try {
                if (request.getIndexName() != null) {
                    splitsRepo.addLocalSplit(
                            request.getTableName(), "", OperationType.ADD,
                            request.getIndexName(), value);
                } else {
                    splitsRepo.addLocalSplit(
                            request.getTableName(), value, OperationType.ADD, null, null);
                }
                createdCount++;
            } catch (Exception e) {
                String msg = "Failed to add split '" + value + "': " + e.getMessage();
                response.getErrors().add(msg);
                log.error(msg);
            }
        }

        response.setSplitsCreated(createdCount);
        response.setSuccess(createdCount > 0 && response.getErrors().isEmpty());
        response.setMessage("Created " + createdCount + " of " + generated.values.size() + " split points");
        return response;
    }

    @GetMapping("/splits/range/validate")
    public RangeValidationResult validateRange(@RequestParam("entity_name") String entityName,
                                               @RequestParam("entity_type") EntityType entityType,
                                               @RequestParam("start_value") String startValue,
                                               @RequestParam("end_value") String endValue) {
        EntityKeySchema schema;
        try {
            schema = (entityType == EntityType.TABLE)
                    ? spannerService.getTableKeySchema(entityName)
                    : spannerService.getIndexKeySchema(entityName);
        } catch (Exception e) {
            return RangeValidationResult.fail(null, "Failed to get entity schema: " + e.getMessage());
        }
        return RangeUtils.validateRangeRequest(schema, startValue, endValue);
    }
}
