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

import com.karthitect.spannersplits.model.EntitySummary;
import com.karthitect.spannersplits.model.EntityType;
import com.karthitect.spannersplits.model.LocalSplitResponse;
import com.karthitect.spannersplits.model.OperationType;
import com.karthitect.spannersplits.model.SpannerSplit;
import com.karthitect.spannersplits.model.SplitPointDisplay;
import com.karthitect.spannersplits.model.SplitStatus;
import com.karthitect.spannersplits.repository.LocalSplitsRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class SplitsAggregator {
    private static final Logger log = LoggerFactory.getLogger(SplitsAggregator.class);

    private final SpannerService spannerService;
    private final LocalSplitsRepository splitsRepo;

    public SplitsAggregator(SpannerService spannerService, LocalSplitsRepository splitsRepo) {
        this.spannerService = spannerService;
        this.splitsRepo = splitsRepo;
    }

    /** key for local-splits lookup: (table, splitValue). */
    private record Key(String table, String splitValue) {}

    private record LocalEntry(long id, OperationType op) {}

    public List<SplitPointDisplay> getCombinedSplits(String entityName, EntityType entityType) {
        List<LocalSplitResponse> localSplits = splitsRepo.getAllLocalSplits();

        Map<Key, LocalEntry> localLookup = new HashMap<>();
        for (LocalSplitResponse ls : localSplits) {
            localLookup.put(new Key(ls.getTableName(), ls.getSplitValue()),
                    new LocalEntry(ls.getId(), ls.getOperationType()));
        }

        List<SplitPointDisplay> combined = new ArrayList<>();
        Set<Key> seen = new HashSet<>();

        if (spannerService.isConfigured()) {
            try {
                List<SpannerSplit> spannerSplits = spannerService.listSplits();
                for (SpannerSplit sp : spannerSplits) {
                    String splitEntityName = StringUtils.hasText(sp.getIndex()) ? sp.getIndex() : sp.getTable();
                    EntityType splitEntityType = StringUtils.hasText(sp.getIndex()) ? EntityType.INDEX : EntityType.TABLE;

                    if (entityName != null && !entityName.equals(splitEntityName)) continue;
                    if (entityType != null && entityType != splitEntityType) continue;

                    Key key = new Key(sp.getTable(), sp.getSplitKey());
                    seen.add(key);

                    SplitStatus status;
                    Long localId = null;
                    LocalEntry entry = localLookup.get(key);
                    if (entry != null && entry.op() == OperationType.DELETE) {
                        status = SplitStatus.PENDING_DELETE;
                        localId = entry.id();
                    } else {
                        status = SplitStatus.SYNCED;
                    }

                    SplitPointDisplay d = new SplitPointDisplay();
                    d.setTableName(sp.getTable());
                    d.setSplitValue(sp.getSplitKey());
                    d.setStatus(status);
                    d.setExpireTime(sp.getExpireTime());
                    d.setLocalId(localId);
                    d.setInitiator(sp.getInitiator());
                    d.setIndex(sp.getIndex());
                    combined.add(d);
                }
            } catch (Exception e) {
                log.error("Error fetching Spanner splits: {}", e.getMessage());
            }
        }

        for (LocalSplitResponse ls : localSplits) {
            Key key = new Key(ls.getTableName(), ls.getSplitValue());
            if (seen.contains(key) || ls.getOperationType() != OperationType.ADD) continue;

            boolean isIndexSplit = StringUtils.hasText(ls.getIndexName());
            String splitEntityName = isIndexSplit ? ls.getIndexName() : ls.getTableName();
            EntityType splitEntityType = isIndexSplit ? EntityType.INDEX : EntityType.TABLE;

            if (entityName != null && !entityName.equals(splitEntityName)) continue;
            if (entityType != null && entityType != splitEntityType) continue;

            SplitPointDisplay d = new SplitPointDisplay();
            d.setTableName(ls.getTableName());
            d.setSplitValue(isIndexSplit ? ls.getIndexKey() : ls.getSplitValue());
            d.setStatus(SplitStatus.PENDING_ADD);
            d.setLocalId(ls.getId());
            if (isIndexSplit) {
                d.setIndex(ls.getIndexName());
                d.setIndexKey(ls.getIndexKey());
                if (StringUtils.hasText(ls.getSplitValue())) {
                    d.setTableKey(ls.getSplitValue());
                }
            }
            combined.add(d);
        }

        return combined;
    }

    public List<EntitySummary> getEntitySummaries() {
        Map<String, EntitySummary> entityMap = new HashMap<>();

        if (spannerService.isConfigured()) {
            try {
                for (String tableName : spannerService.listTables()) {
                    entityMap.put(EntityType.TABLE + "::" + tableName,
                            new EntitySummary(tableName, EntityType.TABLE, null));
                }
            } catch (Exception e) {
                log.error("Error fetching tables: {}", e.getMessage());
            }
            try {
                for (String[] entry : spannerService.listIndexes()) {
                    entityMap.put(EntityType.INDEX + "::" + entry[0],
                            new EntitySummary(entry[0], EntityType.INDEX, entry[1]));
                }
            } catch (Exception e) {
                log.error("Error fetching indexes: {}", e.getMessage());
            }
        }

        for (SplitPointDisplay split : getCombinedSplits(null, null)) {
            String entityName;
            EntityType entityType;
            String parentTable;
            if (StringUtils.hasText(split.getIndex())) {
                entityName = split.getIndex();
                entityType = EntityType.INDEX;
                parentTable = split.getTableName();
            } else {
                entityName = split.getTableName();
                entityType = EntityType.TABLE;
                parentTable = null;
            }

            String mapKey = entityType + "::" + entityName;
            EntitySummary s = entityMap.computeIfAbsent(mapKey,
                    k -> new EntitySummary(entityName, entityType, parentTable));
            s.setTotalSplits(s.getTotalSplits() + 1);
            switch (split.getStatus()) {
                case SYNCED -> s.setSyncedCount(s.getSyncedCount() + 1);
                case PENDING_ADD -> s.setPendingAddCount(s.getPendingAddCount() + 1);
                case PENDING_DELETE -> s.setPendingDeleteCount(s.getPendingDeleteCount() + 1);
            }
        }

        List<EntitySummary> summaries = new ArrayList<>(entityMap.values());
        summaries.sort(Comparator
                .comparing((EntitySummary e) -> e.getEntityType().name())
                .thenComparing(EntitySummary::getEntityName));
        return summaries;
    }
}
