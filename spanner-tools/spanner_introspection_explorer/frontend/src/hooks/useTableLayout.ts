/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { useState, useEffect, useMemo, useCallback } from 'react';
import { ColumnMetadata } from '../types';

export interface ColumnLayoutState {
  columnOrder: string[];
  columnWidths: Record<string, number>;
  pinnedColumns: string[];
}

const STORAGE_PREFIX = 'spanner_table_layout_v2_';

export function useTableLayout(tableName: string, defaultColumns: ColumnMetadata[]) {
  const storageKey = `${STORAGE_PREFIX}${tableName}`;

  // Default widths based on column types and names
  const defaultWidths = useMemo(() => {
    const widths: Record<string, number> = {};
    defaultColumns.forEach((col) => {
      const lower = col.name.toLowerCase();
      if (col.filter_type === 'date' || lower === 'interval_end') {
        widths[col.name] = 180;
      } else if (lower === 'text' || lower === 'query_text') {
        widths[col.name] = 360;
      } else if (lower.includes('fingerprint') || lower === 'fprint') {
        widths[col.name] = 210;
      } else if (col.filter_type === 'numeric') {
        // e.g. latency_p95 -> 180px, AVG_COMMIT_LATENCY_SECONDS -> 280px
        widths[col.name] = Math.max(180, Math.round(col.name.length * 9.5 + 45));
      } else {
        widths[col.name] = Math.max(190, Math.round(col.name.length * 9.5 + 45));
      }
    });
    return widths;
  }, [defaultColumns]);

  const defaultOrder = useMemo(() => defaultColumns.map((c) => c.name), [defaultColumns]);

  // Load initial state from localStorage or defaults
  const [layout, setLayout] = useState<ColumnLayoutState>(() => {
    try {
      const saved = localStorage.getItem(storageKey);
      if (saved) {
        const parsed: ColumnLayoutState = JSON.parse(saved);
        const currentNames = new Set(defaultColumns.map((c) => c.name));
        const validOrder = parsed.columnOrder.filter((name) => currentNames.has(name));
        defaultColumns.forEach((c) => {
          if (!validOrder.includes(c.name)) {
            validOrder.push(c.name);
          }
        });
        const validPinned = (parsed.pinnedColumns || []).filter((name) => currentNames.has(name));
        return {
          columnOrder: validOrder,
          columnWidths: { ...defaultWidths, ...parsed.columnWidths },
          pinnedColumns: validPinned,
        };
      }
    } catch {
      // Ignore JSON error
    }
    return {
      columnOrder: defaultOrder,
      columnWidths: defaultWidths,
      pinnedColumns: [],
    };
  });

  // Re-sync when table or defaultColumns change
  useEffect(() => {
    try {
      const saved = localStorage.getItem(storageKey);
      if (saved) {
        const parsed: ColumnLayoutState = JSON.parse(saved);
        const currentNames = new Set(defaultColumns.map((c) => c.name));
        const validOrder = parsed.columnOrder.filter((name) => currentNames.has(name));
        defaultColumns.forEach((c) => {
          if (!validOrder.includes(c.name)) {
            validOrder.push(c.name);
          }
        });
        const validPinned = (parsed.pinnedColumns || []).filter((name) => currentNames.has(name));
        setLayout({
          columnOrder: validOrder,
          columnWidths: { ...defaultWidths, ...parsed.columnWidths },
          pinnedColumns: validPinned,
        });
        return;
      }
    } catch {
      // Fallback
    }

    setLayout({
      columnOrder: defaultColumns.map((c) => c.name),
      columnWidths: defaultWidths,
      pinnedColumns: [],
    });
  }, [tableName, defaultColumns, defaultWidths, storageKey]);

  // Persist state to localStorage on changes
  const saveLayout = useCallback(
    (newState: ColumnLayoutState) => {
      setLayout(newState);
      try {
        localStorage.setItem(storageKey, JSON.stringify(newState));
      } catch {
        // Storage might be full or disabled
      }
    },
    [storageKey]
  );

  // Column Resizing
  const handleResize = useCallback(
    (columnName: string, newWidth: number) => {
      const minWidth = 70;
      const finalWidth = Math.max(minWidth, Math.round(newWidth));
      saveLayout({
        ...layout,
        columnWidths: {
          ...layout.columnWidths,
          [columnName]: finalWidth,
        },
      });
    },
    [layout, saveLayout]
  );

  // Column Pinning
  const handleTogglePin = useCallback(
    (columnName: string) => {
      const isPinned = layout.pinnedColumns.includes(columnName);
      let newPinned: string[];
      let newOrder = [...layout.columnOrder];

      if (isPinned) {
        newPinned = layout.pinnedColumns.filter((c) => c !== columnName);
      } else {
        newPinned = [...layout.pinnedColumns, columnName];
        // Move pinned column to the left in columnOrder
        newOrder = newOrder.filter((c) => c !== columnName);
        const lastPinnedIdx = newOrder.findIndex((c) => !newPinned.includes(c));
        if (lastPinnedIdx === -1) {
          newOrder.push(columnName);
        } else {
          newOrder.splice(lastPinnedIdx, 0, columnName);
        }
      }

      saveLayout({
        ...layout,
        pinnedColumns: newPinned,
        columnOrder: newOrder,
      });
    },
    [layout, saveLayout]
  );

  // Column Reordering (Drag and Drop)
  const handleReorder = useCallback(
    (sourceCol: string, targetCol: string) => {
      if (sourceCol === targetCol) return;

      const newOrder = [...layout.columnOrder];
      const sourceIdx = newOrder.indexOf(sourceCol);
      const targetIdx = newOrder.indexOf(targetCol);

      if (sourceIdx === -1 || targetIdx === -1) return;

      newOrder.splice(sourceIdx, 1);
      newOrder.splice(targetIdx, 0, sourceCol);

      // Maintain pinned group integrity: pinned columns should remain on the left
      const pinnedSet = new Set(layout.pinnedColumns);
      const pinnedList = newOrder.filter((c) => pinnedSet.has(c));
      const unpinnedList = newOrder.filter((c) => !pinnedSet.has(c));
      const finalizedOrder = [...pinnedList, ...unpinnedList];

      saveLayout({
        ...layout,
        columnOrder: finalizedOrder,
      });
    },
    [layout, saveLayout]
  );

  // Reset to default layout
  const handleResetLayout = useCallback(() => {
    try {
      localStorage.removeItem(storageKey);
    } catch {
      // Ignore
    }
    setLayout({
      columnOrder: defaultOrder,
      columnWidths: defaultWidths,
      pinnedColumns: [],
    });
  }, [storageKey, defaultOrder, defaultWidths]);

  // Map ordered column names back to ColumnMetadata objects
  const colMap = useMemo(() => {
    const map = new Map<string, ColumnMetadata>();
    defaultColumns.forEach((c) => map.set(c.name, c));
    return map;
  }, [defaultColumns]);

  const orderedColumns = useMemo(() => {
    const pinnedSet = new Set(layout.pinnedColumns);
    const pinned = layout.columnOrder.filter((name) => pinnedSet.has(name));
    const unpinned = layout.columnOrder.filter((name) => !pinnedSet.has(name));
    const fullOrder = [...pinned, ...unpinned];

    return fullOrder
      .map((name) => colMap.get(name))
      .filter((c): c is ColumnMetadata => c !== undefined);
  }, [layout.columnOrder, layout.pinnedColumns, colMap]);

  // Compute sticky left offsets for pinned columns (taking 50px index column into account)
  const stickyLeftOffsets = useMemo(() => {
    const offsets: Record<string, number> = {};
    let currentLeft = 50; // Width of row index column '#'

    orderedColumns.forEach((col) => {
      if (layout.pinnedColumns.includes(col.name)) {
        offsets[col.name] = currentLeft;
        const width = layout.columnWidths[col.name] || defaultWidths[col.name] || 150;
        currentLeft += width;
      }
    });

    return offsets;
  }, [orderedColumns, layout.pinnedColumns, layout.columnWidths, defaultWidths]);

  const isCustomized = useMemo(() => {
    if (layout.pinnedColumns.length > 0) return true;
    if (layout.columnOrder.length !== defaultOrder.length) return true;
    for (let i = 0; i < defaultOrder.length; i++) {
      if (layout.columnOrder[i] !== defaultOrder[i]) return true;
    }
    return false;
  }, [layout, defaultOrder]);

  return {
    orderedColumns,
    columnWidths: layout.columnWidths,
    pinnedColumns: layout.pinnedColumns,
    stickyLeftOffsets,
    isCustomized,
    handleResize,
    handleTogglePin,
    handleReorder,
    handleResetLayout,
  };
}
