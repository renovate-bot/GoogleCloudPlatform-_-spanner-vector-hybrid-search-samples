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

import React, { useState, useEffect, useCallback } from 'react';
import {
  Box,
  Typography,
  Breadcrumbs,
  Link,
  Chip,
  CircularProgress,
  Alert,
  Paper,
} from '@mui/material';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import TableChartIcon from '@mui/icons-material/TableChart';

import { ColumnFilter, ColumnMetadata, QueryResultPage, ScatterDataPoint, SortConfig, TableMetadata } from '../types';
import { api } from '../services/api';
import { FilterBar } from '../components/FilterBar';
import { ServerDataGrid } from '../components/ServerDataGrid';
import { TimelineHistogram } from '../components/TimelineHistogram';
import { ScatterPlotOutliers } from '../components/ScatterPlotOutliers';
import AutoGraphIcon from '@mui/icons-material/AutoGraph';
import { Button } from '@mui/material';
import { gcpPalette } from '../theme';

interface TableExplorerViewProps {
  tableName: string;
  utcOffset: number;
  utcOffsetLabel: string;
  db?: string;
  onNavigateHome: () => void;
}

export const TableExplorerView: React.FC<TableExplorerViewProps> = ({
  tableName,
  utcOffset,
  utcOffsetLabel,
  db,
  onNavigateHome,
}) => {
  const [tableMeta, setTableMeta] = useState<TableMetadata | null>(null);
  const [metaLoading, setMetaLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Outlier scatter plot toggle
  const [showScatter, setShowScatter] = useState(false);

  // Query state
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [sort, setSort] = useState<SortConfig | null>(null);
  const [filters, setFilters] = useState<Record<string, ColumnFilter>>({});
  const [globalSearch, setGlobalSearch] = useState('');

  // Data results
  const [data, setData] = useState<QueryResultPage | null>(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [exporting, setExporting] = useState(false);

  // Interval column helpers
  const intervalCol = tableMeta?.columns.find((c) => c.name.toLowerCase() === 'interval_end');
  const intervalColName = intervalCol?.name || 'interval_end';
  const selectedIntervalTimestamps = filters[intervalColName]?.selected_timestamps || [];

  const handleTimelineSelect = (timestamps: string[]) => {
    const newFilters = { ...filters };
    if (timestamps.length > 0) {
      newFilters[intervalColName] = {
        type: 'date',
        selected_timestamps: timestamps,
      };
    } else {
      delete newFilters[intervalColName];
    }
    setFilters(newFilters);
    setPage(1);
  };

  const handleSelectOutlier = (point: ScatterDataPoint, labelCol: string) => {
    const newFilters = { ...filters };
    if (point.label && labelCol) {
      newFilters[labelCol] = {
        type: 'text',
        operator: 'exact',
        value: point.label,
      };
    } else if (point.interval && intervalColName) {
      newFilters[intervalColName] = {
        type: 'date',
        selected_timestamps: [point.interval],
      };
    }
    setFilters(newFilters);
    setPage(1);
  };

  const handleSelectRange = (range: { xCol: string; minX: number; maxX: number; yCol: string; minY: number; maxY: number }) => {
    const newFilters = { ...filters };
    if (range.xCol) {
      newFilters[range.xCol] = {
        type: 'numeric',
        min: Number(range.minX.toFixed(2)),
        max: Number(range.maxX.toFixed(2)),
      };
    }
    if (range.yCol) {
      newFilters[range.yCol] = {
        type: 'numeric',
        min: Number(range.minY.toFixed(2)),
        max: Number(range.maxY.toFixed(2)),
      };
    }
    setFilters(newFilters);
    setPage(1);
  };

  const handleApplyRangeFilter = (colName: string, min: number, max: number) => {
    const newFilters = { ...filters };
    newFilters[colName] = {
      type: 'numeric',
      min: Number(min.toFixed(2)),
      max: Number(max.toFixed(2)),
    };
    setFilters(newFilters);
    setPage(1);
  };

  const handleApplyCategoryFilter = (colName: string, val: string) => {
    const newFilters = { ...filters };
    newFilters[colName] = {
      type: 'text',
      operator: 'exact',
      value: val,
    };
    setFilters(newFilters);
    setPage(1);
  };

  const handleClearRange = (xCol?: string, yCol?: string) => {
    const newFilters = { ...filters };
    if (xCol) delete newFilters[xCol];
    if (yCol) delete newFilters[yCol];
    if (!xCol && !yCol) {
      Object.keys(newFilters).forEach((k) => {
        if (newFilters[k].type === 'numeric') {
          delete newFilters[k];
        }
      });
    }
    setFilters(newFilters);
    setPage(1);
  };

  // Load Table Schema on table change
  useEffect(() => {
    let isMounted = true;
    setMetaLoading(true);
    setError(null);
    setPage(1);
    setFilters({});
    setGlobalSearch('');
    setSort(null);

    api.getTableSchema(tableName, db)
      .then((meta) => {
        if (isMounted) {
          setTableMeta(meta);
          setMetaLoading(false);
        }
      })
      .catch((err) => {
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || 'Failed to load table metadata');
          setMetaLoading(false);
        }
      });

    return () => {
      isMounted = false;
    };
  }, [tableName, db]);

  // Execute Server-Side Query
  const fetchTableData = useCallback(async () => {
    if (!tableName) return;
    setQueryLoading(true);
    try {
      const res = await api.queryTable(tableName, {
        page,
        page_size: pageSize,
        sort,
        filters,
        utc_offset: utcOffset,
        global_search: globalSearch,
      }, db);
      setData(res);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Query execution failed');
    } finally {
      setQueryLoading(false);
    }
  }, [tableName, page, pageSize, sort, filters, utcOffset, globalSearch, db]);

  useEffect(() => {
    fetchTableData();
  }, [fetchTableData]);

  // Handle Export CSV
  const handleExportCsv = async () => {
    setExporting(true);
    try {
      await api.exportTableCsv(tableName, {
        page: 1,
        page_size: 1000000,
        sort,
        filters,
        utc_offset: utcOffset,
        global_search: globalSearch,
      }, db);
    } catch (err) {
      console.error('Export failed', err);
    } finally {
      setExporting(false);
    }
  };

  const handleFilterChange = (newFilters: Record<string, ColumnFilter>) => {
    setFilters(newFilters);
    setPage(1); // Reset to page 1 on filter change
  };

  const handleGlobalSearchChange = (search: string) => {
    setGlobalSearch(search);
    setPage(1);
  };

  const handleReset = () => {
    setFilters({});
    setGlobalSearch('');
    setSort(null);
    setPage(1);
  };

  if (metaLoading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', py: 10 }}>
        <CircularProgress size={40} />
      </Box>
    );
  }

  if (error && !tableMeta) {
    return (
      <Alert severity="error" sx={{ my: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}>
      {/* Breadcrumbs Navigation */}
      <Box sx={{ flexShrink: 0, mb: 1.5 }}>
        <Breadcrumbs separator={<NavigateNextIcon fontSize="small" />}>
          <Link
            component="button"
            variant="body2"
            onClick={onNavigateHome}
            sx={{ textDecoration: 'none', color: gcpPalette.neutral.textSecondary, '&:hover': { textDecoration: 'underline' } }}
          >
            Overview
          </Link>
          <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
            {tableMeta?.category}
          </Typography>
          <Typography variant="body2" sx={{ color: gcpPalette.neutral.textPrimary, fontWeight: 600 }}>
            {tableName}
          </Typography>
        </Breadcrumbs>
      </Box>

      {/* Error Alert */}
      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      {/* Header Info */}
      <Box sx={{ flexShrink: 0, display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', mb: 2, gap: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <TableChartIcon sx={{ color: gcpPalette.primary.main, fontSize: 28 }} />
          <Box>
            <Typography variant="h1" sx={{ fontSize: '1.4rem' }}>
              {tableName}
            </Typography>
            <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
              Displaying timestamps in <strong>{utcOffsetLabel}</strong>
            </Typography>
          </Box>
        </Box>

        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
          <Button
            size="small"
            variant={showScatter ? "contained" : "outlined"}
            startIcon={<AutoGraphIcon />}
            onClick={() => setShowScatter(!showScatter)}
            sx={{
              textTransform: 'none',
              fontSize: '0.8125rem',
              fontWeight: 500,
              backgroundColor: showScatter ? gcpPalette.primary.main : '#ffffff',
            }}
          >
            {showScatter ? 'Hide Outliers' : 'Outlier Scatter'}
          </Button>
          <Chip label={tableMeta?.category} size="small" sx={{ backgroundColor: gcpPalette.primary.light, color: gcpPalette.primary.main, fontWeight: 600 }} />
          <Chip label={`${tableMeta?.columns.length} columns`} size="small" variant="outlined" />
        </Box>
      </Box>

      {/* Interactive Timeline Histogram if table has INTERVAL_END */}
      {tableMeta && intervalCol && (
        <Box sx={{ flexShrink: 0 }}>
          <TimelineHistogram
            tableName={tableName}
            utcOffset={utcOffset}
            selectedTimestamps={selectedIntervalTimestamps}
            db={db}
            onSelectIntervals={handleTimelineSelect}
          />
        </Box>
      )}

      {/* Multivariate Outlier Scatter Plot (Collapsible / Toggleable) */}
      {tableMeta && showScatter && (
        <Box sx={{ flexShrink: 0 }}>
          <ScatterPlotOutliers
            tableName={tableName}
            filters={filters}
            utcOffset={utcOffset}
            db={db}
            onSelectOutlier={handleSelectOutlier}
            onSelectRange={handleSelectRange}
            onClearRange={handleClearRange}
            onClose={() => setShowScatter(false)}
          />
        </Box>
      )}

      {/* Dynamic Filter Bar */}
      {tableMeta && (
        <Box sx={{ flexShrink: 0 }}>
          <FilterBar
            tableName={tableName}
            columns={tableMeta.columns}
            filters={filters}
            globalSearch={globalSearch}
            utcOffset={utcOffset}
            onFilterChange={handleFilterChange}
            onGlobalSearchChange={handleGlobalSearchChange}
            onReset={handleReset}
          />
        </Box>
      )}

      {/* High-Performance Server-Side Data Grid */}
      {tableMeta && (
        <Box sx={{ flexGrow: 1, minHeight: 480, display: 'flex', flexDirection: 'column', mt: 1 }}>
          <ServerDataGrid
            tableName={tableName}
            columns={tableMeta.columns}
            data={data}
            loading={queryLoading}
            page={page}
            pageSize={pageSize}
            sort={sort}
            utcOffset={utcOffset}
            db={db}
            onPageChange={setPage}
            onPageSizeChange={(newPageSize) => {
              setPageSize(newPageSize);
              setPage(1);
            }}
            onSortChange={(newSort) => {
              setSort(newSort);
              setPage(1);
            }}
            onExportCsv={handleExportCsv}
            onApplyRangeFilter={handleApplyRangeFilter}
            onApplyCategoryFilter={handleApplyCategoryFilter}
            exporting={exporting}
          />
        </Box>
      )}
    </Box>
  );
};
