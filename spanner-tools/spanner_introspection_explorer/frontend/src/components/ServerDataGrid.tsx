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

import React, { useState, useRef, useEffect, useCallback } from 'react';
import {
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TableSortLabel,
  Paper,
  Typography,
  Button,
  CircularProgress,
  LinearProgress,
  Tooltip,
  IconButton,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import DownloadIcon from '@mui/icons-material/Download';
import SpeedIcon from '@mui/icons-material/Speed';
import CodeIcon from '@mui/icons-material/Code';
import BarChartIcon from '@mui/icons-material/BarChart';
import PushPinIcon from '@mui/icons-material/PushPin';
import PushPinOutlinedIcon from '@mui/icons-material/PushPinOutlined';
import DragIndicatorIcon from '@mui/icons-material/DragIndicator';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import OpenInFullIcon from '@mui/icons-material/OpenInFull';
import CloseIcon from '@mui/icons-material/Close';

import { ColumnMetadata, QueryResultPage, SortConfig, ColumnProfile } from '../types';
import { api } from '../services/api';
import { CliExporter } from './CliExporter';
import { ColumnSparkline } from './ColumnSparkline';
import { useTableLayout } from '../hooks/useTableLayout';
import { gcpPalette } from '../theme';

interface ServerDataGridProps {
  tableName: string;
  columns: ColumnMetadata[];
  data: QueryResultPage | null;
  loading: boolean;
  page: number;
  pageSize: number;
  sort: SortConfig | null;
  utcOffset: number;
  db?: string;
  onPageChange: (newPage: number) => void;
  onPageSizeChange: (newPageSize: number) => void;
  onSortChange: (newSort: SortConfig | null) => void;
  onExportCsv: () => void;
  onApplyRangeFilter?: (column: string, min: number, max: number) => void;
  onApplyCategoryFilter?: (column: string, value: string) => void;
  exporting: boolean;
}

export const ServerDataGrid: React.FC<ServerDataGridProps> = ({
  tableName,
  columns,
  data,
  loading,
  page,
  pageSize,
  sort,
  utcOffset,
  db,
  onPageChange,
  onPageSizeChange,
  onSortChange,
  onExportCsv,
  onApplyRangeFilter,
  onApplyCategoryFilter,
  exporting,
}) => {
  const [showExporter, setShowExporter] = useState(false);
  const tableContainerRef = useRef<HTMLDivElement>(null);

  // Column Distribution Profiles State (Enabled by default)
  const [profiles, setProfiles] = useState<Record<string, ColumnProfile>>({});
  const [showProfiles, setShowProfiles] = useState<boolean>(() => {
    const saved = localStorage.getItem('span_explorer_show_column_profiles');
    return saved !== 'false';
  });

  useEffect(() => {
    let isMounted = true;
    api.getTableColumnProfiles(tableName, db)
      .then((res) => {
        if (isMounted) {
          setProfiles(res.profiles || {});
        }
      })
      .catch((err) => {
        console.error('Failed to load column profiles', err);
      });
    return () => { isMounted = false; };
  }, [tableName, db]);

  // Full Text Cell Detail Modal State
  const [cellModal, setCellModal] = useState<{
    open: boolean;
    columnName: string;
    value: string;
    rowNumber: number;
  }>({
    open: false,
    columnName: '',
    value: '',
    rowNumber: 1,
  });
  const [copied, setCopied] = useState(false);

  const handleOpenCellModal = (columnName: string, value: string, rowNumber: number) => {
    setCellModal({
      open: true,
      columnName,
      value,
      rowNumber,
    });
    setCopied(false);
  };

  const handleCopyText = async () => {
    if (!cellModal.value) return;
    try {
      await navigator.clipboard.writeText(cellModal.value);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback
    }
  };

  // Table Layout State (Order, Widths, Pinned, LocalStorage)
  const {
    orderedColumns,
    columnWidths,
    pinnedColumns,
    stickyLeftOffsets,
    isCustomized,
    handleResize,
    handleTogglePin,
    handleReorder,
    handleResetLayout,
  } = useTableLayout(tableName, columns);

  // Drag-to-Resize State
  const [resizingCol, setResizingCol] = useState<string | null>(null);
  const resizeStartX = useRef<number>(0);
  const resizeStartWidth = useRef<number>(0);

  const startResize = useCallback(
    (e: React.MouseEvent, colName: string) => {
      e.preventDefault();
      e.stopPropagation();
      setResizingCol(colName);
      resizeStartX.current = e.clientX;
      resizeStartWidth.current = columnWidths[colName] || 160;

      const onMouseMove = (moveEvent: MouseEvent) => {
        const delta = moveEvent.clientX - resizeStartX.current;
        const newWidth = Math.max(70, resizeStartWidth.current + delta);
        handleResize(colName, newWidth);
      };

      const onMouseUp = () => {
        setResizingCol(null);
        window.removeEventListener('mousemove', onMouseMove);
        window.removeEventListener('mouseup', onMouseUp);
      };

      window.addEventListener('mousemove', onMouseMove);
      window.addEventListener('mouseup', onMouseUp);
    },
    [columnWidths, handleResize]
  );

  // Drag-and-Drop Column Reordering State
  const [draggedCol, setDraggedCol] = useState<string | null>(null);
  const [dragOverCol, setDragOverCol] = useState<string | null>(null);

  const handleDragStart = (e: React.DragEvent, colName: string) => {
    setDraggedCol(colName);
    e.dataTransfer.setData('text/plain', colName);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e: React.DragEvent, colName: string) => {
    e.preventDefault();
    if (draggedCol && draggedCol !== colName) {
      setDragOverCol(colName);
    }
  };

  const handleDrop = (e: React.DragEvent, targetCol: string) => {
    e.preventDefault();
    if (draggedCol && draggedCol !== targetCol) {
      handleReorder(draggedCol, targetCol);
    }
    setDraggedCol(null);
    setDragOverCol(null);
  };

  const handleDragEnd = () => {
    setDraggedCol(null);
    setDragOverCol(null);
  };

  // Auto-scroll to top on page or table change
  useEffect(() => {
    if (tableContainerRef.current) {
      tableContainerRef.current.scrollTop = 0;
    }
  }, [page, tableName]);

  const handleSort = (columnName: string) => {
    if (sort?.column === columnName) {
      if (sort.order === 'ASC') {
        onSortChange({ column: columnName, order: 'DESC' });
      } else {
        onSortChange(null); // Clear sort
      }
    } else {
      onSortChange({ column: columnName, order: 'ASC' });
    }
  };

  const formatCellValue = (value: any, col: ColumnMetadata, rowNumber: number) => {
    if (value === null || value === undefined || value === '') {
      return <Typography variant="caption" sx={{ color: '#bdc1c6', fontStyle: 'italic' }}>null</Typography>;
    }

    if (typeof value === 'number') {
      if (Number.isInteger(value)) {
        return value.toLocaleString();
      }
      return value.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 2 });
    }

    const strVal = String(value);
    const singleLine = strVal.replace(/[\r\n\t]+/g, ' ').trim();
    const isExpandable = strVal.length > 40 || strVal.includes('\n');

    if (isExpandable) {
      return (
        <Tooltip title="Click to view full text & copy" arrow enterDelay={300}>
          <Box
            component="span"
            onClick={(e) => {
              e.stopPropagation();
              handleOpenCellModal(col.name, strVal, rowNumber);
            }}
            sx={{
              cursor: 'pointer',
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              width: '100%',
              gap: 0.5,
              color: gcpPalette.neutral.textPrimary,
              fontFamily: col.filter_type === 'text' && (strVal.includes('SELECT') || strVal.includes('INSERT') || strVal.includes('UPDATE')) ? 'monospace' : 'inherit',
              '&:hover': {
                color: gcpPalette.primary.main,
                '& .expand-icon': { opacity: 1, color: gcpPalette.primary.main },
              },
            }}
          >
            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {singleLine}
            </span>
            <OpenInFullIcon className="expand-icon" sx={{ fontSize: 12, opacity: 0.35, flexShrink: 0, ml: 0.5 }} />
          </Box>
        </Tooltip>
      );
    }

    return singleLine;
  };

  const total = data?.total || 0;
  const items = data?.items || [];
  const durationMs = data?.duration_ms || 0;
  const executedSql = data?.executed_sql || '';

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, height: '100%', userSelect: resizingCol ? 'none' : 'auto' }}>
      {/* Top Header Bar with Metrics & Actions */}
      <Box sx={{ flexShrink: 0, display: 'flex', flexWrap: 'wrap', alignItems: 'center', justifyContent: 'space-between', mb: 1, gap: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, color: gcpPalette.neutral.textPrimary }}>
            Showing {items.length > 0 ? (page - 1) * pageSize + 1 : 0}–{Math.min(page * pageSize, total)} of {total.toLocaleString()} rows
          </Typography>

          {durationMs > 0 && (
            <Chip
              icon={<SpeedIcon sx={{ fontSize: '14px !important' }} />}
              label={`${durationMs} ms`}
              size="small"
              sx={{
                height: 20,
                fontSize: '0.75rem',
                backgroundColor: gcpPalette.neutral.background,
                color: gcpPalette.neutral.textSecondary,
                border: `1px solid ${gcpPalette.neutral.border}`,
              }}
            />
          )}

          {pinnedColumns.length > 0 && (
            <Chip
              icon={<PushPinIcon sx={{ fontSize: '12px !important', color: `${gcpPalette.primary.main} !important` }} />}
              label={`${pinnedColumns.length} pinned`}
              size="small"
              sx={{
                height: 20,
                fontSize: '0.75rem',
                backgroundColor: gcpPalette.primary.light,
                color: gcpPalette.primary.main,
                fontWeight: 600,
              }}
            />
          )}
        </Box>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          {isCustomized && (
            <Tooltip title="Reset column order, widths, and pinning to default">
              <Button
                size="small"
                variant="text"
                startIcon={<RestartAltIcon />}
                onClick={handleResetLayout}
                sx={{ color: gcpPalette.neutral.textSecondary, py: 0.3 }}
              >
                Reset Columns
              </Button>
            </Tooltip>
          )}

          {/* Column Profiles Toggle */}
          <Tooltip title={showProfiles ? 'Hide column distribution sparklines' : 'Show column distribution sparklines'}>
            <Button
              size="small"
              variant={showProfiles ? 'contained' : 'outlined'}
              startIcon={<BarChartIcon fontSize="small" />}
              onClick={() => {
                const next = !showProfiles;
                setShowProfiles(next);
                localStorage.setItem('span_explorer_show_column_profiles', String(next));
              }}
              sx={{
                py: 0.3,
                fontSize: '0.75rem',
                textTransform: 'none',
                borderColor: showProfiles ? gcpPalette.primary.main : gcpPalette.neutral.border,
                backgroundColor: showProfiles ? gcpPalette.primary.light : 'transparent',
                color: showProfiles ? gcpPalette.primary.main : gcpPalette.neutral.textPrimary,
                '&:hover': {
                  backgroundColor: showProfiles ? 'rgba(26, 115, 232, 0.16)' : 'rgba(0,0,0,0.04)',
                },
              }}
            >
              Profiles
            </Button>
          </Tooltip>

          <Button
            size="small"
            variant="outlined"
            startIcon={<CodeIcon />}
            onClick={() => setShowExporter(!showExporter)}
            sx={{ borderColor: gcpPalette.neutral.border, py: 0.3 }}
          >
            {showExporter ? 'Hide SQL & API' : 'View SQL & API'}
          </Button>

          <Button
            size="small"
            variant="contained"
            startIcon={exporting ? <CircularProgress size={14} color="inherit" /> : <DownloadIcon />}
            onClick={onExportCsv}
            disabled={exporting || total === 0}
            sx={{ py: 0.3 }}
          >
            {exporting ? 'Streaming...' : 'Download CSV'}
          </Button>
        </Box>
      </Box>

      {/* SQL & API Exporter Block */}
      {showExporter && executedSql && (
        <Box sx={{ flexShrink: 0, mb: 1.5 }}>
          <CliExporter sqlQuery={executedSql} tableName={tableName} utcOffset={utcOffset} />
        </Box>
      )}

      {/* Main Server-Side Paginated Table */}
      <Paper sx={{ border: `1px solid ${gcpPalette.neutral.border}`, overflow: 'hidden', display: 'flex', flexDirection: 'column', flexGrow: 1, minHeight: 460 }}>
        {/* Loading Progress Bar */}
        {loading && (
          <LinearProgress
            sx={{
              height: 2,
              backgroundColor: gcpPalette.primary.light,
              '& .MuiLinearProgress-bar': { backgroundColor: gcpPalette.primary.main }
            }}
          />
        )}

        <TableContainer ref={tableContainerRef} sx={{ flexGrow: 1, minHeight: 400, overflowY: 'auto', position: 'relative' }}>
          <Table stickyHeader size="small" sx={{ tableLayout: 'fixed', width: 'max-content', minWidth: '100%' }}>
            <TableHead>
              <TableRow sx={{ height: showProfiles ? 58 : 34 }}>
                {/* Row Index Header (Sticky at left: 0) */}
                <TableCell
                  sx={{
                    width: 50,
                    minWidth: 50,
                    maxWidth: 50,
                    color: '#80868b',
                    fontWeight: 600,
                    fontSize: '0.75rem',
                    textAlign: 'center',
                    px: 1,
                    py: 0.5,
                    position: 'sticky',
                    left: 0,
                    zIndex: 4,
                    backgroundColor: gcpPalette.neutral.background,
                    borderRight: `1px solid ${gcpPalette.neutral.border}`,
                  }}
                >
                  #
                </TableCell>

                {orderedColumns.map((col) => {
                  const isSorted = sort?.column === col.name;
                  const isNumeric = col.filter_type === 'numeric';
                  const isPinned = pinnedColumns.includes(col.name);
                  const isLastPinned = isPinned && pinnedColumns[pinnedColumns.length - 1] === col.name;
                  const leftOffset = isPinned ? stickyLeftOffsets[col.name] : undefined;
                  const width = columnWidths[col.name] || 160;
                  const isDragTarget = dragOverCol === col.name;

                  return (
                    <TableCell
                      key={col.name}
                      align={isNumeric ? 'right' : 'left'}
                      sortDirection={isSorted ? (sort?.order.toLowerCase() as 'asc' | 'desc') : false}
                      draggable={!resizingCol}
                      onDragStart={(e) => handleDragStart(e, col.name)}
                      onDragOver={(e) => handleDragOver(e, col.name)}
                      onDrop={(e) => handleDrop(e, col.name)}
                      onDragEnd={handleDragEnd}
                      sx={{
                        width,
                        minWidth: width,
                        maxWidth: width,
                        py: 0.5,
                        px: 1,
                        whiteSpace: 'nowrap',
                        position: isPinned ? 'sticky' : 'relative',
                        left: leftOffset,
                        zIndex: isPinned ? 4 : 1,
                        backgroundColor: gcpPalette.neutral.background,
                        borderRight: isLastPinned
                          ? `2px solid ${gcpPalette.primary.main}`
                          : `1px solid ${gcpPalette.neutral.border}`,
                        boxShadow: isLastPinned ? '3px 0 6px -2px rgba(0,0,0,0.12)' : 'none',
                        borderLeft: isDragTarget ? `3px solid ${gcpPalette.primary.main}` : undefined,
                        cursor: 'grab',
                        '&:active': { cursor: 'grabbing' },
                      }}
                    >
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: isNumeric ? 'flex-end' : 'space-between', width: '100%', gap: 0.5 }}>
                        {/* Drag Handle Icon (Hover indicator) */}
                        <DragIndicatorIcon
                          sx={{
                            fontSize: 14,
                            color: '#bdc1c6',
                            cursor: 'grab',
                            flexShrink: 0,
                            display: isNumeric ? 'none' : 'inline-block',
                          }}
                        />

                        {/* Sortable Column Name with Full Header Tooltip */}
                        <Tooltip title={`${col.name} (${col.type})`} arrow enterDelay={250}>
                          <Box sx={{ overflow: 'hidden', textOverflow: 'ellipsis', flex: 1, textAlign: isNumeric ? 'right' : 'left' }}>
                            <TableSortLabel
                              active={isSorted}
                              direction={isSorted ? (sort?.order.toLowerCase() as 'asc' | 'desc') : 'asc'}
                              onClick={() => handleSort(col.name)}
                              sx={{
                                maxWidth: '100%',
                                '& .MuiTableSortLabel-icon': {
                                  opacity: isSorted ? 1 : 0.4,
                                },
                              }}
                            >
                              <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.75rem', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                                {col.name}
                              </Typography>
                            </TableSortLabel>
                          </Box>
                        </Tooltip>

                        {/* Pin / Unpin Button */}
                        <Tooltip title={isPinned ? 'Unpin column' : 'Pin column to left'}>
                          <IconButton
                            size="small"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleTogglePin(col.name);
                            }}
                            sx={{
                              p: 0.3,
                              flexShrink: 0,
                              color: isPinned ? gcpPalette.primary.main : '#9aa0a6',
                              '&:hover': { color: gcpPalette.primary.main },
                            }}
                          >
                            {isPinned ? <PushPinIcon sx={{ fontSize: 14 }} /> : <PushPinOutlinedIcon sx={{ fontSize: 14 }} />}
                          </IconButton>
                        </Tooltip>
                      </Box>

                      {/* Micro-Distribution Sparkline */}
                      {showProfiles && (
                        <ColumnSparkline
                          profile={profiles[col.name]}
                          onSelectFilterRange={(min, max) => {
                            if (onApplyRangeFilter) onApplyRangeFilter(col.name, min, max);
                          }}
                          onSelectCategory={(val) => {
                            if (onApplyCategoryFilter) onApplyCategoryFilter(col.name, val);
                          }}
                        />
                      )}

                      {/* Drag-to-Resize Handle */}
                      <Box
                        onMouseDown={(e) => startResize(e, col.name)}
                        sx={{
                          position: 'absolute',
                          top: 0,
                          right: 0,
                          bottom: 0,
                          width: 6,
                          cursor: 'col-resize',
                          zIndex: 10,
                          userSelect: 'none',
                          '&:hover': {
                            backgroundColor: gcpPalette.primary.main,
                          },
                        }}
                      />
                    </TableCell>
                  );
                })}
              </TableRow>
            </TableHead>

            <TableBody>
              {items.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={orderedColumns.length + 1} sx={{ textAlign: 'center', py: 6, color: gcpPalette.neutral.textSecondary }}>
                    {loading ? 'Executing vectorized DuckDB query...' : 'No matching rows found. Adjust filters to broaden your search.'}
                  </TableCell>
                </TableRow>
              ) : (
                items.map((row, idx) => {
                  const rowNumber = (page - 1) * pageSize + idx + 1;
                  return (
                    <TableRow key={`${tableName}-${page}-${idx}`} hover sx={{ height: 34 }}>
                      {/* Row Index Cell (Sticky at left: 0) */}
                      <TableCell
                        sx={{
                          width: 50,
                          minWidth: 50,
                          maxWidth: 50,
                          color: '#80868b',
                          fontSize: '0.75rem',
                          textAlign: 'center',
                          px: 1,
                          py: 0.5,
                          userSelect: 'none',
                          position: 'sticky',
                          left: 0,
                          zIndex: 2,
                          backgroundColor: '#ffffff',
                          borderRight: `1px solid ${gcpPalette.neutral.border}`,
                        }}
                      >
                        {rowNumber.toLocaleString()}
                      </TableCell>

                      {orderedColumns.map((col) => {
                        const isNumeric = col.filter_type === 'numeric';
                        const isPinned = pinnedColumns.includes(col.name);
                        const isLastPinned = isPinned && pinnedColumns[pinnedColumns.length - 1] === col.name;
                        const leftOffset = isPinned ? stickyLeftOffsets[col.name] : undefined;
                        const width = columnWidths[col.name] || 160;

                        return (
                          <TableCell
                            key={col.name}
                            align={isNumeric ? 'right' : 'left'}
                            sx={{
                              py: 0.5,
                              px: 1.5,
                              width,
                              minWidth: width,
                              maxWidth: width,
                              whiteSpace: 'nowrap',
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              position: isPinned ? 'sticky' : 'static',
                              left: leftOffset,
                              zIndex: isPinned ? 2 : 0,
                              backgroundColor: isPinned ? '#ffffff' : 'inherit',
                              borderRight: isLastPinned
                                ? `2px solid ${gcpPalette.primary.main}`
                                : `1px solid ${gcpPalette.neutral.border}`,
                              boxShadow: isLastPinned ? '3px 0 6px -2px rgba(0,0,0,0.08)' : 'none',
                            }}
                          >
                            {formatCellValue(row[col.name], col, rowNumber)}
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </TableContainer>

        {/* Pagination Toolbar */}
        <TablePagination
          component="div"
          count={total}
          page={page - 1}
          onPageChange={(_, newPage) => onPageChange(newPage + 1)}
          rowsPerPage={pageSize}
          onRowsPerPageChange={(e) => onPageSizeChange(parseInt(e.target.value, 10))}
          rowsPerPageOptions={[25, 50, 100, 250, 500, 1000]}
          sx={{
            flexShrink: 0,
            borderTop: `1px solid ${gcpPalette.neutral.border}`,
            backgroundColor: gcpPalette.neutral.surface,
            minHeight: 40,
            '& .MuiTablePagination-toolbar': {
              minHeight: 40,
              py: 0,
            },
          }}
        />
      </Paper>

      {/* Full Text View & Copy Modal */}
      <Dialog
        open={cellModal.open}
        onClose={() => setCellModal({ ...cellModal, open: false })}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <Typography variant="h2" sx={{ fontSize: '1.1rem', fontWeight: 600 }}>
              {cellModal.columnName}
            </Typography>
            <Chip label={`Row #${cellModal.rowNumber}`} size="small" variant="outlined" />
            <Chip label={`${cellModal.value.length.toLocaleString()} characters`} size="small" sx={{ backgroundColor: gcpPalette.neutral.background }} />
          </Box>
          <IconButton size="small" onClick={() => setCellModal({ ...cellModal, open: false })}>
            <CloseIcon fontSize="small" />
          </IconButton>
        </DialogTitle>

        <DialogContent dividers sx={{ p: 2 }}>
          <Box
            sx={{
              p: 2,
              backgroundColor: '#202124',
              color: '#e8eaed',
              borderRadius: 1,
              fontFamily: 'Roboto Mono, Consolas, Monaco, monospace',
              fontSize: '0.85rem',
              lineHeight: 1.6,
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              maxHeight: '60vh',
              overflowY: 'auto',
              border: `1px solid ${gcpPalette.neutral.border}`,
            }}
          >
            {cellModal.value}
          </Box>
        </DialogContent>

        <DialogActions sx={{ px: 2.5, py: 1.5, justifyContent: 'space-between' }}>
          <Button
            variant="contained"
            startIcon={copied ? <CheckIcon /> : <ContentCopyIcon />}
            onClick={handleCopyText}
            color={copied ? 'success' : 'primary'}
          >
            {copied ? 'Copied to Clipboard!' : 'Copy Full Text'}
          </Button>

          <Button onClick={() => setCellModal({ ...cellModal, open: false })} color="inherit">
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};
