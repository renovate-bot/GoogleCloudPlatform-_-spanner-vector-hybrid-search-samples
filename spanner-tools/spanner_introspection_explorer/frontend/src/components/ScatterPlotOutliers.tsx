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

import React, { useState, useEffect, useMemo, useRef } from 'react';
import {
  Box,
  Paper,
  Typography,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  FormControlLabel,
  Switch,
  IconButton,
  Tooltip,
  CircularProgress,
  Chip,
  Button
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import AutoGraphIcon from '@mui/icons-material/AutoGraph';
import FilterListIcon from '@mui/icons-material/FilterList';

import { ColumnFilter, ScatterDataPoint, ScatterDefaultsResponse } from '../types';
import { api } from '../services/api';
import { gcpPalette } from '../theme';

interface ScatterPlotOutliersProps {
  tableName: string;
  filters: Record<string, ColumnFilter>;
  utcOffset: number;
  db?: string;
  onSelectOutlier: (point: ScatterDataPoint, labelCol: string) => void;
  onSelectRange?: (range: { xCol: string; minX: number; maxX: number; yCol: string; minY: number; maxY: number }) => void;
  onClearRange?: (xCol?: string, yCol?: string) => void;
  onClose: () => void;
}

export const ScatterPlotOutliers: React.FC<ScatterPlotOutliersProps> = ({
  tableName,
  filters,
  utcOffset,
  db,
  onSelectOutlier,
  onSelectRange,
  onClearRange,
  onClose,
}) => {
  const [defaults, setDefaults] = useState<ScatterDefaultsResponse | null>(null);
  const [xCol, setXCol] = useState<string>('');
  const [yCol, setYCol] = useState<string>('');
  const [sizeCol, setSizeCol] = useState<string>('');
  const [labelCol, setLabelCol] = useState<string>('');
  const [isLogScale, setIsLogScale] = useState<boolean>(false);

  const [points, setPoints] = useState<ScatterDataPoint[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [hoveredPoint, setHoveredPoint] = useState<ScatterDataPoint | null>(null);
  const [tooltipPos, setTooltipPos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });

  // Drag & drop box selection state
  const [isDragging, setIsDragging] = useState<boolean>(false);
  const [dragStart, setDragStart] = useState<{ x: number; y: number } | null>(null);
  const [dragCurrent, setDragCurrent] = useState<{ x: number; y: number } | null>(null);

  const svgRef = useRef<SVGSVGElement | null>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [svgWidth, setSvgWidth] = useState<number>(900);

  // ResizeObserver to track container width for true 1:1 pixel rendering without stretching
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.contentRect.width > 0) {
          setSvgWidth(Math.floor(entry.contentRect.width));
        }
      }
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  // 1. Fetch smart defaults on mount / table change
  useEffect(() => {
    let isMounted = true;
    api.getScatterDefaults(tableName, db)
      .then((def) => {
        if (!isMounted) return;
        setDefaults(def);
        setXCol(def.x_col || (def.numeric_cols[0] || ''));
        setYCol(def.y_col || (def.numeric_cols[1] || def.numeric_cols[0] || ''));
        setSizeCol(def.size_col || '');
        setLabelCol(def.label_col || '');
      })
      .catch((err) => {
        console.error('Failed to load scatter defaults', err);
      });
    return () => { isMounted = false; };
  }, [tableName, db]);

  // 2. Fetch scatter plot data whenever axes or table filters change
  useEffect(() => {
    if (!xCol || !yCol) return;
    setLoading(true);
    let isMounted = true;

    api.getScatterPlotData(tableName, {
      page: 1,
      page_size: 300,
      filters,
      utc_offset: utcOffset,
    }, {
      x_col: xCol,
      y_col: yCol,
      size_col: sizeCol || undefined,
      label_col: labelCol || undefined,
      limit: 300,
    }, db)
      .then((res) => {
        if (!isMounted) return;
        setPoints(res.points || []);
        setLoading(false);
      })
      .catch((err) => {
        console.error('Failed to load scatter points', err);
        if (isMounted) setLoading(false);
      });

    return () => { isMounted = false; };
  }, [tableName, xCol, yCol, sizeCol, labelCol, filters, utcOffset, db]);

  // Dimensions
  const SVG_WIDTH = Math.max(600, svgWidth);
  const SVG_HEIGHT = 280;
  const MARGIN = { top: 20, right: 30, bottom: 45, left: 65 };
  const PLOT_WIDTH = SVG_WIDTH - MARGIN.left - MARGIN.right;
  const PLOT_HEIGHT = SVG_HEIGHT - MARGIN.top - MARGIN.bottom;

  // Scale calculations
  const { minX, maxX, minY, maxY, minSize, maxSize } = useMemo(() => {
    if (points.length === 0) {
      return { minX: 0, maxX: 1, minY: 0, maxY: 1, minSize: 1, maxSize: 1 };
    }
    const xs = points.map((p) => p.x);
    const ys = points.map((p) => p.y);
    const ss = points.map((p) => p.size);

    return {
      minX: Math.min(...xs),
      maxX: Math.max(...xs),
      minY: Math.min(...ys),
      maxY: Math.max(...ys),
      minSize: Math.min(...ss),
      maxSize: Math.max(...ss),
    };
  }, [points]);

  const transformX = (val: number) => {
    if (isLogScale) {
      const safeVal = Math.max(val, 0.0001);
      const safeMin = Math.max(minX, 0.0001);
      const safeMax = Math.max(maxX, 1);
      const logMin = Math.log10(safeMin);
      const logMax = Math.log10(safeMax);
      const range = logMax - logMin || 1;
      return MARGIN.left + ((Math.log10(safeVal) - logMin) / range) * PLOT_WIDTH;
    }
    const range = maxX - minX || 1;
    return MARGIN.left + ((val - minX) / range) * PLOT_WIDTH;
  };

  const transformY = (val: number) => {
    if (isLogScale) {
      const safeVal = Math.max(val, 0.0001);
      const safeMin = Math.max(minY, 0.0001);
      const safeMax = Math.max(maxY, 1);
      const logMin = Math.log10(safeMin);
      const logMax = Math.log10(safeMax);
      const range = logMax - logMin || 1;
      return MARGIN.top + PLOT_HEIGHT - ((Math.log10(safeVal) - logMin) / range) * PLOT_HEIGHT;
    }
    const range = maxY - minY || 1;
    return MARGIN.top + PLOT_HEIGHT - ((val - minY) / range) * PLOT_HEIGHT;
  };

  const inverseTransformX = (px: number) => {
    const clampedPx = Math.max(MARGIN.left, Math.min(MARGIN.left + PLOT_WIDTH, px));
    const ratio = (clampedPx - MARGIN.left) / PLOT_WIDTH;
    if (isLogScale) {
      const safeMin = Math.max(minX, 0.0001);
      const safeMax = Math.max(maxX, 1);
      const logMin = Math.log10(safeMin);
      const logMax = Math.log10(safeMax);
      const logVal = logMin + ratio * (logMax - logMin);
      return Math.pow(10, logVal);
    }
    return minX + ratio * (maxX - minX);
  };

  const inverseTransformY = (py: number) => {
    const clampedPy = Math.max(MARGIN.top, Math.min(MARGIN.top + PLOT_HEIGHT, py));
    const ratio = (MARGIN.top + PLOT_HEIGHT - clampedPy) / PLOT_HEIGHT;
    if (isLogScale) {
      const safeMin = Math.max(minY, 0.0001);
      const safeMax = Math.max(maxY, 1);
      const logMin = Math.log10(safeMin);
      const logMax = Math.log10(safeMax);
      const logVal = logMin + ratio * (logMax - logMin);
      return Math.pow(10, logVal);
    }
    return minY + ratio * (maxY - minY);
  };

  const getSvgCoordinates = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!svgRef.current) return { x: 0, y: 0 };
    const svg = svgRef.current;
    
    // Use native SVG CTM inverse transformation for pixel-perfect coordinate mapping across all DPIs & aspect ratios
    const ctm = svg.getScreenCTM();
    if (ctm) {
      const point = svg.createSVGPoint();
      point.x = e.clientX;
      point.y = e.clientY;
      const transformed = point.matrixTransform(ctm.inverse());
      const clampedX = Math.max(MARGIN.left, Math.min(SVG_WIDTH - MARGIN.right, transformed.x));
      const clampedY = Math.max(MARGIN.top, Math.min(MARGIN.top + PLOT_HEIGHT, transformed.y));
      return { x: clampedX, y: clampedY };
    }

    // Fallback if CTM is unavailable
    const rect = svg.getBoundingClientRect();
    const scaleX = SVG_WIDTH / rect.width;
    const scaleY = SVG_HEIGHT / rect.height;
    const rawX = (e.clientX - rect.left) * scaleX;
    const rawY = (e.clientY - rect.top) * scaleY;
    const clampedX = Math.max(MARGIN.left, Math.min(SVG_WIDTH - MARGIN.right, rawX));
    const clampedY = Math.max(MARGIN.top, Math.min(MARGIN.top + PLOT_HEIGHT, rawY));
    return { x: clampedX, y: clampedY };
  };

  const isPointInBrush = (p: ScatterDataPoint) => {
    if (!isDragging || !dragStart || !dragCurrent) return true;
    const box = {
      x1: Math.min(dragStart.x, dragCurrent.x),
      x2: Math.max(dragStart.x, dragCurrent.x),
      y1: Math.min(dragStart.y, dragCurrent.y),
      y2: Math.max(dragStart.y, dragCurrent.y),
    };
    const px = transformX(p.x);
    const py = transformY(p.y);
    return px >= box.x1 && px <= box.x2 && py >= box.y1 && py <= box.y2;
  };

  const computeRadius = (sizeVal: number) => {
    if (maxSize === minSize || !sizeCol) return 6;
    const norm = (sizeVal - minSize) / (maxSize - minSize);
    return Math.max(4, Math.min(22, 4 + Math.sqrt(norm) * 18));
  };

  const getColor = (yVal: number) => {
    if (maxY > 0 && yVal >= maxY * 0.75) return '#d93025'; // Red extreme outlier
    if (maxY > 0 && yVal >= maxY * 0.35) return '#f29900'; // Orange/Yellow moderate
    return '#1a73e8'; // Blue normal
  };

  // Smart domain-aware unit formatter
  const formatValue = (val: number, colName?: string) => {
    const name = (colName || '').toLowerCase();

    // 1. Explicit Seconds columns (e.g., AVG_LATENCY_SECONDS, AVG_CPU_SECONDS, AVG_TOTAL_LATENCY_SECONDS)
    if (name.includes('seconds')) {
      if (Math.abs(val) >= 3600) return `${(val / 3600).toFixed(1)} hrs`;
      if (Math.abs(val) >= 60) return `${(val / 60).toFixed(1)} min`;
      if (Math.abs(val) >= 1) return `${val.toFixed(2)}s`;
      if (Math.abs(val) >= 0.001) return `${(val * 1000).toFixed(1)}ms`;
      if (Math.abs(val) > 0) return `${(val * 1_000_000).toFixed(0)}µs`;
      return '0s';
    }

    // 2. Percentile Latency columns in Spanner TXN_STATS / QUERY_STATS (latency_p50, latency_p95, latency_p99 are in MILLISECONDS)
    if (name.includes('latency_p') || name.includes('_ms') || name.includes('millis')) {
      if (Math.abs(val) >= 3_600_000) return `${(val / 3_600_000).toFixed(1)} hrs`;
      if (Math.abs(val) >= 60_000) return `${(val / 60_000).toFixed(1)} min`;
      if (Math.abs(val) >= 1_000) return `${(val / 1_000).toFixed(2)}s`;
      return `${Math.round(val).toLocaleString()}ms`;
    }

    // 3. Bytes columns
    if (name.includes('bytes')) {
      if (Math.abs(val) >= 1024 * 1024 * 1024) return `${(val / (1024 * 1024 * 1024)).toFixed(1)}GB`;
      if (Math.abs(val) >= 1024 * 1024) return `${(val / (1024 * 1024)).toFixed(1)}MB`;
      if (Math.abs(val) >= 1024) return `${(val / 1024).toFixed(1)}KB`;
      return `${Math.round(val).toLocaleString()}B`;
    }

    // 4. Generic counts & volumes (e.g. ATTEMPT_COUNT, EXECUTION_COUNT, ROWS_SCANNED)
    if (Math.abs(val) >= 1_000_000) return `${(val / 1_000_000).toFixed(1)}M`;
    if (Math.abs(val) >= 1_000) return `${(val / 1_000).toFixed(1)}k`;
    if (Math.abs(val) < 0.01 && val !== 0) return val.toExponential(1);
    return Number(val.toFixed(2)).toString();
  };

  const getRawUnit = (colName?: string) => {
    const name = (colName || '').toLowerCase();
    if (name.includes('seconds')) return 's';
    if (name.includes('latency_p') || name.includes('_ms') || name.includes('millis')) return ' ms';
    if (name.includes('bytes')) return ' B';
    return '';
  };

  return (
    <Paper
      elevation={0}
      sx={{
        mb: 2,
        p: 2,
        border: `1px solid ${gcpPalette.neutral.border}`,
        borderRadius: '8px',
        backgroundColor: '#ffffff',
        position: 'relative',
      }}
    >
      {/* Header & Controls Toolbar */}
      <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: 2, mb: 1.5 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <AutoGraphIcon sx={{ color: gcpPalette.primary.main, fontSize: 20 }} />
          <Typography variant="h3" sx={{ fontSize: '0.95rem', fontWeight: 600 }}>
            Outlier Analysis: Multivariate Scatter Plot
          </Typography>
          <Chip
            label={`${points.length} data points`}
            size="small"
            sx={{ height: 20, fontSize: '0.7rem', backgroundColor: gcpPalette.neutral.surface }}
          />
        </Box>

        {/* Axis Selectors & Toggles */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, flexWrap: 'wrap' }}>
          {(Boolean(filters[xCol]) || Boolean(filters[yCol])) && (
            <Button
              size="small"
              variant="outlined"
              color="primary"
              onClick={() => {
                if (onClearRange) onClearRange(xCol, yCol);
              }}
              sx={{ fontSize: '0.72rem', height: 26, textTransform: 'none', py: 0 }}
            >
              Reset Zoom
            </Button>
          )}

          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontSize: '0.72rem', display: { xs: 'none', md: 'inline-block' } }}>
            💡 Drag box to filter outliers
          </Typography>

          {defaults && defaults.numeric_cols.length > 0 && (
            <>
              {/* X-Axis */}
              <FormControl size="small" sx={{ minWidth: 140 }}>
                <InputLabel sx={{ fontSize: '0.75rem' }}>X-Axis</InputLabel>
                <Select
                  value={xCol}
                  label="X-Axis"
                  onChange={(e) => {
                    setXCol(e.target.value);
                  }}
                  sx={{ fontSize: '0.75rem', height: 32 }}
                >
                  {defaults.numeric_cols.map((c) => (
                    <MenuItem key={c} value={c} sx={{ fontSize: '0.75rem' }}>{c}</MenuItem>
                  ))}
                </Select>
              </FormControl>

              {/* Y-Axis */}
              <FormControl size="small" sx={{ minWidth: 140 }}>
                <InputLabel sx={{ fontSize: '0.75rem' }}>Y-Axis</InputLabel>
                <Select
                  value={yCol}
                  label="Y-Axis"
                  onChange={(e) => {
                    setYCol(e.target.value);
                  }}
                  sx={{ fontSize: '0.75rem', height: 32 }}
                >
                  {defaults.numeric_cols.map((c) => (
                    <MenuItem key={c} value={c} sx={{ fontSize: '0.75rem' }}>{c}</MenuItem>
                  ))}
                </Select>
              </FormControl>

              {/* Size Bubble */}
              <FormControl size="small" sx={{ minWidth: 140 }}>
                <InputLabel sx={{ fontSize: '0.75rem' }}>Bubble Size</InputLabel>
                <Select
                  value={sizeCol}
                  label="Bubble Size"
                  onChange={(e) => setSizeCol(e.target.value)}
                  sx={{ fontSize: '0.75rem', height: 32 }}
                >
                  <MenuItem value="" sx={{ fontSize: '0.75rem' }}><em>Fixed Size</em></MenuItem>
                  {defaults.numeric_cols.map((c) => (
                    <MenuItem key={c} value={c} sx={{ fontSize: '0.75rem' }}>{c}</MenuItem>
                  ))}
                </Select>
              </FormControl>
            </>
          )}

          {/* Log Scale Toggle */}
          <FormControlLabel
            control={
              <Switch
                size="small"
                checked={isLogScale}
                onChange={(e) => {
                  setIsLogScale(e.target.checked);
                }}
              />
            }
            label={<Typography variant="caption" sx={{ fontSize: '0.75rem' }}>Log10</Typography>}
            sx={{ m: 0 }}
          />

          <Tooltip title="Close Outlier Scatter Plot">
            <IconButton size="small" onClick={onClose} sx={{ p: 0.5 }}>
              <CloseIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      </Box>

      {/* Main SVG Visualization Canvas with Drag-and-Drop Box Selection */}
      <Box ref={containerRef} sx={{ position: 'relative', width: '100%', overflowX: 'auto', userSelect: 'none' }}>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: SVG_HEIGHT }}>
            <CircularProgress size={32} />
          </Box>
        ) : points.length === 0 ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: SVG_HEIGHT, color: gcpPalette.neutral.textSecondary }}>
            <Typography variant="body2">No numeric data points available to plot for the current filters.</Typography>
          </Box>
        ) : (
          <svg
            ref={svgRef}
            width="100%"
            height={SVG_HEIGHT}
            viewBox={`0 0 ${SVG_WIDTH} ${SVG_HEIGHT}`}
            style={{ display: 'block', cursor: isDragging ? 'crosshair' : 'default' }}
            onMouseDown={(e) => {
              if (e.button !== 0) return;
              const coords = getSvgCoordinates(e);
              setIsDragging(true);
              setDragStart(coords);
              setDragCurrent(coords);
            }}
            onMouseMove={(e) => {
              if (isDragging) {
                const coords = getSvgCoordinates(e);
                setDragCurrent(coords);
              }
            }}
            onMouseUp={() => {
              if (isDragging && dragStart && dragCurrent) {
                const dx = Math.abs(dragCurrent.x - dragStart.x);
                const dy = Math.abs(dragCurrent.y - dragStart.y);
                if (dx > 8 && dy > 8) {
                  const boxX1 = Math.min(dragStart.x, dragCurrent.x);
                  const boxX2 = Math.max(dragStart.x, dragCurrent.x);
                  const boxY1 = Math.min(dragStart.y, dragCurrent.y);
                  const boxY2 = Math.max(dragStart.y, dragCurrent.y);

                  const valX1 = inverseTransformX(boxX1);
                  const valX2 = inverseTransformX(boxX2);
                  const valY1 = inverseTransformY(boxY2); // lower Y in data
                  const valY2 = inverseTransformY(boxY1); // upper Y in data

                  const minDataX = Math.min(valX1, valX2);
                  const maxDataX = Math.max(valX1, valX2);
                  const minDataY = Math.min(valY1, valY2);
                  const maxDataY = Math.max(valY1, valY2);

                  if (onSelectRange) {
                    onSelectRange({
                      xCol,
                      minX: minDataX,
                      maxX: maxDataX,
                      yCol,
                      minY: minDataY,
                      maxY: maxDataY,
                    });
                  }
                }
              }
              setIsDragging(false);
              setDragStart(null);
              setDragCurrent(null);
            }}
            onMouseLeave={() => {
              if (isDragging) {
                setIsDragging(false);
                setDragStart(null);
                setDragCurrent(null);
              }
            }}
          >
            {/* Background Grid Lines */}
            {[0, 0.25, 0.5, 0.75, 1].map((pct, i) => {
              const yPos = MARGIN.top + PLOT_HEIGHT * (1 - pct);
              const tickVal = minY + (maxY - minY) * pct;
              return (
                <g key={`y-grid-${i}`}>
                  <line
                    x1={MARGIN.left}
                    y1={yPos}
                    x2={SVG_WIDTH - MARGIN.right}
                    y2={yPos}
                    stroke="#e8eaed"
                    strokeDasharray="3 3"
                  />
                  <text
                    x={MARGIN.left - 8}
                    y={yPos + 4}
                    textAnchor="end"
                    fontSize="10"
                    fill={gcpPalette.neutral.textSecondary}
                    fontFamily="Roboto, sans-serif"
                  >
                    {formatValue(tickVal, yCol)}
                  </text>
                </g>
              );
            })}

            {[0, 0.25, 0.5, 0.75, 1].map((pct, i) => {
              const xPos = MARGIN.left + PLOT_WIDTH * pct;
              const tickVal = minX + (maxX - minX) * pct;
              return (
                <g key={`x-grid-${i}`}>
                  <line
                    x1={xPos}
                    y1={MARGIN.top}
                    x2={xPos}
                    y2={MARGIN.top + PLOT_HEIGHT}
                    stroke="#e8eaed"
                    strokeDasharray="3 3"
                  />
                  <text
                    x={xPos}
                    y={MARGIN.top + PLOT_HEIGHT + 16}
                    textAnchor="middle"
                    fontSize="10"
                    fill={gcpPalette.neutral.textSecondary}
                    fontFamily="Roboto, sans-serif"
                  >
                    {formatValue(tickVal, xCol)}
                  </text>
                </g>
              );
            })}

            {/* Axes Lines */}
            <line
              x1={MARGIN.left}
              y1={MARGIN.top}
              x2={MARGIN.left}
              y2={MARGIN.top + PLOT_HEIGHT}
              stroke="#dadce0"
              strokeWidth="1.5"
            />
            <line
              x1={MARGIN.left}
              y1={MARGIN.top + PLOT_HEIGHT}
              x2={SVG_WIDTH - MARGIN.right}
              y2={MARGIN.top + PLOT_HEIGHT}
              stroke="#dadce0"
              strokeWidth="1.5"
            />

            {/* Axis Titles */}
            <text
              x={MARGIN.left + PLOT_WIDTH / 2}
              y={SVG_HEIGHT - 6}
              textAnchor="middle"
              fontSize="11"
              fontWeight="600"
              fill={gcpPalette.neutral.textPrimary}
              fontFamily="Roboto, sans-serif"
            >
              {xCol} {isLogScale ? '(Log10 Scale)' : ''}
            </text>

            <text
              x={- (MARGIN.top + PLOT_HEIGHT / 2)}
              y={14}
              transform="rotate(-90)"
              textAnchor="middle"
              fontSize="11"
              fontWeight="600"
              fill={gcpPalette.neutral.textPrimary}
              fontFamily="Roboto, sans-serif"
            >
              {yCol}
            </text>

            {/* Data Points (Bubbles) */}
            {points.map((p) => {
              const cx = transformX(p.x);
              const cy = transformY(p.y);
              const r = computeRadius(p.size);
              const color = getColor(p.y);
              const isHovered = hoveredPoint?.id === p.id;
              const inBrush = isPointInBrush(p);

              return (
                <circle
                  key={p.id}
                  cx={cx}
                  cy={cy}
                  r={isHovered ? r + 3 : r}
                  fill={color}
                  fillOpacity={inBrush ? (isHovered ? 0.95 : 0.7) : 0.12}
                  stroke="#ffffff"
                  strokeWidth={isHovered ? 2.5 : 1.5}
                  strokeOpacity={inBrush ? 1.0 : 0.2}
                  style={{
                    cursor: 'pointer',
                    transition: 'r 0.15s ease, fill-opacity 0.15s ease, stroke-opacity 0.15s ease',
                  }}
                  onMouseEnter={(e) => {
                    if (isDragging) return;
                    const rect = svgRef.current?.getBoundingClientRect();
                    if (rect) {
                      setTooltipPos({
                        x: e.clientX - rect.left,
                        y: e.clientY - rect.top,
                      });
                    }
                    setHoveredPoint(p);
                  }}
                  onMouseMove={(e) => {
                    if (isDragging) return;
                    const rect = svgRef.current?.getBoundingClientRect();
                    if (rect) {
                      setTooltipPos({
                        x: e.clientX - rect.left,
                        y: e.clientY - rect.top,
                      });
                    }
                  }}
                  onMouseLeave={() => setHoveredPoint(null)}
                  onClick={(e) => {
                    e.stopPropagation();
                    onSelectOutlier(p, labelCol || xCol);
                  }}
                />
              );
            })}

            {/* Active Brush / Drag Selection Box (renders only while dragging) */}
            {isDragging && dragStart && dragCurrent && (() => {
              const box = {
                x: Math.min(dragStart.x, dragCurrent.x),
                y: Math.min(dragStart.y, dragCurrent.y),
                w: Math.abs(dragCurrent.x - dragStart.x),
                h: Math.abs(dragCurrent.y - dragStart.y),
              };

              if (!box || box.w < 2 || box.h < 2) return null;

              return (
                <rect
                  x={box.x}
                  y={box.y}
                  width={box.w}
                  height={box.h}
                  fill="rgba(26, 115, 232, 0.12)"
                  stroke="#1a73e8"
                  strokeWidth="1.5"
                  strokeDasharray="4 2"
                  rx="3"
                  pointerEvents="none"
                />
              );
            })()}
          </svg>
        )}

        {/* Hover Outlier Tooltip */}
        {hoveredPoint && (
          <Paper
            elevation={4}
            sx={{
              position: 'absolute',
              left: Math.min(tooltipPos.x + 12, SVG_WIDTH - 280),
              top: Math.max(10, tooltipPos.y - 120),
              p: 1.5,
              maxWidth: 320,
              zIndex: 10,
              backgroundColor: '#ffffff',
              border: `1px solid ${gcpPalette.primary.main}`,
              borderRadius: '6px',
              pointerEvents: 'none',
            }}
          >
            <Typography variant="subtitle2" sx={{ fontWeight: 600, color: gcpPalette.primary.main, mb: 0.5 }}>
              Outlier Details
            </Typography>

            <Box sx={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '4px 8px', fontSize: '0.75rem', mb: 1 }}>
              <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontWeight: 500 }}>{xCol}:</Typography>
              <Typography variant="caption" sx={{ fontWeight: 600 }}>
                {formatValue(hoveredPoint.x, xCol)} <span style={{ opacity: 0.6 }}>({hoveredPoint.x.toLocaleString()}{getRawUnit(xCol)})</span>
              </Typography>

              <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontWeight: 500 }}>{yCol}:</Typography>
              <Typography variant="caption" sx={{ fontWeight: 600, color: gcpPalette.primary.dark }}>
                {formatValue(hoveredPoint.y, yCol)} <span style={{ opacity: 0.6 }}>({hoveredPoint.y.toLocaleString()}{getRawUnit(yCol)})</span>
              </Typography>

              {sizeCol && (
                <>
                  <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontWeight: 500 }}>{sizeCol}:</Typography>
                  <Typography variant="caption" sx={{ fontWeight: 600 }}>
                    {formatValue(hoveredPoint.size, sizeCol)} <span style={{ opacity: 0.6 }}>({hoveredPoint.size.toLocaleString()}{getRawUnit(sizeCol)})</span>
                  </Typography>
                </>
              )}

              {hoveredPoint.interval && (
                <>
                  <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontWeight: 500 }}>Interval:</Typography>
                  <Typography variant="caption">{hoveredPoint.interval}</Typography>
                </>
              )}

              {hoveredPoint.label && (
                <>
                  <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontWeight: 500 }}>Key/Tag:</Typography>
                  <Typography variant="caption" sx={{ fontFamily: 'Roboto Mono, monospace', wordBreak: 'break-all' }}>
                    {hoveredPoint.label}
                  </Typography>
                </>
              )}
            </Box>

            {hoveredPoint.text && (
              <Box sx={{ p: 0.75, backgroundColor: '#f8f9fa', borderRadius: '4px', mb: 1, maxHeight: 60, overflow: 'hidden' }}>
                <Typography variant="caption" sx={{ fontFamily: 'Roboto Mono, monospace', fontSize: '0.7rem', color: '#3c4043', display: '-webkit-box', WebkitLineClamp: 3, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}>
                  {hoveredPoint.text}
                </Typography>
              </Box>
            )}

            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, color: gcpPalette.primary.main }}>
              <FilterListIcon sx={{ fontSize: 13 }} />
              <Typography variant="caption" sx={{ fontWeight: 600, fontSize: '0.7rem' }}>
                Click bubble to filter table to this outlier
              </Typography>
            </Box>
          </Paper>
        )}
      </Box>
    </Paper>
  );
};
