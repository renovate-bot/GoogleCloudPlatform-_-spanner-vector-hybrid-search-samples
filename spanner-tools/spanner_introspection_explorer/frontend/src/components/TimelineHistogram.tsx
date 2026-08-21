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

import React, { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  ButtonGroup,
  IconButton,
  Chip,
  Tooltip,
  CircularProgress,
  Collapse,
} from '@mui/material';
import BarChartIcon from '@mui/icons-material/BarChart';
import KeyboardArrowUpIcon from '@mui/icons-material/KeyboardArrowUp';
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import SpeedIcon from '@mui/icons-material/Speed';

import { TimelineBucket } from '../types';
import { api } from '../services/api';
import { gcpPalette } from '../theme';

interface TimelineHistogramProps {
  tableName: string;
  utcOffset: number;
  selectedTimestamps: string[];
  db?: string;
  onSelectIntervals: (timestamps: string[]) => void;
}

export const TimelineHistogram: React.FC<TimelineHistogramProps> = ({
  tableName,
  utcOffset,
  selectedTimestamps,
  db,
  onSelectIntervals,
}) => {
  const [buckets, setBuckets] = useState<TimelineBucket[]>([]);
  const [loading, setLoading] = useState(false);
  const [expanded, setExpanded] = useState(true);

  // Drag-and-drop Range Brush State
  const [isDragging, setIsDragging] = useState(false);
  const [dragStartIndex, setDragStartIndex] = useState<number | null>(null);
  const [dragCurrentIndex, setDragCurrentIndex] = useState<number | null>(null);

  // Hover Tooltip State
  const [hoveredBucket, setHoveredBucket] = useState<{ bucket: TimelineBucket; index: number; x: number; y: number } | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);

  // Fetch timeline data from backend
  useEffect(() => {
    let active = true;
    const fetchTimeline = async () => {
      setLoading(true);
      try {
        const res = await api.getTableTimeline(tableName, utcOffset, db);
        if (active) {
          setBuckets(res.buckets || []);
        }
      } catch (err) {
        console.error('Failed to load timeline:', err);
      } finally {
        if (active) setLoading(false);
      }
    };

    fetchTimeline();
    return () => {
      active = false;
    };
  }, [tableName, utcOffset]);

  // Selected timestamp set for O(1) lookup
  const selectedSet = useMemo(() => {
    const set = new Set<string>();
    selectedTimestamps.forEach((ts) => {
      const clean = ts.split('|')[1] || ts;
      set.add(clean);
    });
    return set;
  }, [selectedTimestamps]);

  const maxCount = useMemo(() => {
    if (buckets.length === 0) return 1;
    return Math.max(...buckets.map((b) => b.count), 1);
  }, [buckets]);

  const totalRecords = useMemo(() => {
    return buckets.reduce((acc, b) => acc + b.count, 0);
  }, [buckets]);

  const peakBucket = useMemo(() => {
    if (buckets.length === 0) return null;
    return buckets.reduce((max, b) => (b.count > max.count ? b : max), buckets[0]);
  }, [buckets]);

  // Calculate stats for current selection
  const selectionStats = useMemo(() => {
    if (selectedSet.size === 0) return null;
    const matching = buckets.filter((b) => selectedSet.has(b.utc));
    const count = matching.reduce((acc, b) => acc + b.count, 0);
    return {
      intervalCount: matching.length,
      recordCount: count,
      percent: totalRecords > 0 ? Math.round((count / totalRecords) * 100) : 0,
    };
  }, [buckets, selectedSet, totalRecords]);

  // Map mouse coordinate to bucket index
  const getIndexFromMouseEvent = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      if (!svgRef.current || buckets.length === 0) return 0;
      const rect = svgRef.current.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const width = rect.width;
      const ratio = Math.max(0, Math.min(1, x / width));
      return Math.min(buckets.length - 1, Math.floor(ratio * buckets.length));
    },
    [buckets]
  );

  const handleMouseDown = (e: React.MouseEvent<SVGSVGElement>) => {
    e.preventDefault();
    const idx = getIndexFromMouseEvent(e);
    setIsDragging(true);
    setDragStartIndex(idx);
    setDragCurrentIndex(idx);
  };

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const idx = getIndexFromMouseEvent(e);
    if (isDragging) {
      setDragCurrentIndex(idx);
    }

    if (svgRef.current && buckets[idx]) {
      const rect = svgRef.current.getBoundingClientRect();
      setHoveredBucket({
        bucket: buckets[idx],
        index: idx,
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
      });
    }
  };

  const handleMouseLeave = () => {
    if (!isDragging) {
      setHoveredBucket(null);
    }
  };

  const handleMouseUp = () => {
    if (isDragging && dragStartIndex !== null && dragCurrentIndex !== null) {
      const start = Math.min(dragStartIndex, dragCurrentIndex);
      const end = Math.max(dragStartIndex, dragCurrentIndex);

      if (start === end) {
        // Single bucket click toggle
        const target = buckets[start];
        if (target) {
          if (selectedSet.has(target.utc) && selectedSet.size === 1) {
            onSelectIntervals([]);
          } else {
            onSelectIntervals([target.utc]);
          }
        }
      } else {
        // Range selection
        const rangeTimestamps = buckets.slice(start, end + 1).map((b) => b.utc);
        onSelectIntervals(rangeTimestamps);
      }
    }

    setIsDragging(false);
    setDragStartIndex(null);
    setDragCurrentIndex(null);
  };

  // Quick Preset Handlers
  const handleSelectPreset = (hours: number) => {
    if (buckets.length === 0) return;
    if (hours === 0) {
      // All data
      onSelectIntervals([]);
      return;
    }

    const latest = new Date(buckets[buckets.length - 1].utc.replace(' ', 'T') + 'Z').getTime();
    const cutoff = latest - hours * 60 * 60 * 1000;

    const filtered = buckets
      .filter((b) => {
        const t = new Date(b.utc.replace(' ', 'T') + 'Z').getTime();
        return t >= cutoff;
      })
      .map((b) => b.utc);

    onSelectIntervals(filtered);
  };

  if (buckets.length === 0 && !loading) {
    return null;
  }

  // Active brush visual range in index space
  const brushStart = isDragging && dragStartIndex !== null && dragCurrentIndex !== null ? Math.min(dragStartIndex, dragCurrentIndex) : null;
  const brushEnd = isDragging && dragStartIndex !== null && dragCurrentIndex !== null ? Math.max(dragStartIndex, dragCurrentIndex) : null;

  return (
    <Paper sx={{ mb: 2, border: `1px solid ${gcpPalette.neutral.border}`, overflow: 'hidden' }}>
      {/* Top Header Bar */}
      <Box
        sx={{
          px: 1.5,
          py: 0.8,
          backgroundColor: gcpPalette.neutral.surface,
          borderBottom: expanded ? `1px solid ${gcpPalette.neutral.border}` : 'none',
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 1,
        }}
      >
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <IconButton size="small" onClick={() => setExpanded(!expanded)} sx={{ p: 0.3 }}>
            {expanded ? <KeyboardArrowUpIcon fontSize="small" /> : <KeyboardArrowDownIcon fontSize="small" />}
          </IconButton>

          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <BarChartIcon sx={{ color: gcpPalette.primary.main, fontSize: 20 }} />
            <Typography variant="body2" sx={{ fontWeight: 600, color: gcpPalette.neutral.textPrimary }}>
              Timeline Distribution
            </Typography>
            <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
              (Drag & drop over the chart to filter time ranges)
            </Typography>
          </Box>

          <Box sx={{ display: { xs: 'none', md: 'flex' }, alignItems: 'center', gap: 1 }}>
            <Chip
              icon={<AccessTimeIcon sx={{ fontSize: '13px !important' }} />}
              label={`${buckets.length} intervals`}
              size="small"
              sx={{ height: 22, fontSize: '0.75rem', backgroundColor: gcpPalette.neutral.background }}
            />

            {peakBucket && (
              <Chip
                icon={<SpeedIcon sx={{ fontSize: '13px !important' }} />}
                label={`Peak: ${peakBucket.count.toLocaleString()} rows @ ${peakBucket.display.substring(11, 16)}`}
                size="small"
                sx={{ height: 22, fontSize: '0.75rem', backgroundColor: gcpPalette.neutral.background }}
              />
            )}

            {selectionStats && (
              <Chip
                label={`${selectionStats.intervalCount} intervals selected (${selectionStats.recordCount.toLocaleString()} rows • ${selectionStats.percent}%)`}
                size="small"
                color="primary"
                sx={{ height: 22, fontSize: '0.75rem', fontWeight: 600 }}
              />
            )}
          </Box>
        </Box>

        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          {/* Preset Buttons */}
          <ButtonGroup size="small" variant="outlined">
            <Button onClick={() => handleSelectPreset(0)} sx={{ py: 0.2, px: 1, fontSize: '0.75rem' }}>
              All
            </Button>
            <Button onClick={() => handleSelectPreset(1)} sx={{ py: 0.2, px: 1, fontSize: '0.75rem' }}>
              1h
            </Button>
            <Button onClick={() => handleSelectPreset(6)} sx={{ py: 0.2, px: 1, fontSize: '0.75rem' }}>
              6h
            </Button>
            <Button onClick={() => handleSelectPreset(24)} sx={{ py: 0.2, px: 1, fontSize: '0.75rem' }}>
              24h
            </Button>
          </ButtonGroup>

          {selectedTimestamps.length > 0 && (
            <Tooltip title="Clear timeline range filter">
              <Button
                size="small"
                variant="text"
                startIcon={<RestartAltIcon fontSize="small" />}
                onClick={() => onSelectIntervals([])}
                sx={{ color: gcpPalette.neutral.textSecondary, py: 0.2, fontSize: '0.75rem' }}
              >
                Clear
              </Button>
            </Tooltip>
          )}
        </Box>
      </Box>

      {/* Collapsible Chart Area */}
      <Collapse in={expanded}>
        <Box ref={containerRef} sx={{ p: 1.5, position: 'relative', userSelect: 'none' }}>
          {loading ? (
            <Box sx={{ height: 85, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <CircularProgress size={24} />
            </Box>
          ) : (
            <>
              {/* Interactive SVG Histogram */}
              <Box sx={{ height: 80, width: '100%', position: 'relative' }}>
                <svg
                  ref={svgRef}
                  width="100%"
                  height="100%"
                  viewBox={`0 0 ${Math.max(600, buckets.length * 4)} 80`}
                  preserveAspectRatio="none"
                  onMouseDown={handleMouseDown}
                  onMouseMove={handleMouseMove}
                  onMouseLeave={handleMouseLeave}
                  onMouseUp={handleMouseUp}
                  style={{ cursor: isDragging ? 'col-resize' : 'crosshair', display: 'block' }}
                >
                  {/* Grid Lines */}
                  <line x1="0" y1="20" x2="100%" y2="20" stroke="#f1f3f4" strokeWidth="1" />
                  <line x1="0" y1="40" x2="100%" y2="40" stroke="#f1f3f4" strokeWidth="1" />
                  <line x1="0" y1="60" x2="100%" y2="60" stroke="#f1f3f4" strokeWidth="1" />
                  <line x1="0" y1="79" x2="100%" y2="79" stroke="#dadce0" strokeWidth="1" />

                  {/* Histogram Bars */}
                  {buckets.map((b, idx) => {
                    const totalWidth = Math.max(600, buckets.length * 4);
                    const barWidth = Math.max(2, totalWidth / buckets.length - 1);
                    const x = (idx / buckets.length) * totalWidth;
                    const height = Math.max(3, (b.count / maxCount) * 72);
                    const y = 79 - height;

                    const isSelected = selectedSet.size === 0 || selectedSet.has(b.utc);
                    const isHovered = hoveredBucket?.index === idx;
                    const inDragWindow = brushStart !== null && brushEnd !== null && idx >= brushStart && idx <= brushEnd;

                    let fill = '#1a73e8';
                    let opacity = 0.85;

                    if (isHovered) {
                      fill = '#174ea6';
                      opacity = 1;
                    } else if (inDragWindow) {
                      fill = '#1a73e8';
                      opacity = 1;
                    } else if (selectedSet.size > 0 && !isSelected) {
                      fill = '#bdc1c6';
                      opacity = 0.35;
                    }

                    return (
                      <rect
                        key={b.utc}
                        x={x}
                        y={y}
                        width={barWidth}
                        height={height}
                        fill={fill}
                        opacity={opacity}
                        rx={1}
                      />
                    );
                  })}

                  {/* Drag Selection Brush Box */}
                  {brushStart !== null && brushEnd !== null && (
                    <rect
                      x={(brushStart / buckets.length) * Math.max(600, buckets.length * 4)}
                      y={0}
                      width={((brushEnd - brushStart + 1) / buckets.length) * Math.max(600, buckets.length * 4)}
                      height={80}
                      fill="rgba(26, 115, 232, 0.15)"
                      stroke="#1a73e8"
                      strokeWidth={1.5}
                      strokeDasharray="4 2"
                    />
                  )}
                </svg>

                {/* Floating Hover Tooltip */}
                {hoveredBucket && (
                  <Paper
                    elevation={3}
                    sx={{
                      position: 'absolute',
                      top: Math.max(0, hoveredBucket.y - 65),
                      left: Math.min(hoveredBucket.x + 10, (containerRef.current?.clientWidth || 500) - 170),
                      p: 1,
                      backgroundColor: '#202124',
                      color: '#ffffff',
                      borderRadius: 1,
                      pointerEvents: 'none',
                      zIndex: 10,
                      minWidth: 150,
                    }}
                  >
                    <Typography variant="caption" sx={{ display: 'block', fontWeight: 600, color: '#8ab4f8' }}>
                      {hoveredBucket.bucket.display}
                    </Typography>
                    <Typography variant="caption" sx={{ display: 'block', color: '#e8eaed' }}>
                      <strong>{hoveredBucket.bucket.count.toLocaleString()}</strong> rows ({Math.round((hoveredBucket.bucket.count / totalRecords) * 100)}%)
                    </Typography>
                    <Typography variant="caption" sx={{ display: 'block', color: '#9aa0a6', fontSize: '0.65rem' }}>
                      Click or drag to filter
                    </Typography>
                  </Paper>
                )}
              </Box>

              {/* Time Axis Labels */}
              <Box sx={{ display: 'flex', justifyContent: 'space-between', mt: 0.5, px: 0.5 }}>
                <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontSize: '0.7rem' }}>
                  {buckets[0]?.display}
                </Typography>
                {buckets.length > 2 && (
                  <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontSize: '0.7rem' }}>
                    {buckets[Math.floor(buckets.length / 2)]?.display}
                  </Typography>
                )}
                <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontSize: '0.7rem' }}>
                  {buckets[buckets.length - 1]?.display}
                </Typography>
              </Box>
            </>
          )}
        </Box>
      </Collapse>
    </Paper>
  );
};
