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

import React from 'react';
import { Box, Typography, Tooltip } from '@mui/material';
import { ColumnProfile } from '../types';
import { gcpPalette } from '../theme';

interface ColumnSparklineProps {
  profile?: ColumnProfile | null;
  onSelectFilterRange?: (min: number, max: number) => void;
  onSelectCategory?: (category: string) => void;
}

const CATEGORY_COLORS = ['#1a73e8', '#34a853', '#f29900', '#9334e6', '#80868b'];

const formatNum = (val: number | string | null | undefined): string => {
  if (val === null || val === undefined) return '0';
  const num = Number(val);
  if (isNaN(num)) return String(val);
  return num.toLocaleString();
};

export const ColumnSparkline: React.FC<ColumnSparklineProps> = ({
  profile,
  onSelectFilterRange,
  onSelectCategory,
}) => {
  if (!profile || !profile.total_count || profile.total_count === 0) {
    return (
      <Box sx={{ height: 20, display: 'flex', alignItems: 'center', opacity: 0.35 }}>
        <Typography variant="caption" sx={{ fontSize: '0.62rem', color: gcpPalette.neutral.textSecondary }}>
          -
        </Typography>
      </Box>
    );
  }

  // 1. Numeric Micro-Histogram
  if (profile.filter_type === 'numeric') {
    const { histogram, min_value, max_value, null_count = 0, total_count = 0 } = profile;

    if (!histogram || !Array.isArray(histogram) || histogram.length === 0) {
      return (
        <Box sx={{ height: 20, display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <Typography variant="caption" sx={{ fontSize: '0.62rem', color: gcpPalette.neutral.textSecondary }}>
            {min_value !== undefined && min_value !== null && max_value !== undefined && max_value !== null
              ? `${min_value === max_value ? `val: ${formatNum(min_value)}` : `${formatNum(min_value)} ~ ${formatNum(max_value)}`}`
              : 'All null'}
          </Typography>
        </Box>
      );
    }

    const counts = histogram.map((b) => (b && typeof b.count === 'number' ? b.count : 0));
    const maxCount = Math.max(...counts, 1);
    const nullPct = total_count > 0 ? Math.round(((null_count || 0) / total_count) * 100) : 0;

    return (
      <Box sx={{ height: 20, display: 'flex', alignItems: 'flex-end', width: '100%', gap: '1px', pt: 0.2 }}>
        <svg width="100%" height={18} style={{ display: 'block', overflow: 'visible' }}>
          {histogram.map((b, i) => {
            if (!b) return null;
            const count = typeof b.count === 'number' ? b.count : 0;
            const barHeight = count > 0 ? Math.max(2, (count / maxCount) * 16) : 1;
            const y = 17 - barHeight;
            const widthPct = 100 / histogram.length;
            const xPct = i * widthPct;
            const pct = total_count > 0 ? ((count / total_count) * 100).toFixed(1) : '0';
            const isPeak = count === maxCount && count > 0;
            const minStr = formatNum(b.bin_min);
            const maxStr = formatNum(b.bin_max);

            return (
              <Tooltip
                key={b.bin_index ?? i}
                title={`${minStr} to ${maxStr}: ${formatNum(count)} rows (${pct}%)${nullPct > 0 ? ` • ${nullPct}% null` : ''}`}
                arrow
                placement="top"
                enterDelay={150}
              >
                <rect
                  x={`${xPct}%`}
                  y={y}
                  width={`calc(${widthPct}% - 1px)`}
                  height={barHeight}
                  fill={isPeak ? '#174ea6' : '#1a73e8'}
                  opacity={count > 0 ? (isPeak ? 0.95 : 0.75) : 0.2}
                  rx={0.5}
                  style={{
                    cursor: onSelectFilterRange && count > 0 ? 'pointer' : 'default',
                    transition: 'opacity 0.1s',
                  }}
                  onClick={(e) => {
                    if (onSelectFilterRange && count > 0 && b.bin_min !== undefined && b.bin_max !== undefined) {
                      e.stopPropagation();
                      onSelectFilterRange(b.bin_min, b.bin_max);
                    }
                  }}
                />
              </Tooltip>
            );
          })}
        </svg>
      </Box>
    );
  }

  // 2. Categorical Segmented Frequency Bar
  if (profile.filter_type === 'text') {
    const { top_categories, distinct_count = 0, null_count = 0, total_count = 0 } = profile;
    const nullPct = total_count > 0 ? Math.round(((null_count || 0) / total_count) * 100) : 0;

    if (!top_categories || !Array.isArray(top_categories) || top_categories.length === 0) {
      return (
        <Box sx={{ height: 20, display: 'flex', alignItems: 'center' }}>
          <Typography variant="caption" sx={{ fontSize: '0.62rem', color: gcpPalette.neutral.textSecondary }}>
            {nullPct === 100 ? '100% null' : `${formatNum(distinct_count)} distinct`}
          </Typography>
        </Box>
      );
    }

    return (
      <Box sx={{ height: 20, display: 'flex', flexDirection: 'column', justifyContent: 'center', width: '100%' }}>
        <Box
          sx={{
            display: 'flex',
            height: 7,
            width: '100%',
            borderRadius: '2px',
            overflow: 'hidden',
            backgroundColor: '#e8eaed',
          }}
        >
          {top_categories.map((cat, idx) => {
            if (!cat) return null;
            const count = typeof cat.count === 'number' ? cat.count : 0;
            const percent = typeof cat.percent === 'number' ? cat.percent : 0;
            const displayLabel = cat.display_value || cat.value || '';
            return (
              <Tooltip
                key={cat.value || idx}
                title={`"${displayLabel}": ${formatNum(count)} rows (${percent}%) • ${formatNum(distinct_count)} distinct values`}
                arrow
                placement="top"
                enterDelay={150}
              >
                <Box
                  onClick={(e) => {
                    if (onSelectCategory && cat.value !== undefined) {
                      e.stopPropagation();
                      onSelectCategory(cat.value);
                    }
                  }}
                  sx={{
                    width: `${percent}%`,
                    height: '100%',
                    backgroundColor: CATEGORY_COLORS[idx % CATEGORY_COLORS.length],
                    cursor: onSelectCategory ? 'pointer' : 'default',
                    '&:hover': { filter: 'brightness(0.9)' },
                  }}
                />
              </Tooltip>
            );
          })}
          {nullPct > 0 && (
            <Tooltip title={`NULL: ${formatNum(null_count)} rows (${nullPct}%)`} arrow placement="top">
              <Box sx={{ width: `${nullPct}%`, height: '100%', backgroundColor: '#dadce0' }} />
            </Tooltip>
          )}
        </Box>
      </Box>
    );
  }

  // 3. Date / Timestamp Indicator
  if (profile.filter_type === 'date') {
    const { min_date, max_date, distinct_count = 0 } = profile;
    const shortMin = min_date ? String(min_date).substring(5, 16) : '';
    const shortMax = max_date ? String(max_date).substring(5, 16) : '';

    return (
      <Tooltip title={`Timeline: ${min_date || ''} ~ ${max_date || ''} (${formatNum(distinct_count)} timestamps)`} arrow placement="top">
        <Box sx={{ height: 20, display: 'flex', alignItems: 'center', width: '100%', overflow: 'hidden' }}>
          <Typography
            variant="caption"
            sx={{
              fontSize: '0.62rem',
              color: gcpPalette.neutral.textSecondary,
              whiteSpace: 'nowrap',
              textOverflow: 'ellipsis',
              overflow: 'hidden',
            }}
          >
            {shortMin && shortMax ? `${shortMin} ~ ${shortMax}` : `${formatNum(distinct_count)} timestamps`}
          </Typography>
        </Box>
      </Tooltip>
    );
  }

  return null;
};
