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
import {
  AppBar,
  Toolbar,
  Typography,
  Box,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  IconButton,
  Tooltip,
  Badge,
  Button,
  Chip,
} from '@mui/material';
import StorageIcon from '@mui/icons-material/Storage';
import PublicIcon from '@mui/icons-material/Public';
import TuneIcon from '@mui/icons-material/Tune';
import DnsIcon from '@mui/icons-material/Dns';
import { gcpPalette } from '../theme';
import { DatabaseItem } from '../types';

import { TIMEZONE_DESIGNATIONS, getDesignationForOffset, getOffsetForDesignation } from '../utils/timezones';

interface AppHeaderProps {
  selectedDatabaseName?: string;
  selectedTable?: string | null;
  selectedUtcOffset: string;
  utcOffsets: string[];
  onUtcOffsetChange: (offset: string) => void;
  onOpenSettings: () => void;
  onNavigateToConnections?: () => void;
}

export const AppHeader: React.FC<AppHeaderProps> = ({
  selectedDatabaseName,
  selectedTable,
  selectedUtcOffset,
  utcOffsets,
  onUtcOffsetChange,
  onOpenSettings,
  onNavigateToConnections,
}) => {
  const [selectedTzCode, setSelectedTzCode] = React.useState<string>(() => {
    return getDesignationForOffset(selectedUtcOffset);
  });

  // Keep selectedTzCode in sync with selectedUtcOffset without overriding distinct codes that share the same offset
  React.useEffect(() => {
    const currentOffset = getOffsetForDesignation(selectedTzCode);
    if (currentOffset !== selectedUtcOffset) {
      setSelectedTzCode(getDesignationForOffset(selectedUtcOffset));
    }
  }, [selectedUtcOffset, selectedTzCode]);

  return (
    <AppBar
      position="sticky"
      sx={{
        backgroundColor: '#ffffff',
        borderBottom: `1px solid ${gcpPalette.neutral.border}`,
        boxShadow: 'none',
        color: gcpPalette.neutral.textPrimary,
        zIndex: (theme) => theme.zIndex.drawer + 1,
      }}
    >
      <Toolbar sx={{ minHeight: '48px !important', px: 2, display: 'flex', justifyContent: 'space-between' }}>
        {/* Left: Branding & Context Breadcrumb */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 32,
              height: 32,
              borderRadius: '4px',
              backgroundColor: gcpPalette.primary.light,
              color: gcpPalette.primary.main,
            }}
          >
            <StorageIcon fontSize="small" />
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="h3" sx={{ fontSize: '1rem', fontWeight: 600 }}>
              Spanner Introspection Explorer
            </Typography>
            {selectedDatabaseName && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.8, ml: 1 }}>
                <Typography variant="caption" sx={{ color: gcpPalette.neutral.border, fontSize: '0.9rem' }}>
                  /
                </Typography>
                <Chip
                  size="small"
                  label={selectedDatabaseName}
                  sx={{
                    height: 22,
                    fontSize: '0.75rem',
                    fontWeight: 600,
                    backgroundColor: gcpPalette.primary.light,
                    color: gcpPalette.primary.main,
                  }}
                />
                {selectedTable && (
                  <>
                    <Typography variant="caption" sx={{ color: gcpPalette.neutral.border, fontSize: '0.9rem' }}>
                      /
                    </Typography>
                    <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontWeight: 500, fontSize: '0.8rem' }}>
                      {selectedTable}
                    </Typography>
                  </>
                )}
              </Box>
            )}
          </Box>
        </Box>

        {/* Right: Timezone Designations & UTC Offset Pickers + Settings */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Tooltip title="Timezone Display Configuration">
              <PublicIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
            </Tooltip>

            {/* Named Timezone Designation Dropdown (CET, BST, PST, PDT, etc.) */}
            <FormControl size="small" sx={{ minWidth: 125 }}>
              <Select
                value={selectedTzCode}
                onChange={(e) => {
                  const code = e.target.value;
                  setSelectedTzCode(code);
                  const targetOffset = getOffsetForDesignation(code);
                  if (targetOffset) {
                    onUtcOffsetChange(targetOffset);
                  }
                }}
                displayEmpty
                renderValue={(selected) => {
                  if (!selected) {
                    return <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontSize: '0.8rem' }}>Timezone</Typography>;
                  }
                  const tz = TIMEZONE_DESIGNATIONS.find((t) => t.code === selected);
                  return tz ? `${tz.code}` : selected;
                }}
                sx={{
                  height: 32,
                  fontSize: '0.8125rem',
                  fontWeight: 600,
                  backgroundColor: gcpPalette.neutral.background,
                  '& .MuiOutlinedInput-notchedOutline': {
                    borderColor: gcpPalette.neutral.border,
                  },
                }}
              >
                {TIMEZONE_DESIGNATIONS.map((tz) => (
                  <MenuItem
                    key={tz.code}
                    value={tz.code}
                    sx={{ fontSize: '0.8rem', display: 'flex', justifyContent: 'space-between', gap: 2 }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.8rem' }}>
                        {tz.code}
                      </Typography>
                      <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, fontSize: '0.72rem' }}>
                        {tz.name}
                      </Typography>
                    </Box>
                    <Chip
                      label={tz.offset.replace('UTC ', '')}
                      size="small"
                      sx={{ height: 18, fontSize: '0.65rem', backgroundColor: '#e8eaed', fontWeight: 500 }}
                    />
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            {/* Numerical UTC Offset Selector */}
            <FormControl size="small" sx={{ minWidth: 135 }}>
              <Select
                value={selectedUtcOffset}
                onChange={(e) => onUtcOffsetChange(e.target.value)}
                displayEmpty
                sx={{
                  height: 32,
                  fontSize: '0.8125rem',
                  backgroundColor: gcpPalette.neutral.background,
                  '& .MuiOutlinedInput-notchedOutline': {
                    borderColor: gcpPalette.neutral.border,
                  },
                }}
              >
                {utcOffsets.map((offset) => (
                  <MenuItem key={offset} value={offset} sx={{ fontSize: '0.8125rem' }}>
                    {offset}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>

          {/* Settings Action */}
          <Tooltip title="Configuration & Settings">
            <IconButton size="small" onClick={onOpenSettings} sx={{ color: gcpPalette.neutral.textSecondary }}>
              <TuneIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Box>
      </Toolbar>
    </AppBar>
  );
};
