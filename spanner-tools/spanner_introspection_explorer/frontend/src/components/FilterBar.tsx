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

import React, { useState } from 'react';
import {
  Box,
  Button,
  Chip,
  IconButton,
  InputAdornment,
  Menu,
  MenuItem,
  Paper,
  Popover,
  TextField,
  Typography,
  Tooltip,
  FormControl,
  InputLabel,
  Select,
} from '@mui/material';
import FilterListIcon from '@mui/icons-material/FilterList';
import SearchIcon from '@mui/icons-material/Search';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import AddIcon from '@mui/icons-material/Add';
import CloseIcon from '@mui/icons-material/Close';

import { ColumnFilter, ColumnMetadata } from '../types';
import { IntervalPicker } from './IntervalPicker';
import { gcpPalette } from '../theme';

interface FilterBarProps {
  tableName: string;
  columns: ColumnMetadata[];
  filters: Record<string, ColumnFilter>;
  globalSearch: string;
  utcOffset: number;
  onFilterChange: (filters: Record<string, ColumnFilter>) => void;
  onGlobalSearchChange: (search: string) => void;
  onReset: () => void;
}

export const FilterBar: React.FC<FilterBarProps> = ({
  tableName,
  columns,
  filters,
  globalSearch,
  utcOffset,
  onFilterChange,
  onGlobalSearchChange,
  onReset,
}) => {
  const addFilterBtnRef = React.useRef<HTMLButtonElement>(null);
  const [columnMenuAnchor, setColumnMenuAnchor] = useState<null | HTMLElement>(null);
  const [activeFilterCol, setActiveFilterCol] = useState<ColumnMetadata | null>(null);
  const [popoverAnchor, setPopoverAnchor] = useState<null | HTMLElement>(null);

  // Filter input states for active column popover
  const [tempOperator, setTempOperator] = useState<'contains' | 'exact' | 'not_contains' | 'not_exact'>('contains');
  const [tempTextValue, setTempTextValue] = useState('');
  const [tempMin, setTempMin] = useState<string>('');
  const [tempMax, setTempMax] = useState<string>('');

  const handleOpenAddFilter = (event: React.MouseEvent<HTMLElement>) => {
    setColumnMenuAnchor(event.currentTarget);
  };

  const handleSelectColumn = (col: ColumnMetadata) => {
    setColumnMenuAnchor(null);
    setActiveFilterCol(col);
    setPopoverAnchor(addFilterBtnRef.current);

    // Initialize temp state from current filter
    const existing = filters[col.name];
    if (existing) {
      setTempOperator(existing.operator || 'contains');
      setTempTextValue(existing.value || '');
      setTempMin(existing.min !== undefined && existing.min !== null ? String(existing.min) : '');
      setTempMax(existing.max !== undefined && existing.max !== null ? String(existing.max) : '');
    } else {
      setTempOperator('contains');
      setTempTextValue('');
      setTempMin('');
      setTempMax('');
    }
  };

  const handleEditFilter = (colName: string, targetEl: HTMLElement) => {
    const col = columns.find((c) => c.name === colName);
    if (!col) return;
    setActiveFilterCol(col);
    setPopoverAnchor(targetEl);

    const existing = filters[colName];
    if (existing) {
      setTempOperator(existing.operator || 'contains');
      setTempTextValue(existing.value || '');
      setTempMin(existing.min !== undefined && existing.min !== null ? String(existing.min) : '');
      setTempMax(existing.max !== undefined && existing.max !== null ? String(existing.max) : '');
    }
  };

  const handleApplyFilter = () => {
    if (!activeFilterCol) return;

    const newFilters = { ...filters };
    if (activeFilterCol.filter_type === 'text') {
      if (tempTextValue.trim()) {
        newFilters[activeFilterCol.name] = {
          type: 'text',
          operator: tempOperator,
          value: tempTextValue.trim(),
        };
      } else {
        delete newFilters[activeFilterCol.name];
      }
    } else if (activeFilterCol.filter_type === 'numeric') {
      if (tempMin !== '' || tempMax !== '') {
        newFilters[activeFilterCol.name] = {
          type: 'numeric',
          min: tempMin !== '' ? parseFloat(tempMin) : undefined,
          max: tempMax !== '' ? parseFloat(tempMax) : undefined,
        };
      } else {
        delete newFilters[activeFilterCol.name];
      }
    } else if (activeFilterCol.filter_type === 'date') {
      if (tempMin !== '' || tempMax !== '') {
        newFilters[activeFilterCol.name] = {
          type: 'date',
          min: tempMin !== '' ? tempMin : undefined,
          max: tempMax !== '' ? tempMax : undefined,
        };
      } else {
        delete newFilters[activeFilterCol.name];
      }
    }

    onFilterChange(newFilters);
    handleClosePopover();
  };

  const handleClosePopover = () => {
    setPopoverAnchor(null);
    setActiveFilterCol(null);
  };

  const handleRemoveFilter = (colName: string) => {
    const newFilters = { ...filters };
    delete newFilters[colName];
    onFilterChange(newFilters);
  };

  const intervalCol = columns.find((c) => c.name.toLowerCase() === 'interval_end');
  const intervalColName = intervalCol?.name || 'interval_end';

  const handleIntervalFilterChange = (timestamps: string[]) => {
    const newFilters = { ...filters };
    if (timestamps.length > 0) {
      newFilters[intervalColName] = {
        type: 'date',
        selected_timestamps: timestamps,
      };
    } else {
      delete newFilters[intervalColName];
    }
    onFilterChange(newFilters);
  };

  const activeFilterCount = Object.keys(filters).length;
  const hasIntervalCol = Boolean(intervalCol);
  const selectedTimestamps = filters[intervalColName]?.selected_timestamps || [];

  return (
    <Paper sx={{ p: 1.5, mb: 2, border: `1px solid ${gcpPalette.neutral.border}` }}>
      <Box sx={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: 1.5 }}>
        {/* Quick Search */}
        <TextField
          size="small"
          placeholder="Filter rows across all columns..."
          value={globalSearch}
          onChange={(e) => onGlobalSearchChange(e.target.value)}
          sx={{ minWidth: 280, flex: { xs: '1 1 100%', md: '0 1 320px' } }}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
              </InputAdornment>
            ),
            endAdornment: globalSearch && (
              <InputAdornment position="end">
                <IconButton size="small" onClick={() => onGlobalSearchChange('')}>
                  <CloseIcon fontSize="small" />
                </IconButton>
              </InputAdornment>
            ),
          }}
        />

        {/* Add Column Filter Button */}
        <Button
          ref={addFilterBtnRef}
          variant="outlined"
          size="small"
          startIcon={<AddIcon />}
          onClick={handleOpenAddFilter}
          sx={{
            borderColor: gcpPalette.neutral.border,
            color: gcpPalette.neutral.textPrimary,
          }}
        >
          Add Column Filter
        </Button>

        {/* Specialized Interval Picker if interval_end column exists */}
        {hasIntervalCol && (
          <IntervalPicker
            tableName={tableName}
            utcOffset={utcOffset}
            selectedTimestamps={selectedTimestamps}
            onChange={handleIntervalFilterChange}
          />
        )}

        {/* Reset Filters */}
        {(activeFilterCount > 0 || globalSearch) && (
          <Tooltip title="Reset all filters and search">
            <Button
              variant="text"
              size="small"
              startIcon={<RestartAltIcon />}
              onClick={onReset}
              sx={{ color: gcpPalette.neutral.textSecondary }}
            >
              Reset
            </Button>
          </Tooltip>
        )}
      </Box>

      {/* Active Filter Chips */}
      {activeFilterCount > 0 && (
        <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1, mt: 1.5, pt: 1, borderTop: `1px solid ${gcpPalette.neutral.border}` }}>
          <Typography variant="caption" sx={{ display: 'flex', alignItems: 'center', color: gcpPalette.neutral.textSecondary, mr: 0.5 }}>
            Active filters:
          </Typography>
          {Object.entries(filters).map(([colName, filter]) => {
            if (colName.toLowerCase() === 'interval_end' && filter.selected_timestamps && filter.selected_timestamps.length > 0) {
              return null; // Handled by IntervalPicker chip
            }

            let label = `${colName}: `;
            let tooltipContent = '';
            if (filter.type === 'text') {
              let op = 'contains';
              if (filter.operator === 'exact') op = '=';
              else if (filter.operator === 'not_exact') op = '!=';
              else if (filter.operator === 'not_contains') op = 'does not contain';

              const valStr = String(filter.value || '');
              const displayVal = valStr.length > 35 ? valStr.substring(0, 32) + '...' : valStr;
              label += `${op} "${displayVal}"`;
              tooltipContent = `${colName} ${op} "${valStr}"`;
            } else if (filter.type === 'numeric') {
              if (filter.min !== undefined && filter.max !== undefined) {
                label += `${filter.min} to ${filter.max}`;
              } else if (filter.min !== undefined) {
                label += `>= ${filter.min}`;
              } else if (filter.max !== undefined) {
                label += `<= ${filter.max}`;
              }
            } else if (filter.type === 'date') {
              if (filter.min && filter.max) {
                label += `${filter.min} to ${filter.max}`;
              } else if (filter.min) {
                label += `>= ${filter.min}`;
              } else if (filter.max) {
                label += `<= ${filter.max}`;
              }
            }

            const chipElem = (
              <Chip
                key={colName}
                label={label}
                size="small"
                onClick={(e) => handleEditFilter(colName, e.currentTarget)}
                onDelete={() => handleRemoveFilter(colName)}
                sx={{
                  backgroundColor: gcpPalette.primary.light,
                  color: gcpPalette.primary.main,
                  fontWeight: 500,
                  fontSize: '0.75rem',
                  cursor: 'pointer',
                }}
              />
            );

            return tooltipContent ? (
              <Tooltip key={colName} title={tooltipContent} arrow>
                {chipElem}
              </Tooltip>
            ) : (
              chipElem
            );
          })}
        </Box>
      )}

      {/* Column Selection Menu */}
      <Menu anchorEl={columnMenuAnchor} open={Boolean(columnMenuAnchor)} onClose={() => setColumnMenuAnchor(null)}>
        {columns.map((col) => (
          <MenuItem key={col.name} onClick={() => handleSelectColumn(col)}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', width: '100%', gap: 2 }}>
              <Typography variant="body2">{col.name}</Typography>
              <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
                {col.type}
              </Typography>
            </Box>
          </MenuItem>
        ))}
      </Menu>

      {/* Filter Value Popover */}
      <Popover
        open={Boolean(popoverAnchor)}
        anchorEl={popoverAnchor}
        onClose={handleClosePopover}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
      >
        <Box sx={{ p: 2, minWidth: 280 }}>
          <Typography variant="h3" sx={{ fontSize: '0.9rem', mb: 1.5 }}>
            Filter: {activeFilterCol?.name}
          </Typography>

          {activeFilterCol?.filter_type === 'text' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
              <FormControl size="small" fullWidth>
                <InputLabel id="text-match-type-label">Match Type</InputLabel>
                <Select
                  labelId="text-match-type-label"
                  label="Match Type"
                  value={tempOperator}
                  onChange={(e) => setTempOperator(e.target.value as 'contains' | 'exact' | 'not_contains' | 'not_exact')}
                >
                  <MenuItem value="contains">Contains (default, substring)</MenuItem>
                  <MenuItem value="exact">Exact Match (=)</MenuItem>
                  <MenuItem value="not_contains">Does not contain</MenuItem>
                  <MenuItem value="not_exact">Does not match (!=)</MenuItem>
                </Select>
              </FormControl>

              <TextField
                fullWidth
                size="small"
                label={
                  tempOperator === 'exact' ? 'Exact value' :
                  tempOperator === 'not_exact' ? 'Value to exclude' :
                  tempOperator === 'not_contains' ? 'Substring to exclude' :
                  'Contains value'
                }
                placeholder={
                  tempOperator === 'exact' ? 'Exact match string' :
                  tempOperator === 'not_exact' ? 'String not matching' :
                  tempOperator === 'not_contains' ? 'Substring not matching' :
                  'Substring to search'
                }
                value={tempTextValue}
                onChange={(e) => setTempTextValue(e.target.value)}
                autoFocus
                onKeyDown={(e) => e.key === 'Enter' && handleApplyFilter()}
              />
            </Box>
          )}

          {activeFilterCol?.filter_type === 'numeric' && (
            <Box sx={{ display: 'flex', gap: 1 }}>
              <TextField
                size="small"
                label="Min"
                type="number"
                value={tempMin}
                onChange={(e) => setTempMin(e.target.value)}
                autoFocus
              />
              <TextField
                size="small"
                label="Max"
                type="number"
                value={tempMax}
                onChange={(e) => setTempMax(e.target.value)}
              />
            </Box>
          )}

          {activeFilterCol?.filter_type === 'date' && (
            <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
              <TextField
                size="small"
                label="From Date"
                type="date"
                InputLabelProps={{ shrink: true }}
                value={tempMin}
                onChange={(e) => setTempMin(e.target.value)}
              />
              <TextField
                size="small"
                label="To Date"
                type="date"
                InputLabelProps={{ shrink: true }}
                value={tempMax}
                onChange={(e) => setTempMax(e.target.value)}
              />
            </Box>
          )}

          <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1, mt: 2 }}>
            <Button size="small" onClick={handleClosePopover} color="inherit">
              Cancel
            </Button>
            <Button size="small" variant="contained" onClick={handleApplyFilter}>
              Apply
            </Button>
          </Box>
        </Box>
      </Popover>
    </Paper>
  );
};
