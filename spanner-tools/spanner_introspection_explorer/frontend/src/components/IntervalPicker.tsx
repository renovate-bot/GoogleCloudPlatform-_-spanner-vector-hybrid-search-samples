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

import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Checkbox,
  CircularProgress,
  InputAdornment,
} from '@mui/material';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import SearchIcon from '@mui/icons-material/Search';
import { api } from '../services/api';
import { IntervalOption } from '../types';
import { gcpPalette } from '../theme';

interface IntervalPickerProps {
  tableName: string;
  utcOffset: number;
  selectedTimestamps: string[];
  onChange: (timestamps: string[]) => void;
}

export const IntervalPicker: React.FC<IntervalPickerProps> = ({
  tableName,
  utcOffset,
  selectedTimestamps,
  onChange,
}) => {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [intervals, setIntervals] = useState<IntervalOption[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [tempSelected, setTempSelected] = useState<string[]>([]);

  useEffect(() => {
    if (open) {
      setTempSelected(selectedTimestamps);
      loadIntervals();
    }
  }, [open, tableName, utcOffset]);

  const loadIntervals = async () => {
    setLoading(true);
    try {
      const data = await api.getTableIntervals(tableName, utcOffset);
      setIntervals(data);
    } catch (err) {
      console.error('Failed to load intervals', err);
    } finally {
      setLoading(false);
    }
  };

  const handleToggle = (utcStr: string) => {
    setTempSelected((prev) =>
      prev.includes(utcStr) ? prev.filter((t) => t !== utcStr) : [...prev, utcStr]
    );
  };

  const handleSelectAll = () => {
    const filteredUtc = filteredIntervals.map((i) => i.utc);
    setTempSelected((prev) => Array.from(new Set([...prev, ...filteredUtc])));
  };

  const handleClearAll = () => {
    setTempSelected([]);
  };

  const handleApply = () => {
    onChange(tempSelected);
    setOpen(false);
  };

  const filteredIntervals = intervals.filter(
    (i) => i.display.toLowerCase().includes(searchTerm.toLowerCase()) || i.utc.includes(searchTerm)
  );

  return (
    <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: 1 }}>
      <Button
        variant="outlined"
        size="small"
        startIcon={<AccessTimeIcon />}
        onClick={() => setOpen(true)}
        sx={{
          borderColor: selectedTimestamps.length > 0 ? gcpPalette.primary.main : gcpPalette.neutral.border,
          backgroundColor: selectedTimestamps.length > 0 ? gcpPalette.primary.light : 'transparent',
          fontWeight: 500,
        }}
      >
        Interval Timestamps {selectedTimestamps.length > 0 && `(${selectedTimestamps.length})`}
      </Button>

      {selectedTimestamps.length > 0 && (
        <Chip
          label={`${selectedTimestamps.length} selected`}
          size="small"
          onDelete={() => onChange([])}
          sx={{
            backgroundColor: gcpPalette.primary.light,
            color: gcpPalette.primary.main,
            fontWeight: 600,
          }}
        />
      )}

      <Dialog open={open} onClose={() => setOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <AccessTimeIcon sx={{ color: gcpPalette.primary.main }} />
            <Typography variant="h3" sx={{ fontSize: '1.1rem' }}>
              Select Interval End Timestamps
            </Typography>
          </Box>
          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
            {intervals.length} distinct intervals
          </Typography>
        </DialogTitle>

        <DialogContent dividers sx={{ p: 2 }}>
          <TextField
            fullWidth
            size="small"
            placeholder="Search timestamp..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
                </InputAdornment>
              ),
            }}
            sx={{ mb: 1.5 }}
          />

          <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
              {tempSelected.length} of {intervals.length} intervals selected
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button size="small" onClick={handleSelectAll} sx={{ fontSize: '0.75rem', p: 0 }}>
                Select All Filtered
              </Button>
              <Button size="small" onClick={handleClearAll} sx={{ fontSize: '0.75rem', p: 0 }}>
                Clear
              </Button>
            </Box>
          </Box>

          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress size={32} />
            </Box>
          ) : (
            <List sx={{ maxHeight: 320, overflowY: 'auto', border: `1px solid ${gcpPalette.neutral.border}`, borderRadius: '4px', p: 0 }}>
              {filteredIntervals.length === 0 ? (
                <Typography variant="body2" sx={{ p: 2, textAlign: 'center', color: gcpPalette.neutral.textSecondary }}>
                  No intervals found.
                </Typography>
              ) : (
                filteredIntervals.map((interval) => {
                  const isChecked = tempSelected.includes(interval.utc);
                  return (
                    <ListItem key={interval.utc} disablePadding divider>
                      <ListItemButton onClick={() => handleToggle(interval.utc)} dense>
                        <ListItemIcon sx={{ minWidth: 36 }}>
                          <Checkbox edge="start" checked={isChecked} tabIndex={-1} disableRipple size="small" />
                        </ListItemIcon>
                        <ListItemText
                          primary={interval.display}
                          secondary={`UTC: ${interval.utc}`}
                          primaryTypographyProps={{ fontSize: '0.8125rem', fontWeight: 500 }}
                          secondaryTypographyProps={{ fontSize: '0.7rem' }}
                        />
                      </ListItemButton>
                    </ListItem>
                  );
                })
              )}
            </List>
          )}
        </DialogContent>

        <DialogActions sx={{ px: 2, py: 1.5 }}>
          <Button onClick={() => setOpen(false)} color="inherit">
            Cancel
          </Button>
          <Button onClick={handleApply} variant="contained" color="primary">
            Apply ({tempSelected.length})
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};
