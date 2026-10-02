// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import React from 'react';
import {
  Autocomplete,
  Box,
  Chip,
  CircularProgress,
  IconButton,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import RefreshIcon from '@mui/icons-material/Refresh';

export interface SelectOption {
  value: string;
  label: string;
  sublabel?: string;
  badge?: string;
}

export interface DynamicSelectProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
  options: (SelectOption | string)[];
  placeholder?: string;
  disabled?: boolean;
  loading?: boolean;
  required?: boolean;
  onRefresh?: () => void;
  helperText?: string;
  error?: boolean;
  freeSolo?: boolean;
}

export const DynamicSelect: React.FC<DynamicSelectProps> = ({
  label,
  value,
  onChange,
  options,
  placeholder,
  disabled = false,
  loading = false,
  required = false,
  onRefresh,
  helperText,
  error = false,
  freeSolo = true,
}) => {
  // Normalize options into SelectOption[]
  const normalizedOptions: SelectOption[] = React.useMemo(() => {
    return options.map((opt) => {
      if (typeof opt === 'string') {
        return { value: opt, label: opt };
      }
      return opt;
    });
  }, [options]);

  // Find the selected option or synthesize an object if freeSolo
  const selectedOption = React.useMemo(() => {
    const found = normalizedOptions.find((o) => o.value === value);
    if (found) return found;
    if (value) return { value, label: value };
    return null;
  }, [normalizedOptions, value]);

  return (
    <Box sx={{ width: '100%' }}>
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          mb: 0.75,
        }}
      >
        <Typography
          variant="body2"
          sx={{
            fontWeight: 600,
            fontSize: '0.85rem',
            color: error ? 'error.main' : 'text.primary',
            display: 'flex',
            alignItems: 'center',
            gap: 0.5,
          }}
        >
          {label}
          {required && (
            <Typography component="span" color="error.main" sx={{ fontWeight: 700 }}>
              *
            </Typography>
          )}
        </Typography>

        {onRefresh && (
          <Tooltip title={loading ? 'Refreshing...' : 'Refresh projects from GCP'}>
            <span>
              <IconButton
                size="small"
                onClick={onRefresh}
                disabled={disabled || loading}
                sx={{
                  p: 0.5,
                  color: 'primary.main',
                  '&:hover': { bgcolor: 'action.hover' },
                }}
              >
                <RefreshIcon
                  fontSize="small"
                  sx={{
                    animation: loading ? 'spin 1s linear infinite' : 'none',
                    '@keyframes spin': {
                      '0%': { transform: 'rotate(0deg)' },
                      '100%': { transform: 'rotate(360deg)' },
                    },
                  }}
                />
              </IconButton>
            </span>
          </Tooltip>
        )}
      </Box>

      <Autocomplete<SelectOption, false, false, boolean>
        value={selectedOption}
        onChange={(_event, newValue) => {
          if (!newValue) {
            onChange('');
          } else if (typeof newValue === 'string') {
            onChange(newValue);
          } else {
            onChange(newValue.value);
          }
        }}
        inputValue={undefined}
        onInputChange={(_event, newInputValue, reason) => {
          if (freeSolo && reason === 'input') {
            onChange(newInputValue);
          }
        }}
        options={normalizedOptions}
        getOptionLabel={(option) => {
          if (typeof option === 'string') return option;
          return option.label || option.value || '';
        }}
        isOptionEqualToValue={(option, val) => {
          if (typeof val === 'string') return option.value === val;
          return option.value === val.value;
        }}
        freeSolo={freeSolo}
        disabled={disabled}
        loading={loading}
        renderOption={(props, option) => (
          <Box
            component="li"
            {...props}
            key={option.value}
            sx={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              py: 1,
              px: 1.5,
              borderBottom: '1px solid',
              borderColor: 'divider',
              '&:last-child': { borderBottom: 'none' },
            }}
          >
            <Box sx={{ display: 'flex', flexDirection: 'column', pr: 1, overflow: 'hidden' }}>
              <Typography
                variant="body2"
                sx={{
                  fontWeight: option.value === value ? 700 : 500,
                  color: option.value === value ? 'primary.main' : 'text.primary',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {option.label}
              </Typography>
              {option.sublabel && (
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{
                    fontSize: '0.72rem',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {option.sublabel}
                </Typography>
              )}
            </Box>

            {option.badge && (
              <Chip
                label={option.badge}
                size="small"
                color={option.badge.toLowerCase().includes('active') ? 'success' : 'primary'}
                variant="outlined"
                sx={{
                  height: 20,
                  fontSize: '0.68rem',
                  fontWeight: 600,
                  textTransform: 'uppercase',
                  letterSpacing: '0.04em',
                }}
              />
            )}
          </Box>
        )}
        renderInput={(params) => (
          <TextField
            {...params}
            placeholder={placeholder || 'Select or enter GCP project...'}
            error={error}
            helperText={helperText}
            size="small"
            InputProps={{
              ...params.InputProps,
              endAdornment: (
                <React.Fragment>
                  {loading ? <CircularProgress color="inherit" size={18} sx={{ mr: 1 }} /> : null}
                  {params.InputProps.endAdornment}
                </React.Fragment>
              ),
            }}
          />
        )}
      />
    </Box>
  );
};
export default DynamicSelect;
