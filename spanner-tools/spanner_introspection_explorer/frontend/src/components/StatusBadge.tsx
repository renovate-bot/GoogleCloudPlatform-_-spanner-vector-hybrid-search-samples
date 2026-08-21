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
import { Box, Typography } from '@mui/material';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import HourglassEmptyIcon from '@mui/icons-material/HourglassEmpty';
import SyncIcon from '@mui/icons-material/Sync';
import { gcpPalette } from '../theme';

interface StatusBadgeProps {
  status: 'PASS' | 'WARNING' | 'FAIL' | 'PENDING' | 'CHECKING' | 'HEALTHY' | 'DEGRADED';
  label?: string;
  size?: 'small' | 'medium';
}

export const StatusBadge: React.FC<StatusBadgeProps> = ({ status, label, size = 'small' }) => {
  const getStyle = () => {
    switch (status) {
      case 'PASS':
      case 'HEALTHY':
        return {
          color: gcpPalette.status.success.main,
          bg: gcpPalette.status.success.light,
          icon: <CheckCircleOutlineIcon sx={{ fontSize: size === 'small' ? 14 : 16 }} />,
          text: label || 'PASS'
        };
      case 'WARNING':
      case 'DEGRADED':
        return {
          color: gcpPalette.status.warning.main,
          bg: gcpPalette.status.warning.light,
          icon: <WarningAmberIcon sx={{ fontSize: size === 'small' ? 14 : 16 }} />,
          text: label || 'WARNING'
        };
      case 'FAIL':
        return {
          color: gcpPalette.status.error.main,
          bg: gcpPalette.status.error.light,
          icon: <ErrorOutlineIcon sx={{ fontSize: size === 'small' ? 14 : 16 }} />,
          text: label || 'FAIL'
        };
      case 'CHECKING':
        return {
          color: gcpPalette.status.info.main,
          bg: gcpPalette.status.info.light,
          icon: <SyncIcon sx={{ fontSize: size === 'small' ? 14 : 16, animation: 'spin 1.5s linear infinite' }} />,
          text: label || 'CHECKING...'
        };
      case 'PENDING':
      default:
        return {
          color: gcpPalette.status.pending.main,
          bg: gcpPalette.status.pending.light,
          icon: <HourglassEmptyIcon sx={{ fontSize: size === 'small' ? 14 : 16 }} />,
          text: label || 'PENDING'
        };
    }
  };

  const style = getStyle();

  return (
    <Box
      sx={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: 0.6,
        px: size === 'small' ? 1 : 1.5,
        py: size === 'small' ? 0.3 : 0.5,
        borderRadius: '12px',
        backgroundColor: style.bg,
        color: style.color,
        fontWeight: 600,
        fontSize: size === 'small' ? '0.75rem' : '0.8125rem',
        border: `1px solid ${style.color}22`,
        '@keyframes spin': {
          '0%': { transform: 'rotate(0deg)' },
          '100%': { transform: 'rotate(360deg)' }
        }
      }}
    >
      {style.icon}
      <span>{style.text}</span>
    </Box>
  );
};
