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
  Box,
  Typography,
  LinearProgress,
  Paper,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Button,
} from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import { PreflightItem } from '../types';
import { StatusBadge } from './StatusBadge';
import { gcpPalette } from '../theme';

interface SseProgressProps {
  title: string;
  items: PreflightItem[];
  progressPercent: number;
  isComplete: boolean;
}

export const SseProgress: React.FC<SseProgressProps> = ({
  title,
  items,
  progressPercent,
  isComplete,
}) => {
  const [copiedId, setCopiedId] = React.useState<string | null>(null);

  const handleCopyFix = (id: string, command: string) => {
    navigator.clipboard.writeText(command);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2000);
  };

  return (
    <Paper sx={{ border: `1px solid ${gcpPalette.neutral.border}`, borderRadius: '8px', overflow: 'hidden' }}>
      {/* Header with Title and Percentage */}
      <Box sx={{ p: 2, backgroundColor: gcpPalette.neutral.surface, borderBottom: `1px solid ${gcpPalette.neutral.border}` }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
          <Typography variant="h3" sx={{ fontSize: '1rem' }}>
            {title}
          </Typography>
          <Typography variant="body2" sx={{ fontWeight: 600, color: gcpPalette.primary.main }}>
            {progressPercent}%
          </Typography>
        </Box>
        <LinearProgress
          variant="determinate"
          value={progressPercent}
          sx={{
            height: 6,
            borderRadius: 3,
            backgroundColor: gcpPalette.neutral.border,
            '& .MuiLinearProgress-bar': {
              backgroundColor: isComplete ? gcpPalette.status.success.main : gcpPalette.primary.main,
            },
          }}
        />
      </Box>

      {/* Checklist items */}
      <List sx={{ p: 0 }}>
        {items.map((item, idx) => (
          <ListItem
            key={item.id}
            divider={idx < items.length - 1}
            sx={{
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'flex-start',
              p: 2,
              backgroundColor: item.status === 'CHECKING' ? 'rgba(26, 115, 232, 0.04)' : 'transparent',
            }}
          >
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
              <Typography variant="body1" sx={{ fontWeight: 500 }}>
                {item.title}
              </Typography>
              <StatusBadge status={item.status} />
            </Box>

            {item.message && (
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mt: 0.5, fontSize: '0.8125rem' }}>
                {item.message}
              </Typography>
            )}

            {item.fix_command && (item.status === 'WARNING' || item.status === 'FAIL') && (
              <Box
                sx={{
                  mt: 1,
                  p: 1,
                  width: '100%',
                  backgroundColor: '#f1f3f4',
                  borderRadius: '4px',
                  border: `1px solid ${gcpPalette.neutral.border}`,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                }}
              >
                <Typography
                  variant="caption"
                  sx={{ fontFamily: 'Roboto Mono, monospace', color: '#202124', wordBreak: 'break-all' }}
                >
                  {item.fix_command}
                </Typography>
                <Button
                  size="small"
                  startIcon={copiedId === item.id ? <CheckIcon fontSize="small" /> : <ContentCopyIcon fontSize="small" />}
                  onClick={() => handleCopyFix(item.id, item.fix_command!)}
                  sx={{ ml: 1, flexShrink: 0, fontSize: '0.75rem', py: 0.2 }}
                >
                  {copiedId === item.id ? 'Copied' : 'Copy Fix'}
                </Button>
              </Box>
            )}
          </ListItem>
        ))}
      </List>
    </Paper>
  );
};
