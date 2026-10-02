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

import React, { useState } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  CircularProgress,
  Alert,
  Chip,
  IconButton,
  Tooltip,
} from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import RefreshIcon from '@mui/icons-material/Refresh';
import SecurityIcon from '@mui/icons-material/Security';
import { PreflightCheckItem, PreflightCheckResponse } from '../types/benchmark';

interface PreflightChecklistProps {
  preflightResult: PreflightCheckResponse | null;
  loading: boolean;
  onRunChecks: () => void;
  executionMode: string;
}

export const PreflightChecklist: React.FC<PreflightChecklistProps> = ({
  preflightResult,
  loading,
  onRunChecks,
  executionMode,
}) => {
  const [copiedId, setCopiedId] = useState<string | null>(null);

  const handleCopyCommand = (id: string, command: string) => {
    navigator.clipboard.writeText(command);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 2500);
  };

  const isDryRun = executionMode === 'dry_run';

  return (
    <Paper
      elevation={0}
      sx={{
        p: 2,
        border: '1px solid',
        borderColor: 'divider',
        borderRadius: 2,
        backgroundColor: '#fafbfc',
        mt: 2,
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <SecurityIcon color="primary" fontSize="small" />
          <Typography variant="subtitle2" sx={{ fontWeight: 600, color: 'text.primary' }}>
            Pre-flight Permissions Checklist
          </Typography>
        </Box>
        <Button
          size="small"
          variant="outlined"
          startIcon={loading ? <CircularProgress size={14} /> : <RefreshIcon fontSize="small" />}
          disabled={loading}
          onClick={onRunChecks}
          sx={{ textTransform: 'none', fontSize: '0.78rem' }}
        >
          {loading ? 'Checking...' : 'Run Checks'}
        </Button>
      </Box>

      {/* Principal / Identity Badge */}
      {preflightResult?.principal && (
        <Box sx={{ mb: 1.5 }}>
          <Chip
            size="small"
            label={`Caller: ${preflightResult.principal} ${isDryRun ? '(Dry-Run Mode)' : '(ADC)'}`}
            variant="outlined"
            color={isDryRun ? 'default' : 'primary'}
            sx={{ fontSize: '0.75rem' }}
          />
        </Box>
      )}

      {/* No checks run yet prompt */}
      {!preflightResult && !loading && (
        <Alert severity="info" sx={{ fontSize: '0.8rem', py: 0.5 }}>
          Run pre-flight checks to verify authentication, project access, APIs, and IAM permissions.
        </Alert>
      )}

      {/* Loading state */}
      {loading && !preflightResult && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, py: 2, justifyContent: 'center' }}>
          <CircularProgress size={20} />
          <Typography variant="body2" color="text.secondary">
            Verifying Google Cloud credentials & permissions...
          </Typography>
        </Box>
      )}

      {/* Checklist items */}
      {preflightResult && (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.2, mt: 1 }}>
          {preflightResult.checks.map((check: PreflightCheckItem) => {
            const isPassed = check.status === 'passed';
            const isFailed = check.status === 'failed';
            const isWarning = check.status === 'warning';

            return (
              <Box
                key={check.id}
                sx={{
                  p: 1.2,
                  borderRadius: 1.5,
                  backgroundColor: '#ffffff',
                  border: '1px solid',
                  borderColor: isPassed ? '#e2e8f0' : isFailed ? '#fed7d7' : '#feebc8',
                }}
              >
                <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1 }}>
                  {isPassed && <CheckCircleIcon sx={{ color: '#2e7d32', fontSize: 18, mt: 0.2 }} />}
                  {isFailed && <CancelIcon sx={{ color: '#d32f2f', fontSize: 18, mt: 0.2 }} />}
                  {isWarning && <WarningAmberIcon sx={{ color: '#ed6c02', fontSize: 18, mt: 0.2 }} />}
                  {check.status === 'checking' && <CircularProgress size={16} sx={{ mt: 0.2 }} />}

                  <Box sx={{ flexGrow: 1 }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.82rem' }}>
                      {check.name}
                    </Typography>
                    <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mt: 0.3 }}>
                      {check.message}
                    </Typography>

                    {/* Remediation command if failed */}
                    {check.remediation_command && (
                      <Box
                        sx={{
                          mt: 1,
                          p: 1,
                          backgroundColor: '#f7fafc',
                          borderRadius: 1,
                          border: '1px dashed #cbd5e0',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'space-between',
                          gap: 1,
                        }}
                      >
                        <Typography
                          variant="caption"
                          component="code"
                          sx={{
                            fontFamily: 'monospace',
                            fontSize: '0.73rem',
                            color: '#2d3748',
                            wordBreak: 'break-all',
                          }}
                        >
                          {check.remediation_command}
                        </Typography>
                        <Tooltip title={copiedId === check.id ? 'Copied!' : 'Copy gcloud command'}>
                          <IconButton
                            size="small"
                            onClick={() => handleCopyCommand(check.id, check.remediation_command!)}
                            sx={{ p: 0.5 }}
                          >
                            {copiedId === check.id ? (
                              <CheckIcon sx={{ fontSize: 14, color: 'success.main' }} />
                            ) : (
                              <ContentCopyIcon sx={{ fontSize: 14 }} />
                            )}
                          </IconButton>
                        </Tooltip>
                      </Box>
                    )}
                  </Box>
                </Box>
              </Box>
            );
          })}

          {/* Overall summary alert */}
          <Alert
            severity={preflightResult.all_passed ? 'success' : 'error'}
            sx={{ mt: 1, py: 0.5, fontSize: '0.8rem' }}
          >
            {preflightResult.summary}
          </Alert>
        </Box>
      )}
    </Paper>
  );
};
