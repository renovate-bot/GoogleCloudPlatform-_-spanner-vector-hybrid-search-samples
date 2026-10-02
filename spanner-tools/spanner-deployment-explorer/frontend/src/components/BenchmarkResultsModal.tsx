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

import React, { useState, useEffect, useRef } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Chip,
  Stack,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
  LinearProgress,
  IconButton,
  Tabs,
  Tab,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  TextField,
  InputAdornment,
  FormControlLabel,
  Checkbox,
  Tooltip,
  Alert,
  CircularProgress,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import SpeedIcon from '@mui/icons-material/Speed';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import DownloadIcon from '@mui/icons-material/Download';
import TerminalIcon from '@mui/icons-material/Terminal';
import FormatListNumberedIcon from '@mui/icons-material/FormatListNumbered';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import SearchIcon from '@mui/icons-material/Search';
import DoneIcon from '@mui/icons-material/Done';

import { BenchmarkCampaignStatus, BenchmarkStep, BenchmarkLogEntry, RegionBenchmarkResult } from '../types/benchmark';

export type SortField =
  | 'region'
  | 'writes_p50'
  | 'writes_p95'
  | 'writes_p99'
  | 'strong_reads_p50'
  | 'strong_reads_p95'
  | 'strong_reads_p99'
  | 'stale_reads_p50'
  | 'stale_reads_p95'
  | 'stale_reads_p99';

export type SortOrder = 'asc' | 'desc';

interface BenchmarkResultsModalProps {
  open: boolean;
  onClose: () => void;
  campaign: BenchmarkCampaignStatus | null;
  onRun?: (campaignId: string) => void;
  onStop?: (campaignId: string) => void;
  onCleanup?: (campaignId: string) => void;
  isCleaningUp?: boolean;
}

const renderReplicaRoleChip = (replicaType?: string | null) => {
  if (!replicaType) return null;
  const roleStyles: Record<string, { bg: string; text: string; border: string }> = {
    'Leader Region': { bg: '#fef2f2', text: '#dc2626', border: '#fca5a5' },
    'R/W Replica Region': { bg: '#fffbeb', text: '#d97706', border: '#fde68a' },
    'Witness Region': { bg: '#f1f5f9', text: '#64748b', border: '#cbd5e1' },
    'Read Only Region': { bg: '#eff6ff', text: '#2563eb', border: '#bfdbfe' },
  };
  const style = roleStyles[replicaType] || { bg: '#f1f5f9', text: '#475569', border: '#cbd5e1' };

  return (
    <Chip
      label={replicaType}
      size="small"
      variant="outlined"
      sx={{
        fontSize: '0.65rem',
        height: 18,
        fontWeight: 600,
        backgroundColor: style.bg,
        color: style.text,
        borderColor: style.border,
        borderRadius: '4px',
      }}
    />
  );
};

export const BenchmarkResultsModal: React.FC<BenchmarkResultsModalProps> = ({
  open,
  onClose,
  campaign,
  onRun,
  onStop,
  onCleanup,
  isCleaningUp = false,
}) => {
  const [activeTab, setActiveTab] = useState<number>(0);
  const [logFilter, setLogFilter] = useState<string>('ALL');
  const [logSearch, setLogSearch] = useState<string>('');
  const [autoScrollLogs, setAutoScrollLogs] = useState<boolean>(true);
  const [copied, setCopied] = useState<boolean>(false);
  const [sortField, setSortField] = useState<SortField>('writes_p50');
  const [sortOrder, setSortOrder] = useState<SortOrder>('asc');

  const handleRequestSort = (field: SortField) => {
    const isAsc = sortField === field && sortOrder === 'asc';
    setSortOrder(isAsc ? 'desc' : 'asc');
    setSortField(field);
  };

  const logsEndRef = useRef<HTMLDivElement | null>(null);

  // Automatically select the most relevant tab based on campaign state
  useEffect(() => {
    if (!campaign) return;
    if (campaign.status === 'completed') {
      setActiveTab(0); // Results & Comparison
    } else if (campaign.status === 'failed') {
      setActiveTab(1); // Execution Steps (show where it failed)
    } else if (
      campaign.status === 'preflight_check' ||
      campaign.status === 'provisioning_spanner' ||
      campaign.status === 'provisioning_vms' ||
      campaign.status === 'running'
    ) {
      // If currently on Results tab (which is empty/partial), switch to Execution Steps
      setActiveTab((prev) => (prev === 0 ? 1 : prev));
    }
  }, [campaign?.status]);

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (activeTab === 2 && autoScrollLogs && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [campaign?.logs, activeTab, autoScrollLogs]);

  if (!campaign) return null;

  const isRunning =
    campaign.status === 'preflight_check' ||
    campaign.status === 'provisioning_spanner' ||
    campaign.status === 'provisioning_vms' ||
    campaign.status === 'running';

  const isFailed = campaign.status === 'failed';

  const steps: BenchmarkStep[] = campaign.steps || [];
  const logs: BenchmarkLogEntry[] = campaign.logs || [];

  // Filtered logs
  const filteredLogs = logs.filter((entry) => {
    if (logFilter !== 'ALL' && entry.level !== logFilter) {
      return false;
    }
    if (logSearch.trim()) {
      const q = logSearch.toLowerCase();
      return (
        entry.message.toLowerCase().includes(q) ||
        entry.stage.toLowerCase().includes(q) ||
        entry.timestamp.toLowerCase().includes(q)
      );
    }
    return true;
  });

  const handleCopyLogs = () => {
    const text = logs
      .map((l) => `[${l.timestamp}] [${l.level}] [${l.stage}] ${l.message}`)
      .join('\n');
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const regionList = Object.values(campaign.region_results || {});

  const sortedRegionList = [...regionList].sort((a, b) => {
    if (sortField === 'region') {
      const nameA = (a.region_name || a.region).toLowerCase();
      const nameB = (b.region_name || b.region).toLowerCase();
      return sortOrder === 'asc' ? nameA.localeCompare(nameB) : nameB.localeCompare(nameA);
    }

    const getMetric = (r: RegionBenchmarkResult): number | undefined => {
      if (sortField.startsWith('writes_')) {
        const metric = sortField.replace('writes_', '') as 'p50' | 'p95' | 'p99';
        if (r.status === 'completed' && r.writes && r.writes.ops_count > 0) return r.writes[metric];
      } else if (sortField.startsWith('strong_reads_')) {
        const metric = sortField.replace('strong_reads_', '') as 'p50' | 'p95' | 'p99';
        if (r.status === 'completed' && r.strong_reads && r.strong_reads.ops_count > 0) return r.strong_reads[metric];
      } else if (sortField.startsWith('stale_reads_')) {
        const metric = sortField.replace('stale_reads_', '') as 'p50' | 'p95' | 'p99';
        if (r.status === 'completed' && r.stale_reads && r.stale_reads.ops_count > 0) return r.stale_reads[metric];
      }
      return undefined;
    };

    const valA = getMetric(a);
    const valB = getMetric(b);

    if (valA === undefined && valB === undefined) return 0;
    if (valA === undefined) return 1;
    if (valB === undefined) return -1;

    return sortOrder === 'asc' ? valA - valB : valB - valA;
  });

  const handleExportJson = () => {
    const dataStr =
      'data:text/json;charset=utf-8,' +
      encodeURIComponent(JSON.stringify(campaign, null, 2));
    const downloadAnchor = document.createElement('a');
    downloadAnchor.setAttribute('href', dataStr);
    downloadAnchor.setAttribute('download', `${campaign.campaign_id}-results.json`);
    document.body.appendChild(downloadAnchor);
    downloadAnchor.click();
    downloadAnchor.remove();
  };

  const getStatusChip = () => {
    switch (campaign.status) {
      case 'completed':
        return <Chip label="Completed" color="success" size="small" sx={{ fontWeight: 700 }} />;
      case 'running':
      case 'provisioning_spanner':
      case 'provisioning_vms':
      case 'preflight_check':
        return (
          <Chip
            label={campaign.message || 'Running'}
            color="primary"
            size="small"
            sx={{ fontWeight: 700 }}
          />
        );
      case 'stopped':
        return <Chip label="Stopped" color="warning" size="small" sx={{ fontWeight: 700 }} />;
      case 'failed':
        return <Chip label="Failed" color="error" size="small" sx={{ fontWeight: 700 }} />;
      default:
        return <Chip label="Draft" size="small" sx={{ fontWeight: 700 }} />;
    }
  };

  const getStepIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircleIcon sx={{ color: '#16a34a', fontSize: 20 }} />;
      case 'running':
        return <CircularProgress size={18} sx={{ color: '#7c3aed' }} />;
      case 'failed':
        return <ErrorIcon sx={{ color: '#dc2626', fontSize: 20 }} />;
      case 'skipped':
        return (
          <Box
            sx={{
              width: 18,
              height: 18,
              borderRadius: '50%',
              bgcolor: '#e2e8f0',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: 10,
              color: '#64748b',
            }}
          >
            -
          </Box>
        );
      default:
        return (
          <Box
            sx={{
              width: 18,
              height: 18,
              borderRadius: '50%',
              border: '2px solid #cbd5e1',
            }}
          />
        );
    }
  };

  const completedStepsCount = steps.filter((s) => s.status === 'completed').length;

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="lg"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 2,
          display: 'flex',
          flexDirection: 'column',
          maxHeight: '90vh',
        },
      }}
    >
      <DialogTitle sx={{ pb: 1, borderBottom: '1px solid #e2e8f0' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Stack direction="row" spacing={1.5} alignItems="center">
            <SpeedIcon sx={{ color: '#7c3aed', fontSize: 28 }} />
            <Box>
              <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
                <Typography variant="h6" sx={{ fontWeight: 700 }}>
                  {campaign.name || `Benchmark ${campaign.campaign_id}`}
                </Typography>
                {getStatusChip()}
                <Chip
                  label={campaign.execution_mode === 'dry_run' ? 'MODELED ESTIMATES (Dry Run)' : 'MEASURED TELEMETRY (Live)'}
                  size="small"
                  variant="outlined"
                  color={campaign.execution_mode === 'dry_run' ? 'default' : 'success'}
                  sx={{ fontSize: '0.72rem', fontWeight: 700, letterSpacing: '0.02em' }}
                />
              </Stack>
              <Typography variant="caption" color="text.secondary">
                Target: <b>{campaign.spanner_config}</b> &bull; Project: <b>{campaign.project_id}</b> &bull; Leader: <b>{campaign.leader_region || 'Auto'}</b>
              </Typography>
            </Box>
          </Stack>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>

      {/* Navigation Tabs */}
      <Box sx={{ borderBottom: 1, borderColor: 'divider', px: 2, bgcolor: '#fafafa' }}>
        <Tabs
          value={activeTab}
          onChange={(_e, val) => setActiveTab(val)}
          textColor="primary"
          indicatorColor="primary"
          sx={{
            minHeight: 44,
            '& .MuiTab-root': {
              minHeight: 44,
              py: 0.5,
              textTransform: 'none',
              fontWeight: 600,
              fontSize: '0.85rem',
            },
          }}
        >
          <Tab
            label="Results & Comparison"
            icon={<SpeedIcon sx={{ fontSize: 18 }} />}
            iconPosition="start"
          />
          <Tab
            label={`Execution Steps (${completedStepsCount}/${steps.length || 7})`}
            icon={<FormatListNumberedIcon sx={{ fontSize: 18 }} />}
            iconPosition="start"
          />
          <Tab
            label={`Execution Logs (${logs.length})`}
            icon={<TerminalIcon sx={{ fontSize: 18 }} />}
            iconPosition="start"
          />
        </Tabs>
      </Box>

      <DialogContent dividers sx={{ p: 2.5, flexGrow: 1, overflowY: 'auto' }}>
        {/* Active Progress Bar across all views when running */}
        {isRunning && (
          <Box sx={{ mb: 2.5, p: 1.5, backgroundColor: '#f5f3ff', borderRadius: 2, border: '1px solid #ddd6fe' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.75 }}>
              <Typography variant="body2" sx={{ fontWeight: 600, color: '#5b21b6' }}>
                {campaign.message}
              </Typography>
              <Typography variant="caption" sx={{ fontWeight: 700, color: '#5b21b6' }}>
                {campaign.progress_pct}%
              </Typography>
            </Box>
            <LinearProgress
              variant="determinate"
              value={campaign.progress_pct}
              sx={{
                height: 8,
                borderRadius: 4,
                backgroundColor: '#e9d5ff',
                '& .MuiLinearProgress-bar': { backgroundColor: '#7c3aed' },
              }}
            />
          </Box>
        )}

        {/* Global Error Banner */}
        {isFailed && (
          <Alert
            severity="error"
            action={
              activeTab !== 1 ? (
                <Button color="inherit" size="small" onClick={() => setActiveTab(1)}>
                  View Failed Step
                </Button>
              ) : undefined
            }
            sx={{ mb: 2.5 }}
          >
            <strong>Benchmark Run Failed:</strong> {campaign.message || 'An error occurred during execution.'}
          </Alert>
        )}

        {/* TAB 0: Results & Comparison */}
        {activeTab === 0 && (
          <Stack spacing={2.5}>
            {/* Full Comparison Matrix Table */}
            <Box>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                <Typography variant="subtitle1" sx={{ fontWeight: 700, color: '#1e293b' }}>
                  Multi-Region Latency Comparison Matrix
                </Typography>
                <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                  Click column headers to sort
                </Typography>
              </Box>

              <TableContainer component={Paper} elevation={0} sx={{ border: '1px solid #e2e8f0', borderRadius: 2 }}>
                <Table size="small">
                  <TableHead sx={{ backgroundColor: '#f8fafc' }}>
                    <TableRow>
                      <TableCell
                        rowSpan={2}
                        sx={{
                          fontWeight: 700,
                          verticalAlign: 'bottom',
                          borderRight: '1px solid #e2e8f0',
                          pb: 1.5,
                        }}
                      >
                        <TableSortLabel
                          active={sortField === 'region'}
                          direction={sortField === 'region' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('region')}
                          sx={{ fontWeight: 700, color: '#1e293b' }}
                        >
                          Client Region
                        </TableSortLabel>
                      </TableCell>
                      <TableCell
                        colSpan={3}
                        align="center"
                        sx={{
                          fontWeight: 700,
                          color: '#1e293b',
                          borderRight: '1px solid #e2e8f0',
                          pb: 0.75,
                          borderBottom: '1px solid #e2e8f0',
                          cursor: 'pointer',
                          userSelect: 'none',
                          '&:hover': { backgroundColor: '#f1f5f9' },
                        }}
                        onClick={() => handleRequestSort('writes_p50')}
                        title="Click to sort by Writes p50"
                      >
                        Writes
                      </TableCell>
                      <TableCell
                        colSpan={3}
                        align="center"
                        sx={{
                          fontWeight: 700,
                          color: '#1e293b',
                          borderRight: '1px solid #e2e8f0',
                          pb: 0.75,
                          borderBottom: '1px solid #e2e8f0',
                          cursor: 'pointer',
                          userSelect: 'none',
                          '&:hover': { backgroundColor: '#f1f5f9' },
                        }}
                        onClick={() => handleRequestSort('strong_reads_p50')}
                        title="Click to sort by Strong Reads p50"
                      >
                        Strong Reads
                      </TableCell>
                      <TableCell
                        colSpan={3}
                        align="center"
                        sx={{
                          fontWeight: 700,
                          color: '#1e293b',
                          pb: 0.75,
                          borderBottom: '1px solid #e2e8f0',
                          cursor: 'pointer',
                          userSelect: 'none',
                          '&:hover': { backgroundColor: '#f1f5f9' },
                        }}
                        onClick={() => handleRequestSort('stale_reads_p50')}
                        title="Click to sort by Stale Reads p50"
                      >
                        Stale Reads (15s)
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      {/* Writes: p50, p95, p99 */}
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'writes_p50'}
                          direction={sortField === 'writes_p50' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('writes_p50')}
                        >
                          p50
                        </TableSortLabel>
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'writes_p95'}
                          direction={sortField === 'writes_p95' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('writes_p95')}
                        >
                          p95
                        </TableSortLabel>
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, borderRight: '1px solid #e2e8f0' }}>
                        <TableSortLabel
                          active={sortField === 'writes_p99'}
                          direction={sortField === 'writes_p99' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('writes_p99')}
                        >
                          p99
                        </TableSortLabel>
                      </TableCell>

                      {/* Strong Reads: p50, p95, p99 */}
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'strong_reads_p50'}
                          direction={sortField === 'strong_reads_p50' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('strong_reads_p50')}
                        >
                          p50
                        </TableSortLabel>
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'strong_reads_p95'}
                          direction={sortField === 'strong_reads_p95' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('strong_reads_p95')}
                        >
                          p95
                        </TableSortLabel>
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75, borderRight: '1px solid #e2e8f0' }}>
                        <TableSortLabel
                          active={sortField === 'strong_reads_p99'}
                          direction={sortField === 'strong_reads_p99' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('strong_reads_p99')}
                        >
                          p99
                        </TableSortLabel>
                      </TableCell>

                      {/* Stale Reads: p50, p95, p99 */}
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'stale_reads_p50'}
                          direction={sortField === 'stale_reads_p50' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('stale_reads_p50')}
                        >
                          p50
                        </TableSortLabel>
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'stale_reads_p95'}
                          direction={sortField === 'stale_reads_p95' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('stale_reads_p95')}
                        >
                          p95
                        </TableSortLabel>
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.75rem', py: 0.75 }}>
                        <TableSortLabel
                          active={sortField === 'stale_reads_p99'}
                          direction={sortField === 'stale_reads_p99' ? sortOrder : 'asc'}
                          onClick={() => handleRequestSort('stale_reads_p99')}
                        >
                          p99
                        </TableSortLabel>
                      </TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {sortedRegionList.map((r) => {
                      const isCompleted = r.status === 'completed';
                      const isRunning = r.status === 'running';
                      const statusText = isRunning ? 'Testing...' : r.status === 'failed' ? 'Failed' : 'Pending';

                      return (
                        <TableRow key={r.region} hover>
                          <TableCell sx={{ borderRight: '1px solid #e2e8f0' }}>
                            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                              <Box>
                                <Typography variant="body2" sx={{ fontWeight: 600 }}>
                                  {r.region_name}
                                </Typography>
                                <Typography variant="caption" color="text.secondary" sx={{ fontFamily: 'monospace' }}>
                                  {r.region}
                                </Typography>
                              </Box>
                              {renderReplicaRoleChip(r.replica_type)}
                            </Box>
                          </TableCell>

                          {/* Writes: p50, p95, p99 */}
                          <TableCell align="right">
                            {isCompleted && r.writes && r.writes.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ fontWeight: 600, color: '#0f172a' }}>
                                {r.writes.p50} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">
                                {isCompleted ? 'N/A' : statusText}
                              </Typography>
                            )}
                          </TableCell>
                          <TableCell align="right">
                            {isCompleted && r.writes && r.writes.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ color: '#4b5563', fontWeight: 500 }}>
                                {r.writes.p95} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>
                          <TableCell align="right" sx={{ borderRight: '1px solid #e2e8f0' }}>
                            {isCompleted && r.writes && r.writes.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ color: '#4b5563', fontWeight: 500 }}>
                                {r.writes.p99} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>

                          {/* Strong Reads: p50, p95, p99 */}
                          <TableCell align="right">
                            {isCompleted && r.strong_reads && r.strong_reads.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ fontWeight: 600, color: '#0f172a' }}>
                                {r.strong_reads.p50} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">
                                {isCompleted ? 'N/A' : statusText}
                              </Typography>
                            )}
                          </TableCell>
                          <TableCell align="right">
                            {isCompleted && r.strong_reads && r.strong_reads.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ color: '#4b5563', fontWeight: 500 }}>
                                {r.strong_reads.p95} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>
                          <TableCell align="right" sx={{ borderRight: '1px solid #e2e8f0' }}>
                            {isCompleted && r.strong_reads && r.strong_reads.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ color: '#4b5563', fontWeight: 500 }}>
                                {r.strong_reads.p99} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>

                          {/* Stale Reads: p50, p95, p99 */}
                          <TableCell align="right">
                            {isCompleted && r.stale_reads && r.stale_reads.ops_count > 0 ? (
                              <Typography
                                variant="body2"
                                sx={{
                                  fontWeight: 600,
                                  color: '#0f172a',
                                }}
                              >
                                {r.stale_reads.p50} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">
                                {isCompleted ? 'N/A' : statusText}
                              </Typography>
                            )}
                          </TableCell>
                          <TableCell align="right">
                            {isCompleted && r.stale_reads && r.stale_reads.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ color: '#4b5563', fontWeight: 500 }}>
                                {r.stale_reads.p95} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>
                          <TableCell align="right">
                            {isCompleted && r.stale_reads && r.stale_reads.ops_count > 0 ? (
                              <Typography variant="body2" sx={{ color: '#4b5563', fontWeight: 500 }}>
                                {r.stale_reads.p99} ms
                              </Typography>
                            ) : (
                              <Typography variant="caption" color="text.secondary">-</Typography>
                            )}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
            </Box>
          </Stack>
        )}

        {/* TAB 1: Execution Steps */}
        {activeTab === 1 && (
          <Box sx={{ maxWidth: 900, mx: 'auto', py: 1 }}>
            <Box sx={{ mb: 2 }}>
              <Typography variant="subtitle1" sx={{ fontWeight: 700, color: '#1e293b' }}>
                Benchmark Lifecycle Stepper
              </Typography>
            </Box>

            {steps.length === 0 ? (
              <Alert severity="info">
                No lifecycle step data recorded yet. Steps will appear as soon as the benchmark execution begins.
              </Alert>
            ) : (
              <Stepper orientation="vertical" nonLinear>
                {steps.map((step, idx) => {
                  const isCurrent = step.status === 'running';
                  return (
                    <Step key={step.id || idx} active={isCurrent || step.status === 'failed'} completed={step.status === 'completed'}>
                      <StepLabel
                        icon={getStepIcon(step.status)}
                        error={step.status === 'failed'}
                        sx={{
                          '& .MuiStepLabel-label': {
                            fontWeight: isCurrent ? 700 : 600,
                            color: isCurrent ? '#7c3aed' : 'text.primary',
                            fontSize: '0.92rem',
                          },
                        }}
                      >
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
                          <span>{step.name}</span>
                          {step.duration_seconds !== undefined && step.duration_seconds !== null && (
                            <Chip
                              label={`${step.duration_seconds.toFixed(1)}s`}
                              size="small"
                              variant="outlined"
                              sx={{ height: 18, fontSize: '0.65rem' }}
                            />
                          )}
                          <Chip
                            label={step.status.toUpperCase()}
                            size="small"
                            color={
                              step.status === 'completed'
                                ? 'success'
                                : step.status === 'failed'
                                ? 'error'
                                : step.status === 'running'
                                ? 'primary'
                                : 'default'
                            }
                            sx={{ height: 18, fontSize: '0.62rem', fontWeight: 700 }}
                          />
                        </Box>
                      </StepLabel>

                      <StepContent>
                        <Box sx={{ mb: 1.5, pl: 0.5 }}>
                          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                            {step.id === 'execute_workload'
                              ? 'Running load test'
                              : step.id === 'setup_clients'
                              ? 'Provision load test runner across client locations'
                              : step.description}
                          </Typography>

                          {step.id === 'setup_clients' && campaign.gce_instances && Object.keys(campaign.gce_instances).length > 0 && (
                            <Box sx={{ mt: 1, mb: 1, p: 1, bgcolor: 'background.default', borderRadius: 1, border: '1px solid', borderColor: 'divider' }}>
                              <Typography variant="caption" sx={{ fontWeight: 700, display: 'block', mb: 0.5, color: 'text.secondary' }}>
                                PROVISIONED GCE RUNNER VMS (e2-standard-2):
                              </Typography>
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.8 }}>
                                {Object.entries(campaign.gce_instances).map(([reg, entry]) => {
                                  const parts = entry.split(':');
                                  return (
                                    <Chip
                                      key={reg}
                                      label={`${reg}: ${parts[0]} (${parts[1] || 'zone'})`}
                                      size="small"
                                      variant="outlined"
                                      sx={{ fontFamily: 'monospace', fontSize: '0.72rem' }}
                                    />
                                  );
                                })}
                              </Box>
                            </Box>
                          )}

                          {step.id === 'execute_workload' && campaign.region_results && Object.keys(campaign.region_results).length > 0 && (
                            <Box sx={{ mt: 1, mb: 1, p: 1, bgcolor: 'background.default', borderRadius: 1, border: '1px solid', borderColor: 'divider' }}>
                              <Typography variant="caption" sx={{ fontWeight: 700, display: 'block', mb: 0.5, color: 'text.secondary' }}>
                                REGIONAL WORKLOAD EXECUTION STATUS:
                              </Typography>
                              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.8 }}>
                                {Object.entries(campaign.region_results).map(([reg, r]) => (
                                  <Chip
                                    key={reg}
                                    label={`${reg}: ${r.status.toUpperCase()}${r.status === 'completed' && r.writes ? ` (p50: ${r.writes.p50}ms)` : ''}`}
                                    size="small"
                                    color={r.status === 'completed' ? 'success' : r.status === 'running' ? 'primary' : r.status === 'failed' ? 'error' : 'default'}
                                    sx={{ fontSize: '0.72rem', fontWeight: 600 }}
                                  />
                                ))}
                              </Box>
                            </Box>
                          )}


                          {step.error_message && (
                            <Alert severity="error" sx={{ mt: 1, mb: 1 }}>
                              <Typography variant="body2" sx={{ fontWeight: 600, mb: 0.5 }}>
                                Step Failure Details:
                              </Typography>
                              <Typography variant="body2" sx={{ fontFamily: 'monospace', whiteSpace: 'pre-wrap', fontSize: '0.8rem' }}>
                                {step.error_message}
                              </Typography>
                            </Alert>
                          )}
                        </Box>
                      </StepContent>
                    </Step>
                  );
                })}
              </Stepper>
            )}
          </Box>
        )}

        {/* TAB 2: Execution Logs */}
        {activeTab === 2 && (
          <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
            {/* Log Controls Bar */}
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                flexWrap: 'wrap',
                gap: 1.5,
                mb: 1.5,
              }}
            >
              <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
                {['ALL', 'INFO', 'SUCCESS', 'WARN', 'ERROR'].map((lvl) => (
                  <Chip
                    key={lvl}
                    label={lvl}
                    size="small"
                    onClick={() => setLogFilter(lvl)}
                    color={logFilter === lvl ? 'primary' : 'default'}
                    variant={logFilter === lvl ? 'filled' : 'outlined'}
                    sx={{ fontWeight: 600, fontSize: '0.72rem', cursor: 'pointer' }}
                  />
                ))}
              </Stack>

              <Stack direction="row" spacing={1.5} alignItems="center">
                <TextField
                  size="small"
                  placeholder="Filter logs..."
                  value={logSearch}
                  onChange={(e) => setLogSearch(e.target.value)}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <SearchIcon sx={{ fontSize: 16, color: 'text.secondary' }} />
                      </InputAdornment>
                    ),
                  }}
                  sx={{ width: 180, '& .MuiInputBase-input': { fontSize: '0.78rem', py: 0.5 } }}
                />

                <FormControlLabel
                  control={
                    <Checkbox
                      size="small"
                      checked={autoScrollLogs}
                      onChange={(e) => setAutoScrollLogs(e.target.checked)}
                    />
                  }
                  label={<Typography variant="caption">Auto-scroll</Typography>}
                  sx={{ mr: 0 }}
                />

                <Tooltip title={copied ? 'Copied to clipboard!' : 'Copy all logs'}>
                  <Button
                    size="small"
                    variant="outlined"
                    startIcon={copied ? <DoneIcon fontSize="small" /> : <ContentCopyIcon fontSize="small" />}
                    onClick={handleCopyLogs}
                    sx={{ textTransform: 'none', fontSize: '0.75rem', py: 0.25 }}
                  >
                    {copied ? 'Copied' : 'Copy'}
                  </Button>
                </Tooltip>
              </Stack>
            </Box>

            {/* Dark Terminal Window */}
            <Paper
              elevation={0}
              sx={{
                flexGrow: 1,
                minHeight: 380,
                maxHeight: 520,
                backgroundColor: '#18181b',
                color: '#e4e4e7',
                p: 2,
                borderRadius: 2,
                overflowY: 'auto',
                fontFamily: '"JetBrains Mono", "Fira Code", "Roboto Mono", Consolas, monospace',
                fontSize: '0.78rem',
                lineHeight: 1.6,
                border: '1px solid #27272a',
              }}
            >
              {filteredLogs.length === 0 ? (
                <Typography variant="body2" sx={{ color: '#71717a', fontStyle: 'italic' }}>
                  {logs.length === 0
                    ? 'No logs recorded yet. Execution logs will stream here during execution...'
                    : 'No logs match the current search / level filters.'}
                </Typography>
              ) : (
                filteredLogs.map((entry, i) => {
                  let levelColor = '#38bdf8'; // INFO: sky blue
                  if (entry.level === 'SUCCESS') levelColor = '#4ade80'; // bright green
                  if (entry.level === 'WARN') levelColor = '#facc15'; // yellow
                  if (entry.level === 'ERROR') levelColor = '#f87171'; // red

                  return (
                    <Box key={i} sx={{ display: 'flex', gap: 1, alignItems: 'flex-start', mb: 0.4 }}>
                      <Typography
                        component="span"
                        sx={{
                          color: '#71717a',
                          fontFamily: 'inherit',
                          fontSize: 'inherit',
                          flexShrink: 0,
                          userSelect: 'none',
                        }}
                      >
                        {entry.timestamp}
                      </Typography>
                      <Typography
                        component="span"
                        sx={{
                          color: levelColor,
                          fontWeight: 700,
                          fontFamily: 'inherit',
                          fontSize: 'inherit',
                          flexShrink: 0,
                          minWidth: 55,
                        }}
                      >
                        [{entry.level}]
                      </Typography>
                      <Typography
                        component="span"
                        sx={{
                          color: '#a1a1aa',
                          fontFamily: 'inherit',
                          fontSize: 'inherit',
                          flexShrink: 0,
                        }}
                      >
                        [{entry.stage}]
                      </Typography>
                      <Typography
                        component="span"
                        sx={{
                          color: entry.level === 'ERROR' ? '#fca5a5' : '#f4f4f5',
                          fontFamily: 'inherit',
                          fontSize: 'inherit',
                          wordBreak: 'break-word',
                          whiteSpace: 'pre-wrap',
                        }}
                      >
                        {entry.message}
                      </Typography>
                    </Box>
                  );
                })
              )}
              <div ref={logsEndRef} />
            </Paper>
          </Box>
        )}
      </DialogContent>

      <DialogActions sx={{ p: 2, justifyContent: 'space-between', borderTop: '1px solid #e2e8f0' }}>
        <Button
          startIcon={<DownloadIcon />}
          onClick={handleExportJson}
          size="small"
          sx={{ textTransform: 'none' }}
        >
          Export JSON
        </Button>

        <Stack direction="row" spacing={1.5} alignItems="center">
          {isRunning && onStop && (
            <Button
              variant="outlined"
              color="warning"
              startIcon={<StopIcon />}
              onClick={() => onStop(campaign.campaign_id)}
              sx={{ textTransform: 'none' }}
            >
              Stop Benchmark
            </Button>
          )}

          {!isRunning && onRun && (
            <Button
              variant="contained"
              startIcon={<PlayArrowIcon />}
              onClick={() => onRun(campaign.campaign_id)}
              sx={{
                textTransform: 'none',
                backgroundColor: '#7c3aed',
                '&:hover': { backgroundColor: '#6d28d9' },
              }}
            >
              {isFailed ? 'Retry Benchmark' : 'Re-Run Benchmark'}
            </Button>
          )}

          {onCleanup && campaign.spanner_instance_id && (
            <Button
              variant="outlined"
              color="error"
              disabled={isCleaningUp || campaign.status === 'deleting'}
              startIcon={
                isCleaningUp || campaign.status === 'deleting' ? (
                  <CircularProgress size={16} color="error" />
                ) : (
                  <DeleteSweepIcon />
                )
              }
              onClick={() => onCleanup(campaign.campaign_id)}
              sx={{ textTransform: 'none' }}
            >
              {isCleaningUp || campaign.status === 'deleting'
                ? 'Cleaning Up Resources...'
                : 'Clean Up Cloud Resources'}
            </Button>
          )}

          <Button onClick={onClose} variant="outlined" sx={{ textTransform: 'none' }}>
            Close
          </Button>
        </Stack>
      </DialogActions>
    </Dialog>
  );
};
export default BenchmarkResultsModal;
