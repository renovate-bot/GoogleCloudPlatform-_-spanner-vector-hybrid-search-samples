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
  Drawer,
  Box,
  Typography,
  IconButton,
  TextField,
  Button,
  Chip,
  Alert,
  CircularProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  LinearProgress,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  OutlinedInput,
  SelectChangeEvent,
  Tooltip,
  Stack,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import DeleteForeverIcon from '@mui/icons-material/DeleteForever';
import SpeedIcon from '@mui/icons-material/Speed';
import DnsIcon from '@mui/icons-material/Dns';
import LanIcon from '@mui/icons-material/Lan';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';

import { SpannerConfig } from '../types/spanner';
import {
  BenchmarkCampaignStatus,
  ClientRegionInfo,
  PreflightCheckResponse,
  RegionBenchmarkResult,
} from '../types/benchmark';
import {
  cleanupCampaignResources,
  fetchCampaignStatus,
  fetchClientRegions,
  runPreflightCheck,
  startBenchmarkCampaign,
  stopBenchmarkCampaign,
} from '../services/api';
import { useAppConfig } from '../context/ConfigContext';
import { PreflightChecklist } from './PreflightChecklist';

interface BenchmarkSidePaneProps {
  open: boolean;
  onClose: () => void;
  config: SpannerConfig | null;
  selectedClientRegions: string[];
  onClientRegionsChange: (regions: string[]) => void;
  allConfigs?: SpannerConfig[];
  onSelectConfig?: (configname: string) => void;
}

export const BenchmarkSidePane: React.FC<BenchmarkSidePaneProps> = ({
  open,
  onClose,
  config,
  selectedClientRegions,
  onClientRegionsChange,
  allConfigs,
  onSelectConfig,
}) => {
  const { features, load_testing } = useAppConfig();

  const [projectId, setProjectId] = useState<string>('spanner-loadtest-demo');
  const [executionMode, setExecutionMode] = useState<string>(
    features.allow_dry_run ? 'dry_run' : 'live'
  );

  useEffect(() => {
    if (!features.allow_dry_run && executionMode === 'dry_run') {
      setExecutionMode('live');
    }
  }, [features.allow_dry_run, executionMode]);
  const [operations, setOperations] = useState<number>(load_testing.default_operations || 1000);
  const [stalenessSeconds, setStalenessSeconds] = useState<number>(
    load_testing.default_staleness_seconds || 15
  );

  const [allClientRegions, setAllClientRegions] = useState<ClientRegionInfo[]>([]);
  const [loadingClientRegions, setLoadingClientRegions] = useState<boolean>(true);
  const [preflightResult, setPreflightResult] = useState<PreflightCheckResponse | null>(null);
  const [preflightLoading, setPreflightLoading] = useState<boolean>(false);

  const [campaign, setCampaign] = useState<BenchmarkCampaignStatus | null>(null);
  const [starting, setStarting] = useState<boolean>(false);
  const [stopping, setStopping] = useState<boolean>(false);
  const [cleaning, setCleaning] = useState<boolean>(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState<boolean>(false);
  const [sideSortField, setSideSortField] = useState<'region' | 'dist' | 'replica' | 'writes_p50' | 'strong_reads_p50' | 'stale_reads_p50'>('writes_p50');
  const [sideSortOrder, setSideSortOrder] = useState<'asc' | 'desc'>('asc');

  const handleSideRequestSort = (field: 'region' | 'dist' | 'replica' | 'writes_p50' | 'strong_reads_p50' | 'stale_reads_p50') => {
    const isAsc = sideSortField === field && sideSortOrder === 'asc';
    setSideSortOrder(isAsc ? 'desc' : 'asc');
    setSideSortField(field);
  };

  const pollIntervalRef = useRef<number | null>(null);

  // Load client regions catalog
  useEffect(() => {
    setLoadingClientRegions(true);
    fetchClientRegions()
      .then((regions) => {
        setAllClientRegions(regions);
      })
      .catch((err) => console.error('Failed to load client regions', err))
      .finally(() => setLoadingClientRegions(false));
  }, []);

  // Poll status when campaign is running
  useEffect(() => {
    if (campaign && (campaign.status === 'provisioning_spanner' || campaign.status === 'provisioning_vms' || campaign.status === 'running')) {
      if (!pollIntervalRef.current) {
        pollIntervalRef.current = window.setInterval(async () => {
          try {
            const updated = await fetchCampaignStatus(campaign.campaign_id);
            setCampaign(updated);
            if (updated.status === 'completed' || updated.status === 'failed' || updated.status === 'stopped') {
              if (pollIntervalRef.current) {
                clearInterval(pollIntervalRef.current);
                pollIntervalRef.current = null;
              }
            }
          } catch (err) {
            console.error('Failed to poll status', err);
          }
        }, 1500);
      }
    } else {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    }

    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    };
  }, [campaign?.status, campaign?.campaign_id]);

  // Execute pre-flight checks
  const handleRunPreflight = async () => {
    if (!config) return;
    setPreflightLoading(true);
    try {
      const res = await runPreflightCheck({
        project_id: projectId,
        client_regions: selectedClientRegions,
        spanner_config: config.configname,
        execution_mode: executionMode,
      });
      setPreflightResult(res);
    } catch (err: any) {
      console.error('Preflight check failed', err);
      setPreflightResult({
        all_passed: false,
        project_id: projectId,
        execution_mode: executionMode,
        checks: [
          {
            id: 'network_error',
            name: 'API Communication',
            scope: 'network',
            status: 'failed',
            message: `Could not reach backend pre-flight service: ${err.message}`,
          },
        ],
        summary: 'Pre-flight check execution failed.',
      });
    } finally {
      setPreflightLoading(false);
    }
  };

  // Start campaign
  const handleStartCampaign = async () => {
    if (!config) return;
    setStarting(true);
    try {
      const newCampaign = await startBenchmarkCampaign({
        project_id: projectId,
        spanner_config: config.configname,
        client_regions: selectedClientRegions,
        operations,
        staleness_seconds: stalenessSeconds,
        execution_mode: executionMode,
      });
      setCampaign(newCampaign);
    } catch (err: any) {
      console.error('Failed to start campaign', err);
      alert(`Failed to start benchmark: ${err.response?.data?.detail || err.message}`);
    } finally {
      setStarting(false);
    }
  };

  // Stop campaign
  const handleStopCampaign = async () => {
    if (!campaign) return;
    setStopping(true);
    try {
      await stopBenchmarkCampaign(campaign.campaign_id);
      const updated = await fetchCampaignStatus(campaign.campaign_id);
      setCampaign(updated);
    } catch (err) {
      console.error('Failed to stop campaign', err);
    } finally {
      setStopping(false);
    }
  };

  // Cleanup resources
  const handleConfirmCleanup = async () => {
    if (!campaign) return;
    setCleaning(true);
    setDeleteDialogOpen(false);
    try {
      await cleanupCampaignResources(campaign.campaign_id);
      setCampaign(null);
    } catch (err) {
      console.error('Failed to clean up resources', err);
    } finally {
      setCleaning(false);
    }
  };

  const handleRegionsChange = (event: SelectChangeEvent<string[]>) => {
    const value = event.target.value;
    const next = typeof value === 'string' ? value.split(',') : value;
    if (next.length > 0) {
      onClientRegionsChange(next);
    }
  };

  const isRunning =
    campaign?.status === 'provisioning_spanner' ||
    campaign?.status === 'provisioning_vms' ||
    campaign?.status === 'running';

  const canStart =
    Boolean(projectId.trim()) &&
    selectedClientRegions.length > 0 &&
    (executionMode === 'dry_run' || preflightResult?.all_passed);

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          width: { xs: '100%', sm: 680, md: 740 },
          p: 3,
          backgroundColor: '#f8fafc',
          boxSizing: 'border-box',
        },
      }}
    >
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <SpeedIcon color="primary" sx={{ fontSize: 28 }} />
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 700, lineHeight: 1.2 }}>
              Spanner Latency Benchmark
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Multi-Region Load Testing with Direct Access
            </Typography>
          </Box>
        </Box>
        <IconButton onClick={onClose} size="small">
          <CloseIcon />
        </IconButton>
      </Box>

      {/* Target Spanner Instance Info Card / Inline Picker */}
      {!config ? (
        <Paper
          elevation={0}
          sx={{
            p: 2.5,
            mb: 2.5,
            borderRadius: 2,
            border: '1.5px dashed #7c3aed',
            backgroundColor: '#faf5ff',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
            <DnsIcon sx={{ color: '#7c3aed', fontSize: 22 }} />
            <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#4c1d95' }}>
              Select a Spanner Configuration to Benchmark
            </Typography>
          </Box>
          <Typography variant="caption" sx={{ color: '#6d28d9', display: 'block', mb: 1.5 }}>
            No configuration currently selected. Pick an instance configuration below to begin testing:
          </Typography>
          <FormControl fullWidth size="small" sx={{ backgroundColor: '#ffffff', borderRadius: 1 }}>
            <InputLabel id="select-spanner-config-inline-label">Choose Spanner Config</InputLabel>
            <Select
              labelId="select-spanner-config-inline-label"
              label="Choose Spanner Config"
              value=""
              onChange={(e) => onSelectConfig && onSelectConfig(e.target.value as string)}
            >
              {(allConfigs || []).map((c) => (
                <MenuItem key={c.configname} value={c.configname}>
                  <strong>{c.configname}</strong>&nbsp;({c.instancetype} &bull; {c.continentregion})
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Paper>
      ) : (
        <Paper
          elevation={0}
          sx={{
            p: 2,
            mb: 2.5,
            borderRadius: 2,
            border: '1px solid',
            borderColor: 'primary.light',
            backgroundColor: '#f0f7ff',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <DnsIcon color="primary" fontSize="small" />
              <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#00297a' }}>
                Target Spanner Config: {config.configname}
              </Typography>
            </Box>
            <Stack direction="row" spacing={1} alignItems="center">
              <Chip
                size="small"
                label={`${config.instancetype.toUpperCase()}`}
                color="primary"
                variant="outlined"
                sx={{ fontWeight: 600, fontSize: '0.7rem' }}
              />
              {onSelectConfig && (
                <Button
                  size="small"
                  variant="text"
                  onClick={() => onSelectConfig('')}
                  sx={{ textTransform: 'none', fontSize: '0.75rem', p: 0, minWidth: 'auto', color: '#1a73e8' }}
                >
                  Change
                </Button>
              )}
            </Stack>
          </Box>
          <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mt: 0.8 }}>
            Leader: <b>{config.leader_region || 'Auto'}</b> | Replicas: <b>{config.replicas.length}</b> | Provisioned Nodes: <b>1</b> (Automatic backups disabled)
          </Typography>
        </Paper>
      )}

      {/* Configuration Form Card */}
      <Paper elevation={0} sx={{ p: 2.5, borderRadius: 2, border: '1px solid #e2e8f0', mb: 2.5 }}>
        <Typography variant="subtitle2" sx={{ fontWeight: 700, mb: 1.5, color: '#1a202c' }}>
          Benchmark Campaign Settings
        </Typography>

        {/* Execution Mode Toggle */}
        {features.allow_dry_run ? (
          <Box sx={{ mb: 2 }}>
            <Typography variant="caption" sx={{ fontWeight: 600, display: 'block', mb: 0.5 }}>
              Execution Mode:
            </Typography>
            <Box sx={{ display: 'flex', gap: 1 }}>
              <Button
                size="small"
                variant={executionMode === 'dry_run' ? 'contained' : 'outlined'}
                onClick={() => setExecutionMode('dry_run')}
                sx={{ textTransform: 'none', fontSize: '0.8rem', flex: 1 }}
              >
                Physics Simulator (Dry-Run)
              </Button>
              <Button
                size="small"
                variant={executionMode === 'live' ? 'contained' : 'outlined'}
                color="secondary"
                onClick={() => setExecutionMode('live')}
                sx={{ textTransform: 'none', fontSize: '0.8rem', flex: 1 }}
              >
                Live GCP Provisioning
              </Button>
            </Box>
          </Box>
        ) : (
          <Box sx={{ mb: 2 }}>
            <Typography variant="caption" sx={{ fontWeight: 600, display: 'block', mb: 0.5 }}>
              Execution Mode:
            </Typography>
            <Chip label="Live GCP Provisioning" color="secondary" size="small" variant="outlined" sx={{ fontWeight: 600 }} />
          </Box>
        )}

        {/* Project ID */}
        <TextField
          fullWidth
          size="small"
          label="GCP Project ID"
          value={projectId}
          onChange={(e) => setProjectId(e.target.value)}
          placeholder="my-gcp-project"
          disabled={isRunning}
          helperText="Target project to host 1-node Spanner instance and GCE client benchmark VMs"
          sx={{ mb: 2 }}
        />

        {/* Multi-Region Client Selector */}
        <FormControl fullWidth size="small" sx={{ mb: 1 }}>
          <InputLabel id="client-regions-label">Client Locations (GCE Regions)</InputLabel>
          <Select
            labelId="client-regions-label"
            multiple
            value={selectedClientRegions}
            onChange={handleRegionsChange}
            disabled={isRunning}
            input={<OutlinedInput label="Client Locations (GCE Regions)" />}
            renderValue={(selected) => {
              if (loadingClientRegions && allClientRegions.length === 0) {
                return (
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <CircularProgress size={14} />
                    <Typography variant="body2" color="text.secondary">Loading client regions...</Typography>
                  </Box>
                );
              }
              return (
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                  {selected.map((value) => {
                    const reg = allClientRegions.find((r) => r.region === value);
                    return (
                      <Chip
                        key={value}
                        size="small"
                        label={`${value} (${reg?.name || 'GCE'})`}
                        sx={{ fontSize: '0.75rem' }}
                      />
                    );
                  })}
                </Box>
              );
            }}
          >
            {loadingClientRegions && allClientRegions.length === 0 ? (
              <MenuItem disabled value="">
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, py: 0.5 }}>
                  <CircularProgress size={16} />
                  <Typography variant="body2" color="text.secondary">
                    Loading client regions...
                  </Typography>
                </Box>
              </MenuItem>
            ) : (
              allClientRegions.map((reg) => (
                <MenuItem key={reg.region} value={reg.region}>
                  {reg.region} — {reg.name} ({reg.continent})
                </MenuItem>
              ))
            )}
          </Select>
        </FormControl>

        {/* Operations & Staleness */}
        <Box sx={{ display: 'flex', gap: 2 }}>
          <TextField
            size="small"
            type="number"
            label="Operations per Workload"
            value={operations}
            onChange={(e) => setOperations(Math.max(10, parseInt(e.target.value) || 10))}
            disabled={isRunning}
            sx={{ flex: 1 }}
          />
          <TextField
            size="small"
            type="number"
            label="Staleness Bound (seconds)"
            value={stalenessSeconds}
            onChange={(e) => setStalenessSeconds(Math.max(1, parseInt(e.target.value) || 15))}
            disabled={isRunning}
            sx={{ flex: 1 }}
          />
        </Box>

        {/* Pre-flight Checklist */}
        <PreflightChecklist
          preflightResult={preflightResult}
          loading={preflightLoading}
          onRunChecks={handleRunPreflight}
          executionMode={executionMode}
        />
      </Paper>

      {/* Campaign Action Control Bar */}
      <Box sx={{ display: 'flex', gap: 1.5, mb: 3 }}>
        {!isRunning ? (
          <Button
            variant="contained"
            color="primary"
            size="medium"
            startIcon={starting ? <CircularProgress size={18} color="inherit" /> : <PlayArrowIcon />}
            onClick={handleStartCampaign}
            disabled={!canStart || starting}
            sx={{ flex: 2, textTransform: 'none', fontWeight: 600, py: 1 }}
          >
            {campaign?.status === 'completed' ? 'Re-Run Campaign' : 'Start Benchmark Campaign'}
          </Button>
        ) : (
          <Button
            variant="contained"
            color="warning"
            size="medium"
            startIcon={stopping ? <CircularProgress size={18} color="inherit" /> : <StopIcon />}
            onClick={handleStopCampaign}
            disabled={stopping}
            sx={{ flex: 2, textTransform: 'none', fontWeight: 600, py: 1 }}
          >
            Stop Benchmark
          </Button>
        )}

        {campaign && (
          <Button
            variant="outlined"
            color="error"
            startIcon={cleaning ? <CircularProgress size={18} color="inherit" /> : <DeleteForeverIcon />}
            onClick={() => setDeleteDialogOpen(true)}
            disabled={cleaning || isRunning}
            sx={{ flex: 1, textTransform: 'none', fontWeight: 600 }}
          >
            Delete All Resources
          </Button>
        )}
      </Box>

      {/* Progress & Stepper Tracker */}
      {campaign && (
        <Paper elevation={0} sx={{ p: 2.5, borderRadius: 2, border: '1px solid #e2e8f0', mb: 3 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
              Campaign Progress ({campaign.progress_pct}%)
            </Typography>
            <Chip
              size="small"
              label={campaign.status.replace('_', ' ').toUpperCase()}
              color={
                campaign.status === 'completed'
                  ? 'success'
                  : campaign.status === 'running' || campaign.status.startsWith('provisioning')
                  ? 'primary'
                  : campaign.status === 'failed'
                  ? 'error'
                  : 'default'
              }
              sx={{ fontWeight: 700, fontSize: '0.7rem' }}
            />
          </Box>
          <LinearProgress
            variant="determinate"
            value={campaign.progress_pct}
            sx={{ height: 8, borderRadius: 4, mb: 1.5 }}
          />
          <Typography variant="body2" color="text.secondary" sx={{ fontSize: '0.85rem' }}>
            {campaign.message}
          </Typography>
        </Paper>
      )}

      {/* Multi-Region Latency Comparison Table */}
      {campaign && Object.keys(campaign.region_results).length > 0 && (
        <Paper elevation={0} sx={{ p: 2.5, borderRadius: 2, border: '1px solid #e2e8f0', mb: 3 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <CompareArrowsIcon color="primary" />
              <Typography variant="subtitle2" sx={{ fontWeight: 700 }}>
                Multi-Region Latency Comparison
              </Typography>
            </Box>
            <Chip
              size="small"
              icon={<LanIcon sx={{ fontSize: '14px !important' }} />}
              label="Direct Access Active"
              color="success"
              variant="outlined"
              sx={{ fontWeight: 600, fontSize: '0.72rem' }}
            />
          </Box>

          <TableContainer component={Box} sx={{ border: '1px solid #edf2f7', borderRadius: 1.5 }}>
            <Table size="small">
              <TableHead sx={{ backgroundColor: '#edf2f7' }}>
                <TableRow>
                  <TableCell sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                    <TableSortLabel
                      active={sideSortField === 'region'}
                      direction={sideSortField === 'region' ? sideSortOrder : 'asc'}
                      onClick={() => handleSideRequestSort('region')}
                    >
                      Client Region
                    </TableSortLabel>
                  </TableCell>
                  <TableCell align="right" sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                    <TableSortLabel
                      active={sideSortField === 'dist'}
                      direction={sideSortField === 'dist' ? sideSortOrder : 'asc'}
                      onClick={() => handleSideRequestSort('dist')}
                    >
                      Dist to Leader
                    </TableSortLabel>
                  </TableCell>
                  <TableCell align="center" sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                    <TableSortLabel
                      active={sideSortField === 'replica'}
                      direction={sideSortField === 'replica' ? sideSortOrder : 'asc'}
                      onClick={() => handleSideRequestSort('replica')}
                    >
                      Replica Role
                    </TableSortLabel>
                  </TableCell>
                  <TableCell align="right" sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                    <TableSortLabel
                      active={sideSortField === 'writes_p50'}
                      direction={sideSortField === 'writes_p50' ? sideSortOrder : 'asc'}
                      onClick={() => handleSideRequestSort('writes_p50')}
                    >
                      Write p50
                    </TableSortLabel>
                  </TableCell>
                  <TableCell align="right" sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                    <TableSortLabel
                      active={sideSortField === 'strong_reads_p50'}
                      direction={sideSortField === 'strong_reads_p50' ? sideSortOrder : 'asc'}
                      onClick={() => handleSideRequestSort('strong_reads_p50')}
                    >
                      Strong Rd p50
                    </TableSortLabel>
                  </TableCell>
                  <TableCell align="right" sx={{ fontWeight: 700, fontSize: '0.75rem' }}>
                    <TableSortLabel
                      active={sideSortField === 'stale_reads_p50'}
                      direction={sideSortField === 'stale_reads_p50' ? sideSortOrder : 'asc'}
                      onClick={() => handleSideRequestSort('stale_reads_p50')}
                    >
                      Stale Rd (15s) p50
                    </TableSortLabel>
                  </TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {Object.values(campaign.region_results)
                  .sort((a: RegionBenchmarkResult, b: RegionBenchmarkResult) => {
                    if (sideSortField === 'region') {
                      const nameA = (a.region_name || a.region).toLowerCase();
                      const nameB = (b.region_name || b.region).toLowerCase();
                      return sideSortOrder === 'asc' ? nameA.localeCompare(nameB) : nameB.localeCompare(nameA);
                    }
                    if (sideSortField === 'dist') {
                      return sideSortOrder === 'asc'
                        ? a.distance_to_leader_km - b.distance_to_leader_km
                        : b.distance_to_leader_km - a.distance_to_leader_km;
                    }
                    if (sideSortField === 'replica') {
                      const repA = a.replica_type || '';
                      const repB = b.replica_type || '';
                      return sideSortOrder === 'asc' ? repA.localeCompare(repB) : repB.localeCompare(repA);
                    }

                    const getVal = (r: RegionBenchmarkResult) => {
                      if (sideSortField === 'writes_p50') return r.writes && r.writes.ops_count > 0 ? r.writes.p50 : undefined;
                      if (sideSortField === 'strong_reads_p50') return r.strong_reads && r.strong_reads.ops_count > 0 ? r.strong_reads.p50 : undefined;
                      if (sideSortField === 'stale_reads_p50') return r.stale_reads && r.stale_reads.ops_count > 0 ? r.stale_reads.p50 : undefined;
                      return undefined;
                    };
                    const valA = getVal(a);
                    const valB = getVal(b);
                    if (valA === undefined && valB === undefined) return 0;
                    if (valA === undefined) return 1;
                    if (valB === undefined) return -1;
                    return sideSortOrder === 'asc' ? valA - valB : valB - valA;
                  })
                  .map((r: RegionBenchmarkResult) => (
                  <TableRow key={r.region} hover>
                    <TableCell sx={{ fontWeight: 600, fontSize: '0.8rem' }}>
                      {r.region}
                      <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', fontSize: '0.7rem' }}>
                        {r.region_name}
                      </Typography>
                    </TableCell>
                    <TableCell align="right" sx={{ fontSize: '0.8rem' }}>
                      {r.distance_to_leader_km} km
                    </TableCell>
                    <TableCell align="center">
                      {r.replica_type ? (
                        <Chip
                          size="small"
                          variant="outlined"
                          label={r.replica_type}
                          sx={{
                            fontSize: '0.65rem',
                            height: 18,
                            fontWeight: 600,
                            borderRadius: '4px',
                            ...(r.replica_type === 'Leader Region'
                              ? { backgroundColor: '#fef2f2', color: '#dc2626', borderColor: '#fca5a5' }
                              : r.replica_type === 'R/W Replica Region'
                              ? { backgroundColor: '#fffbeb', color: '#d97706', borderColor: '#fde68a' }
                              : r.replica_type === 'Witness Region'
                              ? { backgroundColor: '#f1f5f9', color: '#64748b', borderColor: '#cbd5e1' }
                              : r.replica_type === 'Read Only Region'
                              ? { backgroundColor: '#eff6ff', color: '#2563eb', borderColor: '#bfdbfe' }
                              : { backgroundColor: '#f1f5f9', color: '#475569', borderColor: '#cbd5e1' }),
                          }}
                        />
                      ) : (
                        <Typography variant="caption" color="text.secondary">-</Typography>
                      )}
                    </TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.82rem', color: '#1e293b' }}>
                      {r.writes.p50 > 0 ? `${r.writes.p50} ms` : '-'}
                    </TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.82rem', color: '#1e293b' }}>
                      {r.strong_reads.p50 > 0 ? `${r.strong_reads.p50} ms` : '-'}
                    </TableCell>
                    <TableCell align="right" sx={{ fontWeight: 600, fontSize: '0.82rem', color: '#1e293b' }}>
                      {r.stale_reads.p50 > 0 ? (
                        <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: 0.5 }}>
                          <span>{r.stale_reads.p50} ms</span>
                          {r.has_local_replica && (
                            <Tooltip title="Served locally by nearest replica via Direct Access without cross-region leader trip!">
                              <Chip label="Local" size="small" color="success" sx={{ height: 16, fontSize: '0.62rem' }} />
                            </Tooltip>
                          )}
                        </Box>
                      ) : (
                        '-'
                      )}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          {/* Educational Insight Banner */}
          <Alert severity="success" sx={{ mt: 2, fontSize: '0.78rem', py: 0.5 }}>
            💡 <b>Spanner Multi-Region Architecture in Action:</b> Notice how Stale Reads (15s) with Direct Access in regions with a local replica achieve ultra-low ~2ms latencies, while cross-region Writes require round-trips to the leader and consensus quorum.
          </Alert>
        </Paper>
      )}

      {/* Confirmation Dialog for Delete All Resources */}
      <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
        <DialogTitle sx={{ fontWeight: 700 }}>Delete All Provisioned Resources?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            This action will immediately drop the <code>benchmark-db</code> database and delete the 1-node Spanner instance. This guarantees zero ongoing Google Cloud billing costs.
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteDialogOpen(false)} sx={{ textTransform: 'none' }}>
            Cancel
          </Button>
          <Button onClick={handleConfirmCleanup} color="error" variant="contained" sx={{ textTransform: 'none' }}>
            Yes, Delete All Resources
          </Button>
        </DialogActions>
      </Dialog>
    </Drawer>
  );
};
