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

import React, { useState, useEffect, useMemo, useRef } from 'react';
import {
  Box,
  Typography,
  Button,
  IconButton,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  Stack,
  Grid,
  TextField,
  InputAdornment,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  LinearProgress,
  Tooltip,
  Alert,
  CircularProgress,
} from '@mui/material';
import SpeedIcon from '@mui/icons-material/Speed';
import AddIcon from '@mui/icons-material/Add';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import EditIcon from '@mui/icons-material/Edit';
import AssessmentIcon from '@mui/icons-material/Assessment';
import DeleteForeverIcon from '@mui/icons-material/DeleteForever';
import DeleteSweepIcon from '@mui/icons-material/DeleteSweep';
import RefreshIcon from '@mui/icons-material/Refresh';
import SearchIcon from '@mui/icons-material/Search';
import DnsIcon from '@mui/icons-material/Dns';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';

import { SpannerConfig } from '../types/spanner';
import {
  BenchmarkCampaignConfig,
  BenchmarkCampaignStatus,
  ProjectItem,
} from '../types/benchmark';
import {
  fetchCampaigns,
  createCampaign,
  updateCampaign,
  deleteCampaign,
  runCampaign,
  stopBenchmarkCampaign,
  cleanupCampaignResources,
  cleanupAllResources,
} from '../services/api';
import { BenchmarkEditorDialog } from './BenchmarkEditorDialog';
import { BenchmarkResultsModal } from './BenchmarkResultsModal';
import { useAppConfig } from '../context/ConfigContext';

interface BenchmarkManagementViewProps {
  allConfigs: SpannerConfig[];
  onCreateBenchmark?: () => void;
  onEditBenchmark?: (campaign: BenchmarkCampaignStatus) => void;
  initialConfigToCreate?: string;
  initialClientRegions?: string[];
  onClearInitialConfig?: () => void;
  initialOpenCampaignId?: string;
  onClearInitialOpenCampaignId?: () => void;
  projects?: ProjectItem[];
  activeProject?: string;
  loadingProjects?: boolean;
  onRefreshProjects?: () => void;
}

export const BenchmarkManagementView: React.FC<BenchmarkManagementViewProps> = ({
  allConfigs,
  onCreateBenchmark,
  onEditBenchmark,
  initialConfigToCreate,
  initialClientRegions,
  onClearInitialConfig,
  initialOpenCampaignId,
  onClearInitialOpenCampaignId,
  projects = [],
  activeProject,
  loadingProjects = false,
  onRefreshProjects,
}) => {
  const { features } = useAppConfig();
  const [campaigns, setCampaigns] = useState<BenchmarkCampaignStatus[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [searchQuery, setSearchQuery] = useState<string>('');

  // Dialog states
  const [editorOpen, setEditorOpen] = useState<boolean>(false);
  const [editingCampaign, setEditingCampaign] = useState<BenchmarkCampaignStatus | null>(null);

  const [resultsOpen, setResultsOpen] = useState<boolean>(false);
  const [activeResultsCampaign, setActiveResultsCampaign] = useState<BenchmarkCampaignStatus | null>(null);

  const [deleteDialogOpen, setDeleteDialogOpen] = useState<boolean>(false);
  const [campaignToDelete, setCampaignToDelete] = useState<BenchmarkCampaignStatus | null>(null);
  const [isDeleting, setIsDeleting] = useState<boolean>(false);
  const [deletingCampaignId, setDeletingCampaignId] = useState<string | null>(null);

  const [cleaningUpCampaignId, setCleaningUpCampaignId] = useState<string | null>(null);
  const [cleanupAllDialogOpen, setCleanupAllDialogOpen] = useState<boolean>(false);
  const [isCleaningUpAll, setIsCleaningUpAll] = useState<boolean>(false);
  const [actionMessage, setActionMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null);

  const pollingRef = useRef<number | null>(null);

  // Load campaigns from backend
  const loadCampaigns = async () => {
    try {
      setLoading(true);
      const data = await fetchCampaigns();
      setCampaigns(data);
    } catch (err: any) {
      console.error('Failed to fetch benchmark campaigns', err);
      setActionMessage({ type: 'error', text: 'Failed to load benchmark tests.' });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadCampaigns();
  }, []);

  const [pendingInitialConfig, setPendingInitialConfig] = useState<string | undefined>(undefined);

  // If prompted with an initial Spanner config to create from Explorer
  useEffect(() => {
    if (initialConfigToCreate) {
      setPendingInitialConfig(initialConfigToCreate);
      setEditingCampaign(null);
      setEditorOpen(true);
      onClearInitialConfig?.();
    }
  }, [initialConfigToCreate, onClearInitialConfig]);

  // If navigated with a campaign to open immediately in results modal
  useEffect(() => {
    if (initialOpenCampaignId) {
      const found = campaigns.find((c) => c.campaign_id === initialOpenCampaignId);
      if (found) {
        setActiveResultsCampaign(found);
        setResultsOpen(true);
        onClearInitialOpenCampaignId?.();
      } else {
        // Fetch freshly in case campaign was just created
        fetchCampaigns().then((freshList) => {
          setCampaigns(freshList);
          const fresh = freshList.find((c) => c.campaign_id === initialOpenCampaignId);
          if (fresh) {
            setActiveResultsCampaign(fresh);
            setResultsOpen(true);
            onClearInitialOpenCampaignId?.();
          }
        });
      }
    }
  }, [initialOpenCampaignId, campaigns, onClearInitialOpenCampaignId]);

  // Periodic status polling for in-flight running campaigns
  useEffect(() => {
    const hasActiveRuns =
      campaigns.some(
        (c) =>
          c.status === 'preflight_check' ||
          c.status === 'provisioning_spanner' ||
          c.status === 'provisioning_vms' ||
          c.status === 'running' ||
          (c.steps && c.steps.some((s) => s.status === 'running'))
      ) ||
      (resultsOpen &&
        activeResultsCampaign &&
        (activeResultsCampaign.status === 'running' ||
          activeResultsCampaign.status === 'preflight_check' ||
          activeResultsCampaign.status === 'provisioning_spanner' ||
          activeResultsCampaign.status === 'provisioning_vms' ||
          (activeResultsCampaign.steps &&
            activeResultsCampaign.steps.some((s) => s.status === 'running'))));

    if (hasActiveRuns) {
      if (!pollingRef.current) {
        pollingRef.current = window.setInterval(async () => {
          try {
            const updated = await fetchCampaigns();
            setCampaigns(updated);
            // If viewing results of an active campaign, update modal too
            if (activeResultsCampaign) {
              const fresh = updated.find((c) => c.campaign_id === activeResultsCampaign.campaign_id);
              if (fresh) setActiveResultsCampaign(fresh);
            }
          } catch (err) {
            console.error('Polling error', err);
          }
        }, 1500);
      }
    } else {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    }

    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
      }
    };
  }, [campaigns, activeResultsCampaign, resultsOpen]);

  // Create or Update handler
  const handleSaveCampaign = async (config: BenchmarkCampaignConfig) => {
    if (editingCampaign) {
      const updated = await updateCampaign(editingCampaign.campaign_id, config);
      setCampaigns((prev) =>
        prev.map((c) => (c.campaign_id === updated.campaign_id ? updated : c))
      );
      setActionMessage({ type: 'success', text: `Updated "${updated.name}" successfully.` });
    } else {
      const created = await createCampaign(config);
      setCampaigns((prev) => [created, ...prev]);
      setActionMessage({ type: 'success', text: `Created "${created.name}" successfully.` });
    }
  };

  // Run or Re-Run
  const handleRunCampaign = async (campaignId: string) => {
    try {
      const updated = await runCampaign(campaignId);
      setCampaigns((prev) =>
        prev.map((c) => (c.campaign_id === campaignId ? updated : c))
      );
      if (activeResultsCampaign?.campaign_id === campaignId) {
        setActiveResultsCampaign(updated);
      }
      setActionMessage({ type: 'success', text: `Launched benchmark "${updated.name}".` });
    } catch (err: any) {
      console.error('Failed to run benchmark', err);
      setActionMessage({
        type: 'error',
        text: err?.response?.data?.detail || err.message || 'Failed to start benchmark run.',
      });
    }
  };

  // Stop
  const handleStopCampaign = async (campaignId: string) => {
    try {
      await stopBenchmarkCampaign(campaignId);
      await loadCampaigns();
      setActionMessage({ type: 'success', text: 'Benchmark run stopped.' });
    } catch (err: any) {
      console.error('Failed to stop benchmark', err);
    }
  };

  // Cleanup single test resources
  const handleCleanupCampaign = async (campaignId: string) => {
    try {
      setCleaningUpCampaignId(campaignId);
      await cleanupCampaignResources(campaignId);
      await loadCampaigns();
      setActionMessage({ type: 'success', text: 'Cloud resources torn down successfully.' });
    } catch (err: any) {
      console.error('Failed to cleanup resources', err);
      setActionMessage({
        type: 'error',
        text: err?.response?.data?.detail || err?.message || 'Failed to cleanup resources.',
      });
    } finally {
      setCleaningUpCampaignId(null);
    }
  };

  // Confirm delete single campaign
  const handleConfirmDelete = async () => {
    if (!campaignToDelete) return;
    const cid = campaignToDelete.campaign_id;
    const cname = campaignToDelete.name || cid;
    setIsDeleting(true);
    setDeletingCampaignId(cid);
    try {
      await deleteCampaign(cid);
      setCampaigns((prev) => prev.filter((c) => c.campaign_id !== cid));
      setActionMessage({
        type: 'success',
        text: `Deleted "${cname}" and purged all associated cloud resources.`,
      });
      setCampaignToDelete(null);
      setDeleteDialogOpen(false);
    } catch (err: any) {
      console.error('Failed to delete benchmark', err);
      setActionMessage({
        type: 'error',
        text: err?.response?.data?.detail || err?.message || 'Failed to delete benchmark test.',
      });
    } finally {
      setIsDeleting(false);
      setDeletingCampaignId(null);
    }
  };

  // Cleanup all resources across all campaigns
  const handleConfirmCleanupAll = async () => {
    setIsCleaningUpAll(true);
    try {
      const res = await cleanupAllResources();
      await loadCampaigns();
      setActionMessage({ type: 'success', text: res.message || 'All cloud resources purged.' });
      setCleanupAllDialogOpen(false);
    } catch (err: any) {
      console.error('Failed to cleanup all resources', err);
      setActionMessage({
        type: 'error',
        text: err?.response?.data?.detail || err?.message || 'Failed to clean up cloud resources.',
      });
    } finally {
      setIsCleaningUpAll(false);
    }
  };

  // Quick Starter Templates
  const handleCreateFromTemplate = async (templateName: string, spannerCfg: string, regions: string[]) => {
    try {
      const created = await createCampaign({
        name: templateName,
        description: `Pre-configured benchmark template for ${spannerCfg}`,
        project_id: activeProject || (projects && projects[0]?.project_id) || 'spanner-loadtest-demo',
        spanner_config: spannerCfg,
        client_regions: regions,
        operations: 1000,
        staleness_seconds: 15,
        execution_mode: features.allow_dry_run ? 'dry_run' : 'live',
      });
      setCampaigns((prev) => [created, ...prev]);
      setActionMessage({ type: 'success', text: `Created template benchmark "${templateName}". Click Run to execute!` });
    } catch (err: any) {
      console.error('Failed to create template', err);
    }
  };

  // Filtered campaigns
  const filteredCampaigns = useMemo(() => {
    if (!searchQuery.trim()) return campaigns;
    const q = searchQuery.toLowerCase();
    return campaigns.filter(
      (c) =>
        (c.name && c.name.toLowerCase().includes(q)) ||
        c.campaign_id.toLowerCase().includes(q) ||
        c.spanner_config.toLowerCase().includes(q) ||
        c.project_id.toLowerCase().includes(q) ||
        c.status.toLowerCase().includes(q)
    );
  }, [campaigns, searchQuery]);


  return (
    <Box sx={{ p: { xs: 2, sm: 3 }, maxWidth: 1440, mx: 'auto' }}>
      {/* Top Header & Actions */}
      <Box
        sx={{
          display: 'flex',
          alignItems: { xs: 'flex-start', sm: 'center' },
          justifyContent: 'space-between',
          flexDirection: { xs: 'column', sm: 'row' },
          gap: 2,
          mb: 3,
        }}
      >
        <Box>
          <Stack direction="row" spacing={1.5} alignItems="center">
            <SpeedIcon sx={{ color: '#7c3aed', fontSize: 32 }} />
            <Typography variant="h5" sx={{ fontWeight: 700, color: '#1e293b' }}>
              Latency Benchmark Suite
            </Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
            Configure, execute and analyze Cloud Spanner latency
          </Typography>
        </Box>

        <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap">
          <Button
            variant="outlined"
            color="error"
            size="small"
            disabled={isCleaningUpAll}
            startIcon={
              isCleaningUpAll ? (
                <CircularProgress size={14} color="error" />
              ) : (
                <DeleteSweepIcon />
              )
            }
            onClick={() => setCleanupAllDialogOpen(true)}
            sx={{ textTransform: 'none', fontWeight: 600 }}
          >
            {isCleaningUpAll ? 'Purging All Resources...' : 'Delete All Resources'}
          </Button>

          <Button
            variant="contained"
            size="small"
            startIcon={<AddIcon />}
            onClick={() => {
              if (onCreateBenchmark) {
                onCreateBenchmark();
              } else {
                setEditingCampaign(null);
                setEditorOpen(true);
              }
            }}
            sx={{
              textTransform: 'none',
              fontWeight: 600,
              backgroundColor: '#7c3aed',
              color: '#ffffff',
              boxShadow: 'none',
              '&:hover': { backgroundColor: '#6d28d9', boxShadow: 'none' },
            }}
          >
            New Benchmark Test
          </Button>

          <Tooltip title="Refresh benchmarks list">
            <IconButton onClick={loadCampaigns} size="small">
              <RefreshIcon />
            </IconButton>
          </Tooltip>
        </Stack>
      </Box>

      {/* Action Notification Alert */}
      {actionMessage && (
        <Alert
          severity={actionMessage.type}
          onClose={() => setActionMessage(null)}
          sx={{ mb: 2.5 }}
        >
          {actionMessage.text}
        </Alert>
      )}


      {/* Search & Filter Bar */}
      <Box sx={{ mb: 2 }}>
        <TextField
          size="small"
          placeholder="Search by benchmark name, Spanner config, or project..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          sx={{ maxWidth: 450, backgroundColor: '#ffffff' }}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
              </InputAdornment>
            ),
          }}
        />
      </Box>

      {/* Main Benchmarks Table */}
      {loading ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', p: 6 }}>
          <CircularProgress />
        </Box>
      ) : filteredCampaigns.length === 0 ? (
        <Paper
          elevation={0}
          sx={{
            p: 5,
            textAlign: 'center',
            borderRadius: 2,
            border: '1px dashed #cbd5e1',
            backgroundColor: '#ffffff',
          }}
        >
          <SpeedIcon sx={{ fontSize: 48, color: '#94a3b8', mb: 1.5 }} />
          <Typography variant="h6" sx={{ fontWeight: 700, color: '#334155' }}>
            {searchQuery ? 'No matching benchmarks found' : 'No latency benchmarks created yet'}
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 500, mx: 'auto', mb: 3 }}>
            Create a benchmark to test P50, P90, and P99 read/write latencies across distributed client regions with Direct Access.
          </Typography>

          {/* Template Quick Starters */}
          <Box sx={{ maxWidth: 700, mx: 'auto', mb: 3 }}>
            <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.secondary', display: 'block', mb: 1.5, textTransform: 'uppercase' }}>
              Or start instantly with a template:
            </Typography>
            <Grid container spacing={1.5}>
              <Grid item xs={12} sm={6}>
                <Paper
                  variant="outlined"
                  sx={{
                    p: 1.5,
                    textAlign: 'left',
                    cursor: 'pointer',
                    '&:hover': { borderColor: '#7c3aed', backgroundColor: '#faf5ff' },
                  }}
                  onClick={() =>
                    handleCreateFromTemplate(
                      'Regional Spanner (us-east1)',
                      'us-east1',
                      ['us-east1', 'us-west1']
                    )
                  }
                >
                  <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#7c3aed' }}>
                    Regional Spanner (2 Clients)
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    us-east1 &bull; us-east1, us-west1
                  </Typography>
                </Paper>
              </Grid>

              <Grid item xs={12} sm={6}>
                <Paper
                  variant="outlined"
                  sx={{
                    p: 1.5,
                    textAlign: 'left',
                    cursor: 'pointer',
                    '&:hover': { borderColor: '#7c3aed', backgroundColor: '#faf5ff' },
                  }}
                  onClick={() =>
                    handleCreateFromTemplate(
                      'Multi-Region Spanner (nam6)',
                      'nam6',
                      ['us-central1', 'us-east1', 'us-west1', 'us-west2']
                    )
                  }
                >
                  <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#7c3aed' }}>
                    Multi-Region Spanner (4 Clients)
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    nam6 &bull; us-central1, us-east1, us-west1, us-west2
                  </Typography>
                </Paper>
              </Grid>
            </Grid>
          </Box>

          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => {
              if (onCreateBenchmark) {
                onCreateBenchmark();
              } else {
                setEditingCampaign(null);
                setEditorOpen(true);
              }
            }}
            sx={{
              backgroundColor: '#7c3aed',
              textTransform: 'none',
              fontWeight: 600,
              '&:hover': { backgroundColor: '#6d28d9' },
            }}
          >
            Create Custom Benchmark
          </Button>
        </Paper>
      ) : (
        <TableContainer component={Paper} elevation={0} sx={{ border: '1px solid #e2e8f0', borderRadius: 2 }}>
          <Table>
            <TableHead sx={{ backgroundColor: '#f8fafc' }}>
              <TableRow>
                <TableCell sx={{ fontWeight: 700 }}>Benchmark</TableCell>
                <TableCell sx={{ fontWeight: 700 }}>Target Spanner Config</TableCell>
                <TableCell sx={{ fontWeight: 700 }}>Client Regions</TableCell>
                <TableCell sx={{ fontWeight: 700 }}>Engine & Profile</TableCell>
                <TableCell sx={{ fontWeight: 700 }}>Status</TableCell>
                <TableCell align="right" sx={{ fontWeight: 700 }}>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {filteredCampaigns.map((c) => {
                const regions = c.client_regions || Object.keys(c.region_results || {});
                const isRunning =
                  c.status === 'preflight_check' ||
                  c.status === 'provisioning_spanner' ||
                  c.status === 'provisioning_vms' ||
                  c.status === 'running';
                const isRowDeleting = c.campaign_id === deletingCampaignId || c.status === 'deleting';

                return (
                  <TableRow key={c.campaign_id} hover>
                    {/* Benchmark Name & ID */}
                    <TableCell>
                      <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#1e293b' }}>
                        {c.name || `Benchmark ${c.campaign_id}`}
                      </Typography>
                      <Typography variant="caption" color="text.secondary" sx={{ fontFamily: 'monospace' }}>
                        ID: {c.campaign_id} &bull; Project: {c.project_id}
                      </Typography>
                    </TableCell>

                    {/* Spanner Config */}
                    <TableCell>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <DnsIcon sx={{ fontSize: 18, color: '#1a73e8' }} />
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          {c.spanner_config}
                        </Typography>
                      </Box>
                      <Typography variant="caption" color="text.secondary">
                        Leader: <b>{c.leader_region || 'Auto'}</b>
                      </Typography>
                      {c.optional_replicas && c.optional_replicas.length > 0 && (
                        <Box sx={{ mt: 0.5 }}>
                          <Chip
                            label={`+${c.optional_replicas.length} opt replica${c.optional_replicas.length > 1 ? 's' : ''}`}
                            size="small"
                            sx={{
                              fontSize: '0.68rem',
                              height: 18,
                              backgroundColor: '#e6f4ea',
                              color: '#137333',
                              fontWeight: 600,
                            }}
                          />
                        </Box>
                      )}
                    </TableCell>

                    {/* Client Regions */}
                    <TableCell>
                      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, maxWidth: 380 }}>
                        {regions.slice(0, 5).map((reg) => (
                          <Chip key={reg} label={reg} size="small" sx={{ fontSize: '0.7rem', height: 20 }} />
                        ))}
                        {regions.length > 5 && (
                          <Chip
                            label={`+${regions.length - 5} more`}
                            size="small"
                            variant="outlined"
                            sx={{ fontSize: '0.7rem', height: 20 }}
                          />
                        )}
                      </Box>
                    </TableCell>

                    {/* Engine & Profile */}
                    <TableCell>
                      <Chip
                        label={c.execution_mode === 'dry_run' ? 'Physics Sim' : 'Live GCE'}
                        size="small"
                        color={c.execution_mode === 'dry_run' ? 'default' : 'secondary'}
                        sx={{ fontSize: '0.7rem', height: 20, mb: 0.5 }}
                      />
                      <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                        {c.operations || 1000} ops &bull; {c.staleness_seconds || 15}s staleness
                      </Typography>
                    </TableCell>

                    {/* Status */}
                    <TableCell>
                      {isRowDeleting ? (
                        <Box sx={{ width: 140 }}>
                          <Stack direction="row" spacing={0.8} alignItems="center" sx={{ mb: 0.5 }}>
                            <CircularProgress size={12} color="error" />
                            <Typography variant="caption" sx={{ fontWeight: 600, color: '#dc2626' }}>
                              Deleting...
                            </Typography>
                          </Stack>
                          <LinearProgress color="error" sx={{ height: 4, borderRadius: 2 }} />
                        </Box>
                      ) : isRunning ? (
                        <Box sx={{ width: 140 }}>
                          <Stack direction="row" spacing={0.8} alignItems="center" sx={{ mb: 0.5 }}>
                            <CircularProgress size={12} />
                            <Typography variant="caption" sx={{ fontWeight: 600, color: '#7c3aed' }}>
                              {c.status === 'preflight_check'
                                ? 'Preflight'
                                : c.status === 'running'
                                ? 'Running'
                                : 'Provisioning'}{' '}
                              ({c.progress_pct}%)
                            </Typography>
                          </Stack>
                          <LinearProgress
                            variant="determinate"
                            value={c.progress_pct}
                            sx={{ height: 4, borderRadius: 2 }}
                          />
                        </Box>
                      ) : c.status === 'completed' ? (
                        <Chip
                          icon={<CheckCircleIcon sx={{ fontSize: '14px !important' }} />}
                          label="Completed"
                          size="small"
                          color="success"
                          sx={{ fontSize: '0.72rem', fontWeight: 600 }}
                        />
                      ) : c.status === 'stopped' ? (
                        <Chip label="Stopped" size="small" color="warning" sx={{ fontSize: '0.72rem' }} />
                      ) : c.status === 'failed' ? (
                        <Chip
                          icon={<ErrorOutlineIcon sx={{ fontSize: '14px !important' }} />}
                          label="Failed"
                          size="small"
                          color="error"
                          sx={{ fontSize: '0.72rem' }}
                        />
                      ) : (
                        <Chip label="Draft" size="small" variant="outlined" sx={{ fontSize: '0.72rem' }} />
                      )}
                    </TableCell>

                    {/* Actions */}
                    <TableCell align="right">
                      <Stack direction="row" spacing={0.5} justifyContent="flex-end">
                        {/* Run / Stop */}
                        {isRunning ? (
                          <Tooltip title="Stop benchmark">
                            <span>
                              <IconButton
                                size="small"
                                color="warning"
                                disabled={isRowDeleting}
                                onClick={() => handleStopCampaign(c.campaign_id)}
                              >
                                <StopIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                        ) : (
                          <Tooltip title="Run benchmark">
                            <span>
                              <IconButton
                                size="small"
                                sx={{ color: '#7c3aed' }}
                                disabled={isRowDeleting}
                                onClick={() => handleRunCampaign(c.campaign_id)}
                              >
                                <PlayArrowIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                        )}

                        {/* View Results */}
                        <Tooltip title="View results & metrics">
                          <span>
                            <IconButton
                              size="small"
                              color="primary"
                              disabled={isRowDeleting}
                              onClick={() => {
                                setActiveResultsCampaign(c);
                                setResultsOpen(true);
                              }}
                            >
                              <AssessmentIcon fontSize="small" />
                            </IconButton>
                          </span>
                        </Tooltip>

                        {/* Edit */}
                        <Tooltip title="Edit configuration">
                          <span>
                            <IconButton
                              size="small"
                              disabled={isRunning || isRowDeleting}
                              onClick={() => {
                                if (onEditBenchmark) {
                                  onEditBenchmark(c);
                                } else {
                                  setEditingCampaign(c);
                                  setEditorOpen(true);
                                }
                              }}
                            >
                              <EditIcon fontSize="small" />
                            </IconButton>
                          </span>
                        </Tooltip>

                        {/* Delete Single Test */}
                        <Tooltip title={isRowDeleting ? "Deleting resources..." : "Delete benchmark"}>
                          <span>
                            <IconButton
                              size="small"
                              color="error"
                              disabled={isRunning || isRowDeleting}
                              onClick={() => {
                                setCampaignToDelete(c);
                                setDeleteDialogOpen(true);
                              }}
                            >
                              {isRowDeleting ? (
                                <CircularProgress size={16} color="error" />
                              ) : (
                                <DeleteForeverIcon fontSize="small" />
                              )}
                            </IconButton>
                          </span>
                        </Tooltip>
                      </Stack>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      )}

      {/* Benchmark Editor Dialog */}
      <BenchmarkEditorDialog
        open={editorOpen}
        onClose={() => {
          setEditorOpen(false);
          setEditingCampaign(null);
          setPendingInitialConfig(undefined);
        }}
        onSave={handleSaveCampaign}
        editingCampaign={editingCampaign}
        allConfigs={allConfigs}
        initialSpannerConfig={pendingInitialConfig}
        initialClientRegions={initialClientRegions}
        projects={projects}
        activeProject={activeProject}
        loadingProjects={loadingProjects}
        onRefreshProjects={onRefreshProjects}
      />

      {/* Benchmark Results Modal */}
      <BenchmarkResultsModal
        open={resultsOpen}
        onClose={() => {
          setResultsOpen(false);
          setActiveResultsCampaign(null);
        }}
        campaign={activeResultsCampaign}
        onRun={handleRunCampaign}
        onStop={handleStopCampaign}
        onCleanup={handleCleanupCampaign}
        isCleaningUp={Boolean(cleaningUpCampaignId && activeResultsCampaign?.campaign_id === cleaningUpCampaignId)}
      />

      {/* Delete Single Campaign Confirmation Dialog */}
      <Dialog
        open={deleteDialogOpen}
        onClose={() => !isDeleting && setDeleteDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ fontWeight: 700, color: (campaignToDelete?.spanner_instance_id || (campaignToDelete?.gce_instances && Object.keys(campaignToDelete.gce_instances).length > 0)) ? '#dc2626' : 'inherit' }}>
          Delete Benchmark Test{(campaignToDelete?.spanner_instance_id || (campaignToDelete?.gce_instances && Object.keys(campaignToDelete.gce_instances).length > 0)) ? ' & Cloud Resources' : ''}?
        </DialogTitle>
        <DialogContent>
          <DialogContentText sx={{ mb: 1.5 }}>
            Are you sure you want to delete <b>{campaignToDelete?.name || campaignToDelete?.campaign_id}</b>?
          </DialogContentText>

          {campaignToDelete && (() => {
            const hasSpanner = Boolean(campaignToDelete.spanner_instance_id);
            const gceEntries = Object.entries(campaignToDelete.gce_instances || {});
            const hasVms = gceEntries.length > 0;
            const hasAttachedResources = hasSpanner || hasVms;

            if (hasAttachedResources) {
              return (
                <Alert severity="warning" sx={{ mb: 2 }}>
                  <Typography variant="subtitle2" sx={{ fontWeight: 700, mb: 0.5 }}>
                    Attached Google Cloud Resources Will Be Permanently Destroyed:
                  </Typography>
                  <Box component="ul" sx={{ m: 0, pl: 2.5, fontSize: '0.85rem' }}>
                    {hasSpanner && (
                      <Box component="li" sx={{ mb: 0.5 }}>
                        <b>Cloud Spanner Instance:</b> <code>{campaignToDelete.spanner_instance_id}</code>
                        <Typography variant="caption" sx={{ display: 'block', color: 'text.secondary' }}>
                          Database: <code>benchdb</code> &bull; Config: <code>{campaignToDelete.spanner_config}</code> &bull; Project: <code>{campaignToDelete.project_id}</code>
                        </Typography>
                      </Box>
                    )}
                    {hasVms && (
                      <Box component="li">
                        <b>GCE Runner Fleet ({gceEntries.length} VM{gceEntries.length > 1 ? 's' : ''}):</b>
                        <Box component="ul" sx={{ m: 0, pl: 2, fontSize: '0.8rem', mt: 0.5 }}>
                          {gceEntries.map(([region, vmName]) => (
                            <li key={region}>
                              Region <b>{region}</b>: <code>{vmName}</code>
                            </li>
                          ))}
                        </Box>
                      </Box>
                    )}
                  </Box>
                </Alert>
              );
            }

            return (
              <Alert severity="info" sx={{ mb: 2 }}>
                <Typography variant="body2">
                  No active cloud resources detected. Only the local benchmark test definition and historical metrics will be removed.
                </Typography>
              </Alert>
            );
          })()}

          {isDeleting && (
            <Box sx={{ p: 2, bgcolor: '#fef2f2', borderRadius: 2, border: '1px solid #fecaca' }}>
              <Stack direction="row" spacing={1.5} alignItems="center" sx={{ mb: 1 }}>
                <CircularProgress size={20} color="error" />
                <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#dc2626' }}>
                  Tearing Down Cloud Resources...
                </Typography>
              </Stack>
              <LinearProgress color="error" sx={{ height: 6, borderRadius: 3, mb: 1.5 }} />
              <Typography variant="caption" sx={{ color: '#991b1b', display: 'block' }}>
                Tearing down attached GCP resources on Google Cloud in parallel. This typically takes 15–25 seconds. Please do not close this dialog.
              </Typography>
            </Box>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2, px: 3 }}>
          <Button
            onClick={() => setDeleteDialogOpen(false)}
            disabled={isDeleting}
            sx={{ textTransform: 'none' }}
          >
            Cancel
          </Button>
          <Button
            variant="contained"
            color="error"
            disabled={isDeleting}
            onClick={handleConfirmDelete}
            sx={{ textTransform: 'none', fontWeight: 600, minWidth: 160 }}
          >
            {isDeleting ? (
              <Stack direction="row" spacing={1} alignItems="center">
                <CircularProgress size={16} sx={{ color: 'white' }} />
                <span>Deleting Resources...</span>
              </Stack>
            ) : (campaignToDelete?.spanner_instance_id || (campaignToDelete?.gce_instances && Object.keys(campaignToDelete.gce_instances).length > 0)) ? (
              'Delete Test & Resources'
            ) : (
              'Delete Test'
            )}
          </Button>
        </DialogActions>
      </Dialog>

      {/* Delete All Cloud Resources Dialog */}
      <Dialog
        open={cleanupAllDialogOpen}
        onClose={() => !isCleaningUpAll && setCleanupAllDialogOpen(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle sx={{ fontWeight: 700, color: '#dc2626' }}>
          Delete All Cloud Resources?
        </DialogTitle>
        <DialogContent>
          <DialogContentText sx={{ mb: 1.5 }}>
            This operation will terminate and purge all provisioned Cloud Spanner test instances and GCE benchmark VMs across all campaigns in your GCP project.
            <br />
            <br />
            Benchmark test definitions and historical results will be preserved.
          </DialogContentText>
          {isCleaningUpAll && (
            <Box sx={{ mt: 2, p: 2, bgcolor: '#fef2f2', borderRadius: 2, border: '1px solid #fecaca' }}>
              <Stack direction="row" spacing={1.5} alignItems="center" sx={{ mb: 1 }}>
                <CircularProgress size={20} color="error" />
                <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#dc2626' }}>
                  Purging All Test Resources Across Project...
                </Typography>
              </Stack>
              <LinearProgress color="error" sx={{ height: 6, borderRadius: 3, mb: 1.5 }} />
              <Typography variant="caption" sx={{ color: '#991b1b', display: 'block' }}>
                Executing parallel sweeps across Cloud Spanner instances and GCE runner VMs... This may take up to 30 seconds.
              </Typography>
            </Box>
          )}
        </DialogContent>
        <DialogActions sx={{ p: 2, px: 3 }}>
          <Button
            onClick={() => setCleanupAllDialogOpen(false)}
            disabled={isCleaningUpAll}
            sx={{ textTransform: 'none' }}
          >
            Cancel
          </Button>
          <Button
            variant="contained"
            color="error"
            disabled={isCleaningUpAll}
            onClick={handleConfirmCleanupAll}
            sx={{ textTransform: 'none', fontWeight: 600, minWidth: 160 }}
          >
            {isCleaningUpAll ? (
              <Stack direction="row" spacing={1} alignItems="center">
                <CircularProgress size={16} sx={{ color: 'white' }} />
                <span>Purging Resources...</span>
              </Stack>
            ) : (
              'Delete All Resources'
            )}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};
