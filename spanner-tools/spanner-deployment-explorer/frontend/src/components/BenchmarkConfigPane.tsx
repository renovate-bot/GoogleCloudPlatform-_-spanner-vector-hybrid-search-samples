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

import React, { useState, useEffect, useMemo } from 'react';
import {
  Box,
  Typography,
  IconButton,
  TextField,
  Button,
  Chip,
  Alert,
  CircularProgress,
  FormControl,
  Select,
  MenuItem,
  Stack,
  Divider,
  Tooltip,
  Paper,
  RadioGroup,
  FormControlLabel,
  Radio,
  Checkbox,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import SpeedIcon from '@mui/icons-material/Speed';
import DnsIcon from '@mui/icons-material/Dns';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import SaveIcon from '@mui/icons-material/Save';
import PublicIcon from '@mui/icons-material/Public';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import TuneIcon from '@mui/icons-material/Tune';

import { SpannerConfig } from '../types/spanner';
import {
  BenchmarkCampaignConfig,
  BenchmarkCampaignStatus,
  ClientRegionInfo,
  ProjectItem,
} from '../types/benchmark';
import { useAppConfig } from '../context/ConfigContext';
import { createCampaign, updateCampaign } from '../services/api';
import DynamicSelect, { SelectOption } from './DynamicSelect';

interface BenchmarkConfigPaneProps {
  open: boolean;
  onClose: () => void;
  editingCampaign: BenchmarkCampaignStatus | null;
  allConfigs: SpannerConfig[];
  selectedSpannerConfig: string;
  onSelectSpannerConfig: (configname: string) => void;
  currentLeader?: string;
  onLeaderChange?: (newLeader: string) => void;
  allClientRegions: ClientRegionInfo[];
  selectedClientRegions: string[];
  onToggleClientRegion: (regionId: string) => void;
  onSelectClientRegions: (regionIds: string[]) => void;
  onClearClientRegions: () => void;
  onSaved: (savedCampaign: BenchmarkCampaignStatus, shouldRun?: boolean) => void;
  projects?: ProjectItem[];
  activeProject?: string;
  loadingProjects?: boolean;
  onRefreshProjects?: () => void;
  loadingConfigs?: boolean;
  loadingClientRegions?: boolean;
  selectedOptionalReplicasMap?: Record<string, string[]>;
}

export const BenchmarkConfigPane: React.FC<BenchmarkConfigPaneProps> = ({
  open,
  onClose,
  editingCampaign,
  allConfigs,
  selectedSpannerConfig,
  onSelectSpannerConfig,
  currentLeader,
  onLeaderChange,
  allClientRegions,
  selectedClientRegions,
  onToggleClientRegion,
  onSelectClientRegions,
  onClearClientRegions,
  onSaved,
  projects = [],
  activeProject,
  loadingProjects = false,
  onRefreshProjects,
  loadingConfigs = false,
  loadingClientRegions = false,
  selectedOptionalReplicasMap,
}) => {
  const { features, load_testing } = useAppConfig();

  const [name, setName] = useState<string>('');
  const [description, setDescription] = useState<string>('');
  const [projectId, setProjectId] = useState<string>('');
  const [executionMode, setExecutionMode] = useState<string>(
    features.allow_dry_run ? 'dry_run' : 'live'
  );
  const [operations, setOperations] = useState<number>(load_testing.default_operations || 1000);
  const [stalenessSeconds, setStalenessSeconds] = useState<number>(
    load_testing.default_staleness_seconds || 15
  );
  const [autoTeardown, setAutoTeardown] = useState<boolean>(true);
  const [stagingBucket, setStagingBucket] = useState<string>('');

  const [saving, setSaving] = useState<boolean>(false);
  const [savingAndRunning, setSavingAndRunning] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const projectOptions: SelectOption[] = useMemo(() => {
    if (!projects || projects.length === 0) return [];
    return projects.map((p) => ({
      value: p.project_id,
      label: p.display_name,
      sublabel: p.project_id,
      badge: p.project_id === activeProject ? 'Active Project' : undefined,
    }));
  }, [projects, activeProject]);

  // Ensure executionMode switches to 'live' if dry run is disabled
  useEffect(() => {
    if (!features.allow_dry_run && executionMode === 'dry_run') {
      setExecutionMode('live');
    }
  }, [features.allow_dry_run, executionMode]);

  // Sync state on open or when editingCampaign changes
  useEffect(() => {
    if (!open) return;
    setErrorMessage(null);

    if (editingCampaign) {
      setName(editingCampaign.name || '');
      setDescription(editingCampaign.description || '');
      setProjectId(editingCampaign.project_id || activeProject || (projects[0]?.project_id ?? ''));
      const targetMode = editingCampaign.execution_mode || (features.allow_dry_run ? 'dry_run' : 'live');
      setExecutionMode(!features.allow_dry_run && targetMode === 'dry_run' ? 'live' : targetMode);
      setOperations(editingCampaign.operations || 1000);
      setStalenessSeconds(editingCampaign.staleness_seconds || 15);
      setAutoTeardown(editingCampaign.auto_teardown !== false);
      setStagingBucket(editingCampaign.staging_bucket || '');

      if (editingCampaign.spanner_config && editingCampaign.spanner_config !== selectedSpannerConfig) {
        onSelectSpannerConfig(editingCampaign.spanner_config);
      }
      const testRegions =
        editingCampaign.client_regions || Object.keys(editingCampaign.region_results || {});
      if (testRegions.length > 0) {
        onSelectClientRegions(testRegions);
      }
    } else {
      const cfg = selectedSpannerConfig || (allConfigs.length > 0 ? allConfigs[0].configname : '');
      if (cfg && !selectedSpannerConfig) {
        onSelectSpannerConfig(cfg);
      }
      setName(cfg ? `Benchmark (${cfg})` : 'New Latency Benchmark');
      setDescription('');
      setProjectId(activeProject || (projects[0]?.project_id ?? ''));
      setExecutionMode(features.allow_dry_run ? 'dry_run' : 'live');
      setOperations(load_testing.default_operations || 1000);
      setStalenessSeconds(load_testing.default_staleness_seconds || 15);
      setAutoTeardown(true);
      setStagingBucket('');
    }
  }, [open, editingCampaign, activeProject, projects, features.allow_dry_run]);

  // When Spanner config changes, auto-update default name if not user-customized
  const handleConfigChange = (newConfig: string) => {
    onSelectSpannerConfig(newConfig);
    if (!editingCampaign && (!name || name.startsWith('Benchmark ('))) {
      setName(`Benchmark (${newConfig})`);
    }
  };

  const selectedConfigDetail = useMemo(() => {
    return allConfigs.find((c) => c.configname === selectedSpannerConfig);
  }, [allConfigs, selectedSpannerConfig]);

  const defaultLeader = useMemo(() => {
    return selectedConfigDetail?.leader_region || '';
  }, [selectedConfigDetail]);

  const activeLeader = useMemo(() => {
    if (currentLeader) return currentLeader;
    if (editingCampaign?.leader_region && editingCampaign.spanner_config === selectedSpannerConfig) {
      return editingCampaign.leader_region;
    }
    return defaultLeader;
  }, [currentLeader, editingCampaign, defaultLeader, selectedSpannerConfig]);

  const candidateRwReplicas = useMemo(() => {
    if (!selectedConfigDetail) return [];
    return (selectedConfigDetail.replicas || []).filter(
      (r) => r.replica_type === 'leader' || r.replica_type === 'r/w replica' || r.is_leader
    );
  }, [selectedConfigDetail]);

  const provisionedOptionalReplicas = useMemo(() => {
    if (editingCampaign?.optional_replicas !== undefined) {
      return editingCampaign.optional_replicas;
    }
    const cfg = allConfigs.find((c) => c.configname === selectedSpannerConfig);
    const allOptional = (cfg?.replicas || [])
      .filter((r) => r.replica_type === 'optional read only')
      .map((r) => r.region);
    return selectedOptionalReplicasMap?.[selectedSpannerConfig] ?? allOptional;
  }, [editingCampaign, selectedOptionalReplicasMap, selectedSpannerConfig, allConfigs]);

  // Group client regions by continent for display
  const groupedRegions = useMemo(() => {
    const map: Record<string, ClientRegionInfo[]> = {};
    for (const r of allClientRegions) {
      const c = r.continent || 'Other';
      if (!map[c]) map[c] = [];
      map[c].push(r);
    }
    return map;
  }, [allClientRegions]);

  const handleSelectAllInContinent = (continent: string) => {
    const regs = groupedRegions[continent] || [];
    const regIds = regs.map((r) => r.region);
    const combined = Array.from(new Set([...selectedClientRegions, ...regIds]));
    onSelectClientRegions(combined);
  };

  const handleSave = async (shouldRun: boolean) => {
    setErrorMessage(null);

    if (!selectedSpannerConfig) {
      setErrorMessage('Please select a target Spanner instance configuration.');
      return;
    }
    if (selectedClientRegions.length === 0) {
      setErrorMessage('Please select at least one client region for benchmark execution.');
      return;
    }
    if (!projectId.trim()) {
      setErrorMessage('Google Cloud Project is mandatory. Please select or enter a valid GCP project.');
      return;
    }
    if (!name.trim()) {
      setErrorMessage('Please provide a benchmark test name.');
      return;
    }

    const payload: BenchmarkCampaignConfig = {
      name: name.trim(),
      description: description.trim(),
      project_id: projectId.trim(),
      spanner_config: selectedSpannerConfig,
      leader_region: activeLeader || undefined,
      client_regions: selectedClientRegions,
      operations: Number(operations),
      staleness_seconds: Number(stalenessSeconds),
      execution_mode: executionMode,
      auto_teardown: autoTeardown,
      staging_bucket: stagingBucket.trim() || undefined,
      optional_replicas: provisionedOptionalReplicas,
    };

    if (shouldRun) {
      setSavingAndRunning(true);
    } else {
      setSaving(true);
    }

    try {
      let result: BenchmarkCampaignStatus;
      if (editingCampaign) {
        result = await updateCampaign(editingCampaign.campaign_id, payload);
      } else {
        result = await createCampaign(payload);
      }
      onSaved(result, shouldRun);
    } catch (err: any) {
      console.error('Failed to save benchmark configuration', err);
      setErrorMessage(
        err?.response?.data?.detail || err.message || 'Failed to save benchmark test.'
      );
    } finally {
      setSaving(false);
      setSavingAndRunning(false);
    }
  };

  if (!open) return null;

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        backgroundColor: '#ffffff',
      }}
    >
      {/* Header */}
      <Box
        sx={{
          px: 2.5,
          py: 1.5,
          borderBottom: '1px solid #e2e8f0',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          backgroundColor: '#faf5ff',
        }}
      >
        <Stack direction="row" spacing={1} alignItems="center">
          <SpeedIcon sx={{ color: '#7c3aed', fontSize: 22 }} />
          <Box>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, color: '#581c87', lineHeight: 1.2 }}>
              {editingCampaign ? 'Edit Latency Benchmark' : 'Create Latency Benchmark'}
            </Typography>
            <Typography variant="caption" sx={{ color: '#7e22ce' }}>
              {editingCampaign
                ? `Editing: ${editingCampaign.name || editingCampaign.campaign_id}`
                : 'Configure Spanner target and client locations'}
            </Typography>
          </Box>
        </Stack>
        <Tooltip title="Close side pane">
          <IconButton size="small" onClick={onClose} sx={{ color: '#7c3aed' }}>
            <CloseIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Box>

      {/* Form Body */}
      <Box sx={{ flexGrow: 1, overflowY: 'auto', p: 2.5 }}>
        {errorMessage && (
          <Alert severity="error" onClose={() => setErrorMessage(null)} sx={{ mb: 2 }}>
            {errorMessage}
          </Alert>
        )}

        <Stack spacing={2.5}>
          {/* Target Spanner Configuration */}
          <Box>
            <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.secondary', textTransform: 'uppercase', display: 'block', mb: 0.75 }}>
              1. Target Spanner Configuration
            </Typography>
            <FormControl fullWidth size="small">
              <Select
                value={selectedSpannerConfig}
                onChange={(e) => handleConfigChange(e.target.value)}
                displayEmpty
                renderValue={(selected) => {
                  if (loadingConfigs && allConfigs.length === 0) {
                    return (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <CircularProgress size={16} />
                        <Typography variant="body2" color="text.secondary">
                          Loading Spanner configurations...
                        </Typography>
                      </Box>
                    );
                  }
                  if (!selected) return <span style={{ color: '#94a3b8' }}>Select a Spanner configuration</span>;
                  return (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <DnsIcon sx={{ fontSize: 16, color: '#1a73e8' }} />
                      <Typography variant="body2" sx={{ fontWeight: 600 }}>
                        {selected}
                      </Typography>
                    </Box>
                  );
                }}
              >
                {loadingConfigs && allConfigs.length === 0 ? (
                  <MenuItem disabled value="">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, py: 0.5 }}>
                      <CircularProgress size={16} />
                      <Typography variant="body2" color="text.secondary">
                        Loading Spanner configurations...
                      </Typography>
                    </Box>
                  </MenuItem>
                ) : (
                  allConfigs.map((cfg) => (
                    <MenuItem key={cfg.configname} value={cfg.configname}>
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%', py: 0.25 }}>
                        <Typography variant="body2" sx={{ fontWeight: 500 }}>
                          {cfg.configname}
                        </Typography>
                        <Chip
                          label={cfg.instancetype}
                          size="small"
                          sx={{
                            height: 18,
                            fontSize: '0.62rem',
                            backgroundColor:
                              cfg.instancetype === 'multi-region'
                                ? '#e8f0fe'
                                : cfg.instancetype === 'dual-region'
                                ? '#fef7e0'
                                : '#f1f3f4',
                            color:
                              cfg.instancetype === 'multi-region'
                                ? '#1a73e8'
                                : cfg.instancetype === 'dual-region'
                                ? '#b06000'
                                : '#5f6368',
                          }}
                        />
                      </Box>
                    </MenuItem>
                  ))
                )}
              </Select>
            </FormControl>

            {selectedConfigDetail && (
              <Paper
                variant="outlined"
                sx={{
                  p: 1.5,
                  mt: 1,
                  backgroundColor: '#f8fafc',
                  borderColor: '#e2e8f0',
                  borderRadius: 1.5,
                }}
              >
                <Stack spacing={1}>
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      flexWrap: 'wrap',
                      gap: 1,
                    }}
                  >
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                      <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600 }}>
                        Leader Region:
                      </Typography>
                      <Chip
                        label={activeLeader || 'Auto'}
                        size="small"
                        sx={{
                          height: 22,
                          fontSize: '0.75rem',
                          fontWeight: 700,
                          backgroundColor: '#fee2e2',
                          color: '#dc2626',
                          border: '1px solid #fca5a5',
                        }}
                      />
                    </Box>
                    {candidateRwReplicas.length > 1 && onLeaderChange && (
                      <FormControl size="small" sx={{ minWidth: 150 }}>
                        <Select
                          value={activeLeader}
                          onChange={(e) => onLeaderChange(e.target.value)}
                          sx={{ height: 26, fontSize: '0.75rem', backgroundColor: '#ffffff' }}
                        >
                          {candidateRwReplicas.map((rep) => (
                            <MenuItem key={rep.region} value={rep.region} sx={{ fontSize: '0.75rem' }}>
                              {rep.region} {rep.region === defaultLeader ? '(Default)' : ''}
                            </MenuItem>
                          ))}
                        </Select>
                      </FormControl>
                    )}
                  </Box>
                  <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
                    <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                      Replicas: <b>{selectedConfigDetail.total_replicas}</b>
                    </Typography>
                    <Typography variant="caption" sx={{ color: 'text.secondary' }}>
                      &bull; Continent: <b>{selectedConfigDetail.continentregion}</b>
                    </Typography>
                  </Stack>
                  {provisionedOptionalReplicas.length > 0 && (
                    <Box sx={{ mt: 0.5 }}>
                      <Chip
                        size="small"
                        icon={<DnsIcon sx={{ fontSize: '14px !important' }} />}
                        label={`Custom Config: ${provisionedOptionalReplicas.length} optional read-only replica(s) provisioned (${provisionedOptionalReplicas.join(', ')})`}
                        sx={{
                          backgroundColor: '#e6f4ea',
                          color: '#137333',
                          fontWeight: 600,
                          fontSize: '0.72rem',
                          height: 24,
                        }}
                      />
                    </Box>
                  )}
                </Stack>
              </Paper>
            )}
          </Box>

          <Divider />

          {/* Test Name & Description */}
          <Box>
            <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.secondary', textTransform: 'uppercase', display: 'block', mb: 0.75 }}>
              2. Benchmark Identification
            </Typography>
            <Stack spacing={1.5}>
              <TextField
                fullWidth
                size="small"
                label="Benchmark Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. EU vs US Latency Test"
                required
              />
              <TextField
                fullWidth
                size="small"
                label="Description (optional)"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                multiline
                rows={2}
                placeholder="Notes on the benchmark scenario or hypotheses..."
              />
              <DynamicSelect
                label="Google Cloud Project"
                value={projectId}
                onChange={(val) => setProjectId(val)}
                options={projectOptions}
                placeholder="Select or enter GCP Project ID..."
                required
                loading={loadingProjects}
                onRefresh={onRefreshProjects}
                helperText="Required for Spanner instance provisioning and IAM checks"
                error={!projectId}
              />
            </Stack>
          </Box>

          <Divider />

          {/* Client Locations (GCE) */}
          <Box>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 0.75 }}>
              <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.secondary', textTransform: 'uppercase' }}>
                3. Client Locations ({selectedClientRegions.length} selected)
              </Typography>
              {selectedClientRegions.length > 0 && (
                <Button
                  size="small"
                  onClick={onClearClientRegions}
                  sx={{ p: 0, minWidth: 'auto', fontSize: '0.72rem', textTransform: 'none', color: '#7c3aed' }}
                >
                  Clear all
                </Button>
              )}
            </Box>

            <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mb: 1 }}>
              Client VMs in these regions measure read and write latency against Spanner. Visible as purple pins on the map.
            </Typography>

            {/* Selected Chips */}
            {selectedClientRegions.length > 0 && (
              <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 1.5, p: 1, backgroundColor: '#faf5ff', borderRadius: 1.5, border: '1px solid #e9d5ff' }}>
                {selectedClientRegions.map((reg) => (
                  <Chip
                    key={reg}
                    label={reg}
                    size="small"
                    onDelete={() => onToggleClientRegion(reg)}
                    sx={{
                      height: 22,
                      fontSize: '0.7rem',
                      fontWeight: 600,
                      backgroundColor: '#7c3aed',
                      color: '#ffffff',
                      '& .MuiChip-deleteIcon': { color: '#ffffff' },
                    }}
                  />
                ))}
              </Box>
            )}

            {/* Continent-grouped region selection */}
            <Paper variant="outlined" sx={{ maxHeight: 220, overflowY: 'auto', p: 1, backgroundColor: '#f8fafc', borderColor: '#e2e8f0' }}>
              {loadingClientRegions && allClientRegions.length === 0 ? (
                <Box
                  sx={{
                    p: 3,
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    gap: 1,
                  }}
                >
                  <CircularProgress size={22} sx={{ color: '#7c3aed' }} />
                  <Typography variant="caption" color="text.secondary">
                    Loading client regions...
                  </Typography>
                </Box>
              ) : Object.keys(groupedRegions).length === 0 ? (
                <Box sx={{ p: 2, textAlign: 'center' }}>
                  <Typography variant="caption" color="text.secondary">
                    No client regions available
                  </Typography>
                </Box>
              ) : (
                Object.keys(groupedRegions).map((continent) => {
                  const regs = groupedRegions[continent];
                  return (
                    <Box key={continent} sx={{ mb: 1.5, '&:last-child': { mb: 0 } }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 0.5, py: 0.25 }}>
                        <Stack direction="row" spacing={0.5} alignItems="center">
                          <PublicIcon sx={{ fontSize: 14, color: '#7c3aed' }} />
                          <Typography variant="caption" sx={{ fontWeight: 700, color: '#475569' }}>
                            {continent}
                          </Typography>
                        </Stack>
                        <Button
                          size="small"
                          onClick={() => handleSelectAllInContinent(continent)}
                          sx={{ p: 0, minWidth: 'auto', fontSize: '0.65rem', textTransform: 'none', color: '#7c3aed' }}
                        >
                          Select all
                        </Button>
                      </Box>
                      <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))', gap: 0.5, mt: 0.5 }}>
                        {regs.map((reg) => {
                          const isSelected = selectedClientRegions.includes(reg.region);
                          return (
                            <Box
                              key={reg.region}
                              onClick={() => onToggleClientRegion(reg.region)}
                              sx={{
                                p: 0.5,
                                px: 0.75,
                                borderRadius: 1,
                                border: '1px solid',
                                borderColor: isSelected ? '#c084fc' : '#e2e8f0',
                                backgroundColor: isSelected ? '#f3e8ff' : '#ffffff',
                                cursor: 'pointer',
                                display: 'flex',
                                alignItems: 'center',
                                gap: 0.5,
                                userSelect: 'none',
                                '&:hover': {
                                  backgroundColor: isSelected ? '#ede9fe' : '#f1f5f9',
                                },
                              }}
                            >
                              <Checkbox
                                size="small"
                                checked={isSelected}
                                sx={{ p: 0, color: '#a855f7', '&.Mui-checked': { color: '#7c3aed' } }}
                              />
                              <Box sx={{ minWidth: 0, flexGrow: 1 }}>
                                <Typography variant="caption" sx={{ display: 'block', fontWeight: isSelected ? 700 : 500, fontSize: '0.72rem', color: isSelected ? '#581c87' : 'text.primary', lineHeight: 1.1 }}>
                                  {reg.region}
                                </Typography>
                                <Typography variant="caption" sx={{ display: 'block', color: 'text.secondary', fontSize: '0.64rem', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                                  {reg.name}
                                </Typography>
                              </Box>
                            </Box>
                          );
                        })}
                      </Box>
                    </Box>
                  );
                })
              )}
            </Paper>
          </Box>

          <Divider />

          {/* Execution Mode & Workload Profile */}
          <Box>
            <Typography variant="caption" sx={{ fontWeight: 700, color: 'text.secondary', textTransform: 'uppercase', display: 'block', mb: 0.75 }}>
              4. Execution Mode & Workload Profile
            </Typography>

            <RadioGroup
              value={executionMode}
              onChange={(e) => setExecutionMode(e.target.value)}
              sx={{ mb: 1.5 }}
            >
              {features.allow_dry_run && (
                <Paper
                  variant="outlined"
                  sx={{
                    p: 1.25,
                    mb: 1,
                    borderRadius: 1.5,
                    borderColor: executionMode === 'dry_run' ? '#7c3aed' : '#e2e8f0',
                    backgroundColor: executionMode === 'dry_run' ? '#faf5ff' : '#ffffff',
                  }}
                >
                  <FormControlLabel
                    value="dry_run"
                    control={<Radio size="small" sx={{ color: '#7c3aed', '&.Mui-checked': { color: '#7c3aed' } }} />}
                    label={
                      <Box>
                        <Stack direction="row" spacing={1} alignItems="center">
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            Simulated Dry Run
                          </Typography>
                          <Chip label="Zero GCP Cost" size="small" color="success" sx={{ height: 18, fontSize: '0.62rem', fontWeight: 600 }} />
                        </Stack>
                        <Typography variant="caption" color="text.secondary">
                          Fast iteration using real Google Cloud network latency topology models without cloud billing.
                        </Typography>
                      </Box>
                    }
                    sx={{ m: 0, width: '100%' }}
                  />
                </Paper>
              )}

              <Paper
                variant="outlined"
                sx={{
                  p: 1.25,
                  borderRadius: 1.5,
                  borderColor: executionMode === 'live' ? '#7c3aed' : '#e2e8f0',
                  backgroundColor: executionMode === 'live' ? '#faf5ff' : '#ffffff',
                }}
              >
                <FormControlLabel
                  value="live"
                  control={<Radio size="small" sx={{ color: '#7c3aed', '&.Mui-checked': { color: '#7c3aed' } }} />}
                  label={
                    <Box>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Typography variant="body2" sx={{ fontWeight: 600 }}>
                          Live Cloud Provisioning
                        </Typography>
                        <Chip label="Real GCP" size="small" color="warning" sx={{ height: 18, fontSize: '0.62rem', fontWeight: 600 }} />
                      </Stack>
                      <Typography variant="caption" color="text.secondary">
                        Provisions real GCE e2-standard-4 instances and Cloud Spanner instance in your project.
                      </Typography>
                    </Box>
                  }
                  sx={{ m: 0, width: '100%' }}
                />
              </Paper>
            </RadioGroup>

            {/* Advanced Settings (Collapsed by default) */}
            <Accordion
              disableGutters
              elevation={0}
              sx={{
                mt: 2.5,
                border: '1px solid #e2e8f0',
                borderRadius: '8px !important',
                '&:before': { display: 'none' },
                backgroundColor: '#f8fafc',
                overflow: 'hidden',
              }}
            >
              <AccordionSummary
                expandIcon={<ExpandMoreIcon sx={{ fontSize: 20 }} />}
                sx={{
                  minHeight: 44,
                  px: 2,
                  '& .MuiAccordionSummary-content': { my: 0.5, display: 'flex', alignItems: 'center', gap: 1 },
                  '&:hover': { backgroundColor: '#f1f5f9' },
                }}
              >
                <TuneIcon sx={{ fontSize: 18, color: '#64748b' }} />
                <Typography variant="body2" sx={{ fontWeight: 600, color: '#334155' }}>
                  Advanced Settings
                </Typography>
                <Typography variant="caption" color="text.secondary" sx={{ ml: 0.5 }}>
                  ({operations} ops &bull; {stalenessSeconds}s staleness)
                </Typography>
              </AccordionSummary>
              <AccordionDetails sx={{ p: 2, pt: 1.5, backgroundColor: '#ffffff', borderTop: '1px solid #e2e8f0' }}>
                <Stack spacing={2}>
                  <Stack direction="row" spacing={2}>
                    <TextField
                      label="Target Operations"
                      type="number"
                      size="small"
                      fullWidth
                      value={operations}
                      onChange={(e) => setOperations(Math.max(10, parseInt(e.target.value) || 10))}
                      InputProps={{ inputProps: { min: 10, max: 10000 } }}
                      helperText="ops per client VM"
                    />
                    <TextField
                      label="Staleness (seconds)"
                      type="number"
                      size="small"
                      fullWidth
                      value={stalenessSeconds}
                      onChange={(e) => setStalenessSeconds(Math.max(1, parseInt(e.target.value) || 15))}
                      InputProps={{ inputProps: { min: 1, max: 60 } }}
                      helperText="Max staleness for reads"
                    />
                  </Stack>

                  <TextField
                    label="GCS Staging Bucket (Optional)"
                    size="small"
                    fullWidth
                    placeholder="Auto-Detect (e.g. gs://{project_id}-dbexplorer-staging)"
                    value={stagingBucket}
                    onChange={(e) => setStagingBucket(e.target.value)}
                    helperText="Bucket used to stage the runner JAR to GCE VMs via Google Private Access. If left empty, the tool will reuse an existing project bucket or create one."
                  />

                  {executionMode === 'live' && (
                    <FormControlLabel
                      control={
                        <Checkbox
                          size="small"
                          checked={autoTeardown}
                          onChange={(e) => setAutoTeardown(e.target.checked)}
                          sx={{ color: '#7c3aed', '&.Mui-checked': { color: '#7c3aed' } }}
                        />
                      }
                      label={
                        <Typography variant="body2" sx={{ fontSize: '0.8rem', color: 'text.secondary' }}>
                          Auto-teardown Spanner resources immediately upon completion (avoids ongoing charges)
                        </Typography>
                      }
                    />
                  )}
                </Stack>
              </AccordionDetails>
            </Accordion>
          </Box>
        </Stack>
      </Box>

      {/* Sticky Bottom Actions */}
      <Box
        sx={{
          p: 2,
          px: 2.5,
          borderTop: '1px solid #e2e8f0',
          backgroundColor: '#fafafa',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 1.5,
        }}
      >
        <Button
          variant="outlined"
          color="inherit"
          size="small"
          onClick={onClose}
          disabled={saving || savingAndRunning}
          sx={{ textTransform: 'none' }}
        >
          Cancel
        </Button>

        <Stack direction="row" spacing={1}>
          <Button
            variant="outlined"
            size="small"
            startIcon={saving ? <CircularProgress size={14} color="inherit" /> : <SaveIcon />}
            onClick={() => handleSave(false)}
            disabled={saving || savingAndRunning}
            sx={{
              textTransform: 'none',
              fontWeight: 600,
              color: '#7c3aed',
              borderColor: '#c084fc',
              '&:hover': { borderColor: '#7c3aed', backgroundColor: '#faf5ff' },
            }}
          >
            {saving ? 'Saving...' : 'Save Benchmark'}
          </Button>

          <Button
            variant="contained"
            size="small"
            startIcon={savingAndRunning ? <CircularProgress size={14} color="inherit" /> : <PlayArrowIcon />}
            onClick={() => handleSave(true)}
            disabled={saving || savingAndRunning}
            sx={{
              textTransform: 'none',
              fontWeight: 600,
              backgroundColor: '#7c3aed',
              color: '#ffffff',
              boxShadow: 'none',
              '&:hover': { backgroundColor: '#6d28d9', boxShadow: 'none' },
            }}
          >
            {savingAndRunning ? 'Launching...' : 'Save & Run'}
          </Button>
        </Stack>
      </Box>
    </Box>
  );
};
