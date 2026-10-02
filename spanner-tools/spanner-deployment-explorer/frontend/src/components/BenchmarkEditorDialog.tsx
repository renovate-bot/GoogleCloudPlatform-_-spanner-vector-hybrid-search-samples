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

import React, { useState, useEffect } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Box,
  Typography,
  Chip,
  OutlinedInput,
  Stack,
  Divider,
  Alert,
  CircularProgress,
  FormControlLabel,
  Checkbox,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import SpeedIcon from '@mui/icons-material/Speed';
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
import { fetchClientRegions } from '../services/api';
import DynamicSelect, { SelectOption } from './DynamicSelect';

interface BenchmarkEditorDialogProps {
  open: boolean;
  onClose: () => void;
  onSave: (config: BenchmarkCampaignConfig) => Promise<void>;
  editingCampaign?: BenchmarkCampaignStatus | null;
  allConfigs: SpannerConfig[];
  initialSpannerConfig?: string;
  initialClientRegions?: string[];
  projects?: ProjectItem[];
  activeProject?: string;
  loadingProjects?: boolean;
  onRefreshProjects?: () => void;
  optionalReplicas?: string[];
}

export const BenchmarkEditorDialog: React.FC<BenchmarkEditorDialogProps> = ({
  open,
  onClose,
  onSave,
  editingCampaign,
  allConfigs,
  initialSpannerConfig,
  initialClientRegions,
  projects = [],
  activeProject,
  loadingProjects = false,
  onRefreshProjects,
  optionalReplicas,
}) => {
  const { features, load_testing } = useAppConfig();

  const [name, setName] = useState<string>('');
  const [description, setDescription] = useState<string>('');
  const [projectId, setProjectId] = useState<string>('');
  const [spannerConfig, setSpannerConfig] = useState<string>('');
  const [clientRegions, setClientRegions] = useState<string[]>([]);
  const [executionMode, setExecutionMode] = useState<string>(
    features.allow_dry_run ? 'dry_run' : 'live'
  );
  const [operations, setOperations] = useState<number>(load_testing.default_operations || 1000);
  const [stalenessSeconds, setStalenessSeconds] = useState<number>(
    load_testing.default_staleness_seconds || 15
  );
  const [autoTeardown, setAutoTeardown] = useState<boolean>(true);
  const [stagingBucket, setStagingBucket] = useState<string>('');

  const [allClientRegions, setAllClientRegions] = useState<ClientRegionInfo[]>([]);
  const [loadingClientRegions, setLoadingClientRegions] = useState<boolean>(true);
  const [saving, setSaving] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const projectOptions: SelectOption[] = React.useMemo(() => {
    if (!projects || projects.length === 0) return [];
    return projects.map((p) => ({
      value: p.project_id,
      label: p.display_name,
      sublabel: p.project_id,
      badge: p.project_id === activeProject ? 'Active Project' : undefined,
    }));
  }, [projects, activeProject]);

  useEffect(() => {
    setLoadingClientRegions(true);
    fetchClientRegions()
      .then((regs) => setAllClientRegions(regs))
      .catch((err) => console.error('Failed to load client regions', err))
      .finally(() => setLoadingClientRegions(false));
  }, []);

  // Ensure executionMode switches to 'live' if dry run is disabled
  useEffect(() => {
    if (!features.allow_dry_run && executionMode === 'dry_run') {
      setExecutionMode('live');
    }
  }, [features.allow_dry_run, executionMode]);

  // Initialize form when opened or when editingCampaign changes
  useEffect(() => {
    if (open) {
      setErrorMessage(null);
      if (editingCampaign) {
        setName(editingCampaign.name || '');
        setDescription(editingCampaign.description || '');
        setProjectId(editingCampaign.project_id || activeProject || (projects[0]?.project_id ?? ''));
        setSpannerConfig(editingCampaign.spanner_config || '');
        setClientRegions(editingCampaign.client_regions || Object.keys(editingCampaign.region_results || {}));
        const targetMode = editingCampaign.execution_mode || (features.allow_dry_run ? 'dry_run' : 'live');
        setExecutionMode(!features.allow_dry_run && targetMode === 'dry_run' ? 'live' : targetMode);
        setOperations(editingCampaign.operations || 1000);
        setStalenessSeconds(editingCampaign.staleness_seconds || 15);
        setAutoTeardown(editingCampaign.auto_teardown !== false);
        setStagingBucket(editingCampaign.staging_bucket || '');
      } else {
        const initialCfg = initialSpannerConfig || '';
        setSpannerConfig(initialCfg);
        setName(initialCfg ? `Benchmark (${initialCfg})` : '');
        setDescription('');
        setProjectId(activeProject || (projects[0]?.project_id ?? ''));
        setClientRegions(initialClientRegions && initialClientRegions.length > 0 ? initialClientRegions : []);
        setExecutionMode(features.allow_dry_run ? 'dry_run' : 'live');
        setOperations(load_testing.default_operations || 1000);
        setStalenessSeconds(load_testing.default_staleness_seconds || 15);
        setAutoTeardown(true);
        setStagingBucket('');
      }
    }
  }, [open, editingCampaign, initialSpannerConfig, initialClientRegions, allConfigs, activeProject, projects, features.allow_dry_run]);

  const selectedConfigDetail = allConfigs.find((c) => c.configname === spannerConfig);

  const handleConfigChange = (newConfigName: string) => {
    setSpannerConfig(newConfigName);
    if (!editingCampaign && (!name || name.startsWith('Benchmark ('))) {
      setName(`Benchmark (${newConfigName})`);
    }
  };

  const handleSave = async () => {
    if (!spannerConfig) {
      setErrorMessage('Please select a target Spanner instance configuration.');
      return;
    }
    if (clientRegions.length === 0) {
      setErrorMessage('Please select at least one client region for benchmark execution.');
      return;
    }
    if (!projectId.trim()) {
      setErrorMessage('Google Cloud Project is mandatory. Please select or enter a valid GCP project.');
      return;
    }

    try {
      setSaving(true);
      setErrorMessage(null);
      const effectiveOptionalReplicas = editingCampaign?.optional_replicas || optionalReplicas;
      await onSave({
        name: name.trim() || `Benchmark (${spannerConfig})`,
        description: description.trim(),
        project_id: projectId.trim(),
        spanner_config: spannerConfig,
        client_regions: clientRegions,
        operations: Number(operations) || 500,
        staleness_seconds: Number(stalenessSeconds) || 15,
        execution_mode: executionMode,
        auto_teardown: autoTeardown,
        staging_bucket: stagingBucket.trim() || undefined,
        optional_replicas: effectiveOptionalReplicas,
      });
      onClose();
    } catch (err: any) {
      setErrorMessage(err?.response?.data?.detail || err.message || 'Failed to save benchmark');
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={saving ? undefined : onClose}
      maxWidth="md"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: 2,
          p: 1,
        },
      }}
    >
      <DialogTitle sx={{ pb: 1 }}>
        <Stack direction="row" spacing={1.5} alignItems="center">
          <SpeedIcon sx={{ color: '#7c3aed', fontSize: 28 }} />
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 700 }}>
              {editingCampaign ? 'Edit Benchmark Test' : 'Create Latency Benchmark Test'}
            </Typography>
            <Typography variant="caption" color="text.secondary">
              Configure distributed multi-region latency load testing with Direct Access
            </Typography>
          </Box>
        </Stack>
      </DialogTitle>

      <DialogContent dividers>
        {errorMessage && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {errorMessage}
          </Alert>
        )}

        <Stack spacing={2.5}>
          {/* Test Name & Description */}
          <Box sx={{ display: 'flex', gap: 2, flexDirection: { xs: 'column', sm: 'row' }, alignItems: 'flex-start' }}>
            <Box sx={{ flex: 1, width: '100%', pt: 3.5 }}>
              <TextField
                fullWidth
                size="small"
                label="Benchmark Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. US & Europe Multi-Region Evaluation"
                helperText="Descriptive label for this benchmark test"
              />
            </Box>
            <Box sx={{ flex: 1, width: '100%' }}>
              <DynamicSelect
                label="Google Cloud Project"
                value={projectId}
                onChange={(val) => setProjectId(val)}
                options={projectOptions}
                placeholder="Select or enter GCP Project ID..."
                required
                loading={loadingProjects}
                onRefresh={onRefreshProjects}
                helperText="Target project to host test instance and client VMs"
                error={!projectId}
              />
            </Box>
          </Box>

          <TextField
            fullWidth
            size="small"
            label="Description / Notes (Optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="e.g. Testing paxos write latency vs local stale read performance"
          />

          <Divider />

          {/* Target Spanner Configuration */}
          <Box>
            <Typography variant="subtitle2" sx={{ fontWeight: 700, mb: 1, color: '#1e293b' }}>
              Target Spanner Configuration
            </Typography>
            <FormControl fullWidth size="small">
              <InputLabel id="editor-spanner-config-label">Spanner Configuration</InputLabel>
              <Select
                labelId="editor-spanner-config-label"
                label="Spanner Configuration"
                value={spannerConfig}
                onChange={(e) => handleConfigChange(e.target.value as string)}
                displayEmpty
              >
                {allConfigs.length === 0 ? (
                  <MenuItem value="" disabled>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, py: 0.5 }}>
                      <CircularProgress size={16} />
                      <Typography variant="body2" color="text.secondary">
                        Loading configurations...
                      </Typography>
                    </Box>
                  </MenuItem>
                ) : (
                  <MenuItem value="" disabled>
                    <Typography variant="body2" color="text.secondary">
                      Select a Spanner configuration...
                    </Typography>
                  </MenuItem>
                )}
                {allConfigs.map((cfg) => (
                  <MenuItem key={cfg.configname} value={cfg.configname}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <strong>{cfg.configname}</strong>
                      <Chip
                        size="small"
                        label={cfg.instancetype}
                        sx={{ fontSize: '0.68rem', height: 18 }}
                      />
                      <Typography variant="caption" color="text.secondary">
                        ({cfg.continentregion} &bull; Leader: {cfg.leader_region || 'Auto'})
                      </Typography>
                    </Box>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>

            {selectedConfigDetail && (
              <Box sx={{ mt: 1, p: 1.5, backgroundColor: '#f8fafc', borderRadius: 1.5, border: '1px solid #e2e8f0' }}>
                <Stack direction="row" spacing={2} flexWrap="wrap">
                  <Typography variant="caption" color="text.secondary">
                    Leader Region: <b>{selectedConfigDetail.leader_region || 'Auto'}</b>
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Total Replicas: <b>{selectedConfigDetail.replicas.length}</b>
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Provisioned Nodes: <b>1</b> (Automatic backups disabled)
                  </Typography>
                </Stack>
              </Box>
            )}
          </Box>

          <Divider />

          {/* Client Regions Selection */}
          <Box>
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 700, color: '#1e293b' }}>
                Client Benchmark Locations (GCE Regions)
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {clientRegions.length} region{clientRegions.length === 1 ? '' : 's'} selected
              </Typography>
            </Box>

            <FormControl fullWidth size="small" sx={{ mb: 1.5 }}>
              <InputLabel id="editor-client-regions-label">Client Locations</InputLabel>
              <Select
                labelId="editor-client-regions-label"
                multiple
                value={clientRegions}
                onChange={(e) => setClientRegions(typeof e.target.value === 'string' ? e.target.value.split(',') : e.target.value)}
                input={<OutlinedInput label="Client Locations" />}
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {loadingClientRegions && allClientRegions.length === 0 ? (
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <CircularProgress size={14} />
                        <Typography variant="body2" color="text.secondary">Loading client regions...</Typography>
                      </Box>
                    ) : selected.length === 0 ? (
                      <Typography variant="body2" color="text.secondary">Select regions...</Typography>
                    ) : (
                      selected.map((val) => {
                        const reg = allClientRegions.find((r) => r.region === val);
                        return (
                          <Chip
                            key={val}
                            size="small"
                            label={`${val} (${reg?.name || 'GCE'})`}
                            onDelete={(e) => {
                              e.stopPropagation();
                              setClientRegions((prev) => prev.filter((r) => r !== val));
                            }}
                            onMouseDown={(e) => e.stopPropagation()}
                            sx={{ fontSize: '0.75rem' }}
                          />
                        );
                      })
                    )}
                  </Box>
                )}
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
                      {reg.region} &mdash; {reg.name} ({reg.continent})
                    </MenuItem>
                  ))
                )}
              </Select>
            </FormControl>
          </Box>

          <Divider />

          {/* Execution Mode & Workload Profile */}
          <Box>
            <Typography variant="subtitle2" sx={{ fontWeight: 700, mb: 1.5, color: '#1e293b' }}>
              Execution Mode & Workload
            </Typography>

            {features.allow_dry_run ? (
              <Box sx={{ mb: 2 }}>
                <Typography variant="caption" sx={{ fontWeight: 600, display: 'block', mb: 0.5 }}>
                  Execution Engine:
                </Typography>
                <Stack direction="row" spacing={1.5}>
                  <Button
                    size="small"
                    variant={executionMode === 'dry_run' ? 'contained' : 'outlined'}
                    onClick={() => setExecutionMode('dry_run')}
                    sx={{ textTransform: 'none', flex: 1 }}
                  >
                    ⚡ Physics Simulator (Instant / Dry-Run)
                  </Button>
                  <Button
                    size="small"
                    variant={executionMode === 'live' ? 'contained' : 'outlined'}
                    color="secondary"
                    onClick={() => setExecutionMode('live')}
                    sx={{ textTransform: 'none', flex: 1 }}
                  >
                    ☁️ Live GCE VMs (Direct Access Provisioning)
                  </Button>
                </Stack>
              </Box>
            ) : (
              <Box sx={{ mb: 2 }}>
                <Typography variant="caption" sx={{ fontWeight: 600, display: 'block', mb: 0.5 }}>
                  Execution Engine:
                </Typography>
                <Chip label="☁️ Live GCE VMs (Direct Access Provisioning)" color="secondary" size="small" variant="outlined" sx={{ fontWeight: 600 }} />
              </Box>
            )}

            {/* Advanced Settings (Collapsed by default) */}
            <Accordion
              disableGutters
              elevation={0}
              sx={{
                mt: 2,
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
                      fullWidth
                      size="small"
                      type="number"
                      label="Target Operations"
                      value={operations}
                      onChange={(e) => setOperations(Math.max(10, parseInt(e.target.value) || 100))}
                      InputProps={{ inputProps: { min: 10, max: 10000 } }}
                      helperText="ops per client VM"
                    />
                    <TextField
                      fullWidth
                      size="small"
                      type="number"
                      label="Staleness (seconds)"
                      value={stalenessSeconds}
                      onChange={(e) => setStalenessSeconds(Math.max(1, parseInt(e.target.value) || 15))}
                      InputProps={{ inputProps: { min: 1, max: 60 } }}
                      helperText="Max staleness for reads (default 15s)"
                    />
                  </Stack>

                  <TextField
                    fullWidth
                    size="small"
                    label="GCS Staging Bucket (Optional)"
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
                        <Typography variant="body2" sx={{ fontSize: '0.82rem', color: 'text.secondary' }}>
                          Auto-teardown Spanner instance immediately upon completion (avoids ongoing charges)
                        </Typography>
                      }
                    />
                  )}
                </Stack>
              </AccordionDetails>
            </Accordion>
          </Box>
        </Stack>
      </DialogContent>

      <DialogActions sx={{ p: 2 }}>
        <Button onClick={onClose} disabled={saving} sx={{ textTransform: 'none' }}>
          Cancel
        </Button>
        <Button
          variant="contained"
          onClick={handleSave}
          disabled={saving || !spannerConfig || clientRegions.length === 0}
          startIcon={saving ? <CircularProgress size={16} color="inherit" /> : <SpeedIcon />}
          sx={{
            textTransform: 'none',
            fontWeight: 600,
            backgroundColor: '#7c3aed',
            '&:hover': { backgroundColor: '#6d28d9' },
          }}
        >
          {saving ? 'Saving...' : editingCampaign ? 'Update Benchmark' : 'Save Benchmark'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
