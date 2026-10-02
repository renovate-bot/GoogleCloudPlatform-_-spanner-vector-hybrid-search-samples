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

import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
  Box,
  AppBar,
  Toolbar,
  Typography,
  Button,
  IconButton,
  Chip,
  Stack,
  Drawer,
  Alert,
  Snackbar,
  Tooltip,
  Tab,
  Tabs,
} from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import TerminalIcon from '@mui/icons-material/Terminal';
import RefreshIcon from '@mui/icons-material/Refresh';
import CloudQueueIcon from '@mui/icons-material/CloudQueue';
import SpeedIcon from '@mui/icons-material/Speed';
import MapIcon from '@mui/icons-material/Map';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';

import { ConfigTreeNav } from './components/ConfigTreeNav';
import { MapView } from './components/MapView';
import { ConfigDetailsTable } from './components/ConfigDetailsTable';
import { CliExporterModal } from './components/CliExporterModal';
import { BenchmarkConfigPane } from './components/BenchmarkConfigPane';
import { BenchmarkManagementView } from './components/BenchmarkManagementView';
import { AiChatDrawer } from './components/AiChatDrawer';
import { AiKeyDialog } from './components/AiKeyDialog';

import { fetchConfigs, visualizeDeployments, fetchClientRegions, runCampaign, fetchGcpProjects, fetchAiStatus } from './services/api';
import { SpannerConfig, DeploymentVisualization } from './types/spanner';
import { ClientRegionInfo, BenchmarkCampaignStatus, ProjectItem } from './types/benchmark';
import { ConfigProvider, useAppConfig } from './context/ConfigContext';

const DRAWER_WIDTH = 340;

const AppContent: React.FC = () => {
  const { features } = useAppConfig();

  const [allConfigs, setAllConfigs] = useState<SpannerConfig[]>([]);
  const [selectedConfigs, setSelectedConfigs] = useState<string[]>([]);
  const [nodesMap, setNodesMap] = useState<Record<string, number>>({});
  const [leadersMap, setLeadersMap] = useState<Record<string, string>>({});
  const [selectedOptionalReplicasMap, setSelectedOptionalReplicasMap] = useState<Record<string, string[]>>({});
  const [visualization, setVisualization] = useState<DeploymentVisualization | null>(null);
  const [loadingConfigs, setLoadingConfigs] = useState<boolean>(true);
  const [loadingVisualization, setLoadingVisualization] = useState<boolean>(false);
  const [cliModalOpen, setCliModalOpen] = useState<boolean>(false);
  const [mobileDrawerOpen, setMobileDrawerOpen] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // Top-level navigation tab
  const [activeTab, setActiveTab] = useState<'topology' | 'benchmarks'>('topology');
  const [initialBenchmarkConfig, setInitialBenchmarkConfig] = useState<string | undefined>(undefined);

  // Latency benchmark side pane state
  const [benchmarkPaneOpen, setBenchmarkPaneOpen] = useState<boolean>(false);
  const [editingBenchmark, setEditingBenchmark] = useState<BenchmarkCampaignStatus | null>(null);
  const [allClientRegions, setAllClientRegions] = useState<ClientRegionInfo[]>([]);
  const [loadingClientRegions, setLoadingClientRegions] = useState<boolean>(true);
  const [selectedClientRegions, setSelectedClientRegions] = useState<string[]>([]);

  // GCP Project discovery & caching state
  const [gcpProjects, setGcpProjects] = useState<ProjectItem[]>([]);
  const [activeGcpProject, setActiveGcpProject] = useState<string | undefined>(undefined);
  const [loadingGcpProjects, setLoadingGcpProjects] = useState<boolean>(false);
  const [initialOpenCampaignId, setInitialOpenCampaignId] = useState<string | undefined>(undefined);

  // AI Assistant state
  const [aiDrawerOpen, setAiDrawerOpen] = useState<boolean>(false);
  const [aiKeyDialogOpen, setAiKeyDialogOpen] = useState<boolean>(false);
  const [hasAiKey, setHasAiKey] = useState<boolean>(false);
  const [aiModel, setAiModel] = useState<string>('gemini-3.8-flash');

  // Resizable AI Assistant Drawer width state
  const DEFAULT_AI_DRAWER_WIDTH = 460;
  const MIN_AI_DRAWER_WIDTH = 340;

  const [aiDrawerWidth, setAiDrawerWidth] = useState<number>(() => {
    try {
      const saved = localStorage.getItem('spanner_ai_drawer_width');
      if (saved) {
        const parsed = parseInt(saved, 10);
        if (!isNaN(parsed) && parsed >= MIN_AI_DRAWER_WIDTH && parsed <= 1400) {
          return parsed;
        }
      }
    } catch {
      // fallback
    }
    return DEFAULT_AI_DRAWER_WIDTH;
  });
  const [isDraggingAiDrawer, setIsDraggingAiDrawer] = useState<boolean>(false);

  const handleAiResizeStart = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsDraggingAiDrawer(true);
    const startX = e.clientX;
    const startWidth = aiDrawerWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      // Dragging to the left (decreasing clientX) expands the right-docked drawer
      const delta = startX - moveEvent.clientX;
      const maxAllowed = Math.min(window.innerWidth * 0.85, 1400);
      const newWidth = Math.max(MIN_AI_DRAWER_WIDTH, Math.min(maxAllowed, startWidth + delta));
      setAiDrawerWidth(newWidth);
    };

    const onMouseUp = () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      setIsDraggingAiDrawer(false);
      setAiDrawerWidth((current) => {
        try {
          localStorage.setItem('spanner_ai_drawer_width', String(current));
        } catch {
          // ignore
        }
        return current;
      });
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  const checkAiStatus = useCallback(async () => {
    try {
      const status = await fetchAiStatus();
      setHasAiKey(status.has_key);
      if (status.model) setAiModel(status.model);
    } catch (err) {
      console.warn('Could not fetch AI status:', err);
    }
  }, []);

  useEffect(() => {
    checkAiStatus();
  }, [checkAiStatus]);

  const handleAiApplySizing = useCallback((configName: string, nodes: number) => {
    if (configName && !selectedConfigs.includes(configName)) {
      setSelectedConfigs([configName]);
    }
    const targetConfig = configName || selectedConfigs[0] || 'eur3';
    handleNodesChange(targetConfig, nodes);
    setSuccessMessage(`Sizing calculator updated: ${targetConfig} set to ${nodes} nodes.`);
  }, [selectedConfigs]);

  const handleAiApplyBenchmark = useCallback((data: {
    spanner_config?: string;
    leader_region?: string;
    nodes?: number;
    client_regions?: string[];
    benchmark_name?: string;
    benchmark_description?: string;
    operations?: number;
    staleness_seconds?: number;
  }) => {
    if (data.spanner_config) {
      setSelectedConfigs([data.spanner_config]);
      if (data.leader_region) {
        handleLeaderChange(data.spanner_config, data.leader_region);
      }
      if (data.nodes) {
        handleNodesChange(data.spanner_config, data.nodes);
      }
    }
    if (data.client_regions && data.client_regions.length > 0) {
      setSelectedClientRegions(data.client_regions);
    }
    setBenchmarkPaneOpen(true);
    setSuccessMessage(
      data.spanner_config
        ? `Configured benchmark scenario for ${data.spanner_config}. Review parameters below!`
        : 'Applied benchmark configuration.'
    );
  }, []);

  // Load GCP projects for benchmark configuration
  const loadProjects = useCallback(async (forceRefresh = false) => {
    try {
      setLoadingGcpProjects(true);
      const res = await fetchGcpProjects(forceRefresh);
      setGcpProjects(res.projects);
      if (res.current_active_project) {
        setActiveGcpProject(res.current_active_project);
      }
    } catch (err) {
      console.error('Failed to load GCP projects:', err);
    } finally {
      setLoadingGcpProjects(false);
    }
  }, []);

  // Fetch client regions and projects when benchmarking is enabled
  useEffect(() => {
    if (features.enable_load_testing) {
      setLoadingClientRegions(true);
      fetchClientRegions()
        .then((regs) => setAllClientRegions(regs))
        .catch((err) => console.error('Failed to load client regions:', err))
        .finally(() => setLoadingClientRegions(false));
      loadProjects(false);
    } else {
      setLoadingClientRegions(false);
    }
  }, [features.enable_load_testing, loadProjects]);

  // Open Create Benchmark side pane in Topology Explorer
  const handleCreateBenchmark = useCallback(() => {
    setEditingBenchmark(null);
    setBenchmarkPaneOpen(true);
    setActiveTab('topology');
  }, []);

  // Open Edit Benchmark side pane in Topology Explorer
  const handleEditBenchmark = useCallback((campaign: BenchmarkCampaignStatus) => {
    setEditingBenchmark(campaign);
    if (campaign.spanner_config) {
      setSelectedConfigs([campaign.spanner_config]);
      if (campaign.leader_region) {
        setLeadersMap((prev) => ({
          ...prev,
          [campaign.spanner_config]: campaign.leader_region!,
        }));
      }
    }
    const testRegions =
      campaign.client_regions || Object.keys(campaign.region_results || {});
    if (testRegions.length > 0) {
      setSelectedClientRegions(testRegions);
    }
    setBenchmarkPaneOpen(true);
    setActiveTab('topology');
  }, []);

  // Callback when benchmark is saved in side pane
  const handleBenchmarkSaved = async (saved: BenchmarkCampaignStatus, shouldRun?: boolean) => {
    setBenchmarkPaneOpen(false);
    setEditingBenchmark(null);
    if (shouldRun) {
      // Immediately switch view to Latency Benchmark management tab
      setActiveTab('benchmarks');
      // Instruct BenchmarkManagementView to open the modal for this campaign
      setInitialOpenCampaignId(saved.campaign_id);
      try {
        await runCampaign(saved.campaign_id);
      } catch (err: any) {
        console.error('Failed to start benchmark run:', err);
        setErrorMessage(
          err?.response?.data?.detail || err.message || 'Failed to start benchmark run.'
        );
      }
    } else {
      setSuccessMessage(`Benchmark "${saved.name}" saved successfully.`);
    }
  };

  // Toggle single client benchmark region
  const handleToggleClientRegion = (regionId: string) => {
    setSelectedClientRegions((prev) => {
      if (prev.includes(regionId)) {
        return prev.filter((r) => r !== regionId);
      } else {
        return [...prev, regionId];
      }
    });
  };

  // Load all configurations on startup
  const loadConfigs = useCallback(async () => {
    try {
      setLoadingConfigs(true);
      const data = await fetchConfigs();
      setAllConfigs(data);
      setLoadingConfigs(false);
    } catch (err) {
      console.error('Failed to load configurations:', err);
      setErrorMessage('Failed to load Spanner configurations from backend.');
      setLoadingConfigs(false);
    }
  }, []);

  useEffect(() => {
    loadConfigs();
  }, []);

  // Update visualization whenever selected configs, node counts, or custom leaders change
  useEffect(() => {
    if (selectedConfigs.length === 0) {
      setVisualization(null);
      return;
    }

    const updateVisualization = async () => {
      try {
        setLoadingVisualization(true);
        const viz = await visualizeDeployments(
          selectedConfigs,
          nodesMap,
          leadersMap,
          selectedClientRegions
        );
        setVisualization(viz);
      } catch (err) {
        console.error('Failed to update topology visualization:', err);
        setErrorMessage('Failed to compute deployment topology for selected configurations.');
      } finally {
        setLoadingVisualization(false);
      }
    };

    updateVisualization();
  }, [selectedConfigs, nodesMap, leadersMap, selectedClientRegions]);

  // Handle configuration selection / toggle
  const handleToggleConfig = (configname: string) => {
    if (!features.allow_multiple_selections) {
      // Single selection mode
      setSelectedConfigs([configname]);
      return;
    }

    setSelectedConfigs((prev) => {
      if (prev.includes(configname)) {
        return prev.filter((c) => c !== configname);
      } else {
        return [...prev, configname];
      }
    });
  };

  // Node count changes
  const handleNodesChange = (configname: string, newNodes: number) => {
    setNodesMap((prev) => ({
      ...prev,
      [configname]: newNodes,
    }));
  };

  // Custom leader region changes
  const handleLeaderChange = (configname: string, newLeader: string) => {
    setLeadersMap((prev) => ({
      ...prev,
      [configname]: newLeader,
    }));
  };

  const handleToggleOptionalReplica = useCallback(
    (configname: string, region: string) => {
      setSelectedOptionalReplicasMap((prev) => {
        const cfg = allConfigs.find((c) => c.configname === configname);
        const allOptional = (cfg?.replicas || [])
          .filter((r) => r.replica_type === 'optional read only')
          .map((r) => r.region);
        const current = prev[configname] ?? allOptional;
        const next = current.includes(region)
          ? current.filter((r) => r !== region)
          : [...current, region];
        return {
          ...prev,
          [configname]: next,
        };
      });
    },
    [allConfigs]
  );

  const handleSelectAllOptional = useCallback(
    (configname: string) => {
      setSelectedOptionalReplicasMap((prev) => {
        const cfg = allConfigs.find((c) => c.configname === configname);
        const allOptional = (cfg?.replicas || [])
          .filter((r) => r.replica_type === 'optional read only')
          .map((r) => r.region);
        return {
          ...prev,
          [configname]: allOptional,
        };
      });
    },
    [allConfigs]
  );

  const handleDeselectAllOptional = useCallback((configname: string) => {
    setSelectedOptionalReplicasMap((prev) => ({
      ...prev,
      [configname]: [],
    }));
  }, []);

  const handleClearAll = () => {
    setSelectedConfigs([]);
    setSelectedClientRegions([]);
    setNodesMap({});
    setLeadersMap({});
    setSelectedOptionalReplicasMap({});
  };

  // Active configurations detail objects
  const activeConfigsList = useMemo(() => {
    return allConfigs.filter((c) => selectedConfigs.includes(c.configname));
  }, [allConfigs, selectedConfigs]);


  return (
    <Box sx={{ display: 'flex', height: '100vh', width: '100vw', overflow: 'hidden', backgroundColor: '#f8f9fa' }}>
      {/* Top Header Bar */}
      <AppBar
        position="fixed"
        elevation={0}
        sx={{
          zIndex: (theme) => theme.zIndex.drawer + 1,
          backgroundColor: '#ffffff',
          color: 'text.primary',
          borderBottom: '1px solid #dadce0',
        }}
      >
        <Toolbar variant="dense" sx={{ minHeight: 48, px: { xs: 1, sm: 2 } }}>
          {activeTab === 'topology' && (
            <IconButton
              color="inherit"
              edge="start"
              onClick={() => setMobileDrawerOpen(!mobileDrawerOpen)}
              sx={{ mr: 1, display: { md: 'none' } }}
            >
              <MenuIcon />
            </IconButton>
          )}

          <Stack direction="row" spacing={1.5} alignItems="center" sx={{ mr: 3 }}>
            <CloudQueueIcon sx={{ color: '#1a73e8', fontSize: 24 }} />
            <Typography variant="h6" sx={{ fontSize: '1rem', fontWeight: 600, color: '#202124', letterSpacing: -0.2 }}>
              Google Cloud Spanner
            </Typography>
          </Stack>

          {/* Top Navigation Tabs */}
          {features.enable_load_testing ? (
            <Tabs
              value={activeTab}
              onChange={(_, val) => setActiveTab(val)}
              sx={{
                flexGrow: 1,
                minHeight: 48,
                '& .MuiTab-root': {
                  minHeight: 48,
                  textTransform: 'none',
                  fontWeight: 600,
                  fontSize: '0.85rem',
                  px: 2.5,
                },
                '& .Mui-selected': {
                  color: activeTab === 'benchmarks' ? '#7c3aed' : '#1a73e8',
                },
                '& .MuiTabs-indicator': {
                  backgroundColor: activeTab === 'benchmarks' ? '#7c3aed' : '#1a73e8',
                  height: 3,
                },
              }}
            >
              <Tab
                value="topology"
                icon={<MapIcon sx={{ fontSize: 18 }} />}
                iconPosition="start"
                label="Topology Explorer"
              />
              <Tab
                value="benchmarks"
                icon={<SpeedIcon sx={{ fontSize: 18, color: activeTab === 'benchmarks' ? '#7c3aed' : undefined }} />}
                iconPosition="start"
                label="Latency Benchmarks"
              />
            </Tabs>
          ) : (
            <Box sx={{ flexGrow: 1 }} />
          )}

          <Stack direction="row" spacing={1.5} alignItems="center">
            {/* AI Assistant Button */}
            {features.enable_ai && (
              <Button
                variant={aiDrawerOpen ? 'contained' : 'outlined'}
                size="small"
                startIcon={<AutoAwesomeIcon sx={{ fontSize: 16 }} />}
                onClick={() => {
                  if (!hasAiKey) {
                    setAiKeyDialogOpen(true);
                  } else {
                    setAiDrawerOpen((prev) => !prev);
                  }
                }}
                sx={{
                  textTransform: 'none',
                  fontWeight: 600,
                  fontSize: '0.8rem',
                  backgroundColor: aiDrawerOpen ? '#7c3aed' : '#faf5ff',
                  color: aiDrawerOpen ? '#ffffff' : '#7c3aed',
                  borderColor: '#c084fc',
                  boxShadow: 'none',
                  '&:hover': {
                    backgroundColor: aiDrawerOpen ? '#6d28d9' : '#f3e8ff',
                    borderColor: '#7c3aed',
                    boxShadow: 'none',
                  },
                }}
              >
                AI Assistant
              </Button>
            )}

            <Tooltip title="Reload configurations">
              <IconButton size="small" onClick={loadConfigs} disabled={loadingConfigs}>
                <RefreshIcon fontSize="small" />
              </IconButton>
            </Tooltip>
          </Stack>
        </Toolbar>
      </AppBar>

      {/* Main Content Area */}
      {activeTab === 'topology' ? (
        <Box sx={{ display: 'flex', flexGrow: 1, pt: '48px', height: 'calc(100vh - 48px)', overflow: 'hidden', width: '100%' }}>
          {/* Left Side: Desktop Tree Navigation Drawer */}
          <Drawer
            variant="permanent"
            sx={{
              display: { xs: 'none', md: 'block' },
              width: DRAWER_WIDTH,
              flexShrink: 0,
              '& .MuiDrawer-paper': {
                width: DRAWER_WIDTH,
                boxSizing: 'border-box',
                top: 48,
                height: 'calc(100% - 48px)',
                borderRight: '1px solid #dadce0',
                backgroundColor: '#ffffff',
              },
            }}
            open
          >
            <ConfigTreeNav
              configs={allConfigs}
              selectedConfigs={selectedConfigs}
              onToggleConfig={handleToggleConfig}
              onClearAll={handleClearAll}
              allowMultiple={features.allow_multiple_selections}
              enableBenchmarking={features.enable_load_testing}
              clientRegions={allClientRegions}
              selectedClientRegions={selectedClientRegions}
              onToggleClientRegion={handleToggleClientRegion}
              onClearClientRegions={() => setSelectedClientRegions([])}
              loadingConfigs={loadingConfigs}
              loadingClientRegions={loadingClientRegions}
            />
          </Drawer>

          {/* Left Side: Mobile Responsive Drawer */}
          <Drawer
            variant="temporary"
            open={mobileDrawerOpen}
            onClose={() => setMobileDrawerOpen(false)}
            ModalProps={{ keepMounted: true }}
            sx={{
              display: { xs: 'block', md: 'none' },
              '& .MuiDrawer-paper': {
                width: DRAWER_WIDTH,
                boxSizing: 'border-box',
                backgroundColor: '#ffffff',
              },
            }}
          >
            <ConfigTreeNav
              configs={allConfigs}
              selectedConfigs={selectedConfigs}
              onToggleConfig={(cfg) => {
                handleToggleConfig(cfg);
                if (!features.allow_multiple_selections) {
                  setMobileDrawerOpen(false);
                }
              }}
              onClearAll={handleClearAll}
              allowMultiple={features.allow_multiple_selections}
              enableBenchmarking={features.enable_load_testing}
              clientRegions={allClientRegions}
              selectedClientRegions={selectedClientRegions}
              onToggleClientRegion={handleToggleClientRegion}
              onClearClientRegions={() => setSelectedClientRegions([])}
              loadingConfigs={loadingConfigs}
              loadingClientRegions={loadingClientRegions}
            />
          </Drawer>

          {/* Main Stage Canvas: Map (Top) + Details Table (Bottom) */}
          <Box
            component="main"
            sx={{
              flexGrow: 1,
              height: '100%',
              overflowY: 'auto',
              p: 2.5,
              display: 'flex',
              flexDirection: 'column',
              gap: 2.5,
            }}
          >
            {/* Active Selections Toolbar */}
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 1 }}>
              <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
                <Typography variant="body2" sx={{ fontWeight: 600, color: 'text.secondary' }}>
                  {features.allow_multiple_selections ? 'Selected Instance Configurations:' : 'Selected Instance Configuration:'}
                </Typography>
                {selectedConfigs.length === 0 ? (
                  <Typography variant="body2" sx={{ color: 'text.secondary', fontStyle: 'italic' }}>
                    None selected. Choose from the left sidebar.
                  </Typography>
                ) : (
                  selectedConfigs.map((cfg) => (
                    <Chip
                      key={cfg}
                      label={cfg}
                      size="small"
                      onDelete={() => handleToggleConfig(cfg)}
                      color="primary"
                      variant="outlined"
                      sx={{ fontWeight: 500 }}
                    />
                  ))
                )}

                {/* Selected Client Locations */}
                {features.enable_load_testing && selectedClientRegions.length > 0 && (
                  <Stack direction="row" spacing={0.8} alignItems="center" sx={{ ml: { sm: 1.5 } }}>
                    <Typography variant="body2" sx={{ fontWeight: 600, color: '#7c3aed' }}>
                      Clients:
                    </Typography>
                    {selectedClientRegions.map((reg) => (
                      <Chip
                        key={reg}
                        label={reg}
                        size="small"
                        onDelete={() => handleToggleClientRegion(reg)}
                        sx={{
                          fontWeight: 600,
                          fontSize: '0.75rem',
                          backgroundColor: '#f3e8ff',
                          color: '#7c3aed',
                          borderColor: '#c084fc',
                        }}
                        variant="outlined"
                      />
                    ))}
                  </Stack>
                )}
              </Stack>

              <Stack direction="row" spacing={1.5} alignItems="center">
                {features.enable_load_testing && (
                  <Button
                    variant="contained"
                    size="small"
                    startIcon={<SpeedIcon sx={{ fontSize: 16 }} />}
                    onClick={() => {
                      setEditingBenchmark(null);
                      setBenchmarkPaneOpen((prev) => !prev);
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
                    {benchmarkPaneOpen ? 'Close Benchmark Pane' : 'Create Benchmark'}
                  </Button>
                )}

                <Button
                  variant="outlined"
                  size="small"
                  startIcon={<TerminalIcon sx={{ fontSize: 16 }} />}
                  onClick={() => setCliModalOpen(true)}
                  disabled={selectedConfigs.length === 0}
                  sx={{
                    textTransform: 'none',
                    fontWeight: 500,
                    boxShadow: 'none',
                    '&:hover': { boxShadow: 'none' },
                  }}
                >
                  1-Click CLI & Terraform
                </Button>

                {(selectedConfigs.length > 0 || selectedClientRegions.length > 0) && (
                  <Button
                    size="small"
                    color="secondary"
                    onClick={handleClearAll}
                    sx={{ fontSize: '0.75rem', textTransform: 'none' }}
                  >
                    Clear Selections
                  </Button>
                )}
              </Stack>
            </Box>

            {/* Interactive Leaflet Map with Client VMs */}
            <Box sx={{ height: 460, flexShrink: 0, position: 'relative' }}>
              <MapView
                visualization={visualization}
                loading={loadingVisualization}
                clientRegions={selectedClientRegions}
                onLeaderChange={handleLeaderChange}
                selectedOptionalReplicasMap={selectedOptionalReplicasMap}
              />
            </Box>

            {/* Sizing & Table Section */}
            <Box sx={{ pt: 2, pb: 4 }}>
              <ConfigDetailsTable
                configs={activeConfigsList}
                nodesMap={nodesMap}
                onNodesChange={handleNodesChange}
                leadersMap={leadersMap}
                onLeaderChange={handleLeaderChange}
                selectedOptionalReplicasMap={selectedOptionalReplicasMap}
                onToggleOptionalReplica={handleToggleOptionalReplica}
                onSelectAllOptional={handleSelectAllOptional}
                onDeselectAllOptional={handleDeselectAllOptional}
                onOpenBenchmark={
                  features.enable_load_testing
                    ? (cfg, optionalReplicas) => {
                        if (cfg) {
                          setSelectedConfigs([cfg.configname]);
                          if (optionalReplicas !== undefined) {
                            setSelectedOptionalReplicasMap((prev) => ({
                              ...prev,
                              [cfg.configname]: optionalReplicas,
                            }));
                          }
                        }
                        setEditingBenchmark(null);
                        setBenchmarkPaneOpen(true);
                      }
                    : undefined
                }
              />
            </Box>
          </Box>

          {/* Right Side: Benchmark Configuration Side Pane (does not overlap map!) */}
          {features.enable_load_testing && benchmarkPaneOpen && (
            <Box
              sx={{
                width: { xs: '100%', md: 440, lg: 480 },
                flexShrink: 0,
                height: '100%',
                borderLeft: '1px solid #e2e8f0',
                backgroundColor: '#ffffff',
                boxShadow: '-4px 0 16px rgba(0, 0, 0, 0.05)',
                display: 'flex',
                flexDirection: 'column',
                overflow: 'hidden',
                zIndex: 10,
              }}
            >
              <BenchmarkConfigPane
                open={benchmarkPaneOpen}
                onClose={() => setBenchmarkPaneOpen(false)}
                editingCampaign={editingBenchmark}
                allConfigs={allConfigs}
                selectedSpannerConfig={selectedConfigs[0] || ''}
                onSelectSpannerConfig={(cfgName) => setSelectedConfigs(cfgName ? [cfgName] : [])}
                currentLeader={leadersMap[selectedConfigs[0]] || ''}
                onLeaderChange={(newLeader) => handleLeaderChange(selectedConfigs[0], newLeader)}
                allClientRegions={allClientRegions}
                selectedClientRegions={selectedClientRegions}
                onToggleClientRegion={handleToggleClientRegion}
                onSelectClientRegions={(regions) => setSelectedClientRegions(regions)}
                onClearClientRegions={() => setSelectedClientRegions([])}
                onSaved={handleBenchmarkSaved}
                projects={gcpProjects}
                activeProject={activeGcpProject}
                loadingProjects={loadingGcpProjects}
                onRefreshProjects={() => loadProjects(true)}
                loadingConfigs={loadingConfigs}
                loadingClientRegions={loadingClientRegions}
                selectedOptionalReplicasMap={selectedOptionalReplicasMap}
              />
            </Box>
          )}

          {/* Right Side: Spanner AI Assistant Chat Drawer */}
          <Box
            sx={{
              width: { xs: '100%', md: aiDrawerWidth },
              flexShrink: 0,
              height: '100%',
              borderLeft: aiDrawerOpen ? '1px solid #e2e8f0' : 'none',
              backgroundColor: '#ffffff',
              boxShadow: aiDrawerOpen ? '-4px 0 16px rgba(0, 0, 0, 0.05)' : 'none',
              display: (features.enable_ai && aiDrawerOpen) ? 'flex' : 'none',
              flexDirection: 'column',
              overflow: 'hidden',
              zIndex: 10,
              position: 'relative',
            }}
          >
            {/* Draggable left-edge handle to resize */}
            <Box
              onMouseDown={handleAiResizeStart}
              onDoubleClick={() => {
                setAiDrawerWidth(DEFAULT_AI_DRAWER_WIDTH);
                try {
                  localStorage.setItem('spanner_ai_drawer_width', String(DEFAULT_AI_DRAWER_WIDTH));
                } catch {
                  // ignore
                }
              }}
              title="Drag left/right to resize (Double-click to reset)"
              sx={{
                position: 'absolute',
                top: 0,
                bottom: 0,
                left: 0,
                width: '8px',
                cursor: 'col-resize',
                zIndex: 25,
                display: { xs: 'none', md: 'flex' },
                alignItems: 'center',
                justifyContent: 'center',
                backgroundColor: isDraggingAiDrawer ? '#c084fc' : 'transparent',
                transition: 'background-color 0.15s ease',
                '&:hover': {
                  backgroundColor: '#a855f7',
                  '& .resize-handle-bar': {
                    opacity: 1,
                    backgroundColor: '#ffffff',
                  },
                },
              }}
            >
              <Box
                className="resize-handle-bar"
                sx={{
                  width: '3px',
                  height: '40px',
                  borderRadius: '2px',
                  backgroundColor: isDraggingAiDrawer ? '#ffffff' : '#cbd5e1',
                  opacity: isDraggingAiDrawer ? 1 : 0.6,
                  transition: 'opacity 0.2s, background-color 0.2s',
                }}
              />
            </Box>

            <AiChatDrawer
              open={aiDrawerOpen}
              onClose={() => setAiDrawerOpen(false)}
              hasKey={hasAiKey}
              model={aiModel}
              onOpenKeyDialog={() => setAiKeyDialogOpen(true)}
              currentContext={{
                selected_config: selectedConfigs[0],
                nodes: nodesMap[selectedConfigs[0]] || 1,
                leader_region: leadersMap[selectedConfigs[0]],
                selected_clients: selectedClientRegions,
              }}
              onApplySizing={handleAiApplySizing}
              onApplyBenchmark={handleAiApplyBenchmark}
            />
          </Box>
        </Box>
      ) : (
        /* Dedicated Benchmark Management Workspace */
        <Box
          sx={{
            flexGrow: 1,
            pt: '48px',
            height: 'calc(100vh - 48px)',
            overflowY: 'auto',
            width: '100%',
            backgroundColor: '#f8fafc',
          }}
        >
          <BenchmarkManagementView
            allConfigs={allConfigs}
            onCreateBenchmark={handleCreateBenchmark}
            onEditBenchmark={handleEditBenchmark}
            initialConfigToCreate={initialBenchmarkConfig}
            initialClientRegions={selectedClientRegions}
            onClearInitialConfig={() => setInitialBenchmarkConfig(undefined)}
            initialOpenCampaignId={initialOpenCampaignId}
            onClearInitialOpenCampaignId={() => setInitialOpenCampaignId(undefined)}
            projects={gcpProjects}
            activeProject={activeGcpProject}
            loadingProjects={loadingGcpProjects}
            onRefreshProjects={() => loadProjects(true)}
          />
        </Box>
      )}

      {/* CLI Exporter Modal */}
      <CliExporterModal
        open={cliModalOpen}
        onClose={() => setCliModalOpen(false)}
        selectedConfigs={selectedConfigs}
        nodes={nodesMap[selectedConfigs[0]] || 1}
        nodesMap={nodesMap}
        leadersMap={leadersMap}
        selectedOptionalReplicasMap={selectedOptionalReplicasMap}
        allConfigs={allConfigs}
      />

      {/* AI Key Configuration Dialog */}
      <AiKeyDialog
        open={aiKeyDialogOpen}
        hasKey={hasAiKey}
        model={aiModel}
        onClose={() => setAiKeyDialogOpen(false)}
        onKeySaved={() => {
          checkAiStatus();
          setAiDrawerOpen(true);
        }}
        onKeyRemoved={() => {
          checkAiStatus();
        }}
      />

      {/* Success notification */}
      <Snackbar
        open={!!successMessage}
        autoHideDuration={6000}
        onClose={() => setSuccessMessage(null)}
      >
        <Alert
          severity="success"
          onClose={() => setSuccessMessage(null)}
          action={
            <Button
              color="inherit"
              size="small"
              onClick={() => {
                setSuccessMessage(null);
                setActiveTab('benchmarks');
              }}
              sx={{ fontWeight: 700, textTransform: 'none', ml: 1 }}
            >
              View in Benchmarks &rarr;
            </Button>
          }
        >
          {successMessage}
        </Alert>
      </Snackbar>

      {/* Error notification */}
      <Snackbar
        open={!!errorMessage}
        autoHideDuration={5000}
        onClose={() => setErrorMessage(null)}
      >
        <Alert severity="error" onClose={() => setErrorMessage(null)}>
          {errorMessage}
        </Alert>
      </Snackbar>
    </Box>
  );
};

export const App: React.FC = () => {
  return (
    <ConfigProvider>
      <AppContent />
    </ConfigProvider>
  );
};
