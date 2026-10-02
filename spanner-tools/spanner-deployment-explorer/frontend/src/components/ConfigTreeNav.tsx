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

import React, { useState, useMemo } from 'react';
import {
  Box,
  Typography,
  TextField,
  InputAdornment,
  IconButton,
  Chip,
  Checkbox,
  Radio,
  FormControlLabel,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Stack,
  Button,
  Divider,
  CircularProgress,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import ClearIcon from '@mui/icons-material/Clear';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import PublicIcon from '@mui/icons-material/Public';
import DnsIcon from '@mui/icons-material/Dns';
import SpeedIcon from '@mui/icons-material/Speed';

import { SpannerConfig } from '../types/spanner';
import { ClientRegionInfo } from '../types/benchmark';

interface ConfigTreeNavProps {
  configs: SpannerConfig[];
  selectedConfigs: string[];
  onToggleConfig: (configname: string) => void;
  onClearAll: () => void;
  allowMultiple?: boolean;
  enableBenchmarking?: boolean;
  clientRegions?: ClientRegionInfo[];
  selectedClientRegions?: string[];
  onToggleClientRegion?: (regionId: string) => void;
  onClearClientRegions?: () => void;
  loadingConfigs?: boolean;
  loadingClientRegions?: boolean;
}

export const ConfigTreeNav: React.FC<ConfigTreeNavProps> = ({
  configs,
  selectedConfigs,
  onToggleConfig,
  onClearAll,
  allowMultiple = true,
  enableBenchmarking = false,
  clientRegions = [],
  selectedClientRegions = [],
  onToggleClientRegion,
  onClearClientRegions,
  loadingConfigs = false,
  loadingClientRegions = false,
}) => {
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [typeFilter, setTypeFilter] = useState<'all' | 'multi-region' | 'dual-region' | 'regional'>('all');
  const [expandedContinents, setExpandedContinents] = useState<Record<string, boolean>>({});
  const [clientPaneExpanded, setClientPaneExpanded] = useState<boolean>(false);
  const [clientSearchTerm, setClientSearchTerm] = useState<string>('');
  const [expandedClientContinents, setExpandedClientContinents] = useState<Record<string, boolean>>({});

  // Filter configs based on search and type
  const filteredConfigs = useMemo(() => {
    return configs.filter((cfg) => {
      if (typeFilter !== 'all' && cfg.instancetype !== typeFilter) {
        return false;
      }
      if (searchTerm.trim()) {
        const term = searchTerm.toLowerCase();
        const matchesName = cfg.configname?.toLowerCase().includes(term) ?? false;
        const matchesDisplay = cfg.display_name?.toLowerCase().includes(term) ?? false;
        const matchesContinent = cfg.continentregion?.toLowerCase().includes(term) ?? false;
        const matchesLeader = cfg.leader_region?.toLowerCase().includes(term) ?? false;
        const matchesReplicas = cfg.replicas?.some((r) =>
          r.region?.toLowerCase().includes(term) || r.location_name?.toLowerCase().includes(term)
        ) ?? false;
        return matchesName || matchesDisplay || matchesContinent || matchesLeader || matchesReplicas;
      }
      return true;
    });
  }, [configs, searchTerm, typeFilter]);

  // Group filtered configs: Continent -> Instance Type -> Configs
  const groupedTree = useMemo(() => {
    const map: Record<string, Record<string, SpannerConfig[]>> = {};

    for (const cfg of filteredConfigs) {
      if (!map[cfg.continentregion]) {
        map[cfg.continentregion] = {};
      }
      if (!map[cfg.continentregion][cfg.instancetype]) {
        map[cfg.continentregion][cfg.instancetype] = [];
      }
      map[cfg.continentregion][cfg.instancetype].push(cfg);
    }

    return map;
  }, [filteredConfigs]);

  const isContinentExpanded = (continent: string) => {
    if (searchTerm.trim().length > 0) {
      return expandedContinents[continent] !== undefined ? expandedContinents[continent] : true;
    }
    return !!expandedContinents[continent];
  };

  const handleToggleContinent = (continent: string) => {
    const currentExpanded = isContinentExpanded(continent);
    setExpandedContinents((prev) => ({
      ...prev,
      [continent]: !currentExpanded,
    }));
  };

  // Filter client regions based on client search
  const filteredClientRegions = useMemo(() => {
    if (!clientRegions) return [];
    const term = clientSearchTerm.toLowerCase().trim();
    if (!term) return clientRegions;
    return clientRegions.filter(
      (r) =>
        r.region.toLowerCase().includes(term) ||
        r.name.toLowerCase().includes(term) ||
        (r.continent && r.continent.toLowerCase().includes(term))
    );
  }, [clientRegions, clientSearchTerm]);

  // Group client regions by continent
  const groupedClientRegions = useMemo(() => {
    const map: Record<string, ClientRegionInfo[]> = {};
    for (const reg of filteredClientRegions) {
      const cont = reg.continent || 'Other';
      if (!map[cont]) {
        map[cont] = [];
      }
      map[cont].push(reg);
    }
    return map;
  }, [filteredClientRegions]);

  const isClientContinentExpanded = (continent: string) => {
    if (clientSearchTerm.trim().length > 0) {
      return true;
    }
    return !!expandedClientContinents[continent];
  };

  const handleToggleClientContinent = (continent: string) => {
    const currentExpanded = isClientContinentExpanded(continent);
    setExpandedClientContinents((prev) => ({
      ...prev,
      [continent]: !currentExpanded,
    }));
  };

  const getRoleBadge = (instType: string, replicaCount: number) => {
    if (instType === 'multi-region') {
      return (
        <Chip
          label={`${replicaCount} regions`}
          size="small"
          sx={{
            height: 18,
            fontSize: '0.65rem',
            backgroundColor: '#e8f0fe',
            color: '#1a73e8',
            fontWeight: 600,
          }}
        />
      );
    }
    if (instType === 'dual-region') {
      return (
        <Chip
          label="dual-region"
          size="small"
          sx={{
            height: 18,
            fontSize: '0.65rem',
            backgroundColor: '#fef7e0',
            color: '#b06000',
            fontWeight: 600,
          }}
        />
      );
    }
    return (
      <Chip
        label="regional"
        size="small"
        sx={{
          height: 18,
          fontSize: '0.65rem',
          backgroundColor: '#f1f3f4',
          color: '#5f6368',
        }}
      />
    );
  };

  const getConfigLocations = (cfg: SpannerConfig): string => {
    const seen: string[] = [];
    for (const r of cfg.replicas || []) {
      if (r.location_name && !seen.includes(r.location_name)) {
        seen.push(r.location_name);
      }
    }
    return seen.length > 0 ? seen.join(', ') : (cfg.leader_region || '');
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Search and Action Bar */}
      <Box sx={{ p: 2, pb: 1, borderBottom: '1px solid #dadce0', backgroundColor: '#ffffff' }}>
        <TextField
          fullWidth
          size="small"
          placeholder="Filter configurations or regions..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          InputProps={{
            startAdornment: (
              <InputAdornment position="start">
                <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
              </InputAdornment>
            ),
            endAdornment: searchTerm ? (
              <InputAdornment position="end">
                <IconButton size="small" onClick={() => setSearchTerm('')}>
                  <ClearIcon fontSize="small" />
                </IconButton>
              </InputAdornment>
            ) : null,
            sx: { fontSize: '0.85rem' },
          }}
        />

        {/* Type Filter Chips */}
        <Stack direction="row" spacing={0.5} sx={{ mt: 1.5, overflowX: 'auto', pb: 0.5 }}>
          <Chip
            label="All"
            size="small"
            clickable
            color={typeFilter === 'all' ? 'primary' : 'default'}
            variant={typeFilter === 'all' ? 'filled' : 'outlined'}
            onClick={() => setTypeFilter('all')}
            sx={{ fontSize: '0.72rem' }}
          />
          <Chip
            label="Multi-Region"
            size="small"
            clickable
            color={typeFilter === 'multi-region' ? 'primary' : 'default'}
            variant={typeFilter === 'multi-region' ? 'filled' : 'outlined'}
            onClick={() => setTypeFilter('multi-region')}
            sx={{ fontSize: '0.72rem' }}
          />
          <Chip
            label="Dual-Region"
            size="small"
            clickable
            color={typeFilter === 'dual-region' ? 'primary' : 'default'}
            variant={typeFilter === 'dual-region' ? 'filled' : 'outlined'}
            onClick={() => setTypeFilter('dual-region')}
            sx={{ fontSize: '0.72rem' }}
          />
          <Chip
            label="Regional"
            size="small"
            clickable
            color={typeFilter === 'regional' ? 'primary' : 'default'}
            variant={typeFilter === 'regional' ? 'filled' : 'outlined'}
            onClick={() => setTypeFilter('regional')}
            sx={{ fontSize: '0.72rem' }}
          />
        </Stack>

        {/* Selection status and clear button */}
        <Stack direction="row" spacing={1} sx={{ mt: 1.5 }} alignItems="center" justifyContent="space-between">
          <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 500 }}>
            {selectedConfigs.length} selected
          </Typography>
          {selectedConfigs.length > 0 && (
            <Button
              size="small"
              color="secondary"
              onClick={onClearAll}
              sx={{ fontSize: '0.72rem', py: 0.25, px: 0.75 }}
            >
              Clear
            </Button>
          )}
        </Stack>
      </Box>

      {/* Tree list */}
      <Box sx={{ flexGrow: 1, overflowY: 'auto', p: 1 }}>
        {loadingConfigs && configs.length === 0 ? (
          <Box
            sx={{
              p: 4,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              gap: 1.5,
            }}
          >
            <CircularProgress size={28} />
            <Typography variant="body2" color="text.secondary">
              Loading Spanner configurations...
            </Typography>
          </Box>
        ) : Object.keys(groupedTree).length === 0 ? (
          <Box sx={{ p: 3, textAlign: 'center' }}>
            <Typography variant="body2" color="text.secondary">
              {searchTerm.trim()
                ? `No configurations match "${searchTerm}"`
                : 'No configurations available'}
            </Typography>
          </Box>
        ) : (
          Object.keys(groupedTree).map((continent) => {
            const typesObj = groupedTree[continent];
            const continentTotal = Object.values(typesObj).reduce((acc, list) => acc + list.length, 0);

            return (
              <Accordion
                key={continent}
                expanded={isContinentExpanded(continent)}
                onChange={() => handleToggleContinent(continent)}
                disableGutters
                sx={{
                  mb: 1,
                  border: '1px solid #dadce0',
                  borderRadius: '6px !important',
                  '&:before': { display: 'none' },
                  boxShadow: 'none',
                }}
              >
                <AccordionSummary
                  expandIcon={<ExpandMoreIcon fontSize="small" />}
                  sx={{
                    minHeight: 40,
                    px: 1.5,
                    backgroundColor: '#f8f9fa',
                    '& .MuiAccordionSummary-content': { my: 0.5 },
                  }}
                >
                  <Stack direction="row" spacing={1} alignItems="center" sx={{ width: '100%', pr: 1 }}>
                    <PublicIcon sx={{ fontSize: 16, color: '#1a73e8' }} />
                    <Typography variant="body2" sx={{ fontWeight: 600, flexGrow: 1 }}>
                      {continent}
                    </Typography>
                    <Chip label={continentTotal} size="small" sx={{ height: 18, fontSize: '0.65rem' }} />
                  </Stack>
                </AccordionSummary>

                <AccordionDetails sx={{ p: 0.5 }}>
                  {Object.keys(typesObj).map((typeKey) => {
                    const cfgs = typesObj[typeKey];
                    const typeLabel =
                      typeKey === 'multi-region'
                        ? 'Multi-Region'
                        : typeKey === 'dual-region'
                        ? 'Dual-Region'
                        : 'Regional';

                    return (
                      <Box key={typeKey} sx={{ mb: 1 }}>
                        <Box sx={{ px: 1, py: 0.5, display: 'flex', alignItems: 'center' }}>
                          <DnsIcon sx={{ fontSize: 13, color: '#5f6368', mr: 0.5 }} />
                          <Typography
                            variant="caption"
                            sx={{ fontWeight: 600, textTransform: 'uppercase', color: '#5f6368' }}
                          >
                            {typeLabel} ({cfgs.length})
                          </Typography>
                        </Box>

                        <Stack spacing={0}>
                          {cfgs.map((cfg) => {
                            const isSelected = selectedConfigs.includes(cfg.configname);

                            return (
                              <Box
                                key={cfg.configname}
                                role={allowMultiple ? 'checkbox' : 'radio'}
                                aria-checked={isSelected}
                                tabIndex={0}
                                onKeyDown={(e) => {
                                  if (e.key === ' ' || e.key === 'Enter') {
                                    e.preventDefault();
                                    onToggleConfig(cfg.configname);
                                  }
                                }}
                                sx={{
                                  display: 'flex',
                                  alignItems: 'center',
                                  justifyContent: 'space-between',
                                  px: 1,
                                  py: 0.25,
                                  borderRadius: 1,
                                  backgroundColor: isSelected ? '#e8f0fe' : 'transparent',
                                  '&:hover': {
                                    backgroundColor: isSelected ? '#d2e3fc' : '#f1f3f4',
                                  },
                                  cursor: 'pointer',
                                  userSelect: 'none',
                                }}
                                onClick={() => onToggleConfig(cfg.configname)}
                              >
                                <FormControlLabel
                                  control={
                                    allowMultiple ? (
                                      <Checkbox
                                        size="small"
                                        checked={isSelected}
                                        tabIndex={-1}
                                        sx={{ p: 0.5 }}
                                      />
                                    ) : (
                                      <Radio
                                        size="small"
                                        checked={isSelected}
                                        tabIndex={-1}
                                        sx={{ p: 0.5 }}
                                      />
                                    )
                                  }
                                  label={
                                    <Box sx={{ minWidth: 0, overflow: 'hidden' }}>
                                      <Typography variant="body2" sx={{ fontWeight: isSelected ? 600 : 400, lineHeight: 1.2 }}>
                                        {cfg.configname}
                                      </Typography>
                                      <Typography
                                        variant="caption"
                                        title={getConfigLocations(cfg)}
                                        sx={{
                                          color: 'text.secondary',
                                          fontSize: '0.7rem',
                                          display: '-webkit-box',
                                          WebkitLineClamp: 2,
                                          WebkitBoxOrient: 'vertical',
                                          overflow: 'hidden',
                                          lineHeight: 1.25,
                                        }}
                                      >
                                        {getConfigLocations(cfg)}
                                      </Typography>
                                    </Box>
                                  }
                                  sx={{ m: 0, flexGrow: 1, pointerEvents: 'none' }}
                                />

                                {getRoleBadge(cfg.instancetype, cfg.total_replicas)}
                              </Box>
                            );
                          })}
                        </Stack>
                        <Divider sx={{ my: 0.5 }} />
                      </Box>
                    );
                  })}
                </AccordionDetails>
              </Accordion>
            );
          })
        )}

        {/* Available Client Locations (when benchmarking is enabled) */}
        {enableBenchmarking && clientRegions && clientRegions.length > 0 && (
          <Box sx={{ mt: 2.5, pt: 1, borderTop: '2px dashed #cbd5e1' }}>
            <Accordion
              expanded={clientPaneExpanded}
              onChange={(_, expanded) => setClientPaneExpanded(expanded)}
              disableGutters
              sx={{
                border: '1px solid #d8b4fe',
                borderRadius: '8px !important',
                backgroundColor: clientPaneExpanded ? '#ffffff' : '#faf5ff',
                '&:before': { display: 'none' },
                boxShadow: clientPaneExpanded ? '0 2px 8px rgba(124, 58, 237, 0.08)' : 'none',
                overflow: 'hidden',
                transition: 'all 0.2s ease',
              }}
            >
              <AccordionSummary
                expandIcon={<ExpandMoreIcon sx={{ color: '#7c3aed' }} />}
                sx={{
                  px: 1.5,
                  py: 0.5,
                  minHeight: 44,
                  backgroundColor: clientPaneExpanded ? '#f5f3ff' : '#faf5ff',
                  borderBottom: clientPaneExpanded ? '1px solid #e9d5ff' : 'none',
                  '& .MuiAccordionSummary-content': {
                    my: 0.5,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                  },
                  '&:hover': {
                    backgroundColor: '#f3e8ff',
                  },
                }}
              >
                <Stack direction="row" spacing={0.75} alignItems="center">
                  <SpeedIcon sx={{ fontSize: 18, color: '#7c3aed' }} />
                  <Typography
                    variant="subtitle2"
                    sx={{
                      fontWeight: 700,
                      fontSize: '0.8rem',
                      color: '#6b21a8',
                      textTransform: 'uppercase',
                      letterSpacing: 0.5,
                    }}
                  >
                    Client Locations (GCE)
                  </Typography>
                </Stack>
                {selectedClientRegions.length > 0 && (
                  <Chip
                    label={`${selectedClientRegions.length} selected`}
                    size="small"
                    sx={{
                      mr: 1,
                      height: 20,
                      fontSize: '0.68rem',
                      fontWeight: 700,
                      backgroundColor: '#7c3aed',
                      color: '#ffffff',
                    }}
                  />
                )}
              </AccordionSummary>
              <AccordionDetails sx={{ p: 1.5, backgroundColor: '#ffffff' }}>
                {/* Search Bar for Client Locations */}
                <TextField
                  fullWidth
                  size="small"
                  placeholder="Filter client locations or regions..."
                  value={clientSearchTerm}
                  onChange={(e) => setClientSearchTerm(e.target.value)}
                  InputProps={{
                    startAdornment: (
                      <InputAdornment position="start">
                        <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                      </InputAdornment>
                    ),
                    endAdornment: clientSearchTerm ? (
                      <InputAdornment position="end">
                        <IconButton size="small" onClick={() => setClientSearchTerm('')}>
                          <ClearIcon fontSize="small" />
                        </IconButton>
                      </InputAdornment>
                    ) : null,
                    sx: { fontSize: '0.82rem' },
                  }}
                  sx={{ mb: 1.5 }}
                />

                {/* Sub-bar with selection count and Clear button */}
                <Stack
                  direction="row"
                  spacing={1}
                  sx={{ mb: 1 }}
                  alignItems="center"
                  justifyContent="space-between"
                >
                  <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 500 }}>
                    {selectedClientRegions.length} selected
                  </Typography>
                  {selectedClientRegions.length > 0 && (
                    <Button
                      size="small"
                      onClick={(e) => {
                        e.stopPropagation();
                        onClearClientRegions?.();
                      }}
                      sx={{
                        fontSize: '0.72rem',
                        py: 0.25,
                        px: 0.75,
                        minWidth: 'auto',
                        textTransform: 'none',
                        color: '#7c3aed',
                        fontWeight: 600,
                      }}
                    >
                      Clear
                    </Button>
                  )}
                </Stack>

                <Typography
                  variant="caption"
                  sx={{ color: 'text.secondary', display: 'block', mb: 1.5 }}
                >
                  Select GCE client benchmark locations to visualize on map:
                </Typography>

                {loadingClientRegions && clientRegions.length === 0 ? (
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
                      Loading client benchmark locations...
                    </Typography>
                  </Box>
                ) : Object.keys(groupedClientRegions).length === 0 ? (
                  <Box sx={{ p: 2, textAlign: 'center' }}>
                    <Typography variant="caption" color="text.secondary">
                      {clientSearchTerm.trim()
                        ? `No client locations match "${clientSearchTerm}"`
                        : 'No client locations available'}
                    </Typography>
                  </Box>
                ) : (
                  Object.keys(groupedClientRegions).map((continent) => {
                    const regs = groupedClientRegions[continent];
                    const selectedInContinent = regs.filter((r) =>
                      selectedClientRegions.includes(r.region)
                    ).length;

                    return (
                      <Accordion
                        key={`client-continent-${continent}`}
                        expanded={isClientContinentExpanded(continent)}
                        onChange={() => handleToggleClientContinent(continent)}
                        disableGutters
                        sx={{
                          mb: 1,
                          border: '1px solid #e2e8f0',
                          borderRadius: '6px !important',
                          '&:before': { display: 'none' },
                          boxShadow: 'none',
                        }}
                      >
                        <AccordionSummary
                          expandIcon={<ExpandMoreIcon fontSize="small" />}
                          sx={{
                            minHeight: 36,
                            px: 1.5,
                            backgroundColor: '#f8fafc',
                            '& .MuiAccordionSummary-content': { my: 0.5 },
                          }}
                        >
                          <Stack direction="row" spacing={1} alignItems="center" sx={{ width: '100%', pr: 1 }}>
                            <PublicIcon sx={{ fontSize: 16, color: '#7c3aed' }} />
                            <Typography variant="body2" sx={{ fontWeight: 600, flexGrow: 1, fontSize: '0.82rem' }}>
                              {continent}
                            </Typography>
                            {selectedInContinent > 0 && (
                              <Chip
                                label={selectedInContinent}
                                size="small"
                                sx={{
                                  height: 18,
                                  fontSize: '0.65rem',
                                  fontWeight: 700,
                                  backgroundColor: '#7c3aed',
                                  color: '#ffffff',
                                }}
                              />
                            )}
                            <Chip
                              label={regs.length}
                              size="small"
                              sx={{ height: 18, fontSize: '0.65rem', backgroundColor: '#e2e8f0', color: '#475569' }}
                            />
                          </Stack>
                        </AccordionSummary>
                        <AccordionDetails sx={{ px: 1, py: 0.5 }}>
                          <Stack spacing={0.25}>
                            {regs.map((reg) => {
                              const isSelected = selectedClientRegions.includes(reg.region);
                              return (
                                <Box
                                  key={reg.region}
                                  role="checkbox"
                                  aria-checked={isSelected}
                                  tabIndex={0}
                                  onKeyDown={(e) => {
                                    if (e.key === ' ' || e.key === 'Enter') {
                                      e.preventDefault();
                                      onToggleClientRegion?.(reg.region);
                                    }
                                  }}
                                  onClick={() => onToggleClientRegion?.(reg.region)}
                                  sx={{
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'space-between',
                                    px: 1,
                                    py: 0.35,
                                    borderRadius: 1,
                                    backgroundColor: isSelected ? '#f3e8ff' : 'transparent',
                                    '&:hover': {
                                      backgroundColor: isSelected ? '#ede9fe' : '#f1f5f9',
                                    },
                                    cursor: 'pointer',
                                    userSelect: 'none',
                                  }}
                                >
                                  <FormControlLabel
                                    control={
                                      <Checkbox
                                        size="small"
                                        checked={isSelected}
                                        tabIndex={-1}
                                        sx={{
                                          p: 0.5,
                                          color: '#a855f7',
                                          '&.Mui-checked': { color: '#7c3aed' },
                                        }}
                                      />
                                    }
                                    label={
                                      <Box sx={{ minWidth: 0 }}>
                                        <Typography
                                          variant="body2"
                                          sx={{
                                            fontWeight: isSelected ? 600 : 400,
                                            fontSize: '0.8rem',
                                            lineHeight: 1.2,
                                            color: isSelected ? '#581c87' : 'text.primary',
                                          }}
                                        >
                                          {reg.region}
                                        </Typography>
                                        <Typography
                                          variant="caption"
                                          sx={{ color: 'text.secondary', fontSize: '0.7rem' }}
                                        >
                                          {reg.name}
                                        </Typography>
                                      </Box>
                                    }
                                    sx={{ m: 0, flexGrow: 1, pointerEvents: 'none' }}
                                  />
                                  <Chip
                                    label="GCE"
                                    size="small"
                                    sx={{
                                      height: 18,
                                      fontSize: '0.62rem',
                                      backgroundColor: isSelected ? '#7c3aed' : '#f1f5f9',
                                      color: isSelected ? '#ffffff' : '#64748b',
                                      fontWeight: 600,
                                    }}
                                  />
                                </Box>
                              );
                            })}
                          </Stack>
                        </AccordionDetails>
                      </Accordion>
                    );
                  })
                )}
              </AccordionDetails>
            </Accordion>
          </Box>
        )}
      </Box>
    </Box>
  );
};
