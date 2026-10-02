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

import React, { useState, useMemo, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  TableSortLabel,
  TextField,
  InputAdornment,
  Chip,
  Slider,
  Stack,
  Card,
  CardContent,
  Grid,
  Tooltip,
  Button,
  Checkbox,
  FormControlLabel,
} from '@mui/material';
import SpeedIcon from '@mui/icons-material/Speed';
import DnsIcon from '@mui/icons-material/Dns';
import PublicIcon from '@mui/icons-material/Public';
import VerifiedUserIcon from '@mui/icons-material/VerifiedUser';
import StorageIcon from '@mui/icons-material/Storage';
import SwapHorizIcon from '@mui/icons-material/SwapHoriz';

import { SpannerConfig, ReplicaType } from '../types/spanner';
import { gcpPalette } from '../theme';

interface ConfigDetailsTableProps {
  configs: SpannerConfig[];
  nodesMap?: Record<string, number>;
  nodes?: number;
  onNodesChange: (configname: string, nodes: number) => void;
  leadersMap?: Record<string, string>;
  onLeaderChange?: (configname: string, newLeader: string) => void;
  onOpenBenchmark?: (config?: SpannerConfig, optionalReplicas?: string[]) => void;
  selectedOptionalReplicasMap?: Record<string, string[]>;
  onToggleOptionalReplica?: (configname: string, region: string) => void;
  onSelectAllOptional?: (configname: string) => void;
  onDeselectAllOptional?: (configname: string) => void;
}

interface SingleConfigDetailsCardProps {
  config: SpannerConfig;
  nodes: number;
  onNodesChange: (nodes: number) => void;
  currentLeader: string;
  onLeaderChange?: (newLeader: string) => void;
  onOpenBenchmark?: (config?: SpannerConfig, optionalReplicas?: string[]) => void;
  selectedOptionalReplicas: string[];
  onToggleOptionalReplica: (region: string) => void;
  onSelectAllOptional: () => void;
  onDeselectAllOptional: () => void;
}

interface FlattenedRow {
  id: string;
  configName: string;
  instanceType: string;
  continent: string;
  sla: string;
  region: string;
  locationName: string;
  replicaType: ReplicaType;
  isLeader: boolean;
  isWitness: boolean;
  isReadOnly: boolean;
  isOptional: boolean;
  isSelected: boolean;
  readsPerSec: number;
  writesPerSec: number;
}

type SortField = 'region' | 'locationName' | 'replicaType' | 'readsPerSec' | 'writesPerSec';
type SortOrder = 'asc' | 'desc';

const SingleConfigDetailsCard: React.FC<SingleConfigDetailsCardProps> = ({
  config,
  nodes,
  onNodesChange,
  currentLeader,
  onLeaderChange,
  onOpenBenchmark,
  selectedOptionalReplicas,
  onToggleOptionalReplica,
  onSelectAllOptional,
  onDeselectAllOptional,
}) => {
  const [page, setPage] = useState<number>(0);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const [sortField, setSortField] = useState<SortField>('region');
  const [sortOrder, setSortOrder] = useState<SortOrder>('asc');
  const [inputValue, setInputValue] = useState<string>(String(nodes));

  // Candidate R/W replicas for leader swap (multi-region and dual-region)
  const rwReplicas = useMemo(() => {
    return (config.replicas || []).filter(
      (r) => r.replica_type === 'leader' || r.replica_type === 'r/w replica'
    );
  }, [config.replicas]);

  const otherRwReplica = useMemo(() => {
    return rwReplicas.find((r) => r.region !== currentLeader)?.region;
  }, [rwReplicas, currentLeader]);

  const handleSwapLeader = () => {
    if (otherRwReplica && onLeaderChange) {
      onLeaderChange(otherRwReplica);
    }
  };

  useEffect(() => {
    setInputValue(String(nodes));
  }, [nodes]);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    setInputValue(val);
    const parsed = parseInt(val, 10);
    if (!isNaN(parsed) && parsed >= 1) {
      onNodesChange(parsed);
    }
  };

  const handleInputBlur = () => {
    const parsed = parseInt(inputValue, 10);
    if (isNaN(parsed) || parsed < 1) {
      setInputValue('1');
      onNodesChange(1);
    } else {
      setInputValue(String(parsed));
      onNodesChange(parsed);
    }
  };

  const sliderMax = Math.max(100, Math.ceil(nodes / 50) * 50);

  const optionalReplicasCount = useMemo(() => {
    return (config.replicas || []).filter((r) => r.replica_type === 'optional read only').length;
  }, [config.replicas]);

  const selectedOptionalCount = selectedOptionalReplicas.length;

  // Flatten replicas for fast table display and search
  const flattenedRows: FlattenedRow[] = useMemo(() => {
    const rows: FlattenedRow[] = [];

    for (const rep of config.replicas || []) {
      let repType = rep.replica_type;
      let isLeader = rep.is_leader;

      if (config.instancetype !== 'regional') {
        if (rep.region === currentLeader) {
          repType = 'leader';
          isLeader = true;
        } else if (rep.replica_type === 'leader' || rep.replica_type === 'r/w replica') {
          repType = 'r/w replica';
          isLeader = false;
        }
      }

      const isOptional = repType === 'optional read only';
      const isSelected = isOptional ? selectedOptionalReplicas.includes(rep.region) : true;

      let reads = 0;
      let writes = 0;

      // Only selected replicas contribute to throughput sizing
      if (isSelected) {
        if (config.instancetype === 'regional') {
          if (repType === 'leader') {
            reads = 22500 * nodes;
            writes = 3500 * nodes;
          } else {
            reads = 22500 * nodes;
            writes = 0;
          }
        } else {
          // Multi-region and Dual-region
          if (repType === 'leader') {
            reads = 15000 * nodes;
            writes = 2700 * nodes;
          } else if (
            repType === 'r/w replica' ||
            repType === 'read only' ||
            repType === 'optional read only'
          ) {
            reads = 15000 * nodes;
            writes = 0;
          } else if (repType === 'witness') {
            reads = 0;
            writes = 0;
          }
        }
      }

      rows.push({
        id: `${config.configname}-${rep.region}-${repType}`,
        configName: config.configname,
        instanceType: config.instancetype,
        continent: config.continentregion,
        sla: config.availability_sla || (config.instancetype === 'regional' ? '99.99%' : '99.999%'),
        region: rep.region,
        locationName: rep.location_name,
        replicaType: repType,
        isLeader: isLeader,
        isWitness: rep.is_witness,
        isReadOnly: rep.is_read_only,
        isOptional: isOptional,
        isSelected: isSelected,
        readsPerSec: reads,
        writesPerSec: writes,
      });
    }

    return rows;
  }, [config, nodes, currentLeader, selectedOptionalReplicas]);

  // Aggregate metrics based on active/selected replicas
  const totals = useMemo(() => {
    let reads = 0;
    let writes = 0;
    const uniqueRegions = new Set<string>();
    let selectedCount = 0;

    for (const r of flattenedRows) {
      if (r.isSelected) {
        reads += r.readsPerSec;
        writes += r.writesPerSec;
        uniqueRegions.add(r.region);
        selectedCount++;
      }
    }

    const sla = config.availability_sla || (config.instancetype === 'regional' ? '99.99%' : '99.999%');

    return {
      totalReads: reads,
      totalWrites: writes,
      totalReplicas: selectedCount,
      allAvailableReplicas: flattenedRows.length,
      uniqueRegionsCount: uniqueRegions.size,
      sla,
      addressableStorageTB: nodes * 10,
    };
  }, [flattenedRows, config, nodes]);

  // Sorted rows
  const sortedRows = useMemo(() => {
    return [...flattenedRows].sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];

      if (typeof aVal === 'string') {
        return sortOrder === 'asc'
          ? (aVal as string).localeCompare(bVal as string)
          : (bVal as string).localeCompare(aVal as string);
      }

      return sortOrder === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number);
    });
  }, [flattenedRows, sortField, sortOrder]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortOrder('asc');
    }
  };

  const getReplicaBadge = (type: ReplicaType) => {
    switch (type) {
      case 'leader':
        return (
          <Chip
            label="Leader"
            size="small"
            sx={{
              backgroundColor: '#fce8e6',
              color: gcpPalette.replicaRole.leader,
              fontWeight: 600,
            }}
          />
        );
      case 'r/w replica':
        return (
          <Chip
            label="R/W Replica"
            size="small"
            sx={{
              backgroundColor: '#fef7e0',
              color: '#b06000',
              fontWeight: 600,
            }}
          />
        );
      case 'witness':
        return (
          <Chip
            label="Witness"
            size="small"
            sx={{
              backgroundColor: '#f1f3f4',
              color: gcpPalette.replicaRole.witness,
              fontWeight: 600,
            }}
          />
        );
      case 'read only':
      case 'optional read only':
        return (
          <Chip
            label="Read-Only"
            size="small"
            sx={{
              backgroundColor: '#e8f0fe',
              color: gcpPalette.replicaRole.readOnly,
              fontWeight: 600,
            }}
          />
        );
      default:
        return <Chip label={type} size="small" />;
    }
  };

  return (
    <Box sx={{ mb: 4 }}>
      {/* Configuration Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
        <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap">
          <DnsIcon sx={{ color: '#1a73e8', fontSize: 22 }} />
          <Typography variant="h6" sx={{ fontWeight: 600, color: '#202124', fontSize: '1.15rem' }}>
            {config.configname}
          </Typography>
          <Chip
            label={
              config.instancetype === 'multi-region'
                ? 'Multi-Region'
                : config.instancetype === 'dual-region'
                ? 'Dual-Region'
                : 'Regional'
            }
            size="small"
            sx={{
              fontWeight: 600,
              fontSize: '0.72rem',
              backgroundColor:
                config.instancetype === 'multi-region'
                  ? '#e8f0fe'
                  : config.instancetype === 'dual-region'
                  ? '#fef7e0'
                  : '#e6f4ea',
              color:
                config.instancetype === 'multi-region'
                  ? '#1a73e8'
                  : config.instancetype === 'dual-region'
                  ? '#b06000'
                  : '#137333',
            }}
          />
          <Chip
            label={config.continentregion}
            size="small"
            variant="outlined"
            sx={{ fontSize: '0.72rem', color: '#5f6368', borderColor: '#dadce0' }}
          />
        </Stack>
        <Stack direction="row" spacing={1.5} alignItems="center">
          <Typography variant="body2" sx={{ color: 'text.secondary' }}>
            Leader: <strong style={{ color: '#d93025' }}>{currentLeader}</strong>
          </Typography>
          {config.instancetype !== 'regional' && otherRwReplica && (
            <Tooltip title={`Swap leader region with ${otherRwReplica}`}>
              <Button
                size="small"
                variant="outlined"
                startIcon={<SwapHorizIcon sx={{ fontSize: 16 }} />}
                onClick={handleSwapLeader}
                sx={{
                  fontSize: '0.75rem',
                  textTransform: 'none',
                  fontWeight: 600,
                  py: 0.35,
                  px: 1.25,
                  borderColor: '#dadce0',
                  color: '#1a73e8',
                  backgroundColor: '#ffffff',
                  boxShadow: 'none',
                  '&:hover': {
                    borderColor: '#1a73e8',
                    backgroundColor: '#e8f0fe',
                    boxShadow: 'none',
                  },
                }}
              >
                Swap Leader
              </Button>
            </Tooltip>
          )}

          {onOpenBenchmark && (
            <Button
              size="small"
              variant="contained"
              startIcon={<SpeedIcon sx={{ fontSize: 16 }} />}
              onClick={() => onOpenBenchmark(config, selectedOptionalReplicas)}
              sx={{
                fontSize: '0.75rem',
                textTransform: 'none',
                fontWeight: 600,
                py: 0.35,
                px: 1.25,
                backgroundColor: '#7c3aed',
                color: '#ffffff',
                boxShadow: 'none',
                '&:hover': {
                  backgroundColor: '#6d28d9',
                  boxShadow: 'none',
                },
              }}
            >
              Run Latency Benchmark
            </Button>
          )}
        </Stack>
      </Box>

      {/* Aggregate KPI Summary Cards */}
      <Grid container spacing={2} sx={{ mb: 2 }}>
        <Grid item xs={12} sm={4} md={2}>
          <Card sx={{ height: '100%', border: '1px solid #dadce0', boxShadow: 'none' }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <DnsIcon sx={{ color: '#1a73e8', fontSize: 20 }} />
                <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600, textTransform: 'uppercase' }}>
                  Configs
                </Typography>
              </Stack>
              <Tooltip title={config.configname}>
                <Typography
                  variant="h2"
                  sx={{
                    mt: 0.5,
                    fontWeight: 600,
                    fontSize: config.configname.length > 7 ? '1.35rem' : '1.75rem',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {config.configname}
                </Typography>
              </Tooltip>
              <Typography variant="caption" color="text.secondary">
                {totals.totalReplicas} active {totals.totalReplicas === 1 ? 'replica' : 'replicas'}
                {totals.allAvailableReplicas > totals.totalReplicas ? ` (${totals.allAvailableReplicas} total)` : ''}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4} md={2}>
          <Card sx={{ height: '100%', border: '1px solid #dadce0', boxShadow: 'none' }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <PublicIcon sx={{ color: '#1a73e8', fontSize: 20 }} />
                <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600, textTransform: 'uppercase' }}>
                  Regions Spanned
                </Typography>
              </Stack>
              <Typography variant="h2" sx={{ mt: 0.5, fontWeight: 600 }}>
                {totals.uniqueRegionsCount}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {totals.uniqueRegionsCount} active {totals.uniqueRegionsCount === 1 ? 'region' : 'regions'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4} md={2}>
          <Card sx={{ height: '100%', border: '1px solid #dadce0', boxShadow: 'none' }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <VerifiedUserIcon sx={{ color: '#1e8e3e', fontSize: 20 }} />
                <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600, textTransform: 'uppercase' }}>
                  Availability SLA
                </Typography>
              </Stack>
              <Typography variant="h2" sx={{ mt: 0.5, fontWeight: 600, color: '#1e8e3e' }}>
                {totals.sla}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                monthly uptime commitment
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4} md={2}>
          <Card sx={{ height: '100%', border: '1px solid #dadce0', boxShadow: 'none' }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <SpeedIcon sx={{ color: '#1a73e8', fontSize: 20 }} />
                <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600, textTransform: 'uppercase' }}>
                  Est. Reads/Sec
                </Typography>
              </Stack>
              <Typography variant="h2" sx={{ mt: 0.5, fontWeight: 600, color: '#1a73e8' }}>
                {totals.totalReads.toLocaleString()}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                across {totals.totalReplicas} active {totals.totalReplicas === 1 ? 'replica' : 'replicas'}
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4} md={2}>
          <Card sx={{ height: '100%', border: '1px solid #dadce0', boxShadow: 'none' }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <SpeedIcon sx={{ color: '#d93025', fontSize: 20 }} />
                <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600, textTransform: 'uppercase' }}>
                  Est. Writes/Sec
                </Typography>
              </Stack>
              <Typography variant="h2" sx={{ mt: 0.5, fontWeight: 600, color: '#d93025' }}>
                {totals.totalWrites.toLocaleString()}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                at paxos leader replica
              </Typography>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4} md={2}>
          <Card sx={{ height: '100%', border: '1px solid #dadce0', boxShadow: 'none' }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
              <Stack direction="row" spacing={1} alignItems="center">
                <StorageIcon sx={{ color: '#e37400', fontSize: 20 }} />
                <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600, textTransform: 'uppercase' }}>
                  Storage
                </Typography>
              </Stack>
              <Typography variant="h2" sx={{ mt: 0.5, fontWeight: 600, color: '#e37400' }}>
                {totals.addressableStorageTB.toLocaleString()} TB
              </Typography>
              <Typography variant="caption" color="text.secondary">
                addressable storage (10TB/node)
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Node Scaling & Capacity Sizing Header */}
      <Paper sx={{ p: 2, mb: 2, borderRadius: 2, border: '1px solid #dadce0', boxShadow: 'none' }}>
        <Stack spacing={1.5}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 1 }}>
            <Box>
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Node Scaling & Capacity Sizing:
              </Typography>
              <Typography variant="caption" color="text.secondary">
                Drag slider or type exact nodes. Throughput estimates update in real-time.
              </Typography>
            </Box>

            <Stack direction="row" spacing={1.5} alignItems="center">
              <TextField
                type="number"
                size="small"
                value={inputValue}
                onChange={handleInputChange}
                onBlur={handleInputBlur}
                inputProps={{ min: 1, max: 10000, step: 1 }}
                sx={{
                  width: 110,
                  '& input': {
                    fontWeight: 700,
                    color: '#1a73e8',
                    textAlign: 'right',
                    py: 0.75,
                  },
                }}
                InputProps={{
                  endAdornment: (
                    <InputAdornment position="end" sx={{ mr: 0 }}>
                      <Typography variant="caption" sx={{ fontWeight: 600, color: 'text.secondary' }}>
                        {nodes === 1 ? 'Node' : 'Nodes'}
                      </Typography>
                    </InputAdornment>
                  ),
                }}
              />
              <Chip
                label={`${(nodes * 1000).toLocaleString()} PUs`}
                size="small"
                sx={{
                  backgroundColor: '#e8f0fe',
                  color: '#1a73e8',
                  fontWeight: 600,
                  height: 28,
                  fontSize: '0.75rem',
                }}
              />
            </Stack>
          </Box>

          <Slider
            value={Math.min(nodes, sliderMax)}
            min={1}
            max={sliderMax}
            step={1}
            onChange={(_, val) => {
              const num = val as number;
              onNodesChange(num);
              setInputValue(String(num));
            }}
            valueLabelDisplay="auto"
            sx={{ color: '#1a73e8', py: 1 }}
          />

          <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
            <Typography variant="caption" sx={{ color: 'text.secondary', fontWeight: 600 }}>
              Quick Presets:
            </Typography>
            {[1, 3, 5, 10, 25, 50, 100].map((preset) => (
              <Chip
                key={preset}
                label={`${preset} ${preset === 1 ? 'node' : 'nodes'}`}
                size="small"
                clickable
                variant={nodes === preset ? 'filled' : 'outlined'}
                color={nodes === preset ? 'primary' : 'default'}
                onClick={() => {
                  onNodesChange(preset);
                  setInputValue(String(preset));
                }}
                sx={{
                  height: 22,
                  fontSize: '0.7rem',
                  fontWeight: nodes === preset ? 600 : 400,
                }}
              />
            ))}
            {nodes > 100 && (
              <Chip
                label={`Custom: ${nodes} nodes`}
                size="small"
                color="primary"
                variant="filled"
                sx={{ height: 22, fontSize: '0.7rem', fontWeight: 600 }}
              />
            )}
          </Stack>
        </Stack>
      </Paper>

      {/* Main Table with Pagination */}
      <Paper sx={{ borderRadius: 2, overflow: 'hidden', border: '1px solid #dadce0', boxShadow: 'none' }}>
        {optionalReplicasCount > 0 && (
          <Box
            sx={{
              px: 2,
              py: 1.25,
              backgroundColor: '#f8f9fa',
              borderBottom: '1px solid #dadce0',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              flexWrap: 'wrap',
              gap: 1.5,
            }}
          >
            <Stack direction="row" spacing={1.5} alignItems="center" flexWrap="wrap">
              <Typography variant="body2" sx={{ fontWeight: 600, color: '#3c4043' }}>
                Optional Read-Only Replicas:
              </Typography>
              <Chip
                label={`${selectedOptionalCount} of ${optionalReplicasCount} provisioned`}
                size="small"
                sx={{
                  fontWeight: 600,
                  fontSize: '0.72rem',
                  backgroundColor: selectedOptionalCount > 0 ? '#e8f0fe' : '#f1f3f4',
                  color: selectedOptionalCount > 0 ? '#1a73e8' : '#5f6368',
                }}
              />
              <Typography variant="caption" color="text.secondary">
                (Deselecting removes replica from map canvas and excludes from load tests)
              </Typography>
            </Stack>

            <Stack direction="row" spacing={1}>
              <Button
                size="small"
                variant="outlined"
                onClick={onSelectAllOptional}
                disabled={selectedOptionalCount === optionalReplicasCount}
                sx={{
                  fontSize: '0.72rem',
                  textTransform: 'none',
                  py: 0.25,
                  px: 1.25,
                  borderColor: '#dadce0',
                  color: '#1a73e8',
                }}
              >
                Select All Optional
              </Button>
              <Button
                size="small"
                variant="outlined"
                onClick={onDeselectAllOptional}
                disabled={selectedOptionalCount === 0}
                sx={{
                  fontSize: '0.72rem',
                  textTransform: 'none',
                  py: 0.25,
                  px: 1.25,
                  borderColor: '#dadce0',
                  color: '#5f6368',
                }}
              >
                Deselect All Optional
              </Button>
            </Stack>
          </Box>
        )}

        <TableContainer sx={{ maxHeight: 380 }}>
          <Table stickyHeader size="small">
            <TableHead>
              <TableRow>
                <TableCell sx={{ width: 145, fontWeight: 600 }}>Provision / Map</TableCell>
                <TableCell>
                  <TableSortLabel
                    active={sortField === 'region'}
                    direction={sortOrder}
                    onClick={() => handleSort('region')}
                  >
                    Region
                  </TableSortLabel>
                </TableCell>
                <TableCell>
                  <TableSortLabel
                    active={sortField === 'locationName'}
                    direction={sortOrder}
                    onClick={() => handleSort('locationName')}
                  >
                    Location Name
                  </TableSortLabel>
                </TableCell>
                <TableCell>
                  <TableSortLabel
                    active={sortField === 'replicaType'}
                    direction={sortOrder}
                    onClick={() => handleSort('replicaType')}
                  >
                    Replica Role
                  </TableSortLabel>
                </TableCell>
                <TableCell align="right">
                  <TableSortLabel
                    active={sortField === 'readsPerSec'}
                    direction={sortOrder}
                    onClick={() => handleSort('readsPerSec')}
                  >
                    Reads / sec
                  </TableSortLabel>
                </TableCell>
                <TableCell align="right">
                  <TableSortLabel
                    active={sortField === 'writesPerSec'}
                    direction={sortOrder}
                    onClick={() => handleSort('writesPerSec')}
                  >
                    Writes / sec
                  </TableSortLabel>
                </TableCell>
              </TableRow>
            </TableHead>

            <TableBody>
              {sortedRows.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                    <Typography variant="body2" color="text.secondary">
                      No replicas available for this configuration.
                    </Typography>
                  </TableCell>
                </TableRow>
              ) : (
                sortedRows
                  .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                  .map((row) => (
                    <TableRow
                      key={row.id}
                      hover
                      sx={{
                        opacity: row.isSelected ? 1 : 0.55,
                        backgroundColor: row.isSelected ? 'inherit' : '#fafafa',
                      }}
                    >
                      <TableCell sx={{ py: 0.5 }}>
                        <Tooltip
                          title={
                            row.isOptional
                              ? row.isSelected
                                ? 'Optional Read-Only Replica: Deselect to remove from map and load-test provisioning'
                                : 'Optional Read-Only Replica: Select to include on map and in load-test provisioning'
                              : row.replicaType === 'read only'
                              ? 'Base Read-Only Replica (automatically attached to config, cannot be deselected)'
                              : 'Base Quorum Replica (required, cannot be deselected)'
                          }
                        >
                          <span>
                            <FormControlLabel
                              control={
                                <Checkbox
                                  size="small"
                                  checked={row.isSelected}
                                  disabled={!row.isOptional}
                                  onChange={() => onToggleOptionalReplica(row.region)}
                                  sx={{
                                    p: 0.5,
                                    color: row.isOptional ? '#1a73e8' : '#9aa0a6',
                                    '&.Mui-checked': {
                                      color: row.isOptional ? '#1a73e8' : '#5f6368',
                                    },
                                  }}
                                />
                              }
                              label={
                                <Typography
                                  variant="caption"
                                  sx={{
                                    fontSize: '0.75rem',
                                    fontWeight: row.isSelected ? 600 : 400,
                                    color: !row.isOptional
                                      ? 'text.secondary'
                                      : row.isSelected
                                      ? '#1a73e8'
                                      : 'text.disabled',
                                  }}
                                >
                                  {row.isOptional
                                    ? row.isSelected
                                      ? 'Selected'
                                      : 'Deselected'
                                    : 'Required'}
                                </Typography>
                              }
                              sx={{ m: 0 }}
                            />
                          </span>
                        </Tooltip>
                      </TableCell>
                      <TableCell sx={{ fontFamily: 'monospace', fontSize: '0.78rem' }}>
                        {row.region}
                      </TableCell>
                      <TableCell>{row.locationName}</TableCell>
                      <TableCell>{getReplicaBadge(row.replicaType)}</TableCell>
                      <TableCell align="right" sx={{ fontWeight: 500 }}>
                        {row.readsPerSec > 0 ? row.readsPerSec.toLocaleString() : '—'}
                      </TableCell>
                      <TableCell align="right" sx={{ fontWeight: 500, color: row.writesPerSec > 0 ? '#d93025' : 'inherit' }}>
                        {row.writesPerSec > 0 ? row.writesPerSec.toLocaleString() : '—'}
                      </TableCell>
                    </TableRow>
                  ))
              )}
            </TableBody>
          </Table>
        </TableContainer>

        <TablePagination
          rowsPerPageOptions={[5, 10, 25]}
          component="div"
          count={flattenedRows.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={(_, newPage) => setPage(newPage)}
          onRowsPerPageChange={(e) => {
            setRowsPerPage(parseInt(e.target.value, 10));
            setPage(0);
          }}
          sx={{ borderTop: '1px solid #dadce0' }}
        />
      </Paper>
    </Box>
  );
};

export const ConfigDetailsTable: React.FC<ConfigDetailsTableProps> = ({
  configs,
  nodesMap = {},
  nodes = 1,
  onNodesChange,
  leadersMap = {},
  onLeaderChange,
  onOpenBenchmark,
  selectedOptionalReplicasMap = {},
  onToggleOptionalReplica,
  onSelectAllOptional,
  onDeselectAllOptional,
}) => {
  if (configs.length === 0) {
    return (
      <Paper
        sx={{
          p: 5,
          textAlign: 'center',
          borderRadius: 2,
          border: '1px dashed #dadce0',
          backgroundColor: '#ffffff',
        }}
      >
        <DnsIcon sx={{ fontSize: 44, color: '#9aa0a6', mb: 1.5 }} />
        <Typography variant="h6" color="text.primary" gutterBottom sx={{ fontSize: '1.1rem', fontWeight: 600 }}>
          No Spanner configurations selected
        </Typography>
        <Typography variant="body2" color="text.secondary" sx={{ maxWidth: 500, mx: 'auto', mb: 2.5 }}>
          Select an instance configuration from the left navigation panel to view topology, sizing, and replica details.
        </Typography>

        {onOpenBenchmark && (
          <Button
            variant="outlined"
            startIcon={<SpeedIcon sx={{ color: '#7c3aed' }} />}
            onClick={() => onOpenBenchmark()}
            sx={{
              borderColor: '#7c3aed',
              color: '#7c3aed',
              textTransform: 'none',
              fontWeight: 600,
              px: 2.5,
              py: 0.8,
              '&:hover': {
                borderColor: '#6d28d9',
                backgroundColor: '#faf5ff',
              },
            }}
          >
            Launch Latency Benchmark Suite
          </Button>
        )}
      </Paper>
    );
  }

  return (
    <Stack spacing={3}>
      {configs.map((cfg) => {
        const configNodes = nodesMap[cfg.configname] !== undefined ? nodesMap[cfg.configname] : nodes;
        const currentLeader = leadersMap[cfg.configname] || cfg.leader_region;
        const allOptionalRegions = (cfg.replicas || [])
          .filter((r) => r.replica_type === 'optional read only')
          .map((r) => r.region);
        const selectedOptional = selectedOptionalReplicasMap[cfg.configname] ?? allOptionalRegions;

        return (
          <SingleConfigDetailsCard
            key={cfg.configname}
            config={cfg}
            nodes={configNodes}
            onNodesChange={(val) => onNodesChange(cfg.configname, val)}
            currentLeader={currentLeader}
            onLeaderChange={(newLeader) => onLeaderChange?.(cfg.configname, newLeader)}
            onOpenBenchmark={onOpenBenchmark}
            selectedOptionalReplicas={selectedOptional}
            onToggleOptionalReplica={(reg) => onToggleOptionalReplica?.(cfg.configname, reg)}
            onSelectAllOptional={() => onSelectAllOptional?.(cfg.configname)}
            onDeselectAllOptional={() => onDeselectAllOptional?.(cfg.configname)}
          />
        );
      })}
    </Stack>
  );
};
