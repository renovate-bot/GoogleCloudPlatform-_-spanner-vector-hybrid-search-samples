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

import React, { useState, useRef, useEffect } from 'react';
import L from 'leaflet';
import { Box, Paper, Typography, Divider, Stack, IconButton, Tooltip, Collapse } from '@mui/material';
import LayersIcon from '@mui/icons-material/Layers';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CloseIcon from '@mui/icons-material/Close';
import { gcpPalette } from '../theme';

interface LegendItemProps {
  color: string;
  label: string;
  shape?: 'circle' | 'line-solid' | 'line-dashed' | 'line-dotted';
}

const LegendItem: React.FC<LegendItemProps> = ({
  color,
  label,
  shape = 'circle',
}) => {
  return (
    <Stack direction="row" spacing={1} alignItems="center">
      {shape === 'circle' && (
        <Box
          sx={{
            width: 12,
            height: 12,
            borderRadius: '50%',
            backgroundColor: color,
            border: '2px solid #ffffff',
            boxShadow: '0 0 0 1px rgba(0,0,0,0.2)',
            flexShrink: 0,
          }}
        />
      )}
      {shape === 'line-solid' && (
        <Box
          sx={{
            width: 20,
            height: 3,
            backgroundColor: color,
            flexShrink: 0,
          }}
        />
      )}
      {shape === 'line-dashed' && (
        <Box
          sx={{
            width: 20,
            height: 0,
            borderTop: `2px dashed ${color}`,
            flexShrink: 0,
          }}
        />
      )}
      {shape === 'line-dotted' && (
        <Box
          sx={{
            width: 20,
            height: 0,
            borderTop: `2px dashed ${color}`,
            flexShrink: 0,
          }}
        />
      )}
      <Typography variant="caption" sx={{ fontWeight: 500, display: 'block', lineHeight: 1.2 }}>
        {label}
      </Typography>
    </Stack>
  );
};

export interface LegendProps {
  onClose?: () => void;
}

export const Legend: React.FC<LegendProps> = ({ onClose }) => {
  const [isCollapsed, setIsCollapsed] = useState<boolean>(false);
  const legendRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (legendRef.current) {
      L.DomEvent.disableClickPropagation(legendRef.current);
      L.DomEvent.disableScrollPropagation(legendRef.current);
    }
  }, []);
  const [rolesExpanded, setRolesExpanded] = useState<boolean>(true);
  const [linksExpanded, setLinksExpanded] = useState<boolean>(true);

  return (
    <Paper
      ref={legendRef}
      onMouseDown={(e) => e.stopPropagation()}
      onPointerDown={(e) => e.stopPropagation()}
      onTouchStart={(e) => e.stopPropagation()}
      onDoubleClick={(e) => e.stopPropagation()}
      onWheel={(e) => e.stopPropagation()}
      elevation={2}
      sx={{
        p: 1.25,
        backgroundColor: 'rgba(255, 255, 255, 0.95)',
        backdropFilter: 'blur(4px)',
        border: '1px solid #dadce0',
        borderRadius: 2,
        minWidth: 190,
        maxWidth: 220,
        transition: 'all 0.2s ease',
        pointerEvents: 'auto',
      }}
    >
      {/* Legend Card Header */}
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        sx={{ mb: isCollapsed ? 0 : 1 }}
      >
        <Stack
          direction="row"
          spacing={0.75}
          alignItems="center"
          sx={{ cursor: 'pointer', userSelect: 'none' }}
          onClick={() => setIsCollapsed(!isCollapsed)}
        >
          <LayersIcon sx={{ fontSize: 16, color: '#5f6368' }} />
          <Typography
            variant="caption"
            sx={{ fontWeight: 600, fontSize: '0.75rem', color: '#3c4043' }}
          >
            Legend
          </Typography>
        </Stack>
        <Stack direction="row" spacing={0.25} alignItems="center">
          <Tooltip title={isCollapsed ? 'Expand legend' : 'Collapse legend'}>
            <IconButton
              size="small"
              onClick={() => setIsCollapsed(!isCollapsed)}
              sx={{ p: 0.25 }}
            >
              {isCollapsed ? (
                <ExpandMoreIcon sx={{ fontSize: 16 }} />
              ) : (
                <ExpandLessIcon sx={{ fontSize: 16 }} />
              )}
            </IconButton>
          </Tooltip>
          {onClose && (
            <Tooltip title="Hide legend">
              <IconButton size="small" onClick={onClose} sx={{ p: 0.25 }}>
                <CloseIcon sx={{ fontSize: 16 }} />
              </IconButton>
            </Tooltip>
          )}
        </Stack>
      </Stack>

      <Collapse in={!isCollapsed}>
        {/* Replica Roles Section */}
        <Box sx={{ mb: 1 }}>
          <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            sx={{ cursor: 'pointer', userSelect: 'none', py: 0.25 }}
            onClick={() => setRolesExpanded(!rolesExpanded)}
          >
            <Typography
              variant="caption"
              sx={{
                fontWeight: 600,
                textTransform: 'uppercase',
                color: '#5f6368',
                fontSize: '0.68rem',
              }}
            >
              Replica Roles
            </Typography>
            <IconButton size="small" sx={{ p: 0 }}>
              {rolesExpanded ? (
                <ExpandLessIcon sx={{ fontSize: 14 }} />
              ) : (
                <ExpandMoreIcon sx={{ fontSize: 14 }} />
              )}
            </IconButton>
          </Stack>
          <Collapse in={rolesExpanded}>
            <Stack spacing={0.75} sx={{ mt: 0.5 }}>
              <LegendItem color={gcpPalette.replicaRole.leader} label="Leader Region" />
              <LegendItem color={gcpPalette.replicaRole.rwReplica} label="R/W Replica" />
              <LegendItem color={gcpPalette.replicaRole.witness} label="Witness Region" />
              <LegendItem color={gcpPalette.replicaRole.readOnly} label="Read-Only / Optional" />
              <LegendItem color="#7c3aed" label="Client Benchmark VM" />
            </Stack>
          </Collapse>
        </Box>

        <Divider sx={{ my: 0.75 }} />

        {/* Topology Links Section */}
        <Box>
          <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            sx={{ cursor: 'pointer', userSelect: 'none', py: 0.25 }}
            onClick={() => setLinksExpanded(!linksExpanded)}
          >
            <Typography
              variant="caption"
              sx={{
                fontWeight: 600,
                textTransform: 'uppercase',
                color: '#5f6368',
                fontSize: '0.68rem',
              }}
            >
              Topology Links
            </Typography>
            <IconButton size="small" sx={{ p: 0 }}>
              {linksExpanded ? (
                <ExpandLessIcon sx={{ fontSize: 14 }} />
              ) : (
                <ExpandMoreIcon sx={{ fontSize: 14 }} />
              )}
            </IconButton>
          </Stack>
          <Collapse in={linksExpanded}>
            <Stack spacing={0.75} sx={{ mt: 0.5 }}>
              <LegendItem color="#1a73e8" shape="line-solid" label="Replication Quorum" />
              <LegendItem color="#5f6368" shape="line-dashed" label="Witness Consensus" />
              <LegendItem color="#12b5cb" shape="line-dotted" label="Read-Only Sync" />
              <LegendItem color="#7c3aed" shape="line-dashed" label="Client Latency Path" />
            </Stack>
          </Collapse>
        </Box>
      </Collapse>
    </Paper>
  );
};
