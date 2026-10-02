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
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Tabs,
  Tab,
  Box,
  Typography,
  IconButton,
  Tooltip,
  Alert,
  Snackbar,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Stack,
  CircularProgress,
} from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import CloseIcon from '@mui/icons-material/Close';
import TerminalIcon from '@mui/icons-material/Terminal';
import CodeIcon from '@mui/icons-material/Code';

import { exportCli } from '../services/api';
import { CliExportResponse, SpannerConfig } from '../types/spanner';

interface CliExporterModalProps {
  open: boolean;
  onClose: () => void;
  selectedConfigs: string[];
  currentConfig?: string;
  nodes?: number;
  nodesMap?: Record<string, number>;
  leadersMap?: Record<string, string>;
  selectedOptionalReplicasMap?: Record<string, string[]>;
  allConfigs?: SpannerConfig[];
}

export const CliExporterModal: React.FC<CliExporterModalProps> = ({
  open,
  onClose,
  selectedConfigs,
  currentConfig,
  nodes = 1,
  nodesMap = {},
  leadersMap = {},
  selectedOptionalReplicasMap = {},
  allConfigs = [],
}) => {
  const [activeConfig, setActiveConfig] = useState<string>(
    currentConfig || selectedConfigs[0] || 'nam3'
  );
  const [activeTab, setActiveTab] = useState<number>(0);
  const [exportData, setExportData] = useState<CliExportResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [copied, setCopied] = useState<boolean>(false);
  const [snackbarOpen, setSnackbarOpen] = useState<boolean>(false);

  useEffect(() => {
    if (currentConfig && selectedConfigs.includes(currentConfig)) {
      setActiveConfig(currentConfig);
    } else if (selectedConfigs.length > 0 && !selectedConfigs.includes(activeConfig)) {
      setActiveConfig(selectedConfigs[0]);
    }
  }, [currentConfig, selectedConfigs]);

  const effectiveOptionalReplicas = useMemo(() => {
    if (selectedOptionalReplicasMap[activeConfig] !== undefined) {
      return selectedOptionalReplicasMap[activeConfig];
    }
    const cfg = (allConfigs || []).find((c) => c.configname === activeConfig);
    return (cfg?.replicas || [])
      .filter((r) => r.replica_type === 'optional read only')
      .map((r) => r.region);
  }, [selectedOptionalReplicasMap, activeConfig, allConfigs]);

  useEffect(() => {
    if (!open || !activeConfig) return;

    let isMounted = true;
    setLoading(true);
    const effectiveNodes = nodesMap[activeConfig] !== undefined ? nodesMap[activeConfig] : nodes;
    const effectiveLeader = leadersMap[activeConfig];
    exportCli(activeConfig, effectiveNodes, effectiveLeader, effectiveOptionalReplicas)
      .then((data) => {
        if (isMounted) {
          setExportData(data);
          setLoading(false);
        }
      })
      .catch((err) => {
        console.error('Error fetching CLI export:', err);
        if (isMounted) setLoading(false);
      });

    return () => {
      isMounted = false;
    };
  }, [open, activeConfig, nodes, nodesMap, leadersMap, effectiveOptionalReplicas]);

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setSnackbarOpen(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const getActiveCode = (): string => {
    if (!exportData) return '';
    if (activeTab === 0) return exportData.gcloud_create;
    if (activeTab === 1) return exportData.gcloud_describe;
    return exportData.terraform_hcl;
  };

  const effectiveNodes = nodesMap[activeConfig] !== undefined ? nodesMap[activeConfig] : nodes;
  const activeOptionalReplicas = effectiveOptionalReplicas;

  return (
    <>
      <Dialog
        open={open}
        onClose={onClose}
        maxWidth="md"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: 2,
            border: '1px solid #dadce0',
          },
        }}
      >
        <DialogTitle
          sx={{
            m: 0,
            p: 2,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            borderBottom: '1px solid #dadce0',
            backgroundColor: '#f8f9fa',
          }}
        >
          <Stack direction="row" spacing={1.5} alignItems="center">
            <TerminalIcon color="primary" />
            <Typography variant="h3" sx={{ fontSize: '1.15rem' }}>
              Reproducible CLI & Terraform Exporter
            </Typography>
          </Stack>
          <IconButton
            aria-label="close"
            onClick={onClose}
            size="small"
            sx={{ color: (theme) => theme.palette.grey[500] }}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
        </DialogTitle>

        <DialogContent dividers sx={{ p: 2.5 }}>
          <Box sx={{ mb: 2 }}>
            <Stack direction="row" spacing={2} alignItems="center">
              {selectedConfigs.length > 1 && (
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <InputLabel id="select-config-label">Configuration</InputLabel>
                  <Select
                    labelId="select-config-label"
                    value={activeConfig}
                    label="Configuration"
                    onChange={(e) => setActiveConfig(e.target.value)}
                  >
                    {selectedConfigs.map((cfg) => (
                      <MenuItem key={cfg} value={cfg}>
                        {cfg}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              )}

              <Box sx={{ flexGrow: 1 }} />

              <Tabs
                value={activeTab}
                onChange={(_, val) => setActiveTab(val)}
                textColor="primary"
                indicatorColor="primary"
                sx={{
                  minHeight: 36,
                  '& .MuiTab-root': {
                    minHeight: 36,
                    py: 0.5,
                    px: 1.5,
                    fontSize: '0.8125rem',
                    textTransform: 'none',
                    fontWeight: 500,
                  },
                }}
              >
                <Tab icon={<TerminalIcon sx={{ fontSize: 16 }} />} iconPosition="start" label="gcloud create" />
                <Tab icon={<TerminalIcon sx={{ fontSize: 16 }} />} iconPosition="start" label="gcloud describe" />
                <Tab icon={<CodeIcon sx={{ fontSize: 16 }} />} iconPosition="start" label="Terraform HCL" />
              </Tabs>
            </Stack>
          </Box>

          {loading ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', p: 4 }}>
              <CircularProgress size={32} />
            </Box>
          ) : (
            <Box
              sx={{
                position: 'relative',
                backgroundColor: '#202124',
                color: '#e8eaed',
                p: 2,
                borderRadius: 1.5,
                fontFamily: 'Consolas, Monaco, "Courier New", Courier, monospace',
                fontSize: '0.85rem',
                lineHeight: 1.6,
                overflowX: 'auto',
                whiteSpace: 'pre',
                border: '1px solid #3c4043',
              }}
            >
              <Tooltip title={copied ? 'Copied!' : 'Copy to Clipboard'}>
                <IconButton
                  size="small"
                  onClick={() => handleCopy(getActiveCode())}
                  sx={{
                    position: 'absolute',
                    top: 8,
                    right: 8,
                    color: copied ? '#81c995' : '#dadce0',
                    backgroundColor: 'rgba(255, 255, 255, 0.08)',
                    '&:hover': {
                      backgroundColor: 'rgba(255, 255, 255, 0.16)',
                    },
                  }}
                >
                  {copied ? <CheckIcon fontSize="small" /> : <ContentCopyIcon fontSize="small" />}
                </IconButton>
              </Tooltip>
              <code>{getActiveCode()}</code>
            </Box>
          )}

          <Stack spacing={1} sx={{ mt: 2 }}>
            <Alert severity="info" sx={{ py: 0.5 }}>
              Nodes configured: <strong>{effectiveNodes}</strong> ({effectiveNodes * 1000} Processing Units). Sizing adjusts throughput calculations in real-time.
            </Alert>
            {activeOptionalReplicas.length > 0 && (
              <Alert severity="success" sx={{ py: 0.5 }}>
                Custom Instance Config: <strong>{activeOptionalReplicas.length}</strong> optional read-only replica(s) provisioned ({activeOptionalReplicas.join(', ')}). Exporter commands generate custom instance configuration definitions.
              </Alert>
            )}
          </Stack>
        </DialogContent>

        <DialogActions sx={{ px: 2.5, py: 1.5, backgroundColor: '#f8f9fa', borderTop: '1px solid #dadce0' }}>
          <Button onClick={onClose} variant="outlined" size="small">
            Close
          </Button>
          <Button
            onClick={() => handleCopy(getActiveCode())}
            variant="contained"
            size="small"
            startIcon={copied ? <CheckIcon /> : <ContentCopyIcon />}
          >
            {copied ? 'Copied' : 'Copy Command'}
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={snackbarOpen}
        autoHideDuration={2500}
        onClose={() => setSnackbarOpen(false)}
        message="Command copied to clipboard!"
      />
    </>
  );
};
