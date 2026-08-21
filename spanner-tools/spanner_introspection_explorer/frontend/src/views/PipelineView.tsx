/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Button,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  CircularProgress,
  Divider,
} from '@mui/material';
import SyncAltIcon from '@mui/icons-material/SyncAlt';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';

import { StagingFile } from '../types';
import { api } from '../services/api';
import { streamEvents } from '../services/sse';
import { CodeBlock } from '../components/CodeBlock';
import { gcpPalette } from '../theme';

interface PipelineViewProps {
  selectedDatabase?: string;
  onRefreshData: () => void;
}

export const PipelineView: React.FC<PipelineViewProps> = ({ selectedDatabase, onRefreshData }) => {
  const [stagingFiles, setStagingFiles] = useState<StagingFile[]>([]);
  const [loadingFiles, setLoadingFiles] = useState(true);
  const [ingesting, setIngesting] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);

  const loadFiles = () => {
    setLoadingFiles(true);
    api.getStagingFiles(selectedDatabase)
      .then((files) => {
        setStagingFiles(files);
        setLoadingFiles(false);
      })
      .catch((err) => {
        console.error('Failed to load staging files', err);
        setLoadingFiles(false);
      });
  };

  useEffect(() => {
    loadFiles();
  }, [selectedDatabase]);

  const handleRunIngestion = () => {
    setIngesting(true);
    setLogs([`🚀 Starting DuckDB ingestion for ${selectedDatabase || 'all databases'}...`]);

    const url = selectedDatabase && selectedDatabase !== 'default' && selectedDatabase !== 'legacy'
      ? `/api/v1/pipeline/reload/stream?db=${encodeURIComponent(selectedDatabase)}`
      : '/api/v1/pipeline/reload/stream';

    streamEvents(url, {
      onMessage: (data) => {
        if (data.log) {
          setLogs((prev) => [...prev, data.log]);
        }
        if (data.done) {
          setIngesting(false);
          onRefreshData();
          loadFiles();
        }
      },
      onError: (err) => {
        setLogs((prev) => [...prev, `❌ Error: ${err.message}`]);
        setIngesting(false);
      },
      onDone: () => {
        setIngesting(false);
        onRefreshData();
      },
    });
  };

  return (
    <Box>
      <Box sx={{ mb: 3 }}>
        <Typography variant="h1" sx={{ mb: 0.5, display: 'flex', alignItems: 'center', gap: 1 }}>
          <SyncAltIcon sx={{ color: gcpPalette.primary.main }} />
          Data Pipeline & DuckDB Ingestion
        </Typography>
        <Typography variant="body1" sx={{ color: gcpPalette.neutral.textSecondary }}>
          Manage local staging CSV exports, inspect raw files, and trigger fast DuckDB ingestion with live progress logs.
        </Typography>
      </Box>

      {/* Staging Files Inventory */}
      <Paper sx={{ mb: 3, border: `1px solid ${gcpPalette.neutral.border}` }}>
        <Box sx={{ p: 2, borderBottom: `1px solid ${gcpPalette.neutral.border}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h3" sx={{ fontSize: '1rem' }}>
            Staging Directory Files ({stagingFiles.length})
          </Typography>
          <Button
            size="small"
            variant="contained"
            startIcon={ingesting ? <CircularProgress size={16} color="inherit" /> : <PlayArrowIcon />}
            onClick={handleRunIngestion}
            disabled={ingesting || stagingFiles.length === 0}
          >
            {ingesting ? 'Ingesting...' : 'Reload DuckDB'}
          </Button>
        </Box>

        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>File Name</TableCell>
                <TableCell align="right">Size</TableCell>
                <TableCell align="right">Last Modified</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {loadingFiles ? (
                <TableRow>
                  <TableCell colSpan={3} sx={{ textAlign: 'center', py: 4 }}>
                    <CircularProgress size={24} />
                  </TableCell>
                </TableRow>
              ) : stagingFiles.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={3} sx={{ textAlign: 'center', py: 4, color: gcpPalette.neutral.textSecondary }}>
                    No files found in <code>staging/</code>. Run <code>./export_all.sh</code> to export Spanner tables.
                  </TableCell>
                </TableRow>
              ) : (
                stagingFiles.map((f) => (
                  <TableRow key={f.name} hover>
                    <TableCell sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <InsertDriveFileIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        {f.name}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">{f.size_human}</TableCell>
                    <TableCell align="right" sx={{ color: gcpPalette.neutral.textSecondary }}>
                      {new Date(f.modified * 1000).toLocaleString()}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>

      {/* Live Ingestion Logs Console */}
      {logs.length > 0 && (
        <Box sx={{ mt: 2 }}>
          <Typography variant="h3" sx={{ fontSize: '0.95rem', mb: 1 }}>
            Execution Console Logs
          </Typography>
          <CodeBlock code={logs.join('\n')} language="bash" maxHeight={280} title="DuckDB Ingestion Runner" />
        </Box>
      )}
    </Box>
  );
};
