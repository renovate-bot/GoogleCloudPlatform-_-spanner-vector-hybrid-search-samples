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
  Grid,
  Card,
  CardContent,
  CardActions,
  Chip,
  IconButton,
  Tooltip,
  CircularProgress,
  Alert,
  Divider,
  Paper,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogContentText,
  DialogActions,
  FormControlLabel,
  Checkbox,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import StorageIcon from '@mui/icons-material/Storage';
import CloudQueueIcon from '@mui/icons-material/CloudQueue';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import SyncIcon from '@mui/icons-material/Sync';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';
import LaunchIcon from '@mui/icons-material/Launch';
import TableChartIcon from '@mui/icons-material/TableChart';
import LayersIcon from '@mui/icons-material/Layers';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';

import { DatabaseConnection } from '../types';
import { api } from '../services/api';
import { streamEvents } from '../services/sse';
import { AddConnectionModal } from '../components/AddConnectionModal';
import { CodeBlock } from '../components/CodeBlock';
import { gcpPalette } from '../theme';

interface ConnectionsViewProps {
  onSelectDatabase: (dbId: string) => void;
  onOpenExplorer: (dbId: string) => void;
  onConnectionsChanged?: () => void;
}

export const ConnectionsView: React.FC<ConnectionsViewProps> = ({
  onSelectDatabase,
  onOpenExplorer,
  onConnectionsChanged,
}) => {
  const [connections, setConnections] = useState<DatabaseConnection[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [modalOpen, setModalOpen] = useState<boolean>(false);

  // Syncing state
  const [syncingId, setSyncingId] = useState<string | null>(null);
  const [syncLogs, setSyncLogs] = useState<string[]>([]);

  // Delete Confirmation state
  const [deleteTarget, setDeleteTarget] = useState<DatabaseConnection | null>(null);
  const [deleteStaging, setDeleteStaging] = useState<boolean>(false);
  const [deleting, setDeleting] = useState<boolean>(false);

  const loadConnections = async () => {
    setLoading(true);
    try {
      const res = await api.getConnections();
      setConnections(res);
    } catch (err) {
      console.error('Failed to load database connections', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadConnections();
  }, []);

  const handleSync = (conn: DatabaseConnection) => {
    setSyncingId(conn.id);
    setSyncLogs([`🚀 Starting ingestion for ${conn.name}...`]);

    streamEvents(`/api/v1/connections/${encodeURIComponent(conn.id)}/sync/stream`, {
      onMessage: (data) => {
        if (data.log) {
          setSyncLogs((prev) => [...prev, data.log]);
        }
        if (data.done) {
          setSyncingId(null);
          loadConnections();
          onConnectionsChanged?.();
        }
      },
      onError: (err) => {
        setSyncLogs((prev) => [...prev, `❌ Error: ${err.message || 'Sync failed'}`]);
        setSyncingId(null);
      },
      onDone: () => {
        setSyncingId(null);
        loadConnections();
        onConnectionsChanged?.();
      },
    });
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      await api.deleteConnection(deleteTarget.id, true, deleteStaging);
      setDeleteTarget(null);
      setDeleteStaging(false);
      await loadConnections();
      onConnectionsChanged?.();
    } catch (err) {
      console.error('Failed to delete connection', err);
    } finally {
      setDeleting(false);
    }
  };

  return (
    <Box>
      {/* View Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h1" sx={{ mb: 0.5, display: 'flex', alignItems: 'center', gap: 1.5 }}>
            <StorageIcon sx={{ color: gcpPalette.primary.main, fontSize: '1.75rem' }} />
            Database Connections & Storage Manager
          </Typography>
          <Typography variant="body1" sx={{ color: gcpPalette.neutral.textSecondary }}>
            Manage registered Google Cloud Spanner instances and local staging databases. Ingest raw exports into fast DuckDB storage.
          </Typography>
        </Box>

        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setModalOpen(true)}
          sx={{ fontWeight: 600 }}
        >
          Register Connection
        </Button>
      </Box>

      {/* Connections Grid */}
      {loading && connections.length === 0 ? (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 8 }}>
          <CircularProgress size={36} />
        </Box>
      ) : connections.length === 0 ? (
        <Paper sx={{ p: 4, textAlign: 'center', border: `1px dashed ${gcpPalette.neutral.border}` }}>
          <Typography variant="h3" sx={{ mb: 1, color: gcpPalette.neutral.textSecondary }}>
            No Database Connections Registered
          </Typography>
          <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mb: 2 }}>
            Register a connection to a live Cloud Spanner instance or point to a local staging directory.
          </Typography>
          <Button variant="outlined" startIcon={<AddIcon />} onClick={() => setModalOpen(true)}>
            Add Database Connection
          </Button>
        </Paper>
      ) : (
        <Grid container spacing={3}>
          {connections.map((conn) => {
            const isSyncing = syncingId === conn.id;
            const isReady = conn.status === 'READY';

            return (
              <Grid item xs={12} md={6} key={conn.id}>
                <Card
                  variant="outlined"
                  sx={{
                    borderColor: isReady ? gcpPalette.neutral.border : '#ffab00',
                    display: 'flex',
                    flexDirection: 'column',
                    height: '100%',
                    transition: 'box-shadow 0.2s',
                    '&:hover': {
                      boxShadow: '0 2px 8px rgba(0,0,0,0.08)',
                    },
                  }}
                >
                  <CardContent sx={{ flexGrow: 1, pb: 1.5 }}>
                    {/* Top Type & Status Badges */}
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1.5 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Chip
                          icon={conn.type === 'gcp' ? <CloudQueueIcon /> : <FolderOpenIcon />}
                          label={conn.type === 'gcp' ? 'GCP Cloud Spanner' : 'Local Staging'}
                          size="small"
                          sx={{
                            backgroundColor: conn.type === 'gcp' ? 'rgba(26, 115, 232, 0.08)' : 'rgba(95, 99, 104, 0.08)',
                            color: conn.type === 'gcp' ? gcpPalette.primary.main : gcpPalette.neutral.textSecondary,
                            fontWeight: 600,
                          }}
                        />
                        <Chip
                          label={conn.dialect === 'POSTGRESQL' ? 'PG' : 'GSQL'}
                          size="small"
                          sx={{ height: 20, fontSize: '0.65rem', fontWeight: 600 }}
                        />
                      </Box>

                      <Chip
                        icon={isReady ? <CheckCircleOutlineIcon /> : <ErrorOutlineIcon />}
                        label={isReady ? 'READY' : 'NEEDS INGESTION'}
                        size="small"
                        color={isReady ? 'success' : 'warning'}
                        sx={{ fontWeight: 600, fontSize: '0.7rem' }}
                      />
                    </Box>

                    {/* Connection Name */}
                    <Typography variant="h2" sx={{ fontSize: '1.15rem', fontWeight: 600, mb: 1 }}>
                      {conn.name}
                    </Typography>

                    {/* Target Information */}
                    {conn.type === 'gcp' ? (
                      <Box sx={{ mb: 2, fontSize: '0.8125rem', color: gcpPalette.neutral.textSecondary }}>
                        <div><strong>Project:</strong> <code>{conn.project_id}</code></div>
                        <div><strong>Instance:</strong> <code>{conn.instance_id}</code></div>
                        <div><strong>Database:</strong> <code>{conn.database_id}</code></div>
                      </Box>
                    ) : (
                      <Box sx={{ mb: 2, fontSize: '0.8125rem', color: gcpPalette.neutral.textSecondary }}>
                        <div><strong>Staging Folder:</strong> <code>{conn.staging_path}</code></div>
                      </Box>
                    )}

                    {/* Storage & Record Metrics */}
                    <Box
                      sx={{
                        display: 'flex',
                        gap: 2,
                        p: 1.5,
                        borderRadius: '6px',
                        backgroundColor: gcpPalette.neutral.background,
                        border: `1px solid ${gcpPalette.neutral.border}`,
                      }}
                    >
                      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
                        <TableChartIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
                        <Box>
                          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, display: 'block' }}>
                            Tables
                          </Typography>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {conn.total_tables}
                          </Typography>
                        </Box>
                      </Box>

                      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
                        <LayersIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
                        <Box>
                          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, display: 'block' }}>
                            Total Rows
                          </Typography>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {conn.total_rows.toLocaleString()}
                          </Typography>
                        </Box>
                      </Box>

                      <Box sx={{ flex: 1, display: 'flex', alignItems: 'center', gap: 1 }}>
                        <StorageIcon fontSize="small" sx={{ color: gcpPalette.neutral.textSecondary }} />
                        <Box>
                          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary, display: 'block' }}>
                            DuckDB Size
                          </Typography>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {conn.size_mb} MB
                          </Typography>
                        </Box>
                      </Box>
                    </Box>
                  </CardContent>

                  <Divider />

                  {/* Card Actions */}
                  <CardActions sx={{ px: 2, py: 1.5, display: 'flex', justifyContent: 'space-between' }}>
                    <Box sx={{ display: 'flex', gap: 1 }}>
                      <Button
                        size="small"
                        variant="contained"
                        startIcon={<LaunchIcon />}
                        disabled={!isReady}
                        onClick={() => {
                          onSelectDatabase(conn.id);
                          onOpenExplorer(conn.id);
                        }}
                      >
                        Open Explorer
                      </Button>

                      <Button
                        size="small"
                        variant="outlined"
                        startIcon={isSyncing ? <CircularProgress size={14} /> : <SyncIcon />}
                        disabled={isSyncing}
                        onClick={() => handleSync(conn)}
                      >
                        {isSyncing ? 'Ingesting...' : isReady ? 'Reload DuckDB' : 'Ingest into DuckDB'}
                      </Button>
                    </Box>

                    <Tooltip title="Delete Database Connection">
                      <IconButton
                        size="small"
                        color="error"
                        onClick={() => setDeleteTarget(conn)}
                        disabled={isSyncing}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </CardActions>
                </Card>
              </Grid>
            );
          })}
        </Grid>
      )}

      {/* Live Console Output when Ingesting */}
      {syncLogs.length > 0 && (
        <Box sx={{ mt: 4 }}>
          <Typography variant="h3" sx={{ fontSize: '0.95rem', mb: 1 }}>
            Ingestion Progress Console
          </Typography>
          <CodeBlock
            code={syncLogs.join('\n')}
            language="bash"
            maxHeight={260}
            title="DuckDB Ingestion Stream"
          />
        </Box>
      )}

      {/* Add Connection Modal */}
      <AddConnectionModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onConnectionCreated={(newConn) => {
          loadConnections();
          onConnectionsChanged?.();
          onSelectDatabase(newConn.id);
        }}
      />

      {/* Delete Confirmation Dialog */}
      <Dialog open={Boolean(deleteTarget)} onClose={() => setDeleteTarget(null)}>
        <DialogTitle>Delete Connection?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            Are you sure you want to remove <strong>{deleteTarget?.name}</strong>?
            This will remove the connection from your workspace and delete its local DuckDB database file (<code>{deleteTarget?.duckdb_path}</code>).
          </DialogContentText>

          {deleteTarget?.staging_path && (
            <Box sx={{ mt: 2 }}>
              <FormControlLabel
                control={
                  <Checkbox
                    checked={deleteStaging}
                    onChange={(e) => setDeleteStaging(e.target.checked)}
                    color="error"
                  />
                }
                label={
                  <Typography variant="body2">
                    Also permanently delete raw staging files in <code>{deleteTarget.staging_path}</code>
                  </Typography>
                }
              />
            </Box>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDeleteTarget(null)} disabled={deleting}>
            Cancel
          </Button>
          <Button onClick={handleDelete} color="error" variant="contained" disabled={deleting}>
            {deleting ? 'Deleting...' : 'Delete Connection'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};
