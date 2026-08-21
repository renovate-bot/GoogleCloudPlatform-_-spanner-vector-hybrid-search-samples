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

import React from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  Typography,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip,
  Button,
  Divider,
} from '@mui/material';
import StorageIcon from '@mui/icons-material/Storage';
import TableChartIcon from '@mui/icons-material/TableChart';
import LayersIcon from '@mui/icons-material/Layers';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';

import { DatabaseSummary, TableSummary } from '../types';
import { StatusBadge } from '../components/StatusBadge';
import { gcpPalette } from '../theme';

interface DashboardViewProps {
  dbSummary: DatabaseSummary | null;
  hasAiKey?: boolean;
  onSelectTable: (tableName: string) => void;
  onNavigate: (view: string) => void;
}

export const DashboardView: React.FC<DashboardViewProps> = ({
  dbSummary,
  hasAiKey = false,
  onSelectTable,
  onNavigate,
}) => {
  const totalTables = dbSummary?.total_tables || 0;
  const totalRows = dbSummary?.total_rows || 0;
  const dbFile = dbSummary?.database_file || 'my_duckdb.db';
  const tables = dbSummary?.tables || [];

  return (
    <Box>
      {/* Title & Overview Banner */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h1">
          Spanner Introspection Explorer
        </Typography>
      </Box>

      {/* Top Stat Cards */}
      <Grid container spacing={2} sx={{ mb: 3 }}>
        <Grid item xs={12} sm={4}>
          <Card sx={{ border: `1px solid ${gcpPalette.neutral.border}` }}>
            <CardContent sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Box
                sx={{
                  p: 1.5,
                  borderRadius: '8px',
                  backgroundColor: gcpPalette.primary.light,
                  color: gcpPalette.primary.main,
                }}
              >
                <StorageIcon fontSize="medium" />
              </Box>
              <Box>
                <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
                  Introspection Database
                </Typography>
                <Typography variant="h2" sx={{ fontSize: '1.25rem' }}>
                  {dbFile}
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4}>
          <Card sx={{ border: `1px solid ${gcpPalette.neutral.border}` }}>
            <CardContent sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Box
                sx={{
                  p: 1.5,
                  borderRadius: '8px',
                  backgroundColor: '#e6f4ea',
                  color: gcpPalette.status.success.main,
                }}
              >
                <TableChartIcon fontSize="medium" />
              </Box>
              <Box>
                <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
                  Introspection Tables
                </Typography>
                <Typography variant="h2" sx={{ fontSize: '1.25rem' }}>
                  {totalTables} Loaded
                </Typography>
                <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
                  Lock, Query & Txn Statistics
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>

        <Grid item xs={12} sm={4}>
          <Card sx={{ border: `1px solid ${gcpPalette.neutral.border}` }}>
            <CardContent sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
              <Box
                sx={{
                  p: 1.5,
                  borderRadius: '8px',
                  backgroundColor: '#fef7e0',
                  color: gcpPalette.status.warning.main,
                }}
              >
                <LayersIcon fontSize="medium" />
              </Box>
              <Box>
                <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
                  Total Ingested Records
                </Typography>
                <Typography variant="h2" sx={{ fontSize: '1.25rem' }}>
                  {totalRows.toLocaleString()} Rows
                </Typography>
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* AI DBRE Quick Launch Banner - Only shown when API key is provided */}
      {hasAiKey && (
        <Card
          sx={{
            mb: 3,
            p: 2.5,
            background: 'linear-gradient(90deg, #e8f0fe 0%, #ffffff 100%)',
            border: `1px solid #d2e3fc`,
          }}
        >
          <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: 2 }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <AutoAwesomeIcon sx={{ color: gcpPalette.primary.main, fontSize: 28 }} />
              <Box>
                <Typography variant="h3" sx={{ fontSize: '1.05rem', color: gcpPalette.primary.dark }}>
                  Gemini 2.0 DBRE Diagnostics
                </Typography>
                <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
                  Perform automated schema hotspot audits or investigate queries scanning &gt;100k rows.
                </Typography>
              </Box>
            </Box>
            <Box sx={{ display: 'flex', gap: 1.5 }}>
              <Button
                variant="outlined"
                size="small"
                onClick={() => onNavigate('schema_scanner')}
                sx={{ backgroundColor: '#ffffff' }}
              >
                Schema Scanner
              </Button>
              <Button
                variant="contained"
                size="small"
                onClick={() => onNavigate('query_profile')}
                endIcon={<ArrowForwardIcon />}
              >
                Query Profile Analyzer
              </Button>
            </Box>
          </Box>
        </Card>
      )}

      {/* Available Tables Inventory */}
      <Paper sx={{ border: `1px solid ${gcpPalette.neutral.border}`, overflow: 'hidden' }}>
        <Box sx={{ p: 2, borderBottom: `1px solid ${gcpPalette.neutral.border}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h3" sx={{ fontSize: '1rem' }}>
            Introspection Tables Inventory
          </Typography>
          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
            Click any table to open high-performance pushdown explorer
          </Typography>
        </Box>

        <TableContainer>
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Status</TableCell>
                <TableCell>Table Name</TableCell>
                <TableCell>Category</TableCell>
                <TableCell align="right">Rows</TableCell>
                <TableCell align="right">Columns</TableCell>
                <TableCell align="right">Action</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {tables.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} sx={{ textAlign: 'center', py: 5 }}>
                    <Box sx={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 1.5 }}>
                      <Typography variant="body1" sx={{ color: gcpPalette.neutral.textSecondary }}>
                        No database connection active or no tables ingested.
                      </Typography>
                      <Button
                        variant="contained"
                        size="small"
                        startIcon={<StorageIcon />}
                        onClick={() => onNavigate('connections')}
                        sx={{ textTransform: 'none', fontWeight: 600 }}
                      >
                        Manage Database Connections
                      </Button>
                    </Box>
                  </TableCell>
                </TableRow>
              ) : (
                tables.map((tbl) => (
                  <TableRow key={tbl.name} hover sx={{ cursor: 'pointer' }} onClick={() => onSelectTable(tbl.name)}>
                    <TableCell>
                      {tbl.is_large ? (
                        <Chip
                          icon={<WarningAmberIcon sx={{ fontSize: '14px !important' }} />}
                          label="Large Dataset"
                          size="small"
                          sx={{ height: 20, fontSize: '0.7rem', backgroundColor: '#fef7e0', color: '#b06000' }}
                        />
                      ) : (
                        <StatusBadge status="PASS" label="OK" size="small" />
                      )}
                    </TableCell>
                    <TableCell sx={{ fontWeight: 600, color: gcpPalette.primary.main }}>
                      {tbl.name}
                    </TableCell>
                    <TableCell>
                      <Chip
                        label={tbl.category}
                        size="small"
                        sx={{ height: 20, fontSize: '0.7rem', backgroundColor: gcpPalette.neutral.background }}
                      />
                    </TableCell>
                    <TableCell align="right" sx={{ fontWeight: 500 }}>
                      {tbl.row_count.toLocaleString()}
                    </TableCell>
                    <TableCell align="right" sx={{ color: gcpPalette.neutral.textSecondary }}>
                      {tbl.column_count}
                    </TableCell>
                    <TableCell align="right">
                      <Button size="small" variant="text" onClick={(e) => { e.stopPropagation(); onSelectTable(tbl.name); }}>
                        Explore
                      </Button>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    </Box>
  );
};
