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
  Drawer,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Collapse,
  Typography,
  Divider,
  Chip,
  IconButton,
  Tooltip,
  Button,
} from '@mui/material';
import DashboardIcon from '@mui/icons-material/Dashboard';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import CodeIcon from '@mui/icons-material/Code';
import SpeedIcon from '@mui/icons-material/Speed';
import LockIcon from '@mui/icons-material/Lock';
import SearchIcon from '@mui/icons-material/Search';
import CreditCardIcon from '@mui/icons-material/CreditCard';
import TableChartIcon from '@mui/icons-material/TableChart';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
import UnfoldMoreIcon from '@mui/icons-material/UnfoldMore';
import UnfoldLessIcon from '@mui/icons-material/UnfoldLess';
import SyncAltIcon from '@mui/icons-material/SyncAlt';
import StorageIcon from '@mui/icons-material/Storage';
import DnsIcon from '@mui/icons-material/Dns';
import AddIcon from '@mui/icons-material/Add';
import SettingsIcon from '@mui/icons-material/Settings';

import { DatabaseItem, DatabaseSummary, TableCategory } from '../types';
import { gcpPalette } from '../theme';

interface AppNavigationProps {
  availableDatabases: DatabaseItem[];
  selectedDatabase: string;
  currentView: string;
  selectedTable: string | null;
  summaries: Record<string, DatabaseSummary>;
  hasAiKey?: boolean;
  onSelectView: (databaseId: string, view: string, table?: string) => void;
  onRegisterDatabase?: () => void;
}

const DRAWER_WIDTH = 290;

export const AppNavigation: React.FC<AppNavigationProps> = ({
  availableDatabases,
  selectedDatabase,
  currentView,
  selectedTable,
  summaries,
  hasAiKey = false,
  onSelectView,
  onRegisterDatabase,
}) => {
  // Tree expand/collapse states
  const [openDatabases, setOpenDatabases] = useState<Record<string, boolean>>({});
  const [openCategories, setOpenCategories] = useState<Record<string, boolean>>({});

  // Auto-expand active database
  useEffect(() => {
    if (selectedDatabase) {
      setOpenDatabases((prev) => ({
        ...prev,
        [selectedDatabase]: prev[selectedDatabase] !== undefined ? prev[selectedDatabase] : true,
      }));
    }
  }, [selectedDatabase]);

  // Expand / Collapse all databases and categories
  const handleExpandAll = () => {
    const allDbOpen: Record<string, boolean> = {};
    const allCatOpen: Record<string, boolean> = {};
    availableDatabases.forEach((db) => {
      allDbOpen[db.id] = true;
      const summary = summaries[db.id];
      if (summary?.categories) {
        Object.keys(summary.categories).forEach((cat) => {
          allCatOpen[`${db.id}_${cat}`] = true;
        });
      }
    });
    setOpenDatabases(allDbOpen);
    setOpenCategories(allCatOpen);
  };

  const handleCollapseAll = () => {
    const allDbClosed: Record<string, boolean> = {};
    availableDatabases.forEach((db) => {
      allDbClosed[db.id] = false;
    });
    setOpenDatabases(allDbClosed);
  };

  const toggleDatabase = (dbId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setOpenDatabases((prev) => ({ ...prev, [dbId]: !prev[dbId] }));
  };

  const toggleCategory = (key: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setOpenCategories((prev) => ({ ...prev, [key]: prev[key] === undefined ? false : !prev[key] }));
  };

  const getCategoryIcon = (category: string) => {
    switch (category) {
      case 'Locking':
        return <LockIcon sx={{ fontSize: 16 }} />;
      case 'Query':
        return <SearchIcon sx={{ fontSize: 16 }} />;
      case 'Transactions':
        return <CreditCardIcon sx={{ fontSize: 16 }} />;
      default:
        return <TableChartIcon sx={{ fontSize: 16 }} />;
    }
  };

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: DRAWER_WIDTH,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: DRAWER_WIDTH,
          boxSizing: 'border-box',
          top: 48,
          height: 'calc(100% - 48px)',
          borderRight: `1px solid ${gcpPalette.neutral.border}`,
          backgroundColor: '#ffffff',
        },
      }}
    >
      <Box sx={{ overflowY: 'auto', p: 1.2, display: 'flex', flexDirection: 'column', height: '100%' }}>
        {/* Databases Section Header with Expand/Collapse All and Add Connection */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', px: 0.5, py: 0.5, mb: 1 }}>
          <Typography
            variant="caption"
            sx={{ fontWeight: 700, color: gcpPalette.neutral.textSecondary, letterSpacing: '0.6px', fontSize: '0.72rem' }}
          >
            DATABASES ({availableDatabases.length})
          </Typography>

          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.3 }}>
            <Tooltip title="Expand All Databases">
              <span>
                <IconButton
                  size="small"
                  onClick={handleExpandAll}
                  disabled={availableDatabases.length === 0}
                  sx={{ p: 0.3, color: gcpPalette.neutral.textSecondary, '&:hover': { color: gcpPalette.primary.main } }}
                >
                  <UnfoldMoreIcon sx={{ fontSize: 18 }} />
                </IconButton>
              </span>
            </Tooltip>

            <Tooltip title="Collapse All Databases">
              <span>
                <IconButton
                  size="small"
                  onClick={handleCollapseAll}
                  disabled={availableDatabases.length === 0}
                  sx={{ p: 0.3, color: gcpPalette.neutral.textSecondary, '&:hover': { color: gcpPalette.primary.main } }}
                >
                  <UnfoldLessIcon sx={{ fontSize: 18 }} />
                </IconButton>
              </span>
            </Tooltip>

            {onRegisterDatabase && (
              <Tooltip title="Register New Connection">
                <IconButton size="small" onClick={onRegisterDatabase} sx={{ p: 0.3, color: gcpPalette.primary.main, ml: 0.2 }}>
                  <AddIcon sx={{ fontSize: 19 }} />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        </Box>

        <List dense disablePadding sx={{ flexGrow: 1 }}>
          {availableDatabases.length === 0 ? (
            <Box sx={{ p: 2, textAlign: 'center' }}>
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mb: 1.5 }}>
                No database registered
              </Typography>
              {onRegisterDatabase && (
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={<AddIcon />}
                  onClick={onRegisterDatabase}
                  sx={{ textTransform: 'none', fontSize: '0.75rem' }}
                >
                  Register Connection
                </Button>
              )}
            </Box>
          ) : (
            availableDatabases.map((db) => {
              const isDbOpen = openDatabases[db.id] !== false; // Open by default
              const isDbActive = selectedDatabase === db.id;
              const summary = summaries[db.id];
              const categories = summary?.categories || { Locking: [], Query: [], Transactions: [], Misc: [] };

              return (
                <Box key={db.id} sx={{ mb: 1.5 }}>
                  {/* Database Tree Root Node (Prominent Header) */}
                  <ListItem disablePadding sx={{ mb: 0.5 }}>
                    <ListItemButton
                      onClick={() => onSelectView(db.id, 'dashboard')}
                      sx={{
                        borderRadius: '6px',
                        py: 0.7,
                        px: 1.2,
                        backgroundColor: isDbActive ? 'rgba(26, 115, 232, 0.08)' : '#f8f9fa',
                        border: `1px solid ${isDbActive ? 'rgba(26, 115, 232, 0.35)' : '#e8eaed'}`,
                        borderLeft: `4px solid ${isDbActive ? gcpPalette.primary.main : '#80868b'}`,
                        transition: 'all 0.15s ease-in-out',
                        '&:hover': {
                          backgroundColor: isDbActive ? 'rgba(26, 115, 232, 0.14)' : '#f1f3f4',
                          borderColor: isDbActive ? gcpPalette.primary.main : '#dadce0',
                        },
                      }}
                    >
                      <ListItemIcon sx={{ minWidth: 26, color: isDbActive ? gcpPalette.primary.main : '#5f6368' }}>
                        <StorageIcon sx={{ fontSize: 18 }} />
                      </ListItemIcon>
                      <ListItemText
                        primary={db.name}
                        primaryTypographyProps={{
                          fontSize: '0.84rem',
                          fontWeight: 700,
                          color: isDbActive ? gcpPalette.primary.main : '#202124',
                          noWrap: true,
                        }}
                      />
                      {db.dialect && (
                        <Chip
                          label={db.dialect === 'POSTGRESQL' ? 'PG' : 'GSQL'}
                          size="small"
                          sx={{
                            height: 18,
                            fontSize: '0.62rem',
                            fontWeight: 700,
                            mr: 0.5,
                            backgroundColor: isDbActive ? 'rgba(26, 115, 232, 0.18)' : '#e8eaed',
                            color: isDbActive ? gcpPalette.primary.main : '#3c4043',
                          }}
                        />
                      )}
                      <Chip
                        label={db.total_rows > 0 ? `${(db.total_rows > 999 ? `${(db.total_rows / 1000).toFixed(0)}k` : db.total_rows)}` : '0'}
                        size="small"
                        sx={{
                          height: 18,
                          fontSize: '0.65rem',
                          fontWeight: 600,
                          mr: 0.5,
                          backgroundColor: '#ffffff',
                          color: isDbActive ? gcpPalette.primary.main : gcpPalette.neutral.textSecondary,
                          border: `1px solid ${isDbActive ? 'rgba(26, 115, 232, 0.25)' : '#dadce0'}`,
                        }}
                      />
                      <IconButton
                        size="small"
                        onClick={(e) => toggleDatabase(db.id, e)}
                        sx={{ p: 0.2, color: isDbActive ? gcpPalette.primary.main : gcpPalette.neutral.textSecondary }}
                      >
                        {isDbOpen ? <ExpandLess sx={{ fontSize: 18 }} /> : <ExpandMore sx={{ fontSize: 18 }} />}
                      </IconButton>
                    </ListItemButton>
                  </ListItem>

                  {/* Database Subtree Children */}
                  <Collapse in={isDbOpen} timeout="auto" unmountOnExit>
                    <List component="div" disablePadding sx={{ pl: 1.5 }}>
                      {/* Overview & Stats */}
                      <ListItem disablePadding sx={{ mb: 0.2 }}>
                        <ListItemButton
                          selected={isDbActive && currentView === 'dashboard'}
                          onClick={() => onSelectView(db.id, 'dashboard')}
                          sx={{
                            borderRadius: '4px',
                            py: 0.4,
                            '&.Mui-selected': {
                              backgroundColor: gcpPalette.primary.light,
                              color: gcpPalette.primary.main,
                              '& .MuiListItemIcon-root': { color: gcpPalette.primary.main },
                            },
                          }}
                        >
                          <ListItemIcon sx={{ minWidth: 26, color: gcpPalette.neutral.textSecondary }}>
                            <DashboardIcon sx={{ fontSize: 16 }} />
                          </ListItemIcon>
                          <ListItemText primary="Overview & Stats" primaryTypographyProps={{ fontSize: '0.75rem', fontWeight: 500 }} />
                        </ListItemButton>
                      </ListItem>

                      {/* Introspection Categories */}
                      {Object.entries(categories).map(([catName, tables]) => {
                        if (!tables || tables.length === 0) return null;
                        const catKey = `${db.id}_${catName}`;
                        const isCatOpen = openCategories[catKey] !== false; // Open by default

                        return (
                          <Box key={catKey} sx={{ mb: 0.2 }}>
                            <ListItem disablePadding>
                              <ListItemButton
                                onClick={(e) => toggleCategory(catKey, e)}
                                sx={{ py: 0.3, px: 0.8, borderRadius: '4px' }}
                              >
                                <ListItemIcon sx={{ minWidth: 24, color: gcpPalette.neutral.textSecondary }}>
                                  {getCategoryIcon(catName)}
                                </ListItemIcon>
                                <ListItemText
                                  primary={catName}
                                  primaryTypographyProps={{ fontSize: '0.75rem', fontWeight: 600, color: gcpPalette.neutral.textSecondary }}
                                />
                                <Chip
                                  label={tables.length}
                                  size="small"
                                  sx={{ height: 16, fontSize: '0.6rem', mr: 0.5, backgroundColor: gcpPalette.neutral.background }}
                                />
                                {isCatOpen ? <ExpandLess sx={{ fontSize: 14 }} /> : <ExpandMore sx={{ fontSize: 14 }} />}
                              </ListItemButton>
                            </ListItem>

                            <Collapse in={isCatOpen} timeout="auto" unmountOnExit>
                              <List component="div" disablePadding sx={{ pl: 2 }}>
                                {tables.map((table) => {
                                  const isSelected = isDbActive && currentView === 'table' && selectedTable === table;
                                  return (
                                    <ListItem key={table} disablePadding sx={{ mb: 0.2 }}>
                                      <ListItemButton
                                        selected={isSelected}
                                        onClick={() => onSelectView(db.id, 'table', table)}
                                        sx={{
                                          borderRadius: '4px',
                                          py: 0.3,
                                          '&.Mui-selected': {
                                            backgroundColor: gcpPalette.primary.light,
                                            color: gcpPalette.primary.main,
                                            fontWeight: 600,
                                          },
                                        }}
                                      >
                                        <ListItemText
                                          primary={table}
                                          primaryTypographyProps={{
                                            fontSize: '0.7rem',
                                            noWrap: true,
                                            title: table,
                                          }}
                                        />
                                      </ListItemButton>
                                    </ListItem>
                                  );
                                })}
                              </List>
                            </Collapse>
                          </Box>
                        );
                      })}

                      {/* AI DBRE Agents (if enabled) */}
                      {hasAiKey && (
                        <Box sx={{ mb: 0.2 }}>
                          <ListItem disablePadding>
                            <ListItemButton
                              onClick={(e) => toggleCategory(`${db.id}_agents`, e)}
                              sx={{ py: 0.3, px: 0.8, borderRadius: '4px' }}
                            >
                              <ListItemIcon sx={{ minWidth: 24, color: gcpPalette.primary.main }}>
                                <AutoAwesomeIcon sx={{ fontSize: 15 }} />
                              </ListItemIcon>
                              <ListItemText
                                primary="AI DBRE Agents"
                                primaryTypographyProps={{ fontSize: '0.75rem', fontWeight: 600, color: gcpPalette.primary.main }}
                              />
                              {openCategories[`${db.id}_agents`] !== false ? <ExpandLess sx={{ fontSize: 14 }} /> : <ExpandMore sx={{ fontSize: 14 }} />}
                            </ListItemButton>
                          </ListItem>

                          <Collapse in={openCategories[`${db.id}_agents`] !== false} timeout="auto" unmountOnExit>
                            <List component="div" disablePadding sx={{ pl: 2 }}>
                              <ListItem disablePadding sx={{ mb: 0.2 }}>
                                <ListItemButton
                                  selected={isDbActive && currentView === 'schema_scanner'}
                                  onClick={() => onSelectView(db.id, 'schema_scanner')}
                                  sx={{
                                    borderRadius: '4px',
                                    py: 0.3,
                                    '&.Mui-selected': {
                                      backgroundColor: gcpPalette.primary.light,
                                      color: gcpPalette.primary.main,
                                      fontWeight: 600,
                                    },
                                  }}
                                >
                                  <ListItemIcon sx={{ minWidth: 22, color: gcpPalette.neutral.textSecondary }}>
                                    <CodeIcon sx={{ fontSize: 14 }} />
                                  </ListItemIcon>
                                  <ListItemText primary="Schema Scanner" primaryTypographyProps={{ fontSize: '0.7rem' }} />
                                </ListItemButton>
                              </ListItem>
                              <ListItem disablePadding sx={{ mb: 0.2 }}>
                                <ListItemButton
                                  selected={isDbActive && currentView === 'query_profile'}
                                  onClick={() => onSelectView(db.id, 'query_profile')}
                                  sx={{
                                    borderRadius: '4px',
                                    py: 0.3,
                                    '&.Mui-selected': {
                                      backgroundColor: gcpPalette.primary.light,
                                      color: gcpPalette.primary.main,
                                      fontWeight: 600,
                                    },
                                  }}
                                >
                                  <ListItemIcon sx={{ minWidth: 22, color: gcpPalette.neutral.textSecondary }}>
                                    <SpeedIcon sx={{ fontSize: 14 }} />
                                  </ListItemIcon>
                                  <ListItemText primary="Query Profile Analyzer" primaryTypographyProps={{ fontSize: '0.7rem' }} />
                                </ListItemButton>
                              </ListItem>
                            </List>
                          </Collapse>
                        </Box>
                      )}

                      {/* Pipeline & Ingest */}
                      <ListItem disablePadding sx={{ mb: 0.2 }}>
                        <ListItemButton
                          selected={isDbActive && currentView === 'pipeline'}
                          onClick={() => onSelectView(db.id, 'pipeline')}
                          sx={{
                            borderRadius: '4px',
                            py: 0.4,
                            '&.Mui-selected': {
                              backgroundColor: gcpPalette.primary.light,
                              color: gcpPalette.primary.main,
                              '& .MuiListItemIcon-root': { color: gcpPalette.primary.main },
                            },
                          }}
                        >
                          <ListItemIcon sx={{ minWidth: 26, color: gcpPalette.neutral.textSecondary }}>
                            <SyncAltIcon sx={{ fontSize: 16 }} />
                          </ListItemIcon>
                          <ListItemText primary="Pipeline & Ingest" primaryTypographyProps={{ fontSize: '0.75rem', fontWeight: 500 }} />
                        </ListItemButton>
                      </ListItem>
                    </List>
                  </Collapse>
                </Box>
              );
            })
          )}
        </List>

        {/* Bottom Admin Section */}
        <Box sx={{ mt: 'auto', pt: 1 }}>
          <Divider sx={{ mb: 1 }} />
          <Typography
            variant="caption"
            sx={{ px: 1, py: 0.5, fontWeight: 700, color: gcpPalette.neutral.textSecondary, letterSpacing: '0.5px' }}
          >
            ADMIN
          </Typography>
          <List dense disablePadding>
            <ListItem disablePadding sx={{ mt: 0.5 }}>
              <ListItemButton
                selected={currentView === 'connections'}
                onClick={() => onSelectView('', 'connections')}
                sx={{
                  borderRadius: '4px',
                  py: 0.6,
                  '&.Mui-selected': {
                    backgroundColor: gcpPalette.primary.light,
                    color: gcpPalette.primary.main,
                    '& .MuiListItemIcon-root': { color: gcpPalette.primary.main },
                  },
                }}
              >
                <ListItemIcon sx={{ minWidth: 32, color: gcpPalette.neutral.textSecondary }}>
                  <SettingsIcon fontSize="small" />
                </ListItemIcon>
                <ListItemText primary="Database Connections" primaryTypographyProps={{ fontSize: '0.8125rem', fontWeight: 500 }} />
              </ListItemButton>
            </ListItem>
          </List>
        </Box>
      </Box>
    </Drawer>
  );
};
