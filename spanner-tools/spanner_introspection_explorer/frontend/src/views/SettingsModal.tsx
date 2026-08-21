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
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  Typography,
  Box,
  Alert,
  CircularProgress,
  Divider,
  Switch,
  FormControlLabel,
} from '@mui/material';
import TuneIcon from '@mui/icons-material/Tune';
import KeyIcon from '@mui/icons-material/Key';
import BarChartIcon from '@mui/icons-material/BarChart';
import { api } from '../services/api';
import { AppConfig } from '../types';
import { gcpPalette } from '../theme';

interface SettingsModalProps {
  open: boolean;
  onClose: () => void;
  onConfigSaved: () => void;
}

export const SettingsModal: React.FC<SettingsModalProps> = ({ open, onClose, onConfigSaved }) => {
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [apiKeyInput, setApiKeyInput] = useState('');
  const [saveSuccess, setSaveSuccess] = useState(false);
  const [columnProfilesEnabled, setColumnProfilesEnabled] = useState<boolean>(() => {
    return localStorage.getItem('span_explorer_show_column_profiles') !== 'false';
  });

  useEffect(() => {
    if (open) {
      setLoading(true);
      setSaveSuccess(false);
      api.getConfig()
        .then((cfg) => {
          setConfig(cfg);
          setApiKeyInput('');
          setLoading(false);
        })
        .catch((err) => {
          console.error('Failed to load settings', err);
          setLoading(false);
        });
    }
  }, [open]);

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload: Partial<AppConfig> = {};
      if (apiKeyInput.trim()) {
        payload.google_api_key_configured = true;
        await api.updateConfig({ google_api_key: apiKeyInput.trim() } as any);
      }
      setSaveSuccess(true);
      setTimeout(() => {
        onConfigSaved();
        onClose();
      }, 1000);
    } catch (err) {
      console.error('Failed to save configuration', err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
        <TuneIcon sx={{ color: gcpPalette.primary.main }} />
        <Typography variant="h3" sx={{ fontSize: '1.1rem' }}>
          Application Settings
        </Typography>
      </DialogTitle>

      <DialogContent dividers sx={{ p: 3 }}>
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
            <CircularProgress size={32} />
          </Box>
        ) : (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
            {saveSuccess && (
              <Alert severity="success">Settings saved successfully.</Alert>
            )}

            <Box>
              <Typography variant="h3" sx={{ fontSize: '0.9rem', mb: 0.5, display: 'flex', alignItems: 'center', gap: 0.8 }}>
                <KeyIcon fontSize="small" sx={{ color: gcpPalette.primary.main }} />
                Gemini 2.0 DBRE API Key
              </Typography>
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mb: 1.5 }}>
                Enables AI DBRE Agents (Schema Scanner and Query Profile Analyzer). When no key is configured, AI features are completely hidden.
              </Typography>
              <TextField
                fullWidth
                size="small"
                type="password"
                placeholder={config?.google_api_key_preview ? `Current: ${config.google_api_key_preview}` : 'Enter AI Studio API Key (AIzaSy...)'}
                value={apiKeyInput}
                onChange={(e) => setApiKeyInput(e.target.value)}
                helperText={config?.google_api_key_configured ? "A key is currently configured. AI features are active." : "No key configured. AI features are disabled."}
              />
              {config?.google_api_key_configured && (
                <Button
                  size="small"
                  color="error"
                  variant="text"
                  sx={{ mt: 1, fontSize: '0.75rem' }}
                  onClick={async () => {
                    setSaving(true);
                    try {
                      await api.updateConfig({ google_api_key: "" } as any);
                      setSaveSuccess(true);
                      setTimeout(() => {
                        onConfigSaved();
                        onClose();
                      }, 1000);
                    } catch (err) {
                      console.error('Failed to clear key', err);
                    } finally {
                      setSaving(false);
                    }
                  }}
                >
                  Clear Key & Disable AI Features
                </Button>
              )}
            </Box>

            <Divider />

            <Box>
              <Typography variant="h3" sx={{ fontSize: '0.9rem', mb: 0.5, display: 'flex', alignItems: 'center', gap: 0.8 }}>
                <BarChartIcon fontSize="small" sx={{ color: gcpPalette.primary.main }} />
                Table Column Profiles & Histograms
              </Typography>
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mb: 1.5 }}>
                Renders micro-distribution sparklines and frequency bars directly in table headers for instant visual data profiling.
              </Typography>
              <FormControlLabel
                control={
                  <Switch
                    checked={columnProfilesEnabled}
                    onChange={(e) => {
                      setColumnProfilesEnabled(e.target.checked);
                      localStorage.setItem('span_explorer_show_column_profiles', String(e.target.checked));
                    }}
                    color="primary"
                  />
                }
                label={
                  <Typography variant="body2" sx={{ fontWeight: 500 }}>
                    Enable column distribution histograms by default
                  </Typography>
                }
              />
            </Box>

            <Divider />

            <Box>
              <Typography variant="h3" sx={{ fontSize: '0.9rem', mb: 0.5 }}>
                Local Persistence & Storage
              </Typography>
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mb: 1 }}>
                Database file: <code>{config?.database_file}</code>
              </Typography>
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
                Staging folder: <code>{config?.staging_dir}/</code>
              </Typography>
            </Box>
          </Box>
        )}
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={onClose} color="inherit">
          Cancel
        </Button>
        <Button onClick={handleSave} variant="contained" disabled={saving || (!apiKeyInput.trim() && !config)}>
          {saving ? 'Saving...' : 'Save Changes'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
