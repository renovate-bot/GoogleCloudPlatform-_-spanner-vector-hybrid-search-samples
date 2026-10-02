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

import React, { useState } from 'react';
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
  Stack,
  IconButton,
  InputAdornment,
  Link,
  Chip,
} from '@mui/material';
import KeyIcon from '@mui/icons-material/VpnKey';
import Visibility from '@mui/icons-material/Visibility';
import VisibilityOff from '@mui/icons-material/VisibilityOff';
import CloseIcon from '@mui/icons-material/Close';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutline';

import { saveAiKey, deleteAiKey } from '../services/api';

interface AiKeyDialogProps {
  open: boolean;
  hasKey: boolean;
  model?: string;
  onClose: () => void;
  onKeySaved: () => void;
  onKeyRemoved: () => void;
}

export const AiKeyDialog: React.FC<AiKeyDialogProps> = ({
  open,
  hasKey,
  model = 'gemini-3.8-flash',
  onClose,
  onKeySaved,
  onKeyRemoved,
}) => {
  const [apiKey, setApiKey] = useState<string>('');
  const [showKey, setShowKey] = useState<boolean>(false);
  const [saving, setSaving] = useState<boolean>(false);
  const [removing, setRemoving] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const handleSave = async () => {
    if (!apiKey.trim()) {
      setErrorMessage('Please enter a valid Gemini API key.');
      return;
    }
    setSaving(true);
    setErrorMessage(null);
    setSuccessMessage(null);
    try {
      await saveAiKey(apiKey.trim());
      setSuccessMessage('Gemini API key verified and activated successfully!');
      setApiKey('');
      setTimeout(() => {
        onKeySaved();
        onClose();
      }, 1000);
    } catch (err: any) {
      console.error('Failed to verify Gemini API key', err);
      setErrorMessage(
        err?.response?.data?.detail || err.message || 'Verification failed. Please check your Gemini key.'
      );
    } finally {
      setSaving(false);
    }
  };

  const handleRemove = async () => {
    setRemoving(true);
    setErrorMessage(null);
    setSuccessMessage(null);
    try {
      await deleteAiKey();
      setSuccessMessage('Gemini API key file (gemini.key) removed.');
      setTimeout(() => {
        onKeyRemoved();
        onClose();
      }, 800);
    } catch (err: any) {
      console.error('Failed to remove Gemini API key', err);
      setErrorMessage(err?.response?.data?.detail || err.message || 'Failed to remove key.');
    } finally {
      setRemoving(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle sx={{ m: 0, p: 2, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <Stack direction="row" spacing={1.5} alignItems="center">
          <AutoAwesomeIcon sx={{ color: '#7c3aed', fontSize: 24 }} />
          <Typography variant="h6" sx={{ fontWeight: 600, fontSize: '1.05rem' }}>
            Spanner AI Assistant Configuration
          </Typography>
        </Stack>
        <IconButton size="small" onClick={onClose}>
          <CloseIcon fontSize="small" />
        </IconButton>
      </DialogTitle>

      <DialogContent dividers sx={{ p: 3 }}>
        <Stack spacing={2.5}>
          {/* Status Badge */}
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', backgroundColor: '#f8fafc', p: 1.5, borderRadius: 1.5, border: '1px solid #e2e8f0' }}>
            <Box>
              <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', fontWeight: 500 }}>
                Status:
              </Typography>
              <Stack direction="row" spacing={1} alignItems="center" sx={{ mt: 0.3 }}>
                {hasKey ? (
                  <Chip
                    icon={<CheckCircleIcon sx={{ fontSize: '14px !important', color: '#16a34a !important' }} />}
                    label="Active (gemini.key present)"
                    size="small"
                    sx={{ backgroundColor: '#dcfce7', color: '#15803d', fontWeight: 600, fontSize: '0.75rem' }}
                  />
                ) : (
                  <Chip
                    label="Inactive (No API Key)"
                    size="small"
                    sx={{ backgroundColor: '#fee2e2', color: '#b91c1c', fontWeight: 600, fontSize: '0.75rem' }}
                  />
                )}
                <Chip
                  label={model}
                  size="small"
                  variant="outlined"
                  sx={{ fontSize: '0.75rem', fontWeight: 500, borderColor: '#cbd5e1' }}
                />
              </Stack>
            </Box>

            {hasKey && (
              <Button
                variant="outlined"
                color="error"
                size="small"
                startIcon={removing ? <CircularProgress size={14} color="inherit" /> : <DeleteOutlineIcon sx={{ fontSize: 16 }} />}
                onClick={handleRemove}
                disabled={removing}
                sx={{ textTransform: 'none', fontSize: '0.75rem' }}
              >
                {removing ? 'Removing...' : 'Deactivate Key'}
              </Button>
            )}
          </Box>

          {/* Input field */}
          <Box>
            <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 0.8, color: '#1e293b' }}>
              {hasKey ? 'Replace Gemini API Key' : 'Enter Gemini API Key'}
            </Typography>
            <TextField
              fullWidth
              size="small"
              type={showKey ? 'text' : 'password'}
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              placeholder="AIzaSy..."
              InputProps={{
                startAdornment: (
                  <InputAdornment position="start">
                    <KeyIcon sx={{ fontSize: 18, color: 'text.secondary' }} />
                  </InputAdornment>
                ),
                endAdornment: (
                  <InputAdornment position="end">
                    <IconButton size="small" onClick={() => setShowKey(!showKey)} edge="end">
                      {showKey ? <VisibilityOff fontSize="small" /> : <Visibility fontSize="small" />}
                    </IconButton>
                  </InputAdornment>
                ),
              }}
            />
            <Typography variant="caption" sx={{ color: 'text.secondary', display: 'block', mt: 0.8 }}>
              Need an API key? Generate one for free at{' '}
              <Link href="https://aistudio.google.com/app/apikey" target="_blank" rel="noopener noreferrer">
                Google AI Studio &rarr;
              </Link>
            </Typography>
          </Box>

          {/* Alerts */}
          {errorMessage && <Alert severity="error" sx={{ fontSize: '0.8rem' }}>{errorMessage}</Alert>}
          {successMessage && <Alert severity="success" sx={{ fontSize: '0.8rem' }}>{successMessage}</Alert>}
        </Stack>
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={onClose} color="inherit" sx={{ textTransform: 'none' }}>
          Cancel
        </Button>
        <Button
          variant="contained"
          onClick={handleSave}
          disabled={saving || !apiKey.trim()}
          startIcon={saving ? <CircularProgress size={16} color="inherit" /> : <KeyIcon />}
          sx={{
            textTransform: 'none',
            fontWeight: 600,
            backgroundColor: '#7c3aed',
            '&:hover': { backgroundColor: '#6d28d9' },
          }}
        >
          {saving ? 'Verifying Key...' : 'Save & Activate'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
