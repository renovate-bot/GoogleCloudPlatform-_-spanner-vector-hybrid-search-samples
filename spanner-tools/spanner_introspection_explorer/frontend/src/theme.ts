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

import { createTheme } from '@mui/material/styles';

export const gcpPalette = {
  primary: {
    main: '#1a73e8',       // Google Blue (Buttons, Active Tabs, Links)
    light: '#e8f0fe',      // Light Blue Tint (Active Nav Item, Selected Rows)
    dark: '#174ea6',       // Dark Blue (Button Hover State)
  },
  neutral: {
    background: '#f8f9fa', // App Canvas & Header Background
    surface: '#ffffff',    // Card / Table Surface
    border: '#dadce0',     // 1px Container Borders & Dividers
    textPrimary: '#202124',// Primary Body & Title Text
    textSecondary: '#5f6368', // Subtitles, Metadata, Captions
  },
  status: {
    success: {
      main: '#1e8e3e',     // Google Green (Healthy, Pass, Ready, Drained)
      light: '#e6f4ea',    // Badge Background
    },
    warning: {
      main: '#f29900',     // Google Yellow/Amber (Warnings, Degraded, Stale)
      light: '#fef7e0',    // Badge Background
    },
    error: {
      main: '#d93025',     // Google Red (Failures, Errors, Blockers)
      light: '#fce8e6',    // Badge Background
    },
    info: {
      main: '#1a73e8',     // Info Blue (Running Jobs, Streaming, Config)
      light: '#e8f0fe',    // Badge Background
    },
    pending: {
      main: '#80868b',     // Google Neutral Gray (Pending, Untested)
      light: '#f1f3f4',    // Badge Background
    }
  }
};

export const theme = createTheme({
  palette: {
    primary: gcpPalette.primary,
    background: {
      default: gcpPalette.neutral.background,
      paper: gcpPalette.neutral.surface,
    },
    text: {
      primary: gcpPalette.neutral.textPrimary,
      secondary: gcpPalette.neutral.textSecondary,
    },
    divider: gcpPalette.neutral.border,
  },
  typography: {
    fontFamily: '"Roboto", "Google Sans", "Helvetica", "Arial", sans-serif',
    h1: { fontSize: '1.75rem', fontWeight: 500, color: '#202124' },
    h2: { fontSize: '1.35rem', fontWeight: 500, color: '#202124' },
    h3: { fontSize: '1.15rem', fontWeight: 600, color: '#202124' },
    body1: { fontSize: '0.875rem', lineHeight: 1.5, color: '#202124' },
    body2: { fontSize: '0.8125rem', color: '#5f6368' },
    caption: { fontSize: '0.75rem', color: '#5f6368' },
    button: { textTransform: 'none', fontWeight: 500 }, // No uppercase buttons!
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiPaper: {
      defaultProps: {
        elevation: 0,
      },
      styleOverrides: {
        root: {
          boxShadow: 'none',
          border: `1px solid ${gcpPalette.neutral.border}`,
          borderRadius: '8px',
        },
      },
    },
    MuiCard: {
      defaultProps: {
        elevation: 0,
      },
      styleOverrides: {
        root: {
          boxShadow: 'none',
          border: `1px solid ${gcpPalette.neutral.border}`,
          borderRadius: '8px',
        },
      },
    },
    MuiButton: {
      defaultProps: {
        disableElevation: true,
      },
      styleOverrides: {
        root: {
          textTransform: 'none',
          borderRadius: '4px',
          fontWeight: 500,
        },
        containedPrimary: {
          backgroundColor: gcpPalette.primary.main,
          '&:hover': {
            backgroundColor: gcpPalette.primary.dark,
          },
        },
        outlined: {
          borderColor: gcpPalette.neutral.border,
          color: gcpPalette.primary.main,
          '&:hover': {
            borderColor: gcpPalette.primary.main,
            backgroundColor: gcpPalette.primary.light,
          },
        },
      },
    },
    MuiTableHead: {
      styleOverrides: {
        root: {
          backgroundColor: '#f8f9fa',
          '& .MuiTableCell-head': {
            fontSize: '0.75rem',
            fontWeight: 600,
            color: '#5f6368',
            textTransform: 'uppercase',
            letterSpacing: '0.5px',
            borderBottom: `1px solid ${gcpPalette.neutral.border}`,
            padding: '8px 16px',
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          fontSize: '0.8125rem',
          borderBottom: `1px solid ${gcpPalette.neutral.border}`,
          padding: '8px 16px',
        },
      },
    },
    MuiTableRow: {
      styleOverrides: {
        root: {
          '&:hover': {
            backgroundColor: 'rgba(26, 115, 232, 0.04)',
          },
          '&.Mui-selected': {
            backgroundColor: gcpPalette.primary.light,
            '&:hover': {
              backgroundColor: '#d2e3fc',
            },
          },
        },
      },
    },
  },
});
