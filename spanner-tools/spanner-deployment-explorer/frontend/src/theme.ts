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

import { createTheme } from '@mui/material/styles';

export const gcpPalette = {
  primary: {
    main: '#1a73e8', // Google Blue (Buttons, Active Tabs, Links)
    light: '#e8f0fe', // Light Blue Tint (Active Nav Item, Selected Rows)
    dark: '#174ea6', // Dark Blue (Button Hover State)
  },
  neutral: {
    background: '#f8f9fa', // App Canvas & Header Background
    surface: '#ffffff', // Card / Table Surface
    border: '#dadce0', // 1px Container Borders & Dividers
    textPrimary: '#202124', // Primary Body & Title Text
    textSecondary: '#5f6368', // Subtitles, Metadata, Captions
  },
  status: {
    success: {
      main: '#1e8e3e', // Google Green
      light: '#e6f4ea',
    },
    warning: {
      main: '#f29900', // Google Yellow/Amber
      light: '#fef7e0',
    },
    error: {
      main: '#d93025', // Google Red
      light: '#fce8e6',
    },
    info: {
      main: '#1a73e8', // Info Blue
      light: '#e8f0fe',
    },
    pending: {
      main: '#80868b', // Google Neutral Gray
      light: '#f1f3f4',
    },
  },
  replicaRole: {
    leader: '#d93025', // Google Red for Leader
    rwReplica: '#f29900', // Amber/Orange for Read-Write Replica
    witness: '#5f6368', // Neutral Slate Gray for Witness
    readOnly: '#1a73e8', // Google Blue for Read Only
    optionalReadOnly: '#12b5cb', // Teal for Optional Read Only
  },
};

export const gcpTheme = createTheme({
  palette: {
    primary: {
      main: gcpPalette.primary.main,
      light: gcpPalette.primary.light,
      dark: gcpPalette.primary.dark,
      contrastText: '#ffffff',
    },
    secondary: {
      main: '#5f6368',
      light: '#f1f3f4',
      dark: '#202124',
      contrastText: '#ffffff',
    },
    background: {
      default: gcpPalette.neutral.background,
      paper: gcpPalette.neutral.surface,
    },
    text: {
      primary: gcpPalette.neutral.textPrimary,
      secondary: gcpPalette.neutral.textSecondary,
    },
    success: {
      main: gcpPalette.status.success.main,
      light: gcpPalette.status.success.light,
    },
    warning: {
      main: gcpPalette.status.warning.main,
      light: gcpPalette.status.warning.light,
    },
    error: {
      main: gcpPalette.status.error.main,
      light: gcpPalette.status.error.light,
    },
    info: {
      main: gcpPalette.status.info.main,
      light: gcpPalette.status.info.light,
    },
    divider: gcpPalette.neutral.border,
  },
  typography: {
    fontFamily: '"Roboto", "Google Sans", "Helvetica", "Arial", sans-serif',
    h1: { fontSize: '1.75rem', fontWeight: 500, color: '#202124' },
    h2: { fontSize: '1.35rem', fontWeight: 500, color: '#202124' },
    h3: { fontSize: '1.15rem', fontWeight: 600, color: '#202124' },
    h4: { fontSize: '1rem', fontWeight: 600, color: '#202124' },
    subtitle1: { fontSize: '0.9rem', color: '#5f6368' },
    subtitle2: { fontSize: '0.8125rem', fontWeight: 500, color: '#5f6368' },
    body1: { fontSize: '0.875rem', lineHeight: 1.5, color: '#202124' },
    body2: { fontSize: '0.8125rem', color: '#5f6368' },
    caption: { fontSize: '0.75rem', color: '#5f6368' },
    button: { textTransform: 'none', fontWeight: 500 },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    MuiButton: {
      styleOverrides: {
        root: {
          borderRadius: 4,
          padding: '6px 16px',
          boxShadow: 'none',
          textTransform: 'none',
          fontWeight: 500,
          '&:hover': {
            boxShadow: '0 1px 2px 0 rgba(60,64,67,0.3), 0 1px 3px 1px rgba(60,64,67,0.15)',
          },
        },
        containedPrimary: {
          backgroundColor: '#1a73e8',
          '&:hover': {
            backgroundColor: '#174ea6',
          },
        },
        outlined: {
          borderColor: '#dadce0',
          color: '#1a73e8',
          '&:hover': {
            borderColor: '#1a73e8',
            backgroundColor: 'rgba(26,115,232,0.04)',
          },
        },
      },
    },
    MuiPaper: {
      styleOverrides: {
        root: {
          boxShadow: 'none',
          border: '1px solid #dadce0',
          borderRadius: '8px',
        },
      },
    },
    MuiCard: {
      styleOverrides: {
        root: {
          boxShadow: 'none',
          border: '1px solid #dadce0',
          borderRadius: '8px',
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
            textTransform: 'uppercase',
            color: '#5f6368',
            borderBottom: '1px solid #dadce0',
            backgroundColor: '#f8f9fa',
            padding: '10px 16px',
          },
        },
      },
    },
    MuiTableCell: {
      styleOverrides: {
        root: {
          fontSize: '0.8125rem',
          borderBottom: '1px solid #dadce0',
          padding: '10px 16px',
        },
      },
    },
    MuiChip: {
      styleOverrides: {
        root: {
          borderRadius: 16,
          fontWeight: 500,
          fontSize: '0.75rem',
          height: 24,
        },
      },
    },
    MuiAppBar: {
      styleOverrides: {
        root: {
          backgroundColor: '#ffffff',
          color: '#202124',
          borderBottom: '1px solid #dadce0',
          boxShadow: 'none',
        },
      },
    },
  },
});
