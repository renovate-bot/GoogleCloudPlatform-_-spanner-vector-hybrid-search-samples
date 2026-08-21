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

import React, { useState } from 'react';
import { Box, IconButton, Tooltip, Typography } from '@mui/material';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import CheckIcon from '@mui/icons-material/Check';
import { gcpPalette } from '../theme';

interface CodeBlockProps {
  code: string;
  language?: string;
  maxHeight?: number | string;
  title?: string;
}

export const CodeBlock: React.FC<CodeBlockProps> = ({ code, language = 'sql', maxHeight = 300, title }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Box
      sx={{
        position: 'relative',
        backgroundColor: '#202124',
        color: '#e8eaed',
        borderRadius: '6px',
        overflow: 'hidden',
        border: '1px solid #3c4043',
        my: 1,
      }}
    >
      {title && (
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            px: 2,
            py: 0.8,
            backgroundColor: '#292a2d',
            borderBottom: '1px solid #3c4043',
          }}
        >
          <Typography variant="caption" sx={{ color: '#9aa0a6', fontWeight: 600, textTransform: 'uppercase' }}>
            {title} ({language})
          </Typography>
        </Box>
      )}

      <Box
        sx={{
          position: 'absolute',
          top: title ? 6 : 6,
          right: 8,
          zIndex: 2,
        }}
      >
        <Tooltip title={copied ? 'Copied to clipboard!' : 'Copy Code'}>
          <IconButton
            size="small"
            onClick={handleCopy}
            sx={{
              color: copied ? gcpPalette.status.success.main : '#9aa0a6',
              backgroundColor: 'rgba(255, 255, 255, 0.08)',
              '&:hover': {
                backgroundColor: 'rgba(255, 255, 255, 0.16)',
                color: '#ffffff',
              },
            }}
          >
            {copied ? <CheckIcon fontSize="small" /> : <ContentCopyIcon fontSize="small" />}
          </IconButton>
        </Tooltip>
      </Box>

      <Box
        component="pre"
        sx={{
          p: 1.5,
          m: 0,
          fontFamily: '"Roboto Mono", Menlo, Consolas, Monaco, monospace',
          fontSize: '0.8125rem',
          lineHeight: 1.5,
          overflowX: 'auto',
          maxHeight: maxHeight,
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-word',
        }}
      >
        <code>{code}</code>
      </Box>
    </Box>
  );
};
