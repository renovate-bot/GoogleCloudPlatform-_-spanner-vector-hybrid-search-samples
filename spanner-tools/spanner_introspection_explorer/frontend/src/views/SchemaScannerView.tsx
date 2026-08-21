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
  Accordion,
  AccordionSummary,
  AccordionDetails,
  CircularProgress,
  Alert,
  Divider,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import CodeIcon from '@mui/icons-material/Code';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { api } from '../services/api';
import { streamEvents } from '../services/sse';
import { CodeBlock } from '../components/CodeBlock';
import { gcpPalette } from '../theme';

export const SchemaScannerView: React.FC = () => {
  const [schemaInfo, setSchemaInfo] = useState<{ exists: boolean; total_chars: number; preview: string; is_configured: boolean } | null>(null);
  const [loadingSchema, setLoadingSchema] = useState(true);
  const [streaming, setStreaming] = useState(false);
  const [streamContent, setStreamContent] = useState('');
  const [cancelStream, setCancelStream] = useState<(() => void) | null>(null);

  useEffect(() => {
    api.getSchemaPreview()
      .then((res) => {
        setSchemaInfo(res);
        setLoadingSchema(false);
      })
      .catch((err) => {
        console.error('Failed to load schema preview', err);
        setLoadingSchema(false);
      });

    return () => {
      if (cancelStream) cancelStream();
    };
  }, []);

  const handleStartAnalysis = () => {
    setStreamContent('');
    setStreaming(true);

    const cancel = streamEvents('/api/v1/agent/schema/stream', {
      onMessage: (data) => {
        if (data.text) {
          setStreamContent((prev) => prev + data.text);
        }
        if (data.error) {
          setStreamContent((prev) => prev + `\n\n❌ ${data.error}`);
        }
      },
      onError: (err) => {
        setStreamContent((prev) => prev + `\n\n❌ Stream error: ${err.message}`);
        setStreaming(false);
      },
      onDone: () => {
        setStreaming(false);
      },
    });

    setCancelStream(() => cancel);
  };

  const handleStopAnalysis = () => {
    if (cancelStream) {
      cancelStream();
      setCancelStream(null);
    }
    setStreaming(false);
  };

  return (
    <Box>
      <Box sx={{ mb: 3 }}>
        <Typography variant="h1" sx={{ mb: 0.5, display: 'flex', alignItems: 'center', gap: 1 }}>
          <AutoAwesomeIcon sx={{ color: gcpPalette.primary.main }} />
          Cloud Spanner Schema Scanner
        </Typography>
        <Typography variant="body1" sx={{ color: gcpPalette.neutral.textSecondary }}>
          AI-powered reliability review analyzing table designs, primary keys, indexing strategies, and hotspotting risks against Google Cloud Spanner standards.
        </Typography>
      </Box>

      {/* Schema Staging File Accordion */}
      <Paper sx={{ mb: 3, border: `1px solid ${gcpPalette.neutral.border}` }}>
        <Accordion defaultExpanded={false}>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
              <CodeIcon sx={{ color: gcpPalette.neutral.textSecondary, fontSize: 20 }} />
              <Typography variant="body2" sx={{ fontWeight: 600 }}>
                Staging Schema Source (staging/schema.sql)
              </Typography>
              {schemaInfo?.exists ? (
                <Typography variant="caption" sx={{ color: gcpPalette.status.success.main, fontWeight: 600 }}>
                  ({schemaInfo.total_chars.toLocaleString()} characters loaded)
                </Typography>
              ) : (
                <Typography variant="caption" sx={{ color: gcpPalette.status.warning.main, fontWeight: 600 }}>
                  (schema.sql not found)
                </Typography>
              )}
            </Box>
          </AccordionSummary>
          <AccordionDetails sx={{ pt: 0 }}>
            {schemaInfo?.preview ? (
              <CodeBlock code={schemaInfo.preview} language="sql" maxHeight={240} title="schema.sql preview" />
            ) : (
              <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, py: 1 }}>
                No schema file found in <code>staging/schema.sql</code>. Export schema using <code>span showdatabase</code> or pipeline runner.
              </Typography>
            )}
          </AccordionDetails>
        </Accordion>
      </Paper>

      {/* Action Toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 3 }}>
        {!streaming ? (
          <Button
            variant="contained"
            color="primary"
            startIcon={<PlayArrowIcon />}
            onClick={handleStartAnalysis}
            disabled={!schemaInfo?.exists}
            sx={{ px: 3, py: 1 }}
          >
            Launch Schema Audit
          </Button>
        ) : (
          <Button
            variant="outlined"
            color="error"
            startIcon={<StopIcon />}
            onClick={handleStopAnalysis}
            sx={{ px: 3, py: 1 }}
          >
            Stop Streaming
          </Button>
        )}

        {streaming && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <CircularProgress size={20} />
            <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary }}>
              Reviewing schema with Gemini 2.0 Flash DBRE...
            </Typography>
          </Box>
        )}
      </Box>

      {/* Live AI Streaming Output Panel */}
      {streamContent && (
        <Paper
          sx={{
            p: 3,
            border: `1px solid ${gcpPalette.neutral.border}`,
            borderRadius: '8px',
            backgroundColor: '#ffffff',
            '& h1, & h2, & h3, & h4': {
              color: gcpPalette.neutral.textPrimary,
              fontWeight: 600,
              mt: 2,
              mb: 1,
            },
            '& p': {
              lineHeight: 1.6,
              mb: 1.5,
              fontSize: '0.875rem',
            },
            '& pre': {
              backgroundColor: '#202124',
              color: '#e8eaed',
              p: 1.5,
              borderRadius: '6px',
              overflowX: 'auto',
              my: 1.5,
            },
            '& code': {
              fontFamily: 'Roboto Mono, monospace',
              fontSize: '0.8125rem',
            },
            '& ul, & ol': {
              pl: 3,
              mb: 1.5,
              fontSize: '0.875rem',
            },
            '& li': {
              mb: 0.5,
            },
          }}
        >
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{streamContent}</ReactMarkdown>
        </Paper>
      )}
    </Box>
  );
};
