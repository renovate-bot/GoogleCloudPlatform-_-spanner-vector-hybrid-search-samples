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
  Grid,
  Card,
  CardContent,
  Chip,
  CircularProgress,
  Divider,
} from '@mui/material';
import SpeedIcon from '@mui/icons-material/Speed';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import StopIcon from '@mui/icons-material/Stop';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

import { HighRowScanQuery } from '../types';
import { api } from '../services/api';
import { streamEvents } from '../services/sse';
import { CodeBlock } from '../components/CodeBlock';
import { gcpPalette } from '../theme';

export const QueryProfileView: React.FC = () => {
  const [queries, setQueries] = useState<HighRowScanQuery[]>([]);
  const [loadingQueries, setLoadingQueries] = useState(true);
  const [streaming, setStreaming] = useState(false);
  const [streamContent, setStreamContent] = useState('');
  const [cancelStream, setCancelStream] = useState<(() => void) | null>(null);

  useEffect(() => {
    api.getHighRowScanQueries()
      .then((res) => {
        setQueries(res.queries || []);
        setLoadingQueries(false);
      })
      .catch((err) => {
        console.error('Failed to load high row scan queries', err);
        setLoadingQueries(false);
      });

    return () => {
      if (cancelStream) cancelStream();
    };
  }, []);

  const handleStartAnalysis = () => {
    setStreamContent('');
    setStreaming(true);

    const cancel = streamEvents('/api/v1/agent/query-profile/stream', {
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
          <SpeedIcon sx={{ color: gcpPalette.status.warning.main }} />
          Query Profile Analyzer (High Row Scans)
        </Typography>
        <Typography variant="body1" sx={{ color: gcpPalette.neutral.textSecondary }}>
          Detects queries scanning &gt;100k rows across 1-hour and 10-minute introspection windows and provides automated DBRE recommendations for query rewrites and secondary indexes.
        </Typography>
      </Box>

      {/* Top High Row Scan Queries Cards */}
      <Box sx={{ mb: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1.5 }}>
          <Typography variant="h3" sx={{ fontSize: '1rem' }}>
            Detected High-Scan Queries ({queries.length})
          </Typography>
          <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
            Criteria: &gt;100,000 average rows scanned • &gt;10 executions
          </Typography>
        </Box>

        {loadingQueries ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
            <CircularProgress size={32} />
          </Box>
        ) : queries.length === 0 ? (
          <Paper sx={{ p: 3, textAlign: 'center', border: `1px solid ${gcpPalette.neutral.border}` }}>
            <Typography variant="body2" sx={{ color: gcpPalette.status.success.main, fontWeight: 600 }}>
              ✓ No queries currently match high-scan threshold (&gt;100k rows, &gt;10 execs).
            </Typography>
          </Paper>
        ) : (
          <Grid container spacing={2}>
            {queries.map((q, idx) => (
              <Grid item xs={12} key={idx}>
                <Card sx={{ border: `1px solid ${gcpPalette.neutral.border}` }}>
                  <CardContent sx={{ pb: '16px !important' }}>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', mb: 1, gap: 1 }}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <Chip
                          label={`Fingerprint: ${q.text_fingerprint || 'N/A'}`}
                          size="small"
                          sx={{ fontFamily: 'Roboto Mono, monospace', fontSize: '0.75rem', fontWeight: 600 }}
                        />
                        <Chip
                          icon={<WarningAmberIcon sx={{ fontSize: '14px !important' }} />}
                          label={`Avg Scanned: ${Number(q.avg_rows_scanned).toLocaleString()} rows`}
                          size="small"
                          sx={{ backgroundColor: '#fef7e0', color: '#b06000', fontWeight: 600, fontSize: '0.75rem' }}
                        />
                      </Box>
                      <Box sx={{ display: 'flex', gap: 1 }}>
                        <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
                          Executions: <strong>{q.total_exec}</strong>
                        </Typography>
                        <Typography variant="caption" sx={{ color: gcpPalette.neutral.textSecondary }}>
                          Max: <strong>{Number(q.max_rows_scanned).toLocaleString()}</strong>
                        </Typography>
                      </Box>
                    </Box>

                    <CodeBlock code={q.text} language="sql" maxHeight={150} />
                  </CardContent>
                </Card>
              </Grid>
            ))}
          </Grid>
        )}
      </Box>

      {/* Action Toolbar */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 3 }}>
        {!streaming ? (
          <Button
            variant="contained"
            color="primary"
            startIcon={<PlayArrowIcon />}
            onClick={handleStartAnalysis}
            disabled={queries.length === 0}
            sx={{ px: 3, py: 1 }}
          >
            Run Gemini DBRE Analysis
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
              Analyzing high row-scan query patterns with Gemini 2.0 Flash...
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
          }}
        >
          <ReactMarkdown remarkPlugins={[remarkGfm]}>{streamContent}</ReactMarkdown>
        </Paper>
      )}
    </Box>
  );
};
