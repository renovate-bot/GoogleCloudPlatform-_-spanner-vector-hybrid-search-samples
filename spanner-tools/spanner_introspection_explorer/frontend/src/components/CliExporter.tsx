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
import { Box, Tabs, Tab, Typography, Paper } from '@mui/material';
import CodeIcon from '@mui/icons-material/Code';
import StorageIcon from '@mui/icons-material/Storage';
import HttpIcon from '@mui/icons-material/Http';
import { CodeBlock } from './CodeBlock';
import { gcpPalette } from '../theme';

interface CliExporterProps {
  sqlQuery: string;
  tableName: string;
  utcOffset?: number;
}

export const CliExporter: React.FC<CliExporterProps> = ({ sqlQuery, tableName, utcOffset = 0 }) => {
  const [tabIndex, setTabIndex] = useState(0);

  // Generate curl REST command
  const curlCommand = `# Fetch identical paginated dataset via REST API
curl -X POST "http://localhost:8080/api/v1/tables/${tableName}/query" \\
  -H "Content-Type: application/json" \\
  -d '{
    "page": 1,
    "page_size": 50,
    "utc_offset": ${utcOffset},
    "filters": {}
  }'`;

  return (
    <Paper sx={{ p: 2, mt: 2, border: `1px solid ${gcpPalette.neutral.border}`, borderRadius: '8px' }}>
      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <CodeIcon sx={{ color: gcpPalette.primary.main, fontSize: 20 }} />
          <Typography variant="h3" sx={{ fontSize: '0.95rem' }}>
            Query & Automation Export
          </Typography>
        </Box>
      </Box>

      <Typography variant="body2" sx={{ color: gcpPalette.neutral.textSecondary, mb: 1.5 }}>
        Copy the exact SQL query or automated REST API call for scripts and auditing.
      </Typography>

      <Tabs
        value={tabIndex}
        onChange={(_, val) => setTabIndex(val)}
        sx={{
          minHeight: 36,
          borderBottom: `1px solid ${gcpPalette.neutral.border}`,
          '& .MuiTab-root': {
            minHeight: 36,
            py: 0.5,
            px: 2,
            fontSize: '0.8125rem',
            textTransform: 'none',
            fontWeight: 500,
          },
        }}
      >
        <Tab icon={<StorageIcon sx={{ fontSize: 16 }} />} iconPosition="start" label="SQL Query" />
        <Tab icon={<HttpIcon sx={{ fontSize: 16 }} />} iconPosition="start" label="REST API (curl)" />
      </Tabs>

      <Box sx={{ mt: 1 }}>
        {tabIndex === 0 && <CodeBlock code={sqlQuery} language="sql" maxHeight={220} title="Pushdown SQL" />}
        {tabIndex === 1 && <CodeBlock code={curlCommand} language="bash" maxHeight={220} title="cURL REST API" />}
      </Box>
    </Paper>
  );
};
