# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional
import asyncio

class GeminiService:
    def __init__(self, api_key: Optional[str] = None, staging_dir: str = "staging"):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY")
        self.staging_dir = Path(staging_dir)
        self.model_name = 'gemini-2.0-flash'

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_key != "YOUR_GEMINI_API_KEY_HERE")

    def get_schema_content(self) -> str:
        candidates = [
            self.staging_dir / "schema.sql",
            Path("staging/schema.sql"),
            Path("../staging/schema.sql")
        ]
        for p in candidates:
            if p.exists():
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception:
                    pass
        return ""

    async def stream_schema_analysis(self) -> AsyncGenerator[str, None]:
        schema_content = self.get_schema_content()
        if not schema_content:
            yield "❌ **Error**: `schema.sql` was not found in the staging directory. Please ensure the schema export has run.\n"
            return

        if not self.is_configured():
            yield "ℹ️ *Google API Key not configured. Streaming simulated DBRE schema audit...*\n\n"
            mock_response = [
                "### 🔍 Cloud Spanner Schema Architecture Review (Simulation)\n\n",
                "**Schema Overview**: Successfully loaded Spanner schema definition.\n\n",
                "#### ⚠️ 1. Hotspot Risk: Primary Key with Leading Timestamp\n",
                "- **Observed Issue**: High write ingestion tables use timestamps without sharding prefixes.\n",
                "- **Risk**: In Cloud Spanner, monotonically increasing keys direct all write traffic to a single split, bottlenecking CPU.\n",
                "- **Remediation Snippet**:\n",
                "```sql\n",
                "-- Before:\n",
                "CREATE TABLE AuditLogs (\n",
                "  EventTime TIMESTAMP NOT NULL,\n",
                "  EventId STRING(64) NOT NULL\n",
                ") PRIMARY KEY (EventTime, EventId);\n\n",
                "-- After (Recommended: Sharded Prefix or UUID):\n",
                "CREATE TABLE AuditLogs (\n",
                "  ShardId INT64 NOT NULL,\n",
                "  EventTime TIMESTAMP NOT NULL,\n",
                "  EventId STRING(64) NOT NULL\n",
                ") PRIMARY KEY (ShardId, EventTime, EventId);\n",
                "```\n\n",
                "#### ⚡ 2. Secondary Index Strategy\n",
                "- Ensure frequently filtered status columns include `STORING` clauses for covering queries.\n",
                "- Eliminate duplicate or overlapping indexes on interleaved child tables.\n\n",
                "#### 💡 3. Sequence & Bit-Reversed Sequences\n",
                "- Verify all auto-increment integer PKs utilize `BIT_REVERSED_POSITIVE` sequences.\n\n",
                "**Summary**: Schema evaluated against Google Cloud Spanner Reliability Engineering standards."
            ]
            for chunk in mock_response:
                yield chunk
                await asyncio.sleep(0.08)
            return

        # Real Gemini Call
        try:
            from google import genai
            client = genai.Client(api_key=self.api_key)
            prompt = self._construct_schema_prompt(schema_content)

            response_stream = client.models.generate_content_stream(
                model=self.model_name,
                contents=prompt
            )
            for chunk in response_stream:
                if chunk.text:
                    yield chunk.text
                    await asyncio.sleep(0.01)
        except Exception as e:
            yield f"\n\n❌ **Error during Gemini analysis**: {str(e)}\n"

    def _construct_schema_prompt(self, schema_content: str) -> str:
        prompt_path = Path("ui/prompt.md")
        template = ""
        if prompt_path.exists():
            try:
                with open(prompt_path, "r", encoding="utf-8") as f:
                    template = f.read()
            except Exception:
                pass

        if not template:
            template = """
You are an expert Google Cloud Spanner Architect & DBRE.
Your goal is to perform a deep architectural review of the provided Spanner Schema.
Use Spanner schema design best practices.

{{SCHEMA_CONTEXT}}

Instructions:
* IMPORTANT: Don't call out items that look ok and should not have issues. Only focus on issues!
* Scan the schema for primary keys or indexes that have a timestamp and are at the first position.
* Provide DDL snippets as context for each item you raise.
* Ignore Spanner Sequences and foreign keys with built-in indexes.
"""
        schema_text = f"\n```sql\n{schema_content[:100000]}\n```\n"
        return template.replace("{{SCHEMA_CONTEXT}}", schema_text)

    async def stream_query_profile_analysis(self, stats: List[Dict]) -> AsyncGenerator[str, None]:
        if not stats:
            yield "✅ **No high-scan queries detected**: No queries currently exceed >100,000 average rows scanned with >10 executions.\n"
            return

        schema_content = self.get_schema_content()

        if not self.is_configured():
            yield "ℹ️ *Google API Key not configured. Streaming simulated Query Profile DBRE analysis...*\n\n"
            mock_text = [
                f"### 🚨 High Row-Scan Diagnostic Report\n\n",
                f"Identified **{len(stats)} query patterns** scanning $>100,000$ rows per execution.\n\n",
                "#### 🔍 Impact Analysis\n",
                "- High row scans force Spanner storage nodes to read large volumes of data from Colossus, inflating CPU utilization and transaction latency.\n",
                "- Lack of covering indexes turns index lookups into full table scans.\n\n",
                "#### 🛠️ Recommended Action Items\n",
                "1. **Add Filter Indexes**: Verify `WHERE` clauses on unindexed timestamp or status columns.\n",
                "2. **Query Plan Inspections**: Run `EXPLAIN ANALYZE` on the identified query fingerprints.\n",
                "3. **Batching / Key Ranges**: Constrain broad point-in-time scans with tighter interval boundaries.\n"
            ]
            for chunk in mock_text:
                yield chunk
                await asyncio.sleep(0.08)
            return

        # Real Gemini Call
        try:
            from google import genai
            client = genai.Client(api_key=self.api_key)

            stats_text = ""
            for item in stats:
                stats_text += f"""
- **Query Fingerprint**: `{item.get('text_fingerprint')}`
- **Avg Rows Scanned**: {item.get('avg_rows_scanned'):,}
- **Max Rows Scanned**: {item.get('max_rows_scanned'):,}
- **Execution Count**: {item.get('total_exec')}
- **Timestamps**: {item.get('intervals')}
- **Query Text**:
```sql
{item.get('text')}
```
---
"""
            prompt = self._construct_query_prompt(stats_text, schema_content)
            response_stream = client.models.generate_content_stream(
                model=self.model_name,
                contents=prompt
            )
            for chunk in response_stream:
                if chunk.text:
                    yield chunk.text
                    await asyncio.sleep(0.01)
        except Exception as e:
            yield f"\n\n❌ **Error during Gemini Query Profile analysis**: {str(e)}\n"

    def _construct_query_prompt(self, stats_text: str, schema_content: str) -> str:
        prompt_path = Path("ui/prompt_row_scans.md")
        template = ""
        if prompt_path.exists():
            try:
                with open(prompt_path, "r", encoding="utf-8") as f:
                    template = f.read()
            except Exception:
                pass

        if not template:
            template = """
You are an expert Google Cloud Spanner DBRE.
We have detected queries performing high row scans, which might indicate inefficient query plans or missing indexes.

{{SCAN_STATS}}

{{SCHEMA_CONTEXT}}

Instructions:
1. Analyze the Queries and explain why high row scans cause high latency / CPU.
2. Recommend Index or Query rewrites with concrete DDL.
3. Be concise and use clean Markdown formatting.
"""
        schema_text = f"\n### Database Schema Context\n```sql\n{schema_content[:50000]}\n```\n" if schema_content else ""
        prompt = template.replace("{{SCAN_STATS}}", stats_text)
        return prompt.replace("{{SCHEMA_CONTEXT}}", schema_text)
