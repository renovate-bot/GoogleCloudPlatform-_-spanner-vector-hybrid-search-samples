## 🚀 Supercharging AI Coding Agents with Spanner Skills

This repository contains a specialized AI **Skill File** - [spanner_multimodel_skill.md](spanner_multimodel_skill.md) -  designed to teach agentic coding assistants (like **Gemini CLI** or **Antigravity**) the
latest capabilities and syntaxes for Google Cloud Spanner's modern multi-model engine.

Because standard LLMs suffer from knowledge cutoff dates and lack default context on Spanner's custom dialect, supplying this file directly to
your agent ensures it generates high-quality, up-to-date, and syntactically accurate code.

### 📂 What the Skill File Contains

The [`spanner_multimodel_skill.md`](./spanner_multimodel_skill.md) acts as a comprehensive, conceptual reference guide for the following features:

1. **Spanner Graph (Property Graphs & openCypher)**
    - *Concept*: Native property graphs co-located with relational tables.
    - *Contents*: Key mapping strategies, colocation rules, generalized property graph DDL, and embedded openCypher (`GRAPH_TABLE`) query templates.
2. **Vector Search (AI & RAG)**
    - *Concept*: High-dimensional vector storing and searching.
    - *Contents*: `ARRAY<FLOAT64|FLOAT32>` storage formats, vector index creation, distance metrics (`COSINE`, `EUCLIDEAN`, `DOT_PRODUCT`), and
query templates.
3. **Full-Text Search (FTS)**
    - *Concept*: High-performance, tokenized transactional searching.
    - *Contents*: Search index creation, storing optimizations, and the `SEARCH` query function.
4. **Columnar Engine (Dual-Engine HTAP)**
    - *Concept*: Real-time OLAP and OLTP co-existence with zero ETL.
    - *Contents*: Dual-engine architecture principles and `COLUMNAR INDEX` DDL templates.

    ---

    ### 🛠️ How to Use This Skill with Your AI Agent

    You can easily inject these Spanner capabilities into your agentic workflow in two simple steps:

    #### Step 1: Prompt Your Agent to Load the Skill
    When initiating a database-related task in your CLI or agent environment, instruct your agent to ingest the skill file first.

    **Example Prompt:**
    > *"Read `spanner_multimodel_skill.md` as a skill. Now, design a relational schema for our core platform, integrating a property graph to map
  entity networks, vector indexing on description columns, and columnar indexes for analytics."*

    #### Step 2: Pair it with Spanner MCP (Optional but Recommended)
    To get the ultimate development feedback loop, run a **Model Context Protocol (MCP)** server alongside your agent. This allows the agent to load
  the conceptual skills from the markdown file *and* introspect your live database schemas, execution plans, and queries.

    Configure your agent's `mcp_config.json` to include the Spanner MCP:
    ```json
    {
      "mcpServers": {
        "spanner": {
          "command": "npx",
          "args": [
            "-y",
            "@google-cloud/mcp-server-spanner",
            "--project", "YOUR_GCP_PROJECT",
            "--instance", "YOUR_SPANNER_INSTANCE",
            "--database", "YOUR_SPANNER_DATABASE"
          ]
        }
      }
    }
  ──────

### 💡 Pro-Tip: Keep it Updated!

As Google Cloud Spanner rolls out new syntax options or capabilities, simply update the relevant section in  spanner_multimodel_skill.md  with
direct documentation links and syntax patterns. Your AI coding agent will instantly adapt to the new dialect without needing model retraining!