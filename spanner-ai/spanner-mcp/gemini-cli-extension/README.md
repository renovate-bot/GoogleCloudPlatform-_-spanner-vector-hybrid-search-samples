# Gemini CLI Extension - Spanner Managed MCP Endpoint

> [!NOTE]
> This extension is currently in beta (pre-v1.0), and may see breaking changes until the first stable release (v1.0).

This Gemini CLI extension provides a set of tools to interact with [Google Cloud Spanner](https://cloud.google.com/spanner/docs) instances. It allows you to manage your databases, execute queries, and explore schemas directly from the [Gemini CLI](https://google-gemini.github.io/gemini-cli/), using natural language prompts.

Learn more about [Gemini CLI Extensions](https://github.com/google-gemini/gemini-cli/blob/main/docs/extensions/index.md).

## Why Use the Spanner Extension?

* **Natural Language Management:** Stop wrestling with complex commands. Explore schemas and query data by describing what you want in plain English.
* **Seamless Workflow:** As a Google-developed extension, it integrates seamlessly into the Gemini CLI environment. No need to constantly switch contexts for common database tasks.
* **Code Generation:** Accelerate development by asking Gemini to generate data classes and other code snippets based on your table schemas.


## Prerequisites

Before you begin, ensure you have the following:

* [Gemini CLI](https://github.com/google-gemini/gemini-cli) installed with version **+v0.6.0**.
* Setup Gemini CLI [Authentication](https://github.com/google-gemini/gemini-cli/tree/main?tab=readme-ov-file#-authentication-options).
* A Google Cloud project with the **Spanner API** enabled.
* Ensure [Application Default Credentials](https://cloud.google.com/docs/authentication/gcloud) are available in your environment.
* IAM Permissions
    * Cloud Spanner Database Reader (`roles/spanner.databaseReader`)
    * Cloud Spanner Database User (`roles/spanner.databaseUser`)
    * Cloud Spanner Admin (`roles/spanner.admin`)
    * MCP Tool User (`roles/mcp.toolUser`)

## Getting Started

### Installation

To install the extension, use the command:

```bash
# First clone this repo
git clone 

# install extension
gemini extensions install spanner-bb-samples/spanner-ai/spanner-mcp/gemini-cli-extension
```

NOTE: Please adjust the path 

### Configuration

**List Settings:**
*   Terminal: `gemini extensions list`
*   Gemini CLI: `/extensions list`


> [!NOTE]
> * Ensure [Application Default Credentials](https://cloud.google.com/docs/authentication/gcloud) are available in your environment.
> * See [Troubleshooting](#troubleshooting) for debugging your configuration.

### Start Gemini CLI

To start the Gemini CLI, use the following command:

```bash
gemini
```


## Usage

* **Explore Schemas and Data:**
    * "list spanner instances under my test-project"
    * "Show schema for Orders table"
    * "Add an index on ProductCategory column in Products table and store price as part of index"
    * "How many orders were placed in the last 30 days, and what were the top 5 most purchased items?"

* **Generate Code:**
    * "Generate a Python dataclass to represent the 'customers' tab

## Additional Extensions

Find additional extensions to support your entire software development lifecycle at [github.com/gemini-cli-extensions](https://github.com/gemini-cli-extensions).

## Uninstalling the extension

To remove/uninstall the extension, use the command:

```bash
gemini extensions uninstall spanner
```

## Troubleshooting

Use `gemini --debug` to enable debugging.

