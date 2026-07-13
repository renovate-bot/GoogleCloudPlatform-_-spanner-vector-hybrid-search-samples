# Spanner Split Points Manager (Java / Spring Boot)

[Back to Spanner Tools](../../README.md)

Java port of the [Python app](../python/). Identical features and HTTP API; uses Spring Boot + Thymeleaf + SQLite-via-JDBC.

## Features

- **Local Staging**: Stage split point changes locally before syncing to Spanner
- **Batch Sync**: Automatically batches changes to respect Spanner's 100 split points per request limit
- **Visual Status**: See which splits are synced, pending add, or pending delete
- **Safe Deletes**: Mark splits for deletion (sets immediate expiration) before syncing
- **Range Splits**: Generate evenly distributed splits for INT64 and UUID-typed single-column keys
- **Table & Index splits**

## Tech Stack

- **Backend**: Spring Boot 3.3 (Java 17+)
- **Database**: SQLite via JDBC (local staging)
- **Frontend**: Thymeleaf + Alpine.js + TailwindCSS (CDN)
- **Cloud**: `google-cloud-spanner` Java SDK

## Setup

### Prerequisites
- Java 17+
- Maven 3.8+
- Google Cloud credentials (ADC):
  ```bash
  gcloud auth application-default login
  ```

### Build

```bash
mvn -DskipTests package
```

## Configuration

### Option 1: Environment Variables (or .env file)

Export real env vars:
```bash
export PROJECT=your-gcp-project
export SPANNER_INSTANCE=your-instance
export SPANNER_DATABASE=your-database
```

…or drop a `.env` file in the working directory (auto-loaded via `spring-dotenv`):
```env
PROJECT=your-gcp-project
SPANNER_INSTANCE=your-instance
SPANNER_DATABASE=your-database
```

The app also reads `SPANNER_PROJECT` / `INSTANCE` / `DATABASE` as fallbacks.

### Option 2: Web UI Settings

Configure connection through the web UI at `/settings`. UI settings take precedence over env vars.

## Running

```bash
mvn spring-boot:run
```

Or:

```bash
java -jar target/spanner-splits-manager-1.0.0.jar
```

Then open http://localhost:8000

### Running from VS Code Remote

Spring Boot binds to port 8000 by default (see `application.properties`). VS Code's Ports panel will offer to forward it to your local browser.

## REST API

| Method | Endpoint                        | Description                                   |
|--------|---------------------------------|-----------------------------------------------|
| GET    | `/api/entities`                 | List entities (tables + indexes) with counts  |
| GET    | `/api/entity-schema`            | Get key schema for a table or index           |
| GET    | `/api/splits`                   | List splits, optionally filtered by entity    |
| POST   | `/api/splits`                   | Add a new local split                         |
| DELETE | `/api/splits/{id}`              | Remove a local split                          |
| POST   | `/api/splits/clear`             | Clear all pending splits                      |
| POST   | `/api/splits/range`             | Generate range splits (INT64 or UUID)         |
| GET    | `/api/splits/range/validate`    | Validate a range request                      |
| POST   | `/api/sync`                     | Sync pending changes to Spanner               |
| GET    | `/api/settings`                 | Get current settings                          |

JSON field names mirror the Python app's snake_case wire format.

## Architecture

```
User -> Spring MVC controllers (Thymeleaf views + JSON @RestController)
     -> Spring services (SpannerService, SplitsAggregator)
     -> JdbcTemplate repositories (SQLite local staging)
     -> google-cloud-spanner SDK (batched admin API)
     -> Google Cloud Spanner
```

Split point states:
- **SYNCED**: Exists in Spanner, no local changes
- **PENDING_ADD**: Staged locally, waiting to be sent to Spanner
- **PENDING_DELETE**: Exists in Spanner, marked for expiration locally

## Module layout

```
src/main/java/com/karthitect/spannersplits/
├── SpannerSplitsApplication.java   # Spring Boot main
├── controller/                     # @Controller + @RestController
├── model/                          # DTOs (Pydantic equivalents)
├── repository/                     # JdbcTemplate-based DAOs
├── service/                        # SpannerService, SplitsAggregator, SplitKeyParser
└── util/                           # RangeUtils (INT64 + UUID range math)
src/main/resources/
├── application.properties
├── static/css/styles.css
└── templates/                      # Thymeleaf views (base, index, settings)
```
