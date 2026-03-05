# Gnosis Cerebro API Service

This repository contains the API service layer for **dbt-cerebro**. It exposes data transformed by dbt models (stored in ClickHouse) via a high-performance, metadata-driven REST API.

The service is built with **FastAPI** and features automatic route discovery based on your dbt manifest. It includes built-in documentation, rate limiting, and API key management.

---

## Architecture

- **Framework:** Python 3.11 + FastAPI (Async)
- **Database:** ClickHouse (via `clickhouse-connect`)
- **Routing:** Dynamic — endpoints are auto-generated from the dbt `manifest.json`
- **Documentation:** OpenAPI (Swagger UI) & ReDoc (auto-generated)
- **Security:** Header-based API Key authentication (`X-API-Key`)
- **Rate Limiting:** In-memory throttling per tier (Free/Pro/Unlimited) using `slowapi`

---

## Project Structure

```text
/cerebro-api
├── Dockerfile               # Multi-stage Docker build definition
├── requirements.txt         # Python dependencies
├── .env.example             # Template for environment variables
├── api_keys.json            # API keys configuration (git-ignored)
├── .gitignore               # Git ignore rules
└── app
    ├── main.py              # App entry point
    ├── config.py            # Settings & Env var loading
    ├── database.py          # ClickHouse client wrapper
    ├── security.py          # Auth & Rate limiting logic
    ├── manifest.py          # Logic to download & parse dbt manifest
    └── factory.py           # ⚙️ The Engine: auto-generates routes
````

---

## Getting Started (Local Development)

Follow these steps to run the API locally without Docker for development or debugging.

### 1. Prerequisites

* Python **3.10+**
* Access to a **ClickHouse** instance (Local or Cloud)

### 2. Installation

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

Copy the example environment file and configure your ClickHouse credentials:

```bash
cp .env.example .env

# Edit .env with your actual credentials
nano .env
```

**Key settings in `.env`:**

| Variable | Description | Example |
|----------|-------------|---------|
| `CLICKHOUSE_URL` | ClickHouse Cloud hostname | `abc123.eu-central-1.aws.clickhouse.cloud` |
| `CLICKHOUSE_PORT` | ClickHouse port | `8443` |
| `CLICKHOUSE_USER` | Database username | `default` |
| `CLICKHOUSE_PASSWORD` | Database password | `your_password` |
| `CLICKHOUSE_DATABASE` | Database name | `default` |
| `CLICKHOUSE_SECURE` | Use HTTPS | `true` |
| `DBT_MANIFEST_URL` | URL to your live `manifest.json` | `https://gnosischain.github.io/dbt-cerebro/manifest.json` |
| `DBT_MANIFEST_PATH` | Fallback local path | `./manifest.json` |
| `DBT_MANIFEST_REFRESH_ENABLED` | Enable automatic manifest refresh | `true` |
| `DBT_MANIFEST_REFRESH_INTERVAL_SECONDS` | Refresh interval in seconds | `300` |

### 4. Configure API Keys

Create an `api_keys.json` file in your project root:

```json
{
  "sk_live_alice_abc123": {
    "user": "alice",
    "tier": "tier2",
    "org": "Gnosis Core"
  },
  "sk_live_bob_xyz789": {
    "user": "bob",
    "tier": "tier1",
    "org": "Partner Inc"
  },
  "sk_live_public_key": {
    "user": "public",
    "tier": "tier0",
    "org": "Public"
  },
  "sk_live_internal_admin": {
    "user": "admin",
    "tier": "tier3",
    "org": "Gnosis Internal"
  }
}
```

> ⚠️ **Security Note:** Add `api_keys.json` to your `.gitignore` file!

### 5. Run the Server

```bash
uvicorn app.main:app --reload
```

The API will be available at:

* Root: `http://127.0.0.1:8000`
* Interactive Docs (Swagger UI): `http://127.0.0.1:8000/docs`
* Alternative Docs (ReDoc): `http://127.0.0.1:8000/redoc`

---

## API Authentication & Access Tiers

All requests must include the `X-API-Key` header.

### Tier Hierarchy

Higher tier users can access all endpoints at or below their tier level.

| Tier | Access Level | Rate Limit | Can Access |
|------|--------------|------------|------------|
| `tier0` | Public | 20 req/min | `tier0` only |
| `tier1` | Partner | 100 req/min | `tier0`, `tier1` |
| `tier2` | Premium | 500 req/min | `tier0`, `tier1`, `tier2` |
| `tier3` | Internal | 10,000 req/min | All endpoints |

### Example Request

```bash
curl -X 'GET' \
  'http://localhost:8000/v1/consensus/blob_commitments/daily' \
  -H 'accept: application/json' \
  -H 'X-API-Key: sk_live_alice_abc123'
```

### Error Responses

**Missing API Key (403):**
```json
{"detail": "Missing authentication header: X-API-Key"}
```

**Invalid API Key (403):**
```json
{"detail": "Invalid API Key"}
```

**Insufficient Tier Access (403):**
```json
{"detail": "Access denied. This endpoint requires tier2 access. User 'bob' has tier1 access."}
```

---

## Extending the API

The API is **metadata-driven**. You do **not** need to write Python code to add new endpoints.

### Requirements for Endpoint Auto-Discovery

A dbt model will be exposed as an API endpoint if it meets **both** conditions:

1. ✅ Model has the `production` tag
2. ✅ Model has an `api:` tag defining the resource name

### Tag Convention

Use dbt tags to control endpoint paths, Swagger UI grouping, and access control:

```sql
{{
    config(
        materialized='view',
        tags=["production", "consensus", "tier1", "api:blob_commitments", "granularity:daily"]
    )
}}
```

| Tag | Format | Purpose | Required |
|-----|--------|---------|----------|
| `production` | literal | Marks model for API exposure | ✅ Yes |
| Category | `consensus`, `execution`, etc. | Swagger UI section & URL prefix | ✅ Yes |
| Tier | `tier0`, `tier1`, `tier2`, `tier3` | Access control level | No (default: `tier0`) |
| Resource | `api:{resource_name}` | Explicit resource name in URL | ✅ Yes |
| Granularity | `granularity:{period}` | Time dimension suffix in URL | No |

### URL Path Generation

The URL path is built from tags: `/{category}/{resource}/{granularity?}`

| Tags | Generated Path |
|------|----------------|
| `["production", "consensus", "api:blob_commitments", "granularity:daily"]` | `/consensus/blob_commitments/daily` |
| `["production", "consensus", "api:blob_commitments", "granularity:latest"]` | `/consensus/blob_commitments/latest` |
| `["production", "execution", "api:transactions"]` | `/execution/transactions` |
| `["production", "financial", "tier2", "api:treasury"]` | `/financial/treasury` |

### Complete Example

**Model:** `api_consensus_blob_commitments_daily.sql`

```sql
{{
    config(
        materialized='view',
        tags=["production", "consensus", "tier1", "api:blob_commitments", "granularity:daily"]
    )
}}

SELECT
    date,
    total_blob_commitments AS value
FROM {{ ref('int_consensus_blocks_daily') }}
ORDER BY date
```

**Result:**
- **Endpoint:** `GET /v1/consensus/blob_commitments/daily`
- **Swagger Section:** `Consensus`
- **Access:** `tier1` (Partner and above)

### Strict Metadata Contract

Endpoint behavior is now strict and metadata-only.

- No inferred filters.
- No inferred pagination.
- No inferred sort.
- `granularity:*` tags only affect the URL path segment.
- Swagger route listing is deterministic: grouped by `/{category}/{resource}` then granularity priority (`none`, `latest`, `daily`, `weekly`, `monthly`, `last_7d`, `last_30d`, `in_ranges`, `all_time`, unknowns last).

### Legacy vs Metadata-Driven Endpoints

- Legacy endpoint (`production` + `api:*`, no `meta.api`):
  - GET only
  - no declared filters
  - no pagination
  - no sort
  - full result returned
  - any query params return `400`
- Metadata-driven endpoint (`meta.api` present):
  - filters, methods, pagination, and sort come only from `meta.api`
  - undeclared query params or body fields return `400`
  - `limit`/`offset` only work when pagination is enabled

### `meta.api` Reference

| Field | Type | Default | Description |
|------|------|---------|-------------|
| `methods` | list | `["GET"]` | Allowed HTTP methods (`GET`, `POST`) |
| `allow_unfiltered` | bool | `false` | Allow requests with no declared business filters |
| `require_any_of` | list | `[]` | At least one listed filter name must be present |
| `parameters` | list | `[]` | Declared filter contract |
| `pagination` | object | disabled | Enables `limit` and `offset` |
| `sort` | list | `[]` | Explicit `ORDER BY` list |

Parameter object fields:

| Key | Required | Description |
|-----|----------|-------------|
| `name` | Yes | API field name used by clients |
| `column` | Yes | Final projected model column to filter on |
| `operator` | Yes | One of `=`, `>=`, `<=`, `ILIKE`, `IN` |
| `type` | Yes | One of `string`, `date`, `string_list` |
| `description` | No | OpenAPI description |
| `case` | No | `lower` or `upper` (string and string_list only) |
| `max_items` | No | Max list size (string_list only) |

Pagination object fields:

| Key | Required When Enabled | Description |
|-----|------------------------|-------------|
| `enabled` | Yes | Must be `true` to enable pagination |
| `default_limit` | Yes | Default row limit when client omits `limit` |
| `max_limit` | Yes | Hard upper bound for `limit` |

Sort object fields:

| Key | Required | Description |
|-----|----------|-------------|
| `column` | Yes | Column name present in final `SELECT` |
| `direction` | Yes | `ASC` or `DESC` |

### Metadata Example

```sql
{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:token_balances', 'granularity:daily'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": false,
                "require_any_of": ["symbol", "address"],
                "parameters": [
                    {"name": "symbol", "column": "symbol", "operator": "=", "type": "string"},
                    {"name": "address", "column": "address", "operator": "IN", "type": "string_list", "case": "lower", "max_items": 200},
                    {"name": "start_date", "column": "date", "operator": ">=", "type": "date"},
                    {"name": "end_date", "column": "date", "operator": "<=", "type": "date"}
                ],
                "pagination": {"enabled": true, "default_limit": 100, "max_limit": 5000},
                "sort": [{"column": "date", "direction": "DESC"}]
            }
        }
    )
}}
```

### Request Examples

GET list filters accept repeated params and CSV:

```bash
curl "http://localhost:8000/v1/execution/token_balances/daily?symbol=ETH&address=0x1&address=0x2&limit=50" \
  -H 'X-API-Key: sk_live_internal_admin'
```

```bash
curl "http://localhost:8000/v1/execution/token_balances/daily?symbol=ETH&address=0x1,0x2&limit=50" \
  -H 'X-API-Key: sk_live_internal_admin'
```

POST accepts only JSON body fields declared in metadata:

```bash
curl -X POST "http://localhost:8000/v1/execution/token_balances/daily" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_live_internal_admin" \
  -d '{
    "symbol": "ETH",
    "address": ["0x1", "0x2"],
    "start_date": "2024-01-01",
    "end_date": "2024-01-31",
    "limit": 100,
    "offset": 0
  }'
```

### Authoring Guidance

- Prefer thin API-facing dbt views over exposing heavy internal models directly.
- Every `parameters[].column` and `sort[].column` must be present in the final `SELECT`.
- Do not rely on `granularity:latest`, `daily`, or `all_time` for behavior. They only shape URL paths.
- If you want date filters, declare them in `meta.api.parameters`.
- If you want pagination, declare `meta.api.pagination`.
- If you want ordering, declare `meta.api.sort`.
- If you want POST, include `"POST"` in `meta.api.methods`.

### Multiple Granularities for Same Resource

Create separate models for different time granularities:

```sql
-- api_consensus_blob_commitments_daily.sql
{{ config(tags=["production", "consensus", "tier1", "api:blob_commitments", "granularity:daily"]) }}

-- api_consensus_blob_commitments_latest.sql  
{{ config(tags=["production", "consensus", "tier0", "api:blob_commitments", "granularity:latest"]) }}

-- api_consensus_blob_commitments_last_30d.sql
{{ config(tags=["production", "consensus", "tier1", "api:blob_commitments", "granularity:last_30d"]) }}

-- api_consensus_blob_commitments_all_time.sql
{{ config(tags=["production", "consensus", "tier2", "api:blob_commitments", "granularity:all_time"]) }}
```

**Generated Endpoints:**
- `GET /v1/consensus/blob_commitments/daily` (tier1)
- `GET /v1/consensus/blob_commitments/latest` (tier0)
- `GET /v1/consensus/blob_commitments/last_30d` (tier1)
- `GET /v1/consensus/blob_commitments/all_time` (tier2)

### Supported Granularities

| Granularity | Use Case |
|-------------|----------|
| `daily` | Daily aggregated data |
| `weekly` | Weekly aggregated data |
| `monthly` | Monthly aggregated data |
| `latest` | Most recent value(s) only |
| `last_7d` | Rolling 7-day window |
| `last_30d` | Rolling 30-day window |
| `in_ranges` | Data within specified ranges |
| `all_time` | Complete historical data |

### Tags Reference

| Tag Type | Examples | Purpose |
|----------|----------|---------|
| **Required** | `production` | Marks model for API exposure |
| **Category** | `consensus`, `execution`, `financial` | First tag = Swagger UI section + URL prefix |
| **Access** | `tier0`, `tier1`, `tier2`, `tier3` | Required tier level (default: `tier0`) |
| **Resource** | `api:blob_commitments`, `api:validators` | Explicit resource name in URL |
| **Granularity** | `granularity:daily`, `granularity:weekly`, `granularity:monthly`, `granularity:latest`, `granularity:in_ranges`, `granularity:last_30d`, `granularity:last_7d`, `granularity:all_time` | Optional time/range suffix |
| **Ignored** | `view`, `table`, `incremental` | Filtered out from URL/grouping |

### Workflow

1. **Create Model** — Name it descriptively (e.g., `api_consensus_blob_commitments_daily.sql`)
2. **Add Tags** — Include `production` + category + `api:resource` + optional `granularity:` + tier
3. **Add `meta.api`** — Define methods, filters, unfiltered policy, pagination, and sort
4. **Deploy** — Merge PR, CI/CD updates `manifest.json`
5. **Result** — API auto-discovers or updates the endpoint on the next manifest refresh

---

## Deployment (Docker)

This service is designed to run as a **stateless container** on Kubernetes.

### 1. Build the Image

```bash
docker build -t gnosis/cerebro-api:latest .
```

### 2. Run Container Locally

```bash
docker run -d \
  --name cerebro-api \
  -p 8000:8000 \
  --env-file .env \
  -v $(pwd)/api_keys.json:/code/api_keys.json:ro \
  gnosis/cerebro-api:latest
```

### 3. Kubernetes Configuration

When deploying to K8s, inject environment variables via **ConfigMap** or **Secret**.

> **Security Note:**
> Never commit `api_keys.json` or `CLICKHOUSE_PASSWORD` to git.
> Always use K8s Secrets or a Secrets Manager (Vault / AWS SSM / etc).

**Sample `deployment.yaml` snippet:**

```yaml
env:
  - name: CLICKHOUSE_URL
    value: "your-clickhouse-url.com"
  - name: CLICKHOUSE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: cerebro-secrets
        key: clickhouse_password
  - name: DBT_MANIFEST_URL
    value: "https://gnosischain.github.io/dbt-cerebro/manifest.json"

volumeMounts:
  - name: api-keys
    mountPath: /code/api_keys.json
    subPath: api_keys.json
    readOnly: true

volumes:
  - name: api-keys
    secret:
      secretName: cerebro-api-keys
```

---

## Configuration Reference

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CLICKHOUSE_URL` | No | `None` | ClickHouse Cloud URL (takes precedence over HOST) |
| `CLICKHOUSE_HOST` | No | `localhost` | ClickHouse hostname |
| `CLICKHOUSE_PORT` | No | `8443` | ClickHouse port |
| `CLICKHOUSE_USER` | No | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | Yes | `""` | ClickHouse password |
| `CLICKHOUSE_DATABASE` | No | `default` | ClickHouse database |
| `CLICKHOUSE_SECURE` | No | `true` | Use HTTPS connection |
| `DBT_MANIFEST_URL` | No | GitHub Pages URL | Remote manifest URL |
| `DBT_MANIFEST_PATH` | No | `./manifest.json` | Local manifest fallback |
| `DBT_MANIFEST_REFRESH_ENABLED` | No | `true` | Enable automatic manifest refresh |
| `DBT_MANIFEST_REFRESH_INTERVAL_SECONDS` | No | `300` | Refresh interval in seconds |
| `API_KEYS_FILE` | No | `./api_keys.json` | Path to API keys file |
| `DEFAULT_ENDPOINT_TIER` | No | `tier0` | Default tier for untagged endpoints |

### Manifest Refresh

The API polls the manifest URL automatically and rebuilds routes when it changes.

You can force an immediate refresh with a tier3 API key:

```bash
curl -X POST http://localhost:8000/v1/system/manifest/refresh \
  -H 'X-API-Key: sk_live_internal_admin'
```

### API Keys File Format

```json
{
  "sk_live_<unique_id>": {
    "user": "username",
    "tier": "tier0|tier1|tier2|tier3",
    "org": "Organization Name"
  }
}
```

---

## Development

### Project Files

| File | Purpose |
|------|---------|
| `app/main.py` | FastAPI app initialization |
| `app/config.py` | Settings & environment loading |
| `app/database.py` | ClickHouse client wrapper |
| `app/api_metadata.py` | dbt endpoint metadata parsing and validation |
| `app/security.py` | Authentication & tier access control |
| `app/manifest.py` | dbt manifest loader |
| `app/factory.py` | Dynamic route generation engine |

### Adding Custom Endpoints

`api_config.yaml` is now limited to route-level overrides only:

```yaml
endpoints:
  - model: fct_custom_table
    path: /custom/endpoint
    summary: "Custom endpoint"
    tags: ["Custom"]
    tier: "tier1"
```

Behavior is never overridden from `api_config.yaml`.

- filters come only from `meta.api.parameters`
- methods come only from `meta.api.methods`
- pagination comes only from `meta.api.pagination`
- sort comes only from `meta.api.sort`
- unfiltered policy comes only from `meta.api.allow_unfiltered` and `meta.api.require_any_of`

---

## License

MIT
