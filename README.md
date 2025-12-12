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
  'http://localhost:8000/v1/consensus/blob_commitments/daily?limit=5' \
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
3. **Deploy** — Merge PR, CI/CD updates `manifest.json`
4. **Result** — API auto-discovers new endpoint on next restart

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
| `API_KEYS_FILE` | No | `./api_keys.json` | Path to API keys file |
| `DEFAULT_ENDPOINT_TIER` | No | `tier0` | Default tier for untagged endpoints |

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
| `app/security.py` | Authentication & tier access control |
| `app/manifest.py` | dbt manifest loader |
| `app/factory.py` | Dynamic route generation engine |

### Adding Custom Endpoints

For endpoints that can't be auto-generated, add them to `api_config.yaml`:

```yaml
endpoints:
  - model: fct_custom_table
    path: /custom/endpoint
    summary: "Custom endpoint"
    tags: ["Custom"]
    tier: "tier1"
    parameters:
      - name: custom_param
        column: column_name
        operator: "="
        type: string
```

---

## License

MIT