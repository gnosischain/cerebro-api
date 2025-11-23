
# Gnosis Cerebro API Service

This repository contains the API service layer for **dbt-cerebro**. It exposes data transformed by dbt models (stored in ClickHouse) via a high-performance, metadata-driven REST API.

The service is built with **FastAPI** and features automatic route discovery based on your dbt manifest. It includes built-in documentation, rate limiting, and API key management.

---

## Architecture

- **Framework:** Python 3.11 + FastAPI (Async)
- **Database:** ClickHouse (via `clickhouse-connect`)
- **Routing:** Dynamic – endpoints are auto-generated from the dbt `manifest.json`
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

* `DBT_MANIFEST_URL`: The URL to your live `manifest.json` (e.g., hosted on GitHub Pages).
  The app will try to fetch this first.

* `DBT_MANIFEST_PATH`: Fallback local path (default: `./manifest.json`).

### 4. Run the Server

```bash
uvicorn app.main:app --reload
```

The API will be available at:

* Root: `http://127.0.0.1:8000`
* Interactive Docs (Swagger UI): `http://127.0.0.1:8000/docs`
* Alternative Docs (ReDoc): `http://127.0.0.1:8000/redoc`

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
  gnosis/cerebro-api:latest
```

### 3. Kubernetes Configuration Strategy

When deploying to K8s, inject the environment variables via a **ConfigMap** or **Secret**.

> **Security Note:**
> Never commit `API_KEYS` or `CLICKHOUSE_PASSWORD` to git.
> Always use K8s Secrets or a Secrets Manager (Vault / AWS SSM / etc).

**Sample `deployment.yaml` snippet:**

```yaml
env:
  - name: CLICKHOUSE_HOST
    value: "your-clickhouse-url.com"
  - name: DBT_MANIFEST_URL
    value: "https://gnosischain.github.io/dbt-cerebro/manifest.json"
  - name: API_KEYS
    valueFrom:
      secretKeyRef:
        name: cerebro-secrets
        key: api_keys_json
```

---

## API Usage & Tiers

All requests must include the `X-API-Key` header.

### Tiers

| Tier          | Rate Limit     | Description             |
| ------------- | -------------- | ----------------------- |
| **Free**      | 20 req/min     | Public access / Testing |
| **Pro**       | 500 req/min    | Partners & Heavy users  |
| **Unlimited** | 10,000 req/min | Internal services       |

### Example Request

```bash
curl -X 'GET' \
  'http://localhost:8000/v1/consensus/validators/active?limit=5' \
  -H 'accept: application/json' \
  -H 'X-API-Key: sk_live_gnosis_free'
```

---

## Extending the API

The API is **metadata-driven**. You do **not** need to write Python code to add new endpoints.

### 1. Create a Model in dbt

Create a new model in your dbt project. The name must start with `api_`.

Example: `api_financial_treasury_daily.sql`

### 2. Tag it (Optional but Recommended)

Add tags in your `schema.yml`. The API uses these tags to group endpoints in the Swagger UI.

```yaml
models:
  - name: api_financial_treasury_daily
    description: "Daily treasury stats"
    config:
      tags: ["Financial", "Treasury"]
```

### 3. Deploy dbt

Merge your PR. Once your CI/CD updates the `manifest.json` at the hosted URL, the API will automatically detect the new model upon its **next restart (or deployment)**.

### 4. Result

For the example above, the API will generate:

* **Endpoint:** `GET /financial/treasury/daily`
* **Filters:** Automatically generated based on columns (e.g., `start_date`, `end_date`, `project`)


