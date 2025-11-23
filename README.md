Gnosis Cerebro API Service

This repository contains the API service layer for dbt-cerebro. It exposes data transformed by dbt models (stored in ClickHouse) via a high-performance REST API.

The service is built with FastAPI and includes built-in documentation, rate limiting, and API key management.

🏗 Architecture

Framework: Python 3.11 + FastAPI (Async)

Database: ClickHouse (via clickhouse-connect)

Documentation: OpenAPI (Swagger UI) & ReDoc auto-generated

Security: Header-based API Key authentication (X-API-Key)

Rate Limiting: In-memory throttling per tier (Free/Pro/Unlimited) using slowapi

Project Structure

/cerebro-api
├── Dockerfile               # Multi-stage Docker build definition
├── requirements.txt         # Python dependencies
├── .env.example             # Template for environment variables
├── app
│   ├── main.py              # App entry point & exception handlers
│   ├── config.py            # Pydantic settings & Env var loading
│   ├── database.py          # Singleton ClickHouse client
│   ├── security.py          # Auth logic & Rate limiting implementation
│   └── routers
│       └── analytics.py     # API Endpoints mapping to dbt models


🚀 Getting Started (Local Development)

If you want to run the API locally without Docker for development or debugging.

1. Prerequisites

Python 3.10+

Access to a ClickHouse instance (Local or Cloud)

2. Installation

# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt


3. Configuration

Copy the example environment file and configure your ClickHouse credentials.

cp .env.example .env
# Edit .env with your actual credentials
nano .env


4. Run the Server

uvicorn app.main:app --reload


The API will be available at:

Root: http://127.0.0.1:8000

Interactive Docs: http://127.0.0.1:8000/docs

Alternative Docs: http://127.0.0.1:8000/redoc

🐳 Deployment (Docker)

This service is designed to run as a stateless container on Kubernetes.

1. Build the Image

docker build -t gnosis/cerebro-api:latest .


2. Run Container Locally

docker run -d \
  --name cerebro-api \
  -p 8000:8000 \
  --env-file .env \
  gnosis/cerebro-api:latest


3. Kubernetes Configuration Strategy

When deploying to K8s, inject the environment variables via a ConfigMap or Secret.

Security Note: Do not commit API_KEYS or CLICKHOUSE_PASSWORD to git. Use K8s Secrets.

Sample deployment.yaml snippet:

env:
  - name: CLICKHOUSE_HOST
    value: "your-clickhouse-url.com"
  - name: API_KEYS
    valueFrom:
      secretKeyRef:
        name: cerebro-secrets
        key: api_keys_json


🔑 API Usage & Tiers

Requests must include the X-API-Key header.

Tier

Rate Limit

Description

Free

10 req/min

Public access

Pro

100 req/min

Partners & Heavy users

Unlimited

N/A

Internal services

Example Request:

curl -X 'GET' \
  'http://localhost:8000/v1/analytics/block_activity?limit=5' \
  -H 'accept: application/json' \
  -H 'X-API-Key: sk_live_gnosis_free'


🛠 Extending the API

To add a new endpoint for a new dbt model:

Open app/routers/analytics.py.

Define a new SQL query matching your dbt model.

Add a new route function:

@router.get("/daily_transactions")
@limiter.limit(get_limit_string)
async def get_daily_tx(request: Request, api_tier: str = Depends(get_api_key)):
    data = ClickHouseClient.query("SELECT * FROM dbt_schema.daily_tx_stats LIMIT 100")
    return {"data": data}
