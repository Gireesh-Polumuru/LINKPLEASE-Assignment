# LinkPlease Backend Service

LinkPlease is a unified FastAPI backend application built for high-throughput social webhook ingestion, keyword-based rule matching, automated DM dispatch, rate-limiting enforcement, and eventual delivery status reconciliation.

## Tech Stack

- **Language**: Python 3.11+
- **Framework**: FastAPI
- **Database**: PostgreSQL (with asyncpg) / SQLite (with aiosqlite for lightweight dev/tests)
- **ORM**: Async SQLAlchemy 2.0
- **Validation**: Pydantic v2 & Pydantic Settings
- **HTTP Client**: HTTPX (async)
- **Testing**: Pytest & Pytest-Asyncio

## Project Structure

```
linkplease/
├── .env.example              # Environment variables template
├── .gitignore                # Git ignore patterns
├── README.md                 # Setup & documentation
├── ARCHITECTURE.md           # Full system architecture & design specification
├── requirements.txt          # Project dependencies
├── pytest.ini                # Pytest configuration
│
├── app/
│   ├── main.py               # FastAPI entry point & lifespan manager
│   ├── config.py             # Pydantic BaseSettings
│   ├── database.py           # Async SQLAlchemy engine & session dependency
│   ├── models/               # SQLAlchemy ORM models
│   ├── schemas/              # Pydantic validation schemas
│   ├── api/                  # API route handlers (/rules, /webhook, /stats)
│   ├── core/                 # Core security & rate limiting utilities
│   ├── services/             # Business logic services
│   └── workers/              # Background workers (dispatch & reconciliation)
│
└── tests/                    # Test suite
    ├── conftest.py           # Async test fixtures
    └── test_health.py        # Health check validation test
```

## Quick Start

### 1. Installation
Create and activate a virtual environment, then install dependencies:

```bash
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

### 2. Environment Configuration
Copy `.env.example` to `.env` and configure your credentials:

```bash
cp .env.example .env
```

### 3. Run Development Server
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Run Tests
```bash
pytest
```
