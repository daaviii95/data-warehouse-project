# Aperture — ShopZada Data Warehouse

End-to-end **Kimball-style** data warehouse for the ShopZada multi-department dataset, orchestrated with **Apache Airflow** and loaded into **PostgreSQL**. This repository includes ingestion (multi-format files), transformation, dimensional modeling, analytical views, and a Power BI dashboard.

## Quick links

- `docs/core/ARCHITECTURE.md` — architecture overview  
- `docs/core/WORKFLOW.md` — Airflow + ETL workflow (tasks, dependencies)  
- `docs/core/DATA_MODEL.md` — star schema + tables  
- `docs/core/DATA_PROFILING.md` — source data profiling + cleaning rules  
- `docs/core/INCREMENTAL_LOADING.md` — incremental ingestion behavior  
- `docs/core/BUSINESS_QUESTIONS.md` — KPI/report questions and supported views  
- `docs/core/SCENARIOS.md` — Scenario 1–5 demo guide (test files + verification)

## Tech stack

- **Orchestration**: Apache Airflow (Docker)
- **Warehouse DB**: PostgreSQL 15
- **ETL**: Python (pandas), SQL (views, schema)
- **BI**: Power BI (`dashboard/ShopZada_New.pbix`)

## Running the project (Docker)

### Prerequisites

- Docker Desktop (with Docker Compose enabled)

### Start services

From the repository root:

```bash
docker compose -f infra/docker-compose.yml up -d --build
```

### Airflow UI

- **URL**: `http://localhost:8080`
- **Username / Password**: `admin` / `admin`

### Main pipeline

Trigger the DAG:
- `shopzada_etl_pipeline`

This DAG:
- creates schema + staging tables
- populates `dim_date`
- ingests raw files into staging (incremental by default)
- extracts/transforms
- loads dimensions and facts
- creates analytical views (`vw_*`)

### Refresh analytical views only (optional)

Trigger the DAG:
- `shopzada_analytical_views`

Use this when you changed `sql/03_create_analytical_views.sql` and want a fast refresh without re-running the full ETL.

## Data locations

- **Base data**: `data/<Department>/...` (mounted into the Airflow container as `/opt/airflow/data`)
- **Scenario demo test files**: `data/Test/Scenario <N>/...`

The ingestion step searches the `DATA_DIR` recursively for matching filename patterns (e.g., `*order_data*`, `*transactional_campaign_data*`), so test files under `data/Test/` will also be discovered unless you isolate runs.

## Scenario demos (1–5)

Use the walkthrough and test files in:
- `docs/core/SCENARIOS.md`

For Scenario 5 SQL-only verification queries, use:
- `sql/utilities/scenario5_verification_queries.sql`

## Repository layout

- `infra/` — Dockerfile + docker-compose
- `workflows/` — Airflow DAGs
- `scripts/` — Python ETL modules (ingest/extract/transform/load)
- `sql/` — schema, staging, views, utilities
- `data/` — source datasets + scenario test datasets
- `docs/` — documentation
- `dashboard/` — Power BI dashboard file

## Project structure

```text
.
├─ airflow/                     # Airflow logs/plugins (runtime artifacts)
├─ dashboard/                   # Power BI dashboard (.pbix)
├─ data/                        # Source datasets by department
│  ├─ Business Department/
│  ├─ Customer Management Department/
│  ├─ Enterprise Department/
│  ├─ Marketing Department/
│  ├─ Operations Department/
│  └─ Test/                     # Scenario demo datasets (1–4)
│     ├─ Scenario 1/
│     ├─ Scenario 2/
│     ├─ Scenario 3/
│     └─ Scenario 4/
├─ docs/                        # Project documentation
│  └─ core/                     # Architecture, workflow, scenarios, etc.
├─ infra/                       # Dockerfile + docker-compose for Airflow + Postgres
├─ scripts/                     # Python ETL modules (ingest/extract/transform/load)
├─ sql/                         # DDL + staging + analytical views + utilities
└─ workflows/                   # Airflow DAGs
```

## How the workflow happens (high level)

### Primary DAG: `shopzada_etl_pipeline`

This is the end-to-end pipeline you run for full refresh or incremental daily loads:

1. **Create schema** (`sql/01_create_schema_from_physical_model.sql`)
2. **Create staging tables** (`sql/00_create_staging_tables.sql`)
3. **Populate `dim_date`** (`sql/02_populate_dim_date.sql`)
4. **Ingest** (per department into staging; incremental by default) — `scripts/ingest.py`
5. **Extract** (staging → DataFrames) — `scripts/extract.py`
6. **Transform** (clean/normalize) — `scripts/transform.py`
7. **Load dimensions** — `scripts/load_dim.py`
8. **Load facts** (with data quality reject tables) — `scripts/load_fact.py`
9. **Create analytical views** (`sql/03_create_analytical_views.sql`)

### Views-only DAG: `shopzada_analytical_views`

Use this when you only changed view definitions and want a fast refresh:

- Runs `sql/03_create_analytical_views.sql` without re-ingesting/reloading facts.

## Group & Members - Aperture (3CSD)

- **Abelardo, Aasen Sofia P.**
- **Aquino, Matthew Benedict U.**
- **Balingit, Den Mar F.**
- **Gumban, Joevanni Paulo T.**
- **Lazaro, Jesus Carlos G.**
- **Niez, Hesper Kyle L.**
- **Salaya, Davidica Justiniana P.**
- **Tamargo, Jarrod Frank B.**