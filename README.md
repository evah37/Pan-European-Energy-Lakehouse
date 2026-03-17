# Pan-European Energy Lakehouse

An end-to-end data engineering project that ingests, processes, and visualizes European electricity market data using a **Lambda Architecture** (batch + real-time).

## Architecture

```
Kaggle CSV ──► Airflow DAG ──► MinIO (Bronze) ──► Spark (Silver) ──► dbt + DuckDB (Gold) ──► Streamlit
                                                                                                ▲
Awattar API ──► Redpanda ──► Spark Streaming ──► MinIO (Realtime) ─────────────────────────────┘
```

| Component | Role |
|---|---|
| **Airflow** | Orchestration (DAG scheduling) |
| **Spark** | Feature engineering & streaming |
| **MinIO** | S3-compatible data lake (Bronze/Silver/Gold) |
| **DuckDB** | OLAP engine for dbt |
| **dbt** | Dimensional modeling & analytics |
| **Redpanda** | Kafka-compatible message broker |
| **Streamlit** | Interactive dashboard |
| **Terraform** | Azure IaC (Resource Group, VM, ADLS Gen2) |

## Project Structure

```
├── dags/                  # Airflow DAG definitions
├── scripts/               # Ingestion & utility scripts
├── spark_jobs/            # PySpark batch & streaming jobs
├── dbt_energy/            # dbt models (staging → marts)
│   └── models/
│       ├── staging/       # Source views on Silver layer
│       └── marts/         # Core facts/dims & analytics
├── containers/
│   ├── airflow/           # Airflow Dockerfile & requirements
│   └── streamlit/         # Streamlit dashboard app
├── terraform/             # Azure infrastructure (IaC)
├── tests/                 # pytest suite (71 tests)
├── .github/workflows/     # CI/CD pipelines
├── docker-compose.yml
└── Makefile
```

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/engine/install/) (4GB+ RAM)
- [Docker Compose](https://docs.docker.com/compose/install/) v1.27+
- Kaggle CSV: [ENTSOE hourly energy data](https://www.kaggle.com/datasets/philippsommer/power-system-modelling-timeseries) → place in `data/`

### Run

```bash
git clone https://github.com/evah37/Pan-European-Energy-Lakehouse.git
cd Pan-European-Energy-Lakehouse
make up
```

Wait ~60s for services to initialize, then:

| Service | URL |
|---|---|
| Airflow | [localhost:8080](http://localhost:8080) (user/pass: `airflow`) |
| Streamlit | [localhost:8501](http://localhost:8501) |
| MinIO Console | [localhost:9001](http://localhost:9001) (user: `minio` / pass: `minio123`) |
| Spark Master | [localhost:9090](http://localhost:9090) |

## Data Pipeline

1. **Ingestion** — `ingest_kaggle.py` reads CSV, pivots wide-to-long, writes Parquet to MinIO Bronze layer.
2. **Feature Engineering** — Spark calculates ramp rates, 24h volatility, dunkelflaute detection, and residual load.
3. **Analytics** — dbt builds dimensional models: price baselines, green performance, financial risk, anomaly detection.
4. **Real-time** — Awattar API → Redpanda → Spark Streaming compares live prices against historical baselines.
5. **Visualization** — Streamlit dashboard with interactive charts.

## CI/CD

| Workflow | Trigger | Jobs |
|---|---|---|
| **CI** (`ci.yml`) | Push / PR to `main` | SQL lint, Python lint, pytest, dbt parse, Terraform validate |
| **CD** (`cd.yml`) | Version tag / manual | Build Docker → Deploy to Azure VM |

Run tests locally:

```bash
make ci
```

## Infrastructure (Terraform)

The `terraform/` folder contains Azure IaC definitions:

- Resource Group + VNet + NSG
- Linux VM (runs Docker Compose)
- ADLS Gen2 with Bronze/Silver/Gold filesystems
- Monthly budget alert

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

## License

[MIT](LICENSE)
