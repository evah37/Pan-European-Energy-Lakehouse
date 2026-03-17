# Control Commands
# Build and Start all containers
up:
	docker compose up airflow-init && docker compose up --build -d

# Stop all containers
down:
	docker compose down --volumes --remove-orphans

# Restart everything
restart: down up

# Setup & Maintenance
# Create directories and fix permissions (Crucial for Docker on Linux)
perms:
	sudo mkdir -p logs plugins temp dags tests data/raw spark_jobs scripts dbt_energy/models && \
	sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins temp dags tests data spark_jobs scripts dbt_energy

# Clean up docker images
clean:
	docker system prune -f

# Check status
ps:
	docker compose ps

# Access Containers
sh-airflow:
	docker exec -ti airflow-webserver bash

sh-spark:
	docker exec -ti spark-master bash

# Code Quality (Restored from Template)
# Run tests
pytest:
	docker exec airflow-webserver pytest -p no:warnings -v /opt/airflow/tests

# Format code
format:
	docker exec airflow-webserver python -m black -S --line-length 79 .

# Check types
type:
	docker exec airflow-webserver mypy --ignore-missing-imports /opt/airflow

# Lint check
lint: 
	docker exec airflow-webserver flake8 /opt/airflow/dags

# SQL Lint (sqlfluff)
sqlfluff-lint:
	sqlfluff lint dbt_energy/models/ --dialect duckdb --exclude-rules LT05 --ignore templating

# SQL Auto-fix
sqlfluff-fix:
	sqlfluff fix dbt_energy/models/ --dialect duckdb --exclude-rules LT05 --ignore templating

# ── CI Pipeline (lightweight, no Docker) ─────────────────────
ci: ci-lint ci-test
	@echo "✅ CI pipeline passed"

ci-lint:
	flake8 dags/ scripts/ spark_jobs/ --max-line-length=120 --ignore=E501,W503,E402 --exclude=__pycache__

ci-test:
	python -m pytest tests/ -v --tb=short -x