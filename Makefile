# ─────────────────────────────────────────────────────────────────────────────
# ETAR Pipeline — task runner
# Usage: make <target>
# ─────────────────────────────────────────────────────────────────────────────

# Image tag used when building and running test images.
# Override on the CLI:  make build IMAGE_TAG=abc123
IMAGE_TAG ?= latest

# All env files passed to every docker compose command.
ENV_FLAGS := \
  --env-file ./env/airflow.env \
  --env-file ./env/airflow.creds \
  --env-file ./env/clickhouse.env \
  --env-file ./env/clickhouse.creds \
  --env-file ./env/kafka.env \
  --env-file ./env/minio.env \
  --env-file ./env/minio.creds \
  --env-file ./env/postgres.env \
  --env-file ./env/postgres.creds \
  --env-file ./env/spark.env

DC        := docker compose --project-name lp --project-directory . $(ENV_FLAGS)
DC_TEST   := docker compose --project-name test --project-directory .

.PHONY: help build up down logs ps \
        test test-producer test-airflow test-dashboard test-spark test-db \
        init-env

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  make init-env       Copy *.example env files to their real counterparts"
	@echo "  make build          Build all production images"
	@echo "  make up             Start the full stack"
	@echo "  make down           Stop and remove containers + volumes"
	@echo "  make logs           Follow logs for all services"
	@echo "  make ps             Show running containers"
	@echo ""
	@echo "  make test           Run ALL test suites sequentially"
	@echo "  make test-producer  Run producer tests only"
	@echo "  make test-airflow   Run airflow tests only"
	@echo "  make test-dashboard Run dashboard tests only"
	@echo "  make test-spark     Run spark tests only"
	@echo "  make test-db        Run db/schema tests only"
	@echo ""
	@echo "  Override image tag: make build IMAGE_TAG=abc123"
	@echo ""

# ── Env bootstrap ─────────────────────────────────────────────────────────────
init-env:
	@echo "Copying env example files…"
	@for f in env/*.example; do \
	  target=$$(echo $$f | sed 's/\.example$$//'); \
	  if [ ! -f "$$target" ]; then \
	    cp "$$f" "$$target"; \
	    echo "  created $$target"; \
	  else \
	    echo "  skipped $$target (already exists)"; \
	  fi; \
	done
	@echo "Done. Edit the env/ files and replace all REPLACE_WITH_* placeholders."

# ── Production lifecycle ──────────────────────────────────────────────────────
build:
	$(DC) build --build-arg IMAGE_TAG=$(IMAGE_TAG)

up:
	@echo "Initialising Airflow (first run only)…"
	$(DC) up airflow-init -d
	@echo "Waiting for airflow-init to complete…"
	$(DC) wait airflow-init
	@echo "Starting remaining services…"
	$(DC) up -d --remove-orphans

down:
	$(DC) down -v --remove-orphans

logs:
	$(DC) logs -f

ps:
	$(DC) ps

# ── Test suites ───────────────────────────────────────────────────────────────
test: test-db test-producer test-dashboard test-spark test-airflow

test-producer:
	@echo "▶ Running producer tests…"
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  --env-file ./tests/env-test/kafka.env \
	  --env-file ./tests/env-test/clickhouse.env \
	  --env-file ./tests/env-test/clickhouse.creds \
	  -f tests/docker-compose.test.producer.yml \
	  up --build --exit-code-from producer-test-runner
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  -f tests/docker-compose.test.producer.yml \
	  down -v --remove-orphans

test-airflow:
	@echo "▶ Running airflow tests…"
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  --env-file ./tests/env-test/airflow.env \
	  --env-file ./tests/env-test/airflow.creds \
	  --env-file ./tests/env-test/clickhouse.env \
	  --env-file ./tests/env-test/clickhouse.creds \
	  --env-file ./tests/env-test/minio.env \
	  --env-file ./tests/env-test/minio.creds \
	  --env-file ./tests/env-test/postgres.env \
	  --env-file ./tests/env-test/postgres.creds \
	  --env-file ./tests/env-test/spark.env \
	  -f tests/docker-compose.test.airflow.yml \
	  up --build --exit-code-from airflow-test-runner
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  -f tests/docker-compose.test.airflow.yml \
	  down -v --remove-orphans

test-dashboard:
	@echo "▶ Running dashboard tests…"
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  -f tests/docker-compose.test.dashboard.yml \
	  up --build --exit-code-from dashboard-api-test-runner
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  -f tests/docker-compose.test.dashboard.yml \
	  down -v --remove-orphans

test-spark:
	@echo "▶ Running spark tests…"
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  --env-file ./tests/env-test/minio.env \
	  --env-file ./tests/env-test/minio.creds \
	  --env-file ./tests/env-test/spark.env \
	  -f tests/docker-compose.test.spark.yml \
	  up --build --exit-code-from spark-test-runner
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  -f tests/docker-compose.test.spark.yml \
	  down -v --remove-orphans

test-db:
	@echo "▶ Running db/schema tests…"
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  --env-file ./tests/env-test/clickhouse.env \
	  --env-file ./tests/env-test/clickhouse.creds \
	  --env-file ./tests/env-test/minio.env \
	  --env-file ./tests/env-test/minio.creds \
	  -f tests/docker-compose.test.db.yml \
	  up --build --exit-code-from db-test-runner
	IMAGE_TAG=$(IMAGE_TAG) $(DC_TEST) \
	  -f tests/docker-compose.test.db.yml \
	  down -v --remove-orphans
