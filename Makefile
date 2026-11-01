.DEFAULT_GOAL := help
COMPOSE ?= docker compose
SERVICE ?=

.PHONY: help env up up-standalone down reset ps logs psql topics airflow-db test lint

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-15s %s\n", $$1, $$2}'

env: ## Create .env from the template (never overwrites an existing one)
	@test -f .env || (cp .env.example .env && echo "Created .env - fill it in before running 'make up'")

up: env ## Start the pipeline, orchestrated by Airflow (UI on http://localhost:8080)
	$(COMPOSE) up -d --build

up-standalone: env ## Start the pipeline with plain loop containers instead of Airflow
	$(COMPOSE) --profile standalone up -d --build postgres kafka producer spark loader sheets-sink

down: ## Stop all containers (data volumes are kept)
	$(COMPOSE) --profile standalone down

reset: ## Stop all containers AND delete the data volumes (Postgres, Spark cache)
	$(COMPOSE) --profile standalone down -v

ps: ## List running containers
	$(COMPOSE) --profile standalone ps

logs: ## Follow logs; limit to one service with e.g. `make logs SERVICE=spark`
	$(COMPOSE) --profile standalone logs -f --tail=200 $(SERVICE)

psql: ## Open a psql shell in the warehouse
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

topics: ## List Kafka topics
	$(COMPOSE) exec kafka kafka-topics.sh --bootstrap-server localhost:9094 --list

airflow-db: ## Create Airflow's metadata DB inside an existing Postgres volume (upgrading an old setup)
	$(COMPOSE) exec -T postgres bash /docker-entrypoint-initdb.d/02_create_airflow_db.sh

test: ## Run the tests (export TEST_PG_DSN=... to include the Postgres integration tests)
	python -m pytest

lint: ## Lint with ruff
	ruff check .
