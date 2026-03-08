PYTHON ?= python3
PYTHONPATH ?= src
VENV_DIR ?= .venv
VENV_PYTHON ?= $(VENV_DIR)/bin/python
DEPS_STAMP ?= $(VENV_DIR)/.deps-installed
COMPOSE_FILE ?= src/docker-compose.yml
DOCKER_COMPOSE ?= docker compose -f $(COMPOSE_FILE)
COVERAGE_RUN_ARGS ?= --source=src --omit='tst/*,src/scrapers/airflow/dags/*'
COVERAGE_FAIL_UNDER ?= 100
SERVICE ?=
DB_PEEK_SCRIPT ?= src/sql/peek_db.sh
DAG_ID ?= job_scrapers_local
PROXY_SCOPES ?= www.amazon.jobs jobs.apple.com www.google.com www.metacareers.com apply.careers.microsoft.com explore.jobs.netflix.net
TABLE ?=
LIMIT ?= 2
TRUNCATE_CHARS ?= 10
RUN_ID ?=
ARGS ?=

.PHONY: help venv deps lint build test test-unit test-frontend test-integration test-all test-proxy test-proxy-verbose coverage coverage-html compile clean up down local-up local-down local-teardown teardown ps logs web-api db-list db-peek db-count-jobs db-failures proxy-state airflow-open web-open airflow-runs airflow-run-stop airflow-schedule-enable airflow-schedule-disable airflow-schedule-status schedule-enable schedule-disable schedule-status

help:
	@echo "Targets:"
	@echo "  make venv               - Create local virtualenv at .venv"
	@echo "  make deps               - Install dev dependencies into .venv"
	@echo "  make lint               - Run static lint checks"
	@echo "  make build              - Run lint + compile + unit + integration tests"
	@echo "  make test-unit          - Run all unit tests with coverage report + HTML (fails if coverage < $(COVERAGE_FAIL_UNDER)%)"
	@echo "  make test-frontend      - Run frontend unit tests (Vitest)"
	@echo "  make test-integration   - Run integration tests (requires Docker)"
	@echo "  make test               - Run unit tests and integration tests"
	@echo "  make test-all           - Alias for make test"
	@echo "  make test-proxy         - Run proxy unit tests"
	@echo "  make test-proxy-verbose - Run proxy unit tests (verbose)"
	@echo "  make coverage           - Run all unit tests with coverage report (fails if coverage < $(COVERAGE_FAIL_UNDER)%)"
	@echo "  make coverage-html      - Generate HTML coverage report (runs tests if needed)"
	@echo "  make compile            - Compile-check src packages"
	@echo "  make clean              - Remove Python/test artifacts"
	@echo "  make up                 - Start local docker services"
	@echo "  make down               - Stop local docker services"
	@echo "  make local-up           - Alias for make up"
	@echo "  make local-down         - Alias for make down"
	@echo "  make local-teardown     - Stop services and remove compose volumes/images"
	@echo "  make teardown           - Alias for make local-teardown"
	@echo "  make ps                 - Show docker service status"
	@echo "  make logs               - Tail docker service logs (all or SERVICE=<name>)"
	@echo "  make web-api            - Run web backend CLI (ARGS='get-companies' etc.)"
	@echo "  make db-list            - List scraper DB tables"
	@echo "  make db-peek            - Print latest rows (TABLE=<name[,name]> LIMIT=<n> TRUNCATE_CHARS=<n>)"
	@echo "  make db-count-jobs      - Count jobs and job_details (optional RUN_ID=<id>)"
	@echo "  make db-failures        - Show failed publish_runs (optional RUN_ID=<id>, LIMIT=<n>)"
	@echo "  make proxy-state        - Show proxy sizes (use SCOPE=<domain> for one scope)"
	@echo "  make airflow-open       - Open Airflow UI in browser (http://localhost:8080)"
	@echo "  make airflow-runs       - List recent DAG runs (use DAG_ID=<id>)"
	@echo "  make airflow-run-stop   - Stop DAG run by marking it failed (RUN_ID=<run_id>)"
	@echo "  make web-open           - Open web frontend in browser (http://localhost:5173)"
	@echo "  make airflow-schedule-enable  - Unpause DAG schedule (DAG_ID=$(DAG_ID))"
	@echo "  make airflow-schedule-disable - Pause DAG schedule (DAG_ID=$(DAG_ID))"
	@echo "  make airflow-schedule-status  - Show paused status for DAG_ID"

venv:
	$(PYTHON) -m venv $(VENV_DIR)

$(DEPS_STAMP): requirements-dev.txt requirements.txt src/web/backend/requirements.txt src/scrapers/proxy/requirements.txt src/scrapers/airflow/requirements.txt | venv
	$(VENV_PYTHON) -m pip install -r requirements-dev.txt
	touch $(DEPS_STAMP)

deps: $(DEPS_STAMP)

lint: deps
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m ruff check src tst integration --select E9,F63,F7,F82

build: lint compile test

test-unit: deps
	$(VENV_PYTHON) -m coverage erase
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/proxy -p "test_*.py"
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/common -p "test_*.py"
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/airflow -p "test_*.py"
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/web/backend -p "test_*.py"
	$(VENV_PYTHON) -m coverage report -m --fail-under=$(COVERAGE_FAIL_UNDER)
	$(VENV_PYTHON) -m coverage html
	@echo "Coverage HTML report: htmlcov/index.html"
	$(MAKE) test-frontend

test-frontend:
	npm --prefix src/web/frontend install
	npm --prefix src/web/frontend run test:coverage

test-integration: deps
	PYTHONWARNINGS=ignore::ResourceWarning PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m unittest discover -s integration/scrapers/proxy -p "test_*.py" -v
	PYTHONWARNINGS=ignore::ResourceWarning PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m unittest discover -s integration/scrapers/airflow -p "test_*.py" -v
	PYTHONWARNINGS=ignore::ResourceWarning PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m unittest discover -s integration/web/backend -p "test_*.py" -v

test: test-unit test-integration

test-all: test

test-proxy: deps
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m unittest discover -s tst/scrapers/proxy -p "test_*.py"

test-proxy-verbose: deps
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m unittest discover -s tst/scrapers/proxy -p "test_*.py" -v

coverage: deps
	$(VENV_PYTHON) -m coverage erase
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/proxy -p "test_*.py"
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/common -p "test_*.py"
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/airflow -p "test_*.py"
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/web/backend -p "test_*.py"
	$(VENV_PYTHON) -m coverage report -m --fail-under=$(COVERAGE_FAIL_UNDER)

coverage-html: deps
	@if [ ! -f .coverage ]; then \
		$(VENV_PYTHON) -m coverage erase; \
		PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/proxy -p "test_*.py"; \
		PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/common -p "test_*.py"; \
		PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/scrapers/airflow -p "test_*.py"; \
		PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m coverage run -a $(COVERAGE_RUN_ARGS) -m unittest discover -s tst/web/backend -p "test_*.py"; \
	fi
	$(VENV_PYTHON) -m coverage html
	@echo "Coverage HTML report: htmlcov/index.html"

compile: deps
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m compileall -q src/scrapers src/web/backend

clean:
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type f \( -name "*.pyc" -o -name "*.pyo" \) -delete
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +
	rm -f .coverage
	rm -f .coverage.*
	rm -rf htmlcov
	rm -rf $(VENV_DIR)

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

local-up: up

local-down: down

local-teardown:
	$(DOCKER_COMPOSE) down -v --rmi local

teardown: local-teardown

ps:
	$(DOCKER_COMPOSE) ps

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200 $(SERVICE)

web-api: deps
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) src/web/backend/scripts/web_api_cli.py $(ARGS)

db-peek:
	@if [ -n "$(TABLE)" ]; then \
		bash $(DB_PEEK_SCRIPT) --table "$(TABLE)" --limit "$(LIMIT)" --truncate-chars "$(TRUNCATE_CHARS)"; \
	else \
		bash $(DB_PEEK_SCRIPT) --limit "$(LIMIT)" --truncate-chars "$(TRUNCATE_CHARS)"; \
	fi

db-list:
	$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow -c \
	"SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;"

db-count-jobs:
	@if [ -n "$(RUN_ID)" ]; then \
		$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow -c \
		"SELECT 'jobs' AS table_name, COUNT(*) AS row_count FROM jobs WHERE run_id='$(RUN_ID)' \
		 UNION ALL \
		 SELECT 'job_details' AS table_name, COUNT(*) AS row_count FROM job_details WHERE run_id='$(RUN_ID)';"; \
	else \
		$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow -c \
		"SELECT 'jobs' AS table_name, COUNT(*) AS row_count FROM jobs \
		 UNION ALL \
		 SELECT 'job_details' AS table_name, COUNT(*) AS row_count FROM job_details;"; \
	fi

db-failures:
	@if [ -n "$(RUN_ID)" ]; then \
		$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow -c \
		"SELECT run_id, version_ts, status, db_ready, db_published_at, db_error_message, es_ready, es_published_at, es_error_message, updated_at \
		 FROM publish_runs \
		 WHERE status = 'failed' AND run_id='$(RUN_ID)' \
		 ORDER BY updated_at DESC \
		 LIMIT $(LIMIT);"; \
	else \
		$(DOCKER_COMPOSE) exec -T postgres psql -U airflow -d airflow -c \
		"SELECT run_id, version_ts, status, db_ready, db_published_at, db_error_message, es_ready, es_published_at, es_error_message, updated_at \
		 FROM publish_runs \
		 WHERE status = 'failed' \
		 ORDER BY updated_at DESC \
		 LIMIT $(LIMIT);"; \
	fi

proxy-state:
	@if [ -n "$(SCOPE)" ]; then \
		echo "=== proxy sizes: $(SCOPE) ==="; \
		$(DOCKER_COMPOSE) exec -T proxy-api python /opt/jobsearch/src/scrapers/proxy/scripts/proxy_api_cli.py sizes --scope "$(SCOPE)"; \
	else \
		for scope in $(PROXY_SCOPES); do \
			echo "=== proxy sizes: $$scope ==="; \
			$(DOCKER_COMPOSE) exec -T proxy-api python /opt/jobsearch/src/scrapers/proxy/scripts/proxy_api_cli.py sizes --scope "$$scope" || exit $$?; \
			echo ""; \
		done; \
	fi

airflow-open:
	@URL="http://localhost:8080"; \
	HEALTH_URL="$$URL/health"; \
	ready=0; \
	echo "Waiting for Airflow UI to become ready..."; \
	for i in $$(seq 1 30); do \
		if command -v curl >/dev/null 2>&1 && curl -fsS "$$HEALTH_URL" >/dev/null 2>&1; then \
			ready=1; \
			break; \
		fi; \
		sleep 2; \
	done; \
	if [ "$$ready" -ne 1 ]; then \
		echo "Airflow UI is not healthy yet: $$HEALTH_URL"; \
		exit 1; \
	fi; \
	opened=0; \
	if command -v wslview >/dev/null 2>&1; then \
		wslview "$$URL" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 0 ] && command -v powershell.exe >/dev/null 2>&1; then \
		powershell.exe -NoProfile -Command "Start-Process '$$URL'" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 0 ] && command -v xdg-open >/dev/null 2>&1; then \
		xdg-open "$$URL" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 0 ] && command -v open >/dev/null 2>&1; then \
		open "$$URL" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 1 ]; then \
		echo "Opened Airflow UI: $$URL"; \
	else \
		echo "Could not auto-open browser. Open manually: $$URL"; \
	fi

airflow-runs:
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags list-runs -d $(DAG_ID) --no-backfill

airflow-run-stop:
	@if [ -z "$(RUN_ID)" ]; then \
		echo "RUN_ID is required. Example: make airflow-run-stop RUN_ID='scheduled__2026-03-05T04:00:00+00:00'"; \
		exit 2; \
	fi
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags state $(DAG_ID) "$(RUN_ID)" failed

web-open:
	@URL="http://localhost:5173"; \
	HEALTH_URL="$$URL"; \
	echo "Waiting for web frontend to become ready..."; \
	for i in $$(seq 1 30); do \
		if command -v curl >/dev/null 2>&1 && curl -fsS "$$HEALTH_URL" >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 2; \
	done; \
	opened=0; \
	if command -v wslview >/dev/null 2>&1; then \
		wslview "$$URL" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 0 ] && command -v powershell.exe >/dev/null 2>&1; then \
		powershell.exe -NoProfile -Command "Start-Process '$$URL'" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 0 ] && command -v xdg-open >/dev/null 2>&1; then \
		xdg-open "$$URL" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 0 ] && command -v open >/dev/null 2>&1; then \
		open "$$URL" >/dev/null 2>&1 && opened=1; \
	fi; \
	if [ "$$opened" -eq 1 ]; then \
		echo "Opened web frontend: $$URL"; \
	else \
		echo "Could not auto-open browser. Open manually: $$URL"; \
	fi

airflow-schedule-enable:
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags unpause $(DAG_ID)

airflow-schedule-disable:
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags pause $(DAG_ID)

airflow-schedule-status:
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags list | grep -E "^$(DAG_ID)\b|dag_id"

schedule-enable: airflow-schedule-enable

schedule-disable: airflow-schedule-disable

schedule-status: airflow-schedule-status
