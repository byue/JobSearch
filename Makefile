PYTHON ?= python3
PYTHONPATH ?= src
VENV_DIR ?= .venv
VENV_PYTHON ?= $(VENV_DIR)/bin/python
DEPS_STAMP ?= $(VENV_DIR)/.deps-installed
COMPOSE_FILE ?= src/docker-compose.yml
DOCKER_COMPOSE ?= docker compose -f $(COMPOSE_FILE)
COVERAGE_OMIT ?= tst/*
COVERAGE_RCFILE ?= .coveragerc.test
COVERAGE_FAIL_UNDER ?= 100
SERVICE ?=
DB_PEEK_SCRIPT ?= src/sql/peek_db.sh
DAG_ID ?= job_scrapers_local
TABLE ?=
LIMIT ?= 2
TRUNCATE_CHARS ?= 10
RUN_ID ?=
ARGS ?=
PROXY_API_URL ?= http://localhost:8090
PROXY_SCOPE ?=
PROXY_SCOPES ?=
RESOURCE ?=
PROXY_ENV_FILE ?= src/scrapers/airflow/docker.env
UNIT_TEST_START_DIR ?= tst
UNIT_TEST_PATTERN ?= test_*.py
PYTHON_VERSION ?= 3.14.0
PYTHON_MAJOR_MINOR ?= $(word 1,$(subst ., ,$(PYTHON_VERSION))).$(word 2,$(subst ., ,$(PYTHON_VERSION)))
PYENV_ROOT ?= $(HOME)/.pyenv
PYENV_BIN ?= $(PYENV_ROOT)/bin/pyenv
PYENV_INSTALL_FLAGS ?= -v -s
PYENV_MAKE_JOBS ?= $(shell nproc)

.PHONY: help venv deps lint build test test-unit test-frontend test-integration test-all coverage coverage-html coverage-rc compile clean up down local-up local-down local-teardown teardown ps logs web-api db-list db-peek db-count-jobs db-failures proxy-state airflow-open web-open airflow-runs airflow-run-stop airflow-schedule-enable airflow-schedule-disable airflow-schedule-status schedule-enable schedule-disable schedule-status pyenv-setup-python

help:
	@echo "Targets:"
	@echo "  make venv               - Create local virtualenv at .venv"
	@echo "  make deps               - Install dev dependencies into .venv"
	@echo "  make lint               - Run static lint checks"
	@echo "  make build              - Run lint + compile + unit + integration tests"
	@echo "  make test-unit          - Run all unit tests with coverage report + HTML (fails if coverage < $(COVERAGE_FAIL_UNDER)%)"
	@echo "                           coverage opt out: COVERAGE_OMIT='tst/*,src/web/*'"
	@echo "  make test-frontend      - Run frontend unit tests (Vitest)"
	@echo "  make test-integration   - Run integration tests (requires Docker)"
	@echo "  make test               - Run unit tests and integration tests"
	@echo "  make test-all           - Alias for make test"
	@echo "  make coverage           - Run all unit tests with coverage report (fails if coverage < $(COVERAGE_FAIL_UNDER)%)"
	@echo "  make coverage-html      - Generate HTML coverage report (runs tests if needed)"
	@echo "  make compile            - Compile-check src packages"
	@echo "  make pyenv-setup-python - Run pyenv check + install Python + recreate .venv"
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
	@echo "  make proxy-state        - Show proxy sizes by scope (or state with RESOURCE + PROXY_SCOPE)"
	@echo "  make airflow-open       - Open Airflow UI in browser (http://localhost:8080)"
	@echo "  make airflow-runs       - List recent DAG runs (use DAG_ID=<id>)"
	@echo "  make airflow-run-stop   - Stop DAG run by marking it failed (RUN_ID=<run_id>)"
	@echo "  make web-open           - Open web frontend in browser (http://localhost:5173)"
	@echo "  make airflow-schedule-enable  - Unpause DAG schedule (DAG_ID=$(DAG_ID))"
	@echo "  make airflow-schedule-disable - Pause DAG schedule (DAG_ID=$(DAG_ID))"
	@echo "  make airflow-schedule-status  - Show paused status for DAG_ID"

venv:
	$(PYTHON) -m venv $(VENV_DIR)

_pyenv-check:
	@if [ -x "$(PYENV_BIN)" ]; then \
		echo "pyenv found: $$($(PYENV_BIN) --version)"; \
	else \
		echo "pyenv not found on PATH."; \
		echo "Install with: make pyenv-setup-python"; \
		exit 1; \
	fi

_pyenv-bootstrap:
	@if [ ! -x "$(PYENV_BIN)" ]; then \
		echo "Installing pyenv..."; \
		curl https://pyenv.run | bash; \
	else \
		echo "pyenv already installed at $(PYENV_BIN)"; \
	fi
	@grep -q 'export PYENV_ROOT="$$HOME/.pyenv"' ~/.bashrc || echo 'export PYENV_ROOT="$$HOME/.pyenv"' >> ~/.bashrc
	@grep -q 'export PATH="$$PYENV_ROOT/bin:$$PATH"' ~/.bashrc || echo 'export PATH="$$PYENV_ROOT/bin:$$PATH"' >> ~/.bashrc
	@grep -q 'eval "$$(pyenv init -)"' ~/.bashrc || echo 'eval "$$(pyenv init -)"' >> ~/.bashrc
	@echo "pyenv shell init lines ensured in ~/.bashrc"

_pyenv-install-python: _pyenv-bootstrap _pyenv-check
	@echo "Installing Python $(PYTHON_VERSION) with pyenv (this can take several minutes)..."
	@echo "Tip: pyenv build logs are under /tmp/python-build.*"
	MAKE_OPTS=-j$(PYENV_MAKE_JOBS) $(PYENV_BIN) install $(PYENV_INSTALL_FLAGS) $(PYTHON_VERSION)
	$(PYENV_BIN) local $(PYTHON_VERSION)
	PYENV_VERSION=$(PYTHON_VERSION) $(PYENV_BIN) exec python --version

_pyenv-recreate-venv: _pyenv-check
	rm -rf $(VENV_DIR)
	PYENV_VERSION=$(PYTHON_VERSION) $(PYENV_BIN) exec python -m venv $(VENV_DIR)
	. $(VENV_DIR)/bin/activate && pip install -U pip && pip install -r requirements-dev.txt

pyenv-setup-python: _pyenv-install-python _pyenv-recreate-venv

_python-version-check:
	@sys_mm=""; \
	if command -v python3 >/dev/null 2>&1; then \
		sys_mm="$$(python3 -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"; \
	fi; \
	if [ ! -x "$(VENV_PYTHON)" ]; then \
		if [ "$$sys_mm" = "$(PYTHON_MAJOR_MINOR)" ]; then \
			echo "System python3 is $(PYTHON_MAJOR_MINOR).x; creating $(VENV_DIR) from system python3"; \
			python3 -m venv $(VENV_DIR); \
		else \
			echo "System python3 is '$$sys_mm' (expected $(PYTHON_MAJOR_MINOR).x). Running: make pyenv-setup-python"; \
			$(MAKE) pyenv-setup-python; \
		fi; \
	fi; \
	actual="$$($(VENV_PYTHON) -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"; \
	actual_mm="$$($(VENV_PYTHON) -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"; \
	if [ "$$actual_mm" != "$(PYTHON_MAJOR_MINOR)" ]; then \
		if [ "$$sys_mm" = "$(PYTHON_MAJOR_MINOR)" ]; then \
			echo "Recreating $(VENV_DIR) from system python3 ($(PYTHON_MAJOR_MINOR).x)"; \
			rm -rf $(VENV_DIR); \
			python3 -m venv $(VENV_DIR); \
		else \
			echo "Python version mismatch for tests: expected $(PYTHON_MAJOR_MINOR).x, found $$actual"; \
			echo "Running: make pyenv-setup-python"; \
			$(MAKE) pyenv-setup-python; \
		fi; \
		actual="$$($(VENV_PYTHON) -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"; \
		actual_mm="$$($(VENV_PYTHON) -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"; \
		if [ "$$actual_mm" != "$(PYTHON_MAJOR_MINOR)" ]; then \
			echo "Python version still mismatched after setup: expected $(PYTHON_MAJOR_MINOR).x, found $$actual"; \
			exit 1; \
		fi; \
	fi

$(DEPS_STAMP): requirements-dev.txt requirements.txt src/web/backend/requirements.txt src/scrapers/proxy/requirements.txt src/scrapers/airflow/requirements.txt
	$(VENV_PYTHON) -m pip install -r requirements-dev.txt
	touch $(DEPS_STAMP)

deps: _python-version-check $(DEPS_STAMP)

coverage-rc:
	@printf "[run]\nsource = src\nomit = %s\n\n[report]\nomit = %s\n" "$(COVERAGE_OMIT)" "$(COVERAGE_OMIT)" > $(COVERAGE_RCFILE)

lint: deps
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m ruff check src tst integration

build: lint compile test

test-unit: deps coverage-rc _python-version-check
	@echo "=== UNIT TEST START ==="
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m pytest --import-mode=importlib --disable-warnings -p no:warnings \
		--cov=src --cov-config=$(COVERAGE_RCFILE) --cov-report=term-missing --cov-report=html --cov-fail-under=$(COVERAGE_FAIL_UNDER) \
		$(UNIT_TEST_START_DIR)
	@echo "Coverage HTML report: htmlcov/index.html"
	$(MAKE) test-frontend

test-frontend:
	npm --prefix src/web/frontend install
	npm --prefix src/web/frontend run test:coverage

test-integration: deps _python-version-check
	@echo "=== INTEGRATION TEST START ==="
	PYTHONWARNINGS=ignore::ResourceWarning PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m pytest --import-mode=importlib integration

test: test-unit test-integration

test-all: test

coverage: deps coverage-rc _python-version-check
	@echo "=== UNIT TEST START ==="
	PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m pytest --import-mode=importlib --disable-warnings -p no:warnings \
		--cov=src --cov-config=$(COVERAGE_RCFILE) --cov-report=term-missing --cov-fail-under=$(COVERAGE_FAIL_UNDER) \
		$(UNIT_TEST_START_DIR)

coverage-html: deps coverage-rc _python-version-check
	@if [ ! -f .coverage ]; then \
		echo "=== UNIT TEST START ==="; \
		PYTHONPATH=$(PYTHONPATH) $(VENV_PYTHON) -m pytest --import-mode=importlib --disable-warnings -p no:warnings \
			--cov=src --cov-config=$(COVERAGE_RCFILE) --cov-report=term-missing --cov-fail-under=$(COVERAGE_FAIL_UNDER) \
			$(UNIT_TEST_START_DIR); \
	fi
	$(VENV_PYTHON) -m coverage html --rcfile=$(COVERAGE_RCFILE)
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
	rm -f .python-version
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
	@set -e; \
	pretty_print() { \
		if command -v jq >/dev/null 2>&1; then jq .; else cat; fi; \
	}; \
	if [ -n "$(RESOURCE)" ]; then \
		if [ -z "$(PROXY_SCOPE)" ]; then \
			echo "PROXY_SCOPE is required when RESOURCE is set."; \
			echo "Example: make proxy-state RESOURCE='http://10.0.0.1:8080' PROXY_SCOPE='www.amazon.jobs'"; \
			exit 2; \
		fi; \
		curl -fsS --get "$(PROXY_API_URL)/state" \
			--data-urlencode "resource=$(RESOURCE)" \
			--data-urlencode "scope=$(PROXY_SCOPE)" | pretty_print; \
		exit 0; \
	fi; \
	scopes="$(PROXY_SCOPES)"; \
	if [ -z "$$scopes" ] && [ -n "$(PROXY_SCOPE)" ]; then \
		scopes="$(PROXY_SCOPE)"; \
	fi; \
	if [ -z "$$scopes" ] && [ -f "$(PROXY_ENV_FILE)" ]; then \
		scopes="$$(sed -n 's/^JOBSEARCH_PROXY_SCOPES=//p' "$(PROXY_ENV_FILE)" | tail -n 1)"; \
	fi; \
	if [ -z "$$scopes" ]; then \
		echo "No scopes found. Set PROXY_SCOPE or PROXY_SCOPES, or ensure $(PROXY_ENV_FILE) contains JOBSEARCH_PROXY_SCOPES."; \
		exit 2; \
	fi; \
	IFS=','; \
	for scope in $$scopes; do \
		echo "=== $$scope ==="; \
		curl -fsS --get "$(PROXY_API_URL)/sizes" --data-urlencode "scope=$$scope" | pretty_print; \
	done

airflow-open:
	@URL="http://localhost:8080"; \
	HEALTH_URL="$$URL/api/v2/monitor/health"; \
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
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags list-runs $(DAG_ID) --no-backfill

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
