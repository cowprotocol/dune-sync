VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip
PROJECT_ROOT = src


$(VENV)/bin/activate: requirements/dev.txt
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements/dev.txt


install:
	make $(VENV)/bin/activate

clean:
	rm -rf __pycache__

fmt:
	black ./

lint:
	pylint ${PROJECT_ROOT}/

types:
	mypy ${PROJECT_ROOT}/ --strict

check:
	make fmt
	make lint
	make types

test-unit:
	python -m pytest tests/unit

test-integration:
	python -m pytest tests/integration

test:
	python -m pytest tests

run:
	python -m src.main

build-image:
	docker build -t local_dune_sync .

run-image:
	echo "using ${PWD}/data"
	docker run -v ${PWD}/data:/app/data --env-file .env local_dune_sync

prefect:
	@if [ -z "$(VIRTUAL_ENV)" ]; then \
		echo "Error: Not in a virtual environment. Please activate a virtual environment before running this command."; \
		exit 1; \
	else \
		pip install -r requirements/prefect.txt; \
		prefect server start; \
	fi

deployment:
	@if [ -z "$(VIRTUAL_ENV)" ]; then \
		echo "Error: Not in a virtual environment. Please activate a virtual environment before running this command."; \
		exit 1; \
	else \
		pip install -r requirements/prefect.txt; \
		pip install -r requirements/prod.txt; \
		python -m src.prefect.local_deploy; \
	fi
