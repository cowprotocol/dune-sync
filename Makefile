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

test:
	python -m pytest tests

test-unit:
	python -m pytest tests/unit

run:
	python -m src.main

build-image:
	docker build -t local_dune_sync .

run-image:
	echo "using ${PWD}/data"
	docker run -v ${PWD}/data:/app/data --env-file .env local_dune_sync
