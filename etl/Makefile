SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

DC := docker compose -f docker/docker_compose.yml

REQ := requirements/requirements
VER_REGEX := sed -nre 's/.*?([0-9]+\.[0-9]+)\.[0-9]+.*/\1/p'
PY_VER ?= $(shell echo 3.10.0 | $(VER_REGEX))# echo/cat/grep/? to get version from wherever
SYS_PY_BIN := $(shell command -v python)# Go ahead and diagnose env issues now
SYS_PY_VER := $(shell $(SYS_PY_BIN) --version | $(VER_REGEX))
PYENV_ROOT ?= /usr/local/pyenv
PYENV_BIN := $(PYENV_ROOT)/versions/$(PY_VER)*/bin/python
BASE_PY_BIN := $(shell [ $(PY_VER) == $(SYS_PY_VER) ] && echo $(SYS_PY_BIN) || echo $(PYENV_BIN))
BASE_PY ?= $(BASE_PY_BIN)
VENV := .venv

ACTIVATE := source "$(VENV)/bin/activate"
PIP := -m pip install --upgrade
PY := $(ACTIVATE) &&
# Pin below as things break - especially setuptools
PY_INIT := $(PIP) --no-cache-dir pip virtualenv# No sudo; will upgrade packages in ~/.local unless running as root, e.g. in Docker
ENV_INIT := python $(PIP) --no-cache-dir pip wheel setuptools


docker-build:
	$(DC) build

# Refresh the DB between runs
docker-db: docker-db-down docker-up

docker-db-down:
	docker container stop machina-db
	docker container rm machina-db

docker-down:
	$(DC) down -v

docker-nuke:
	docker container stop $(docker container ls -q) 2> /dev/null || true
	docker container rm $(docker container ls -aq) 2> /dev/null || true
	docker image rm $(docker image ls -q) 2> /dev/null || true
	docker system prune

docker-shell: docker-up
	docker exec -it machina-pipeline /bin/bash

docker-up: docker-build
	$(DC) up --detach

$(VENV):
	@echo Application Python version is \'$(PY_VER)\'
	@echo System Python binary is \'$(SYS_PY_BIN)\'
	@echo System Python version is \'$(SYS_PY_VER)\'
	@echo Pyenv binary, if needed, is \'$(PYENV_BIN)\'
	@echo Auto-detected Python binary is \'$(BASE_PY_BIN)\'
	@echo $(BASE_PY) | grep -q '*' && echo ERROR: $(BASE_PY) not found, install Python $(PY_VER) with Pyenv or override, as in BASE_PY=my/special/python make env-install && exit 1 || echo Using base Python at $(BASE_PY)
	$(BASE_PY) $(PY_INIT)
	$(BASE_PY) -m venv $(VENV)

env-install: $(VENV)
	$(ACTIVATE) && \
	$(ENV_INIT) && \
	python $(PIP) -r $(REQ).lock.txt

env-update: $(VENV)
	$(ACTIVATE) && \
	$(ENV_INIT) && \
	python $(PIP) --no-cache-dir -r $(REQ).txt && \
	python -m pip freeze --all > $(REQ).lock.txt

env-clean:
	rm -rf $(VENV)

pipe:
	$(PY) python -m pipeline.harvest_machine_data --profile

# Run in the container or the URL below will not work
prof-run:
	mkdir -p ../cache
	$(PY) python -m cProfile -o ../cache/output.pstats pipeline/harvest_machine_data.py --profile
	$(PY) snakeviz --hostname 0.0.0.0 --server ../cache/output.pstats

# Run on the host
prof-viz:
	xdg-open http://127.0.0.1:8080/snakeviz/%2Fcache%2Foutput.pstats

py-fmt:
	$(PY) black .
	$(PY) isort .

py-lint:
	$(PY) black --check .
	$(PY) isort --check .
	$(PY) flake8
	$(PY) pyright

test:
	$(PY) pytest

view:
	$(PY) python -m reader.print_data --file sample
