SHELL := /bin/bash

POETRY := poetry run

BASE_PATH := $(shell pwd)
BACKEND_PATH := $(BASE_PATH)/backend
FRONTEND_PATH := $(BASE_PATH)/frontend
PYTHONPATH := $(BASE_PATH):$(PYTHONPATH)

DOCKER_IMAGE_NAME := bentenmann/bio-data-harmoniser:latest

AIRFLOW_HOME := $(BASE_PATH)/backend
AIRFLOW_HOST ?= localhost
AIRFLOW_PORT ?= 8080
AIRFLOW_USERNAME := admin
AIRFLOW_PASSWORD := admin
AIRFLOW_OUTPUT_DIR := $(BASE_PATH)/backend/data/airflow

FASTAPI_HOST ?= 0.0.0.0
FASTAPI_PORT ?= 80
FASTAPI_UPLOAD_DIR := $(BASE_PATH)/backend/data/uploads

ONTOLOGY_PATH := $(BASE_PATH)/backend/data/ontology

LLM_PROVIDER := anthropic
LLM_MODEL := claude-3-5-sonnet-20240620
LLM_EMBEDDINGS_MODEL := mixedbread-ai/mxbai-embed-large-v1
LLM_EMBEDDINGS_DEVICE ?= cpu

FRONTEND_HOST ?= localhost
FRONTEND_PORT ?= 3000

# see: https://stackoverflow.com/a/76405182
NO_PROXY := *
# see: https://stackoverflow.com/a/71525517
OBJC_DISABLE_INITIALIZE_FORK_SAFETY := YES
# see: https://github.com/UKPLab/sentence-transformers/issues/1318#issuecomment-1084731111
OMP_NUM_THREADS := 1

export BASE_PATH
export PYTHONPATH

export AIRFLOW_HOME
export AIRFLOW_HOST
export AIRFLOW_PORT
export AIRFLOW_OUTPUT_DIR

export FASTAPI_HOST
export FASTAPI_PORT
export FASTAPI_UPLOAD_DIR

export ONTOLOGY_PATH

export LLM_PROVIDER
export LLM_MODEL
export LLM_EMBEDDINGS_MODEL
export LLM_EMBEDDINGS_DEVICE

export NO_PROXY
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY
export OMP_NUM_THREADS

ifneq (,$(wildcard .env))
	include .env
	export
endif

# --- Setup ---

install_backend:
	cd $(BACKEND_PATH) && $(POETRY) install

install_frontend:
	cd $(FRONTEND_PATH) && npm install

install: install_backend install_frontend

ingest_ontology:
	@echo "Ingesting ontology"
	cd $(BACKEND_PATH) && $(POETRY) bio-data-harmoniser ingest ontology

setup: install ingest_ontology
	@echo "Setup complete"

ingest_ontology_in_docker:
	docker run \
		--rm \
		--name bio-data-harmoniser \
		-u 0 \
		--platform linux/x86_64 \
		-v $(ONTOLOGY_PATH)_test:/app/backend/data/ontology \
		$(DOCKER_IMAGE_NAME) \
		poetry -C backend run bio-data-harmoniser ingest ontology

# --- Development ---

airflow_webserver:
	cd $(BACKEND_PATH) && $(POETRY) airflow webserver \
		--hostname $(AIRFLOW_HOST) \
		--port $(AIRFLOW_PORT)

airflow_scheduler:
	cd $(BACKEND_PATH) && $(POETRY) airflow scheduler 2>&1 | tee -a scheduler.log

airflow_worker:
	cd $(BACKEND_PATH) && \
		trap '$(POETRY) airflow celery stop' EXIT && \
		$(POETRY) airflow celery worker 2>&1 | tee -a celery.log

airflow: airflow_webserver airflow_scheduler airflow_worker

backend_dev:
	cd $(BACKEND_PATH) && $(POETRY) uvicorn bio_data_harmoniser.api.app:app \
		--host $(FASTAPI_HOST) \
		--port $(FASTAPI_PORT) \
		--reload

frontend_dev:
	cd $(FRONTEND_PATH) && npm run dev -- --hostname $(FRONTEND_HOST) --port $(FRONTEND_PORT)

local: airflow backend_dev frontend_dev

start_postgres:
	cd $(BACKEND_PATH) && \
		./scripts/start-postgres.sh

start_redis:
	cd $(BACKEND_PATH) && \
		./scripts/start-redis.sh

setup_airflow: start_postgres start_redis
	@echo "Setting up Airflow"
	cd $(BACKEND_PATH) && \
		export USER=postgres && \
		./scripts/setup-airflow.sh

airflow_reset:
	cd $(BACKEND_PATH) && \
		$(POETRY) airflow db reset -y && \
		$(POETRY) airflow db init && \
		$(POETRY) airflow users create \
			--username $(AIRFLOW_USERNAME) \
			--password $(AIRFLOW_PASSWORD) \
			--firstname admin \
			--lastname admin \
			--email admin@example.com \
			--role Admin && \
		rm -rf $(AIRFLOW_OUTPUT_DIR)/* && \
		rm -rf $(AIRFLOW_HOME)/logs/*

run_in_docker:
	@echo "Running in Dokcer"
	docker run \
		--rm \
		--name bio-data-harmoniser \
		-p 3000:3000 \
		-p 80:80 \
		-p 8080:8080 \
		-u 0 \
		--platform linux/x86_64 \
		-v $(AIRFLOW_OUTPUT_DIR):/app/backend/data/airflow \
		-v $(FASTAPI_UPLOAD_DIR):/app/backend/data/uploads \
		-v $(ONTOLOGY_PATH):/app/backend/data/ontology \
		-e AIRFLOW_HOST=0.0.0.0 \
		-e AIRFLOW_PORT=8080 \
		-e FASTAPI_HOST=0.0.0.0 \
		-e FASTAPI_PORT=80 \
		-e FRONTEND_HOST=0.0.0.0 \
		-e FRONTEND_PORT=3000 \
		$(DOCKER_IMAGE_NAME)

docker_build:
	docker build \
		--platform linux/x86_64 \
		-f Dockerfile \
		-t $(DOCKER_IMAGE_NAME) .

docker_push:
	docker push $(DOCKER_IMAGE_NAME)
