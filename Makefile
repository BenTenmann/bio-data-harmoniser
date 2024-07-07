POETRY := poetry run

BASE_PATH := $(shell pwd)
BACKEND_PATH := $(BASE_PATH)/backend
FRONTEND_PATH := $(BASE_PATH)/frontend
PYTHONPATH := $(BASE_PATH):$(PYTHONPATH)

AIRFLOW_HOME := $(BASE_PATH)/backend
AIRFLOW_HOST := localhost
AIRFLOW_PORT := 8080
AIRFLOW_USERNAME := admin
AIRFLOW_PASSWORD := admin
AIRFLOW_OUTPUT_DIR := $(BASE_PATH)/backend/data/airflow

FASTAPI_HOST := 0.0.0.0
FASTAPI_PORT := 80
FASTAPI_UPLOAD_DIR := $(BASE_PATH)/backend/data/uploads

ONTOLOGY_PATH := $(BASE_PATH)/backend/data/ontology

LLM_PROVIDER := anthropic
LLM_MODEL := claude-3-5-sonnet-20240620
LLM_EMBEDDINGS_MODEL := mixedbread-ai/mxbai-embed-large-v1

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

ingest-ontology:
	@echo "Ingesting ontology"
	cd $(BACKEND_PATH) && $(POETRY) bio-data-harmoniser ingest ontology

setup: install ingest-ontology
	@echo "Setup complete"

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
	cd $(FRONTEND_PATH) && npm run dev

local: airflow backend_dev frontend_dev

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
