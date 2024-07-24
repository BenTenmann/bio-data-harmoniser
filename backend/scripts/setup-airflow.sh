#!/bin/bash

set -xeuo pipefail

AIRFLOW_HOME="$(realpath "$(dirname "$0")/..")"
AIRFLOW_COMMAND="poetry run airflow"

mkdir -p "${AIRFLOW_HOME}"

if ! grep -q "airflow" <<< "$(psql -U "$USER" -l)"; then
    touch tmp.sql
    trap "rm -f tmp.sql" EXIT
    cat > tmp.sql <<EOF
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DATABASE airflow OWNER TO airflow;
EOF
    sudo -u postgres psql -U "$USER" -d postgres -f tmp.sql
fi

export AIRFLOW_HOME

export AIRFLOW__API__ACCESS_CONTROL_ALLOW_HEADERS="*"
export AIRFLOW__API__ACCESS_CONTROL_ALLOW_METHODS="*"
export AIRFLOW__API__ACCESS_CONTROL_ALLOW_ORIGINS="*"
export AIRFLOW__API__AUTH_BACKEND="airflow.api.auth.backend.basic_auth"
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_HOME}/bio_data_harmoniser/api/airflow"
export AIRFLOW__CORE__EXECUTOR="CeleryExecutor"
export AIRFLOW__CORE__PARALLELISM="1"
export AIRFLOW__CORE__EXECUTE_TASKS_NEW_PYTHON_INTERPRETER="True"
export AIRFLOW__CELERY__WORKER_CONCURRENCY="4"
export AIRFLOW__CELERY__BROKER_URL="redis://localhost:6379/0"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:airflow@localhost/airflow"
export AIRFLOW__CORE__TASK_RUNNER="bio_data_harmoniser.core.utils.TaskRunner"

${AIRFLOW_COMMAND} config list \
    --include-env-vars \
    --include-examples \
    --include-descriptions \
    >  "${AIRFLOW_HOME}/airflow.cfg"
${AIRFLOW_COMMAND} db migrate
${AIRFLOW_COMMAND} users create \
    --username "${AIRFLOW_USERNAME:-admin}" \
    --password "${AIRFLOW_PASSWORD:-admin}" \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.org
