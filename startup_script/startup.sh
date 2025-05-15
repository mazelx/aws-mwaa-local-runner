#!/bin/sh
echo "----> run startup script"
# docker build --rm -t sd-engine packages/engine

export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"
export PIP_USER=false

python3 -m venv "${DBT_VENV_PATH}"

${DBT_VENV_PATH}/bin/pip install dbt-duckdb

export PIP_USER=true