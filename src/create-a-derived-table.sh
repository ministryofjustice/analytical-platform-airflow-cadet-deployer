#!/usr/bin/env bash

set -euo pipefail

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-"eu-west-1"}"
export REPOSITORY_PATH="${REPOSITORY_PATH:-"${ANALYTICAL_PLATFORM_DIRECTORY}/create-a-derived-table"}"
export MODE="${MODE}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-"${REPOSITORY_PATH}/.dbt"}"
export DBT_PROFILE_WORKGROUP="${DBT_PROFILE_WORKGROUP}"
export DBT_PROJECT="${DBT_PROJECT}"
export DBT_SELECT_CRITERIA="${DBT_SELECT_CRITERIA}"
export DEPLOY_ENV="${DEPLOY_ENV}"
export S3_BUCKET="${S3_BUCKET:-"mojap-derived-tables"}"
export WORKFLOW_NAME="${WORKFLOW_NAME}"

function run_dbt() {
  local max_retries=5
  local attempt=2

  # Disable immediate exit on error for the loop
  if dbt "${MODE}" --profiles-dir "${REPOSITORY_PATH}"/.dbt --select "${DBT_SELECT_CRITERIA}" --target "${DEPLOY_ENV}"; then
    echo "dbt command succeeded"
    return 0
  else
    echo "dbt command failed, attempting to dbt retry..."
  fi
  while [[ "${attempt}" -le "${max_retries}" ]]; do
    echo "Attempt ${attempt} of ${max_retries} to run dbt command"
    if [[ "${attempt}" -eq "${max_retries}" ]]; then
      echo "dbt command failed after ${max_retries} attempts"
      exit 1
    else
      echo "dbt command failed on attempt ${attempt}, retrying"
      if dbt retry; then
        echo "DBT retry succeeded, continuing to export of artifacts"
        return 0
      else
        ((attempt++))
        sleep 5 # Wait before retrying
      fi
    fi
  done
}

function export_run_artefacts() {
  RUN_TIME=$(date +%Y-%m-%dT%H:%M:%S)
  export RUN_TIME
  export AIRFLOW_WORKFLOW_REF="${WORKFLOW_NAME:-"unknown_workflow"}"

  python "${REPOSITORY_PATH}/scripts/export_run_artefacts.py"
}

echo "Creating virtual environment and installing dependencies"
cd "${REPOSITORY_PATH}"

uv venv

# shellcheck disable=SC1091
source .venv/bin/activate

uv pip install --requirements requirements.txt

echo "Changing to project directory [ ${DBT_PROJECT} ]"
cd "${DBT_PROJECT}"

echo "Generating models"
python "${REPOSITORY_PATH}/scripts/generate_models.py" model_templates/ ./ --target "${DEPLOY_ENV}"

echo "Running dbt debug"
dbt debug

echo "Running dbt clean"
dbt clean

echo "Running dbt deps"
dbt deps

echo "Running in mode [ ${MODE} ] for project [ ${DBT_PROJECT} ] to environment [ ${DEPLOY_ENV} ] with select criteria [ ${DBT_SELECT_CRITERIA} ]"
set +e
run_dbt

echo "Exporting run artefacts"
set -e # Re-enable immediate exit on error
export_run_artefacts
