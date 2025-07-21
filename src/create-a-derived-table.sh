#!/usr/bin/env bash

set -euo pipefail

export REPOSITORY_PATH="${REPOSITORY_PATH:-"${ANALYTICAL_PLATFORM_DIRECTORY}/create-a-derived-table"}"
export MODE="${MODE}"
export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-"${REPOSITORY_PATH}/.dbt"}"
export DBT_PROJECT="${DBT_PROJECT}"
export DEPLOY_ENV="${DEPLOY_ENV}"
export DBT_SELECT_CRITERIA="${DBT_SELECT_CRITERIA}"

function run_dbt() {
  local max_retries=5
  local attempt=1

  # Disable immediate exit on error for the loop
  set +e
  while [[ "${attempt}" -le "${max_retries}" ]]; do
    echo "Attempt ${attempt} of ${max_retries} to run dbt command"
    if dbt "${MODE}" --select "${DBT_SELECT_CRITERIA}" --target "${DEPLOY_ENV}"; then
      echo "dbt command succeeded on attempt ${attempt}"
      break
    elif [[ "${attempt}" -eq "${max_retries}" ]]; then
      echo "dbt command failed after ${max_retries} attempts. Exiting."
      exit 1
    else
      echo "dbt command failed on attempt ${attempt}. Retrying..."
      ((attempt++))
      sleep 5  # Wait before retrying
    fi
  done
  set -e  # Re-enable immediate exit on error
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
run_dbt
