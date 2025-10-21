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
export GITHUB_PAT="${SECRET_GITHUB_PAT}"

function run_dbt() {
  local max_retries=5
  local attempt=2

  # Disable immediate exit on error for the loop
  set +e
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
      return 1
    else
      echo "dbt command failed on attempt ${attempt}, retrying"
      if [[ -f "${REPOSITORY_PATH}/mojap-derived-tables/target/run_results.json" ]]; then
        echo "run_results.json exists, retrying"
        if dbt retry; then
          echo "dbt retry succeeded"
          return 0
        fi
      else
        echo "DBT run failed without artefacts, re-running full command"
        if dbt "${MODE}" --profiles-dir "${REPOSITORY_PATH}"/.dbt --select "${DBT_SELECT_CRITERIA}" --target "${DEPLOY_ENV}"; then
          echo "dbt command succeeded"
          return 0
        fi
      fi
      ((attempt++))
      sleep 5 # Wait before retrying
    fi
  done
  set -e # Re-enable immediate exit on error
}

function export_run_artefacts() {
  RUN_TIME=$(date +%Y-%m-%dT%H:%M:%S)
  export RUN_TIME
  export AIRFLOW_WORKFLOW_REF="${WORKFLOW_NAME:-"unknown_workflow"}"

  python "${REPOSITORY_PATH}/scripts/export_run_artefacts.py"
}

function repository_dispatch() {
  export AIRFLOW_WORKFLOW_REF="${WORKFLOW_NAME:-"unknown_workflow"}"
  RUN_TIME=$(date +%Y-%m-%dT%H:%M:%S)
  export RUN_TIME
  export BRANCH="${GITHUB_REF_NAME:-"refs/heads/main"}"

  echo "Triggering GitHub Actions workflow for post-deploy metadata registration"
  echo "{\"ref\":\"${BRANCH}\",\"inputs\":{\"ref\": \"${BRANCH}\", \"run_id\": \"{123456789}\", \"run_time\": \"${RUN_TIME}\", \"workflow_name\": \"${AIRFLOW_WORKFLOW_REF}\", \"workflow_ref\": \"${AIRFLOW_WORKFLOW_REF}\"}}"

  curl -L \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_PAT}" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/moj-analytical-services/create-a-derived-table/actions/workflows/.github/workflows/post-deploy-register-table-apc/dispatches \
    -d '{"ref":"${BRANCH}","inputs":{"ref": "${{ BRANCH }}", "run_id": "{{ 123456789 }}", "run_time": "${{ RUN_TIME }}", "workflow_name": "${{ AIRFLOW_WORKFLOW_REF }}", "workflow_ref": "${{ AIRFLOW_WORKFLOW_REF}}"}}'

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
if run_dbt; then
  echo "dbt run completed successfully"
  echo "Exporting run artefacts"
  export_run_artefacts
  if ${DEPLOY_ENV} == "prod"; then
    echo "Triggering repository dispatch for post-deploy metadata registration"
    repository_dispatch
  fi
  exit 0
else
  echo "dbt run failed after 5 retries"
  echo "Exporting run artefacts"
  export_run_artefacts
  exit 1
fi
