#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export IS_SUNDAY_DEPLOY="${IS_SUNDAY_DEPLOY:-false}"
export MOJAP_IMAGE_VERSION="${MOJAP_IMAGE_VERSION:-"unknown"}"
export IS_DATASET_CHECK="${IS_DATASET_CHECK:-"False"}"
export DATASET_TARGET="${DATASET_TARGET:-""}"
export UNIQUE_IDS="${UNIQUE_IDS:-}"

function is_true() {
  [[ "${1}" == "True" || "${1}" == "true" ]]
}

function run_sunday_check() {
  echo "=== Is Sunday Deploy: ${IS_SUNDAY_DEPLOY} ==="
  if is_true "${IS_SUNDAY_DEPLOY}"; then
    echo "=== Checking if today is the first Sunday of the month ==="
    "${SCRIPT_DIR}/date-checker.sh"
  fi
}

function clone_dbt_project() {
  echo "=== Running clone-create-a-derived-table.sh ==="
  "${SCRIPT_DIR}/clone-create-a-derived-table.sh"
}

function build_dataset_check_command() {
  DATASET_CHECK_COMMAND=(uv run check_run_results.py)

  if [ -n "${UNIQUE_IDS}" ]; then
    DATASET_CHECK_COMMAND+=(--unique-ids "${UNIQUE_IDS}")
  elif [ -n "${DATASET_TARGET}" ]; then
    : # DATASET_TARGET is used by the python script via env var / yaml lookup
  else
    DATASET_CHECK_COMMAND+=(--check-all-nodes)
  fi
}

function dataset_check() {
  local -a DATASET_CHECK_COMMAND

  build_dataset_check_command

  echo "Running dataset check with command: ${DATASET_CHECK_COMMAND[*]}"
  if "${DATASET_CHECK_COMMAND[@]}"; then
    echo "Dataset Check Passed!"
    exit 0
  else
    echo "Dataset Check Failed! See Logs for Details"
    exit 1
  fi
}

function maybe_run_dataset_check() {
  echo "=== Am I running a dataset check? ${IS_DATASET_CHECK} ==="
  if is_true "${IS_DATASET_CHECK}"; then
    dataset_check
  fi
}

function run_deployment() {
  echo "=== Running create-a-derived-table.sh ==="
  "${SCRIPT_DIR}/create-a-derived-table.sh"
}

function main() {
  cd "${SCRIPT_DIR}"

  echo "=== Running Airflow CaDeT Deployer (version: ${MOJAP_IMAGE_VERSION}) ==="
  run_sunday_check
  clone_dbt_project
  maybe_run_dataset_check
  run_deployment
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
