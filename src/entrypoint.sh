#!/usr/bin/env bash

set -euo pipefail

export IS_SUNDAY_DEPLOY="${IS_SUNDAY_DEPLOY:-false}"
export MOJAP_IMAGE_VERSION="${MOJAP_IMAGE_VERSION:-"unknown"}"
export IS_DATASET_CHECK="${IS_DATASET_CHECK:-"False"}"
export DATASET_TARGET="${DATASET_TARGET:-""}"

echo "=== Running Airflow CaDeT Deployer (version: ${MOJAP_IMAGE_VERSION}) ==="

echo "=== Is Sunday Deploy: ${IS_SUNDAY_DEPLOY} ==="
if [ "${IS_SUNDAY_DEPLOY}" = "True" ]; then
  echo "=== Checking if today is the first Sunday of the month ==="
  ./date-checker.sh
fi

echo "=== Running clone-create-a-derived-table.sh ==="
./clone-create-a-derived-table.sh

echo "=== Am I running a dataset check? ${IS_DATASET_CHECK} ==="
if [ "${IS_DATASET_CHECK}" = "True" ]; then
  if uv run check_run_results.py; then
    echo "Dataset Check Passed!"
    exit 0
  else
    echo "Dataset Check Failed! See Logs for Details"
    exit 1
  fi
fi

echo "=== Running create-a-derived-table.sh ==="
./create-a-derived-table.sh
