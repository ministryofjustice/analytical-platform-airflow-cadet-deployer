#!/usr/bin/env bash

set -euo pipefail

export IS_SUNDAY_DEPLOY="${IS_SUNDAY_DEPLOY:-false}"
export MOJAP_IMAGE_VERSION="${MOJAP_IMAGE_VERSION:-"unknown"}"

echo "=== Running Airflow CaDeT Deployer (version: ${MOJAP_IMAGE_VERSION}) ==="

if [ "${IS_SUNDAY_DEPLOY}" = "true" ]; then
  echo "=== Checking if today is the first Sunday of the month ==="
  ./date-checker.sh
fi

echo "=== Running clone-create-a-derived-table.sh ==="
./clone-create-a-derived-table.sh

echo "=== Running create-a-derived-table.sh ==="
./create-a-derived-table.sh
