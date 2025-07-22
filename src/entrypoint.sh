#!/usr/bin/env bash

set -euo pipefail

echo "=== Running Airflow CaDeT Deployer (version: ${MOJAP_IMAGE_VERSION}) ==="

echo "=== Running clone-create-a-derived-table.sh ==="
./clone-create-a-derived-table.sh

echo "=== Running create-a-derived-table.sh ==="
./create-a-derived-table.sh
