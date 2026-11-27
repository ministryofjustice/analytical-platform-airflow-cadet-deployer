#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export REPOSITORY="${REPOSITORY:-"git@github.com:moj-analytical-services/create-a-derived-table.git"}"
export BRANCH="${BRANCH:-"main"}"
export GITHUB_KEY="${SECRET_GITHUB_KEY}"
export GITHUB_KEY_PATH="${GITHUB_KEY_PATH:-"/tmp/github-key"}"
export REPOSITORY_PATH="${REPOSITORY_PATH:-"${ANALYTICAL_PLATFORM_DIRECTORY}/create-a-derived-table"}"

echo "Writing GitHub deploy key to [ ${GITHUB_KEY_PATH} ]"
mkdir -p "$(dirname "${GITHUB_KEY_PATH}")"

(
  umask 077
  echo "${GITHUB_KEY}" | base64 --decode >"${GITHUB_KEY_PATH}"
)
trap 'rm -f "${GITHUB_KEY_PATH}"' EXIT

echo "Cloning repository [ ${REPOSITORY} ] on branch [ ${BRANCH} ]"
GIT_SSH_COMMAND="ssh -i ${GITHUB_KEY_PATH} -o UserKnownHostsFile=${SCRIPT_DIR}/ssh-known-hosts" \
  git clone --depth 1 --single-branch --branch "${BRANCH}" "${REPOSITORY}" "${REPOSITORY_PATH}"

cd "${REPOSITORY_PATH}"

currentCommit=$(git rev-parse HEAD)

commitActor=$(git log -1 --pretty=format:'%an <%ae>')

echo "HEAD commit SHA [ ${currentCommit} ] by actor [ ${commitActor} ]"
