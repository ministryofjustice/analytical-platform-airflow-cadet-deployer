#!/usr/bin/env bash

set -euo pipefail

export REPOSITORY="${REPOSITORY:-"git@github.com:moj-analytical-services/create-a-derived-table.git"}"
export BRANCH="${BRANCH:-"main"}"
export GITHUB_KEY="${SECRET_GITHUB_KEY}"
export GITHUB_KEY_PATH="${GITHUB_KEY_PATH:-"/tmp/github-key"}"
export REPOSITORY_PATH="${REPOSITORY_PATH:-"${ANALYTICAL_PLATFORM_DIRECTORY}/create-a-derived-table"}"

echo "Writing GitHub deploy key to [ ${GITHUB_KEY_PATH} ]"
mkdir --parents "$(dirname "${GITHUB_KEY_PATH}")"

echo "${GITHUB_KEY}" | base64 --decode >"${GITHUB_KEY_PATH}"

chmod 0600 "${GITHUB_KEY_PATH}"

echo "Cloning repository [ ${REPOSITORY} ] on branch [ ${BRANCH} ]"
GIT_SSH_COMMAND="ssh -i ${GITHUB_KEY_PATH} -o UserKnownHostsFile=ssh-known-hosts" git clone --branch "${BRANCH}" "${REPOSITORY}" "${REPOSITORY_PATH}"

cd "${REPOSITORY_PATH}"

currentCommit=$(git rev-parse HEAD)
export currentCommit

commitActor=$(git log -1 --pretty=format:'%an <%ae>')

echo "HEAD commit SHA [ ${currentCommit} ] by actor [ ${commitActor} ]"
