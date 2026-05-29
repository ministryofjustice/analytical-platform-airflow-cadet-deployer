#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

export REPOSITORY="${REPOSITORY:-"git@github.com:moj-analytical-services/create-a-derived-table.git"}"
export BRANCH="${BRANCH:-"main"}"
export GITHUB_KEY="${SECRET_GITHUB_KEY}"
export GITHUB_KEY_PATH="${GITHUB_KEY_PATH:-"/tmp/github-key"}"
export REPOSITORY_PATH="${REPOSITORY_PATH:-"${ANALYTICAL_PLATFORM_DIRECTORY}/create-a-derived-table"}"

function write_github_key() {
  echo "Writing GitHub deploy key to [ ${GITHUB_KEY_PATH} ]"
  mkdir --parents "$(dirname "${GITHUB_KEY_PATH}")"

  echo "${GITHUB_KEY}" | base64 --decode >"${GITHUB_KEY_PATH}"

  chmod 0600 "${GITHUB_KEY_PATH}"
}

function clone_repository() {
  echo "Cloning repository [ ${REPOSITORY} ] on branch [ ${BRANCH} ]"
  GIT_SSH_COMMAND="ssh -i ${GITHUB_KEY_PATH} -o UserKnownHostsFile=${SCRIPT_DIR}/ssh-known-hosts" \
    git clone --branch "${BRANCH}" "${REPOSITORY}" "${REPOSITORY_PATH}"
}

function log_cloned_commit() {
  local current_commit commit_actor

  cd "${REPOSITORY_PATH}"

  current_commit="$(git rev-parse HEAD)"
  export currentCommit="${current_commit}"

  commit_actor="$(git log -1 --pretty=format:'%an <%ae>')"

  echo "HEAD commit SHA [ ${current_commit} ] by actor [ ${commit_actor} ]"
}

function main() {
  write_github_key
  clone_repository
  log_cloned_commit
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
