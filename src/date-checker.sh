#!/usr/bin/env bash

set -euo pipefail

function is_first_sunday_of_month() {
  local weekday day
  weekday="$(date +%w)"
  day="$(date +%d)"

  [[ "${weekday}" -eq 0 && $((10#${day})) -le 7 ]]
}

function selects_monthly_models() {
  [[ "${DBT_SELECT_CRITERIA}" == *monthly* ]]
}

function validate_sunday_deploy_mode() {
  : "${DBT_SELECT_CRITERIA:?DBT_SELECT_CRITERIA must be set}"

  if is_first_sunday_of_month; then
    echo "Today is the first Sunday of the month, monthly deploys will run and weeklies will error."
    if ! selects_monthly_models; then
      echo "DBT_SELECT_CRITERIA does NOT contain 'tag:monthly'"
      return 1
    fi
  else
    echo "Today is NOT the first Sunday of the month, monthly deploys will error and weeklies will continue."
    if selects_monthly_models; then
      echo "DBT_SELECT_CRITERIA contains 'tag:monthly', exiting with error."
      return 1
    fi
  fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  validate_sunday_deploy_mode "$@"
fi
