#!/usr/bin/env bash

set -euo pipefail

# Get day of week (0=Sunday) and unpadded day of month in a single call
read -r weekday day < <(date '+%w %-d')

if [ "$weekday" -eq 0 ] && [ "$day" -le 7 ]; then
  echo "Today is the first Sunday of the month, monthly deploys will run and weeklies will error."
  if [[ ! "$DBT_SELECT_CRITERIA" == *monthly* ]]; then
    echo "DBT_SELECT_CRITERIA does NOT contain 'tag:monthly'"
    exit 1
  fi
else
  echo "Today is NOT the first Sunday of the month, monthly deploys will error and weeklies will continue."
  if [[ "$DBT_SELECT_CRITERIA" == *monthly* ]]; then
    echo "DBT_SELECT_CRITERIA contains 'tag:monthly', exiting with error."
    exit 1
  fi
fi
