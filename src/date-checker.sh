#!/usr/bin/env bash

# Get day of week (0=Sunday), day of month, and month/year
weekday=$(date +%w)
day=$(date +%d)

if [ "$weekday" -eq 0 ] && [ "$day" -le 7 ]; then
  echo "Today is the first Sunday of the month, monthly deploys will run."
else
  echo "Today is NOT the first Sunday of the month, monthly deploys will be skipped."
  if [[ "$DBT_SELECT_CRITERIA" == *tag:monthly* ]]; then
    echo "DBT_SELECT_CRITERIA contains 'tag:monthly', exiting with error."
    exit 1
  fi
fi
