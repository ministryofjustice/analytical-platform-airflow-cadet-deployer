#!/usr/bin/env python3
"""Validate dbt run_results.json statuses by unique_id."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Iterable

import boto3

DEFAULT_UNIQUE_ID_YAML = Path(
    "./create-a-derived-table/scripts/data/airflow-dag-trigger.yaml"
)


def _normalize_unique_id(value: str) -> str:
    value = value.strip().rstrip(",")
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1].strip()
    return value


def _parse_unique_ids(values: Iterable[str]) -> list[str]:
    unique_ids: list[str] = []
    for value in values:
        for item in value.split(","):
            item = _normalize_unique_id(item)
            if item:
                unique_ids.append(item)
    return unique_ids


def _apply_env_to_model_id(unique_id: str, deploy_env: str | None) -> str:
    if not deploy_env or deploy_env == "prod":
        return unique_id

    match = re.match(r"^model\.([^.]+)\.([^.]+)$", unique_id)
    if not match:
        return unique_id

    database_name, table_name = match.groups()
    env_suffix = f"_{deploy_env}_dbt"
    if deploy_env == "prod":
        return unique_id
    if "__" not in table_name:
        return unique_id

    base_name, rest = table_name.split("__", 1)
    return f"model.{database_name}.{base_name}{env_suffix}__{rest}"


def _parse_unique_ids_yaml(path: Path, dataset_target: str) -> list[str]:
    content = path.read_text(encoding="utf-8")
    unique_ids: list[str] = []
    in_target_block = False
    base_indent: int | None = None
    models_indent: int | None = None
    in_dags_section = False
    dags_indent: int | None = None

    def _extract_quoted(line: str) -> list[str]:
        matches = re.findall(r"\"([^\"]+)\"|'([^']+)'", line)
        extracted: list[str] = []
        for first, second in matches:
            extracted.append(first or second)
        return extracted

    for line in content.splitlines():
        dags_match = re.match(r"^(\s*)dags\s*:\s*$", line)
        if dags_match:
            in_dags_section = True
            dags_indent = len(dags_match.group(1))
            continue

        name_match = re.match(r"^(\s*)-\s*name:\s*(.+)$", line)
        if name_match:
            indent = len(name_match.group(1))
            raw_name = _normalize_unique_id(name_match.group(2))
            if in_dags_section and dags_indent is not None and indent <= dags_indent:
                in_dags_section = False
                dags_indent = None
            if in_target_block and base_indent is not None and indent <= base_indent:
                break
            if in_dags_section and dags_indent is not None and indent <= dags_indent:
                continue
            in_target_block = raw_name == dataset_target
            base_indent = indent
            models_indent = None
            continue

        if not in_target_block:
            continue

        if re.search(r"\bmodels\s*:", line):
            models_indent = len(line) - len(line.lstrip())
            unique_ids.extend(_extract_quoted(line))
            continue

        if models_indent is not None:
            indent = len(line) - len(line.lstrip())
            if indent <= models_indent and re.search(r"\S", line):
                models_indent = None
                continue
            unique_ids.extend(_extract_quoted(line))

    return unique_ids


def _load_run_results(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _download_run_results_from_s3(
    deploy_env: str,
    workflow_name: str,
    bucket: str = "mojap-derived-tables",
) -> list[dict]:
    """Download run_results files from S3 and return parsed JSON objects."""
    prefix = f"{deploy_env}/run_artefacts/{workflow_name}/latest/target/"
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")
            if not key:
                continue
            if re.search(r"run_results_\d+\.json$", key):
                keys.append(key)
            elif key.endswith("run_results.json"):
                keys.append(key)

    if not keys:
        raise FileNotFoundError(
            "No run_results_{n}.json files found in S3 target prefix."
        )

    run_results: list[dict] = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        for key in sorted(keys):
            dest = Path(tmp_dir) / Path(key).name
            logging.info("Downloading s3://%s/%s", bucket, key)
            client.download_file(bucket, key, str(dest))
            run_results.append(_load_run_results(dest))

    return run_results


def _index_statuses(run_results: dict) -> dict[str, str]:
    statuses: dict[str, str] = {}
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id")
        status = result.get("status")
        if unique_id is not None and status is not None:
            statuses[str(unique_id)] = str(status)
    return statuses


def assert_success(
    unique_ids: Iterable[str],
    deploy_env: str | None = None,
    workflow_name: str | None = None,
) -> None:
    """Assert that all unique_ids have a final status of 'success'."""
    unique_ids = list(unique_ids)
    success_map = {unique_id: False for unique_id in unique_ids}
    last_status: dict[str, str] = {}

    if not deploy_env or not workflow_name:
        raise ValueError(
            "DEPLOY_ENV and WORKFLOW_NAME are required to locate run_results files."
        )
    run_results_list = _download_run_results_from_s3(deploy_env, workflow_name)

    for run_results in run_results_list:
        statuses = _index_statuses(run_results)
        for unique_id in unique_ids:
            status = statuses.get(unique_id)
            if status is None:
                continue
            last_status[unique_id] = status
            if status == "success":
                success_map[unique_id] = True

    missing: list[str] = []
    failed: list[tuple[str, str]] = []

    for unique_id in unique_ids:
        status = last_status.get(unique_id)
        if status is None:
            logging.error("%s -> not found", unique_id)
            missing.append(unique_id)
            continue

        logging.info("%s -> %s", unique_id, status)
        if not success_map[unique_id]:
            failed.append((unique_id, status))

    if missing or failed:
        parts: list[str] = []
        if missing:
            parts.append(f"Missing unique_id(s): {', '.join(missing)}")
        if failed:
            failed_msg = ", ".join(f"{uid}={status}" for uid, status in failed)
            parts.append(f"Non-success status(es): {failed_msg}")
        raise RuntimeError("; ".join(parts))

    print(f"All {len(unique_ids)} unique_id(s) finished with status=success.")


def main() -> int:
    """Parse inputs, load run_results data, and validate unique_id statuses."""
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.info("Starting check_run_results")
    parser = argparse.ArgumentParser(
        description=(
            "Check dbt run_results.json for specific unique_id statuses. "
            "Exits non-zero if any are missing or not success."
        )
    )
    parser.add_argument(
        "--unique-id",
        dest="unique_ids",
        nargs="+",
        help=(
            "Unique ID(s) to check. You can pass multiple values or a comma-separated list."
        ),
    )
    parser.add_argument(
        "--unique-id-yaml",
        type=Path,
        help=(
            "Path to a YAML file containing models for a dataset. Uses "
            "DATASET_TARGET to select the name. Defaults to "
            "yaml cloned from repo if present. Mostly for local testing."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()
    logging.info("Parsed CLI arguments")

    logging.getLogger().setLevel(args.log_level.upper())
    logging.info("Configured log level: %s", args.log_level.upper())

    unique_ids: list[str] = []
    if args.unique_ids:
        unique_ids.extend(_parse_unique_ids(args.unique_ids))
    yaml_path = args.unique_id_yaml
    if yaml_path is None and DEFAULT_UNIQUE_ID_YAML.exists():
        yaml_path = DEFAULT_UNIQUE_ID_YAML
        logging.info("Using default unique_id YAML path: %s", yaml_path)
    if not DEFAULT_UNIQUE_ID_YAML.exists():
        logging.info("YAML not found at default path: %s", DEFAULT_UNIQUE_ID_YAML)
    if yaml_path:
        dataset_target = os.environ.get("DATASET_TARGET")
        if not dataset_target:
            raise ValueError("DATASET_TARGET is required when using --unique-id-yaml.")
        unique_ids.extend(_parse_unique_ids_yaml(yaml_path, dataset_target))
    if not unique_ids:
        raise ValueError(
            "At least one unique_id is required via --unique-id or --unique-id-yaml."
        )
    logging.info("Resolved %d unique_id(s)", len(unique_ids))

    deploy_env = os.environ.get("DEPLOY_ENV")
    workflow_name = os.environ.get("WORKFLOW_NAME")
    logging.info(
        "Loaded env DEPLOY_ENV=%s WORKFLOW_NAME=%s",
        deploy_env,
        workflow_name,
    )

    if deploy_env and deploy_env != "prod":
        unique_ids = [_apply_env_to_model_id(uid, deploy_env) for uid in unique_ids]
        logging.info(
            "Adjusted %d unique_id(s) for deploy_env=%s",
            len(unique_ids),
            deploy_env,
        )

    logging.info("Beginning run_results validation")
    assert_success(
        unique_ids,
        deploy_env=deploy_env,
        workflow_name=workflow_name,
    )
    logging.info("Validation completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
