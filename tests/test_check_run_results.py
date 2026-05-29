"""Unit tests for check_run_results.py."""

from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "check_run_results.py"
SPEC = importlib.util.spec_from_file_location("check_run_results", MODULE_PATH)
assert SPEC is not None
check_run_results = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(check_run_results)

AIRFLOW_DAG_TRIGGER_YAML = """
version: 2
default_channel: C01H895893K
dags:
  - name: "example_team.daily_dataset_check"
    models: [
      "model.example_project.daily_model_one",
      "model.example_project.daily_model_two"
    ]

  - name: "reporting_team.weekly_dataset_check"
    models: [
      "model.example_project.weekly_model_one",
      "model.example_project.weekly_model_two"
    ]
"""


class ParseUniqueIdsTest(unittest.TestCase):
    """Tests for unique-id input parsing."""

    def test_parses_comma_separated_values(self) -> None:
        """Comma-separated values with optional quotes are split and stripped."""
        self.assertEqual(
            check_run_results.resolve_unique_ids(
                cli_unique_ids=["model.project.first, 'test.project.second'"],
                yaml_path=None,
                dataset_target=None,
                default_yaml_path=Path("missing.yaml"),
            ),
            ["model.project.first", "test.project.second"],
        )

    def test_parses_structured_unique_ids_from_env_style_value(self) -> None:
        """JSON or Python-literal dict values have their leaf strings extracted."""
        self.assertEqual(
            check_run_results.resolve_unique_ids(
                cli_unique_ids=[
                    "{'daily': ['model.project.first', 'test.project.second']}"
                ],
                yaml_path=None,
                dataset_target=None,
                default_yaml_path=Path("missing.yaml"),
            ),
            ["model.project.first", "test.project.second"],
        )


class ResolveUniqueIdsTest(unittest.TestCase):
    """Tests for selecting model unique ids from runtime inputs."""

    def test_reads_models_for_dataset_target_from_generated_project_yaml(self) -> None:
        """A DAG target in scripts/data/airflow-dag-trigger.yaml selects its models."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yaml_path = (
                Path(tmp_dir)
                / "create-a-derived-table"
                / "scripts"
                / "data"
                / "airflow-dag-trigger.yaml"
            )
            yaml_path.parent.mkdir(parents=True)
            yaml_path.write_text(AIRFLOW_DAG_TRIGGER_YAML, encoding="utf-8")

            self.assertEqual(
                check_run_results.resolve_unique_ids(
                    cli_unique_ids=None,
                    yaml_path=yaml_path,
                    dataset_target="reporting_team.weekly_dataset_check",
                    default_yaml_path=Path(tmp_dir) / "missing.yaml",
                ),
                [
                    "model.example_project.weekly_model_one",
                    "model.example_project.weekly_model_two",
                ],
            )

    def test_requires_dataset_target_when_yaml_is_used(self) -> None:
        """YAML lookup fails clearly if DATASET_TARGET is unavailable."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yaml_path = Path(tmp_dir) / "airflow-dag-trigger.yaml"
            yaml_path.write_text(AIRFLOW_DAG_TRIGGER_YAML, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "DATASET_TARGET"):
                check_run_results.resolve_unique_ids(
                    cli_unique_ids=None,
                    yaml_path=yaml_path,
                    dataset_target=None,
                    default_yaml_path=Path(tmp_dir) / "missing.yaml",
                )


class StatusAssertionTest(unittest.TestCase):
    """Tests for interpreting dbt run result statuses."""

    def test_assert_success_accepts_successful_final_statuses(self) -> None:
        """A node that errors then succeeds later is treated as passing."""
        run_results = [
            {"results": [{"unique_id": "model.project.a", "status": "error"}]},
            {"results": [{"unique_id": "model.project.a", "status": "success"}]},
        ]

        with patch.object(
            check_run_results,
            "_download_run_results_from_s3",
            return_value=run_results,
        ):
            check_run_results.assert_success(
                ["model.project.a"],
                deploy_env="prod",
                workflow_name="workflow",
            )

    def test_assert_all_models_tests_success_accepts_warned_tests(self) -> None:
        """Warned tests and errored seeds do not cause all-node checks to fail."""
        run_results = [
            {
                "results": [
                    {"unique_id": "model.project.a", "status": "success"},
                    {"unique_id": "test.project.a", "status": "warn"},
                    {"unique_id": "seed.project.a", "status": "error"},
                ]
            }
        ]

        with patch.object(
            check_run_results,
            "_download_run_results_from_s3",
            return_value=run_results,
        ):
            check_run_results.assert_all_models_tests_success(
                deploy_env="prod",
                workflow_name="workflow",
            )


if __name__ == "__main__":
    unittest.main()
