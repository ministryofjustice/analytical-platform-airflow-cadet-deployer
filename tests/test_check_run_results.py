from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "check_run_results.py"
SPEC = importlib.util.spec_from_file_location("check_run_results", MODULE_PATH)
check_run_results = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(check_run_results)


class ParseUniqueIdsTest(unittest.TestCase):
    def test_parses_comma_separated_values(self) -> None:
        self.assertEqual(
            check_run_results._parse_unique_ids(
                ["model.project.first, 'test.project.second'"]
            ),
            ["model.project.first", "test.project.second"],
        )

    def test_parses_json_dict_values(self) -> None:
        self.assertEqual(
            check_run_results._parse_unique_ids(
                ['{"dataset": ["model.project.first", "test.project.second"]}']
            ),
            ["model.project.first", "test.project.second"],
        )

    def test_parses_python_literal_dict_values(self) -> None:
        self.assertEqual(
            check_run_results._parse_unique_ids(
                ["{'dataset': ['model.project.first', 'test.project.second']}"]
            ),
            ["model.project.first", "test.project.second"],
        )


class ParseUniqueIdsYamlTest(unittest.TestCase):
    def test_reads_models_for_requested_dataset_target(self) -> None:
        yaml_content = """
datasets:
  - name: daily_dataset
    models:
      - "model.project.daily"
      - 'test.project.daily'
  - name: monthly_dataset
    models: ["model.project.monthly"]
dags:
  - name: ignored_dag
    models:
      - "model.project.ignored"
"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "airflow-dag-trigger.yaml"
            path.write_text(yaml_content, encoding="utf-8")

            self.assertEqual(
                check_run_results._parse_unique_ids_yaml(path, "daily_dataset"),
                ["model.project.daily", "test.project.daily"],
            )


class ResolveUniqueIdsTest(unittest.TestCase):
    def test_uses_default_yaml_when_no_cli_ids_are_supplied(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            default_yaml = Path(tmp_dir) / "airflow-dag-trigger.yaml"
            default_yaml.write_text(
                """
datasets:
  - name: daily_dataset
    models:
      - "model.project.daily"
""",
                encoding="utf-8",
            )

            self.assertEqual(
                check_run_results._resolve_unique_ids(
                    cli_unique_ids=None,
                    yaml_path=None,
                    dataset_target="daily_dataset",
                    default_yaml_path=default_yaml,
                ),
                ["model.project.daily"],
            )

    def test_requires_dataset_target_when_yaml_is_used(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            yaml_path = Path(tmp_dir) / "airflow-dag-trigger.yaml"
            yaml_path.write_text("datasets: []", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "DATASET_TARGET"):
                check_run_results._resolve_unique_ids(
                    cli_unique_ids=None,
                    yaml_path=yaml_path,
                    dataset_target=None,
                    default_yaml_path=Path(tmp_dir) / "missing.yaml",
                )


class StatusAssertionTest(unittest.TestCase):
    def test_assert_success_accepts_successful_final_statuses(self) -> None:
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
