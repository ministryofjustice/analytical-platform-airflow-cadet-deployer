"""Tests for check_run_results.py wildcard functionality"""

import json
from pathlib import Path

import check_run_results # pylint: disable=wrong-import-order
import pytest

TEST_DIR = Path(__file__).parent


@pytest.fixture(name="run_results_json")
def run_results_json_fixture():
    """Load the run-results JSON fixture."""
    fixture_path = TEST_DIR / "test_check_run_results.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "dataset_target,patterns",
    [
        (
            "prison.curated_prisons_history_model_unique_id_with_wildcard",
            [
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__personalrelationships_contact_address",
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__sentenceplanassessmentview_goal_wap",
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__"
                "activities_activity_minimum_education_level_wap",
            ],
        ),
        (
            "prison.curated_prisons_history_test_unique_id_with_wildcard",
            [
                "test.hmpps_digital_prisons_reporting_tables."
                "not_null_curated_prisons_history__"
                "activities_activity_minimum_education_level_wap___"
                "surrogate_key_hash.4bef28b58c",
                "test.hmpps_digital_prisons_reporting_tables."
                "not_null_curated_prisons_history__"
                "activities_activity_minimum_education_level_wap___"
                "source_primary_key_hash.d0aab38dab",
            ],
        ),
        (
            "prison.curated_prisons_history_model_exact_unique_id",
            [
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__"
                "activities_activity_minimum_education_level_wap"
            ],
        ),
        (
            "prison.curated_prisons_history_test_exact_unique_id",
            [
                "test.hmpps_digital_prisons_reporting_tables."
                "not_null_curated_prisons_history__"
                "activities_activity_minimum_education_level_wap___"
                "surrogate_key_hash.4bef28b58c"
            ],
        ),
        (
            "prison.curated_prisons_history_mix",
            [
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__personalrelationships_contact_address",
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__sentenceplanassessmentview_goal_wap",
                "model.hmpps_digital_prisons_reporting_tables."
                "curated_prisons_history__activities_activity_minimum_education_level_wap",
                "test.hmpps_digital_prisons_reporting_tables."
                "not_null_curated_prisons_history__"
                "activities_activity_minimum_education_level_wap___"
                "surrogate_key_hash.4bef28b58c",
                "test.hmpps_digital_prisons_reporting_tables."
                "not_null_curated_prisons_history__"
                "activities_activity_minimum_education_level_wap___"
                "source_primary_key_hash.d0aab38dab",
            ],
        ),
        ("prison.curated_prisons_history_model_exact_unique_id_fail", []),
    ],
)
def test_assert_success(dataset_target, patterns, run_results_json, monkeypatch):
    """Check that matching result IDs are returned for each target pattern."""

    def fake_download_run_results(*_args, **_kwargs):
        return [run_results_json]

    monkeypatch.setattr(
        check_run_results,
        "_download_run_results_from_s3",
        fake_download_run_results,
    )
    # pylint: disable=protected-access
    unique_ids = check_run_results._parse_unique_ids_yaml(
        TEST_DIR / "test_check_run_results.yml",
        dataset_target,
    )
    # pylint: enable=protected-access

    try:
        matched_ids = check_run_results.assert_success(
            unique_ids,
            bucket="test-bucket",
            deploy_env="dev",
            workflow_name="test-workflow",
        )
    except RuntimeError:
        matched_ids = set()

    assert matched_ids == set(sorted(patterns))
