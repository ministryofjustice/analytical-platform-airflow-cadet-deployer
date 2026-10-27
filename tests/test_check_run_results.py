import json
import sys
from pathlib import Path

import pytest

sys.path.append("../src/")
import check_run_results

TEST_DIR = Path(__file__).parent


@pytest.fixture
def run_results_json():
    fixture_path = TEST_DIR / "test_check_run_results.json"
    return json.loads(fixture_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "dataset_target,unique_id",
    [   
        (
            "prison.curated_prisons_history_model_unique_id_with_wildcard",
            [
                "model.hmpps_digital_prisons_reporting_tables.curated_prisons_history__personalrelationships_contact_address",
                "model.hmpps_digital_prisons_reporting_tables.curated_prisons_history__sentenceplanassessmentview_goal_wap",
                "model.hmpps_digital_prisons_reporting_tables.curated_prisons_history__activities_activity_minimum_education_level_wap"
            ]
        ),
        (
            "prison.curated_prisons_history_test_unique_id_with_wildcard",
            [
                "test.hmpps_digital_prisons_reporting_tables.not_null_curated_prisons_history__activities_activity_minimum_education_level_wap___surrogate_key_hash.4bef28b58c",
                "test.hmpps_digital_prisons_reporting_tables.not_null_curated_prisons_history__activities_activity_minimum_education_level_wap___source_primary_key_hash.d0aab38dab"
            ]
        ),
        (
            "prison.curated_prisons_history_model_exact_unique_id",
            [
                "model.hmpps_digital_prisons_reporting_tables.curated_prisons_history__activities_activity_minimum_education_level_wap"
            ]
        ),
        (
            "prison.curated_prisons_history_test_exact_unique_id",
            [
                "test.hmpps_digital_prisons_reporting_tables.not_null_curated_prisons_history__activities_activity_minimum_education_level_wap___surrogate_key_hash.4bef28b58c"
            ]
        )
    ],
)
def test_assert_success_wildcard(dataset_target, unique_id, run_results_json, monkeypatch):
    def fake_download_run_results(deploy_env, workflow_name, bucket):
        return [run_results_json]

    monkeypatch.setattr(
        check_run_results,
        "_download_run_results_from_s3",
        fake_download_run_results,
    )

    unique_ids = check_run_results._parse_unique_ids_yaml(
        TEST_DIR / "test_check_run_results.yml",
        dataset_target,
    )

    matched_ids = check_run_results.assert_success_wildcard(
        unique_ids,
        bucket="test-bucket",
        deploy_env="dev",
        workflow_name="test-workflow",
    )

    assert matched_ids == sorted(set(unique_id))
