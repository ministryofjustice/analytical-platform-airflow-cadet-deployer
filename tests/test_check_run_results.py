"""Unit tests for check_run_results.py."""

from __future__ import annotations

import json
import sys
import textwrap
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make sure the src directory is importable without an install step
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import check_run_results as crr  # noqa: E402


# ===========================================================================
# _normalize_unique_id
# ===========================================================================
class TestNormalizeUniqueId:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ("foo", "foo"),
            ("  foo  ", "foo"),
            ("foo,", "foo"),
            ("  foo,  ", "foo"),
            ('"foo"', "foo"),
            ("'foo'", "foo"),
            ('  "foo"  ', "foo"),
            ("  'foo'  ", "foo"),
            ("", ""),
            (",", ""),
        ],
    )
    def test_normalize(self, value, expected):
        assert crr._normalize_unique_id(value) == expected

    def test_inner_quotes_preserved(self):
        # Only outermost quotes are stripped
        assert crr._normalize_unique_id('"foo \\"bar\\""') == 'foo \\"bar\\"'


# ===========================================================================
# _parse_unique_ids
# ===========================================================================
class TestParseUniqueIds:
    def test_single_value(self):
        assert crr._parse_unique_ids(["model.foo"]) == ["model.foo"]

    def test_comma_separated_single_arg(self):
        assert crr._parse_unique_ids(["model.a,model.b"]) == ["model.a", "model.b"]

    def test_multiple_args(self):
        assert crr._parse_unique_ids(["model.a", "model.b"]) == ["model.a", "model.b"]

    def test_quoted_items(self):
        assert crr._parse_unique_ids(['"model.a","model.b"']) == ["model.a", "model.b"]

    def test_empty_items_filtered(self):
        assert crr._parse_unique_ids(["model.a,,model.b"]) == ["model.a", "model.b"]

    def test_empty_input(self):
        assert crr._parse_unique_ids([]) == []

    def test_whitespace_only_filtered(self):
        assert crr._parse_unique_ids(["  ,  "]) == []


# ===========================================================================
# _parse_unique_ids_yaml
# ===========================================================================
YAML_SAMPLE = textwrap.dedent(
    """\
    dags:
      - name: other_dag
        models: "model.other"
    datasets:
      - name: my_dataset
        models: "model.alpha", "model.beta"
      - name: another_dataset
        models:
          - "model.gamma"
          - "model.delta"
    """
)

YAML_WITH_DAGS = textwrap.dedent(
    """\
    dags:
      - name: dag_one
        models: "model.should_not_appear"
    datasets:
      - name: target_ds
        models: "model.correct"
    """
)


class TestParseUniqueIdsYaml:
    def _write(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "config.yaml"
        p.write_text(content, encoding="utf-8")
        return p

    def test_target_found_inline(self, tmp_path):
        p = self._write(tmp_path, YAML_SAMPLE)
        ids = crr._parse_unique_ids_yaml(p, "my_dataset")
        assert "model.alpha" in ids
        assert "model.beta" in ids

    def test_target_found_multiline(self, tmp_path):
        p = self._write(tmp_path, YAML_SAMPLE)
        ids = crr._parse_unique_ids_yaml(p, "another_dataset")
        assert "model.gamma" in ids
        assert "model.delta" in ids

    def test_target_not_found(self, tmp_path):
        p = self._write(tmp_path, YAML_SAMPLE)
        ids = crr._parse_unique_ids_yaml(p, "nonexistent")
        assert ids == []

    def test_other_target_not_included(self, tmp_path):
        p = self._write(tmp_path, YAML_SAMPLE)
        ids = crr._parse_unique_ids_yaml(p, "my_dataset")
        assert "model.gamma" not in ids
        assert "model.delta" not in ids

    def test_dags_section_isolated(self, tmp_path):
        p = self._write(tmp_path, YAML_WITH_DAGS)
        ids = crr._parse_unique_ids_yaml(p, "target_ds")
        assert "model.should_not_appear" not in ids
        assert "model.correct" in ids


# ===========================================================================
# Schema-compliant fixture helpers (dbt run_results v6)
# ===========================================================================
def _make_timing_entry(
    name: str = "execute",
    started_at: str | None = "2024-01-01T09:59:00Z",
    completed_at: str | None = "2024-01-01T10:00:00Z",
) -> dict:
    return {"name": name, "started_at": started_at, "completed_at": completed_at}


def _make_default_timing() -> list[dict]:
    return [
        _make_timing_entry(name="compile", started_at="2024-01-01T09:59:00Z", completed_at="2024-01-01T09:59:01Z"),
        _make_timing_entry(name="execute", started_at="2024-01-01T09:59:01Z", completed_at="2024-01-01T10:00:00Z"),
    ]


def _make_result(
    unique_id: str = "model.foo",
    status: str = "success",
    timing: list[dict] | None = None,
    thread_id: str = "Thread-1",
    execution_time: float = 1.23,
    adapter_response: dict | None = None,
    message: str | None = "OK",
    failures: int | None = None,
    compiled: bool | None = True,
    compiled_code: str | None = "SELECT 1",
    relation_name: str | None = "schema.table",
) -> dict:
    return {
        "status": status,
        "timing": timing if timing is not None else _make_default_timing(),
        "thread_id": thread_id,
        "execution_time": execution_time,
        "adapter_response": adapter_response or {"_message": "OK"},
        "message": message,
        "failures": failures,
        "unique_id": unique_id,
        "compiled": compiled,
        "compiled_code": compiled_code,
        "relation_name": relation_name,
    }


def _make_run_results(results: list[dict]) -> dict:
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.10.0",
            "generated_at": "2024-01-01T10:00:00Z",
            "invocation_id": "test-invocation-id",
        },
        "results": results,
        "elapsed_time": 42.0,
    }


def _result_with_timing(*completed_ats: str) -> dict:
    timing: list[dict] = []
    for t in completed_ats:
        timing.append(_make_timing_entry(name="compile", completed_at=t))
        timing.append(_make_timing_entry(name="execute", completed_at=t))
    return _make_result(timing=timing)


class TestLoadRunResults:
    def test_returns_data_and_max_completed(self, tmp_path):
        data = _make_run_results(
            [_result_with_timing("2024-01-01T10:00:00Z", "2024-01-01T12:00:00Z")]
        )
        p = tmp_path / "run_results.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        loaded, max_ts = crr._load_run_results(p)
        assert loaded == data
        assert max_ts == "2024-01-01T12:00:00Z"

    def test_no_timing_entries_returns_empty_string(self, tmp_path):
        data = _make_run_results([_make_result(timing=[])])
        p = tmp_path / "run_results.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        _, max_ts = crr._load_run_results(p)
        assert max_ts == ""

    def test_multiple_results_returns_overall_max(self, tmp_path):
        data = _make_run_results(
            [
                _result_with_timing("2024-01-01T08:00:00Z"),
                _result_with_timing("2024-01-01T11:00:00Z"),
                _result_with_timing("2024-01-01T09:00:00Z"),
            ]
        )
        p = tmp_path / "run_results.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        _, max_ts = crr._load_run_results(p)
        assert max_ts == "2024-01-01T11:00:00Z"


# ===========================================================================
# _index_statuses
# ===========================================================================
class TestIndexStatuses:
    def test_normal_mapping(self):
        run_results = _make_run_results([
            _make_result(unique_id="model.foo", status="success"),
            _make_result(unique_id="test.bar", status="pass"),
        ])
        assert crr._index_statuses(run_results) == {
            "model.foo": "success",
            "test.bar": "pass",
        }

    def test_missing_unique_id_skipped(self):
        result = _make_result()
        del result["unique_id"]
        run_results = _make_run_results([result])
        assert crr._index_statuses(run_results) == {}

    def test_missing_status_skipped(self):
        result = _make_result()
        del result["status"]
        run_results = _make_run_results([result])
        assert crr._index_statuses(run_results) == {}

    def test_empty_results(self):
        assert crr._index_statuses(_make_run_results([])) == {}

    def test_missing_results_key(self):
        assert crr._index_statuses({}) == {}


# ===========================================================================
# _download_run_results_from_s3  (boto3 fully mocked)
# ===========================================================================
def _make_s3_mock(keys: list[str], result_data: list[dict] | None = None):
    """Build a mock boto3 S3 client that returns *keys* from the paginator
    and writes *result_data[i]* to disk when download_file is called.

    The paginator filters keys by the Prefix kwarg, simulating a real
    S3 bucket that may contain objects for multiple environments."""
    if result_data is None:
        result_data = [_make_run_results([_result_with_timing("2024-01-01T10:00:00Z")])] * len(keys)

    data_by_key = dict(zip(keys, result_data))

    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator

    def fake_paginate(**kwargs):
        prefix = kwargs.get("Prefix", "")
        matched = [k for k in keys if k.startswith(prefix)]
        return [{"Contents": [{"Key": k} for k in matched]}]

    paginator.paginate.side_effect = fake_paginate

    def fake_download(bucket, key, dest):
        Path(dest).write_text(json.dumps(data_by_key[key]), encoding="utf-8")

    client.download_file.side_effect = fake_download
    return client


class TestDownloadRunResultsFromS3:
    def test_returns_list_for_matching_keys(self):
        keys = ["dev/run_artefacts/wf/latest/target/run_results_1.json"]
        mock_client = _make_s3_mock(keys)
        with patch("boto3.client", return_value=mock_client):
            results = crr._download_run_results_from_s3("dev", "wf")
        assert len(results) == 1

    def test_run_results_json_key_included(self):
        keys = ["dev/run_artefacts/wf/latest/target/run_results.json"]
        mock_client = _make_s3_mock(keys)
        with patch("boto3.client", return_value=mock_client):
            results = crr._download_run_results_from_s3("dev", "wf")
        assert len(results) == 1

    def test_no_keys_raises_file_not_found(self):
        client = MagicMock()
        paginator = MagicMock()
        client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"Contents": []}]
        with patch("boto3.client", return_value=client):
            with pytest.raises(FileNotFoundError):
                crr._download_run_results_from_s3("dev", "wf")

    def test_results_sorted_by_timestamp(self):
        keys = [
            "dev/run_artefacts/wf/latest/target/run_results_1.json",
            "dev/run_artefacts/wf/latest/target/run_results_2.json",
        ]
        data = [
            _make_run_results([_result_with_timing("2024-01-01T12:00:00Z")]),
            _make_run_results([_result_with_timing("2024-01-01T08:00:00Z")]),
        ]
        mock_client = _make_s3_mock(keys, data)
        with patch("boto3.client", return_value=mock_client):
            results = crr._download_run_results_from_s3("dev", "wf")
        # Earlier timestamp should come first
        assert results[0]["results"][0]["timing"][0]["completed_at"] == "2024-01-01T08:00:00Z"
        assert results[1]["results"][0]["timing"][0]["completed_at"] == "2024-01-01T12:00:00Z"

    def test_only_matching_env_keys_returned(self):
        """A single bucket holds files for multiple envs; only the requested env is fetched."""
        dev_result = _make_run_results([_make_result(unique_id="model.dev_only", status="success")])
        prod_result = _make_run_results([_make_result(unique_id="model.prod_only", status="error")])
        keys = [
            "dev/run_artefacts/wf/latest/target/run_results_1.json",
            "prod/run_artefacts/wf/latest/target/run_results_1.json",
        ]
        mock_client = _make_s3_mock(keys, [dev_result, prod_result])
        with patch("boto3.client", return_value=mock_client):
            results = crr._download_run_results_from_s3("dev", "wf")
        assert len(results) == 1
        assert results[0]["results"][0]["unique_id"] == "model.dev_only"

    def test_only_matching_workflow_keys_returned(self):
        """Multiple workflows exist under the same env; only the requested workflow is fetched."""
        wf_a = _make_run_results([_make_result(unique_id="model.wf_a")])
        wf_b = _make_run_results([_make_result(unique_id="model.wf_b")])
        keys = [
            "dev/run_artefacts/wf_a/latest/target/run_results_1.json",
            "dev/run_artefacts/wf_b/latest/target/run_results_1.json",
        ]
        mock_client = _make_s3_mock(keys, [wf_a, wf_b])
        with patch("boto3.client", return_value=mock_client):
            results = crr._download_run_results_from_s3("dev", "wf_a")
        assert len(results) == 1
        assert results[0]["results"][0]["unique_id"] == "model.wf_a"


# ===========================================================================
# assert_success
# ===========================================================================
def _single_run_results(statuses: dict[str, str]) -> list[dict]:
    return [
        _make_run_results([
            _make_result(unique_id=uid, status=st)
            for uid, st in statuses.items()
        ])
    ]


class TestAssertSuccess:
    def test_all_succeed(self, capsys):
        with patch.object(crr, "_download_run_results_from_s3", return_value=_single_run_results({"model.foo": "success"})):
            crr.assert_success(["model.foo"], deploy_env="dev", workflow_name="wf")
        assert "success" in capsys.readouterr().out

    def test_missing_unique_id_raises(self):
        with patch.object(crr, "_download_run_results_from_s3", return_value=_single_run_results({})):
            with pytest.raises(RuntimeError, match="missing"):
                crr.assert_success(["model.foo"], deploy_env="dev", workflow_name="wf")

    def test_non_success_status_raises(self):
        with patch.object(crr, "_download_run_results_from_s3", return_value=_single_run_results({"model.foo": "error"})):
            with pytest.raises(RuntimeError, match="non-success"):
                crr.assert_success(["model.foo"], deploy_env="dev", workflow_name="wf")

    def test_missing_and_failed_both_mentioned(self):
        with patch.object(crr, "_download_run_results_from_s3", return_value=_single_run_results({"model.bar": "error"})):
            with pytest.raises(RuntimeError, match="missing") as exc_info:
                crr.assert_success(["model.foo", "model.bar"], deploy_env="dev", workflow_name="wf")
        assert "non-success" in str(exc_info.value)

    def test_missing_deploy_env_raises(self):
        with pytest.raises(ValueError):
            crr.assert_success(["model.foo"], deploy_env=None, workflow_name="wf")

    def test_missing_workflow_name_raises(self):
        with pytest.raises(ValueError):
            crr.assert_success(["model.foo"], deploy_env="dev", workflow_name=None)

    def test_retry_semantics_later_run_can_fix_failure(self, capsys):
        """A second run_results file with success overrides an earlier failure."""
        run1 = _make_run_results([_make_result(status="error", timing=[
            _make_timing_entry(name="compile", completed_at="2024-01-01T09:00:00Z"),
            _make_timing_entry(name="execute", completed_at="2024-01-01T09:00:00Z"),
        ])])
        run2 = _make_run_results([_make_result(status="success", timing=[
            _make_timing_entry(name="compile", completed_at="2024-01-01T10:00:00Z"),
            _make_timing_entry(name="execute", completed_at="2024-01-01T10:00:00Z"),
        ])])
        with patch.object(crr, "_download_run_results_from_s3", return_value=[run1, run2]):
            crr.assert_success(["model.foo"], deploy_env="dev", workflow_name="wf")
        assert "success" in capsys.readouterr().out


# ===========================================================================
# assert_all_models_tests_success
# ===========================================================================
def _run_results_from_statuses(entries: list[tuple[str, str]]) -> list[dict]:
    return [
        _make_run_results([
            _make_result(unique_id=uid, status=st)
            for uid, st in entries
        ])
    ]


class TestAssertAllModelsTestsSuccess:
    def test_all_acceptable_statuses(self, capsys):
        data = _run_results_from_statuses(
            [("model.foo", "success"), ("test.bar", "pass"), ("test.baz", "warn")]
        )
        with patch.object(crr, "_download_run_results_from_s3", return_value=data):
            crr.assert_all_models_tests_success(deploy_env="dev", workflow_name="wf")
        assert "successfully" in capsys.readouterr().out

    def test_some_nodes_fail_raises(self):
        data = _run_results_from_statuses([("model.foo", "error")])
        with patch.object(crr, "_download_run_results_from_s3", return_value=data):
            with pytest.raises(RuntimeError):
                crr.assert_all_models_tests_success(deploy_env="dev", workflow_name="wf")

    def test_no_model_or_test_nodes_raises(self):
        data = _run_results_from_statuses([("snapshot.foo", "success")])
        with patch.object(crr, "_download_run_results_from_s3", return_value=data):
            with pytest.raises(RuntimeError, match="No model or test"):
                crr.assert_all_models_tests_success(deploy_env="dev", workflow_name="wf")

    def test_non_model_test_nodes_ignored(self, capsys):
        data = _run_results_from_statuses(
            [("model.foo", "success"), ("snapshot.bar", "error")]
        )
        with patch.object(crr, "_download_run_results_from_s3", return_value=data):
            crr.assert_all_models_tests_success(deploy_env="dev", workflow_name="wf")
        assert "successfully" in capsys.readouterr().out

    def test_missing_deploy_env_raises(self):
        with pytest.raises(ValueError):
            crr.assert_all_models_tests_success(deploy_env=None, workflow_name="wf")

    def test_missing_workflow_name_raises(self):
        with pytest.raises(ValueError):
            crr.assert_all_models_tests_success(deploy_env="dev", workflow_name=None)

    def test_retry_semantics_acceptable_status_not_overridden(self, capsys):
        """Once a node reaches an acceptable status it should not be overridden by a later failure."""
        run1 = _make_run_results([_make_result(status="success")])
        run2 = _make_run_results([_make_result(status="error")])
        with patch.object(crr, "_download_run_results_from_s3", return_value=[run1, run2]):
            crr.assert_all_models_tests_success(deploy_env="dev", workflow_name="wf")
        assert "successfully" in capsys.readouterr().out


# ===========================================================================
# main()
# ===========================================================================
class TestMain:
    def test_check_all_nodes_flag(self, monkeypatch):
        monkeypatch.setenv("DEPLOY_ENV", "dev")
        monkeypatch.setenv("WORKFLOW_NAME", "wf")
        with patch("sys.argv", ["prog", "--check-all-nodes"]):
            with patch.object(crr, "assert_all_models_tests_success") as mock_fn:
                result = crr.main()
        mock_fn.assert_called_once_with(deploy_env="dev", workflow_name="wf")
        assert result == 0

    def test_unique_ids_flag(self, monkeypatch):
        monkeypatch.setenv("DEPLOY_ENV", "dev")
        monkeypatch.setenv("WORKFLOW_NAME", "wf")
        with patch("sys.argv", ["prog", "--unique-ids", "model.foo"]):
            with patch.object(crr, "assert_success") as mock_fn:
                result = crr.main()
        mock_fn.assert_called_once_with(["model.foo"], deploy_env="dev", workflow_name="wf")
        assert result == 0

    def test_unique_id_yaml_with_dataset_target(self, monkeypatch, tmp_path):
        yaml_content = textwrap.dedent(
            """\
            datasets:
              - name: my_ds
                models: "model.alpha"
            """
        )
        yaml_file = tmp_path / "dag.yaml"
        yaml_file.write_text(yaml_content, encoding="utf-8")
        monkeypatch.setenv("DEPLOY_ENV", "dev")
        monkeypatch.setenv("WORKFLOW_NAME", "wf")
        monkeypatch.setenv("DATASET_TARGET", "my_ds")
        with patch("sys.argv", ["prog", "--unique-id-yaml", str(yaml_file)]):
            with patch.object(crr, "assert_success") as mock_fn:
                crr.main()
        args, kwargs = mock_fn.call_args
        assert "model.alpha" in args[0]

    def test_no_unique_ids_raises(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DEPLOY_ENV", "dev")
        monkeypatch.setenv("WORKFLOW_NAME", "wf")
        # Ensure default YAML path doesn't exist
        with patch("sys.argv", ["prog"]):
            with patch.object(crr, "DEFAULT_UNIQUE_ID_YAML", tmp_path / "nonexistent.yaml"):
                with pytest.raises(ValueError):
                    crr.main()

    def test_missing_dataset_target_with_yaml_raises(self, monkeypatch, tmp_path):
        yaml_file = tmp_path / "dag.yaml"
        yaml_file.write_text("datasets:\n  - name: ds\n    models: 'model.foo'\n", encoding="utf-8")
        monkeypatch.setenv("DEPLOY_ENV", "dev")
        monkeypatch.setenv("WORKFLOW_NAME", "wf")
        monkeypatch.delenv("DATASET_TARGET", raising=False)
        with patch("sys.argv", ["prog", "--unique-id-yaml", str(yaml_file)]):
            with pytest.raises(ValueError, match="DATASET_TARGET"):
                crr.main()
