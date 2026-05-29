"""Unit tests for the deployer shell scripts."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"


class ShellScriptTest(unittest.TestCase):
    """Shared helpers for invoking sourceable shell functions."""

    def run_bash(
        self,
        command: str,
        *,
        env: dict[str, str] | None = None,
        cwd: Path = SRC_DIR,
    ) -> subprocess.CompletedProcess[str]:
        """Run a bash command with repository defaults."""
        run_env = os.environ.copy()
        run_env.update(env or {})

        return subprocess.run(
            ["bash", "-c", command],
            cwd=cwd,
            env=run_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    @staticmethod
    def write_executable(path: Path, content: str) -> None:
        """Write an executable command shim for an unavailable runtime command."""
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)


class DateCheckerTest(ShellScriptTest):
    """Tests for Sunday deploy date gates."""

    def test_first_sunday_requires_monthly_select_criteria(self) -> None:
        """First Sunday runs fail unless monthly models are selected."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            command_shims = Path(tmp_dir) / "commands"
            command_shims.mkdir()
            self.write_executable(
                command_shims / "date",
                "#!/usr/bin/env bash\n"
                "if [ \"$1\" = '+%w' ]; then echo 0; else echo 01; fi\n",
            )

            result = self.run_bash(
                "./date-checker.sh",
                env={
                    "PATH": f"{command_shims}:{os.environ['PATH']}",
                    "DBT_SELECT_CRITERIA": "tag:daily",
                },
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("does NOT contain", result.stdout)

    def test_later_sundays_reject_monthly_select_criteria(self) -> None:
        """Non-first Sunday runs fail if monthly models are selected."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            command_shims = Path(tmp_dir) / "commands"
            command_shims.mkdir()
            self.write_executable(
                command_shims / "date",
                "#!/usr/bin/env bash\n"
                "if [ \"$1\" = '+%w' ]; then echo 0; else echo 14; fi\n",
            )

            result = self.run_bash(
                "./date-checker.sh",
                env={
                    "PATH": f"{command_shims}:{os.environ['PATH']}",
                    "DBT_SELECT_CRITERIA": "tag:monthly",
                },
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("contains 'tag:monthly'", result.stdout)


class EntryPointDatasetCheckTest(ShellScriptTest):
    """Tests for entrypoint dataset-check command selection."""

    def test_unique_ids_are_passed_to_dataset_check_command(self) -> None:
        """UNIQUE_IDS becomes the explicit check_run_results.py argument."""
        result = self.run_bash(
            "source ./entrypoint.sh; build_dataset_check_command; "
            "printf '%s\\n' \"${DATASET_CHECK_COMMAND[@]}\"",
            env={
                "UNIQUE_IDS": '{"daily": ["model.project.a"]}',
                "DATASET_TARGET": "",
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "uv",
                "run",
                "check_run_results.py",
                "--unique-ids",
                '{"daily": ["model.project.a"]}',
            ],
        )

    def test_dataset_target_uses_default_yaml_lookup(self) -> None:
        """DATASET_TARGET leaves arguments empty so Python resolves the YAML."""
        result = self.run_bash(
            "source ./entrypoint.sh; build_dataset_check_command; "
            "printf '%s\\n' \"${DATASET_CHECK_COMMAND[@]}\"",
            env={
                "UNIQUE_IDS": "",
                "DATASET_TARGET": "parole_board.psog_reports",
            },
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["uv", "run", "check_run_results.py"],
        )

    def test_missing_dataset_target_checks_all_nodes(self) -> None:
        """No UNIQUE_IDS or DATASET_TARGET falls back to all-node validation."""
        result = self.run_bash(
            "source ./entrypoint.sh; build_dataset_check_command; "
            "printf '%s\\n' \"${DATASET_CHECK_COMMAND[@]}\"",
            env={"UNIQUE_IDS": "", "DATASET_TARGET": ""},
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["uv", "run", "check_run_results.py", "--check-all-nodes"],
        )


class CreateDerivedTableRetryTest(ShellScriptTest):
    """Tests for deploy retry decisions from project target artefacts."""

    def create_project_fixture(
        self, tmp_dir: str, with_run_results: bool
    ) -> tuple[Path, Path]:
        """Create a minimal cloned repository layout for a dbt project."""
        repository_path = Path(tmp_dir) / "create-a-derived-table"
        target_path = repository_path / "mojap_derived_tables" / "target"
        target_path.mkdir(parents=True)
        if with_run_results:
            (target_path / "run_results.json").write_text("{}", encoding="utf-8")
        return repository_path, target_path

    def write_retry_shims(self, tmp_dir: str, target_path: Path) -> Path:
        """Create command shims for dbt and sleep used by run_dbt."""
        command_shims = Path(tmp_dir) / "commands"
        command_shims.mkdir()
        dbt_invocations = target_path / "dbt_invocations.txt"
        self.write_executable(
            command_shims / "dbt",
            "#!/usr/bin/env bash\n"
            f"printf '%s\\n' \"$*\" >> {dbt_invocations}\n"
            "exit 1\n",
        )
        self.write_executable(command_shims / "sleep", "#!/usr/bin/env bash\nexit 0\n")
        return command_shims

    def run_dbt_with_project_fixture(
        self,
        tmp_dir: str,
        repository_path: Path,
        command_shims: Path,
    ) -> subprocess.CompletedProcess[str]:
        """Source create-a-derived-table.sh and call run_dbt with fixture env."""
        return self.run_bash(
            "source ./create-a-derived-table.sh; run_dbt",
            env={
                "PATH": f"{command_shims}:{os.environ['PATH']}",
                "ANALYTICAL_PLATFORM_DIRECTORY": tmp_dir,
                "REPOSITORY_PATH": str(repository_path),
                "MODE": "build",
                "DBT_PROFILE_WORKGROUP": "workgroup",
                "DBT_PROJECT": "mojap_derived_tables",
                "DBT_SELECT_CRITERIA": "tag:daily",
                "DEPLOY_ENV": "prod",
                "WORKFLOW_NAME": "workflow",
            },
        )

    def test_partial_dbt_run_succeeds_when_run_results_exist(self) -> None:
        """Existing run_results.json means retries produced usable artefacts."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository_path, target_path = self.create_project_fixture(
                tmp_dir, with_run_results=True
            )
            command_shims = self.write_retry_shims(tmp_dir, target_path)

            result = self.run_dbt_with_project_fixture(
                tmp_dir, repository_path, command_shims
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("dbt command at least partially succeeded", result.stdout)
            self.assertTrue((target_path / "run_results_1.json").exists())

    def test_failed_dbt_run_without_run_results_is_unsuccessful(self) -> None:
        """A complete failure without run artefacts returns a failing exit code."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            repository_path, target_path = self.create_project_fixture(
                tmp_dir, with_run_results=False
            )
            command_shims = self.write_retry_shims(tmp_dir, target_path)

            result = self.run_dbt_with_project_fixture(
                tmp_dir, repository_path, command_shims
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("dbt command failed after 3 attempts", result.stdout)


if __name__ == "__main__":
    unittest.main()
