from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"


class ShellScriptTest(unittest.TestCase):
    def write_stub(self, path: Path, content: str) -> None:
        path.write_text(content, encoding="utf-8")
        path.chmod(0o755)

    def run_bash(
        self,
        command: str,
        *,
        env: dict[str, str] | None = None,
        cwd: Path = SRC_DIR,
    ) -> subprocess.CompletedProcess[str]:
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


class DateCheckerTest(ShellScriptTest):
    def test_first_sunday_requires_monthly_select_criteria(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            date = Path(tmp_dir) / "date"
            date.write_text(
                "#!/usr/bin/env bash\n"
                "if [ \"$1\" = '+%w' ]; then echo 0; else echo 01; fi\n",
                encoding="utf-8",
            )
            date.chmod(0o755)

            result = self.run_bash(
                "./date-checker.sh",
                env={
                    "PATH": f"{tmp_dir}:{os.environ['PATH']}",
                    "DBT_SELECT_CRITERIA": "tag:daily",
                },
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("does NOT contain", result.stdout)

    def test_non_first_sunday_rejects_monthly_select_criteria(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            date = Path(tmp_dir) / "date"
            date.write_text(
                "#!/usr/bin/env bash\n"
                "if [ \"$1\" = '+%w' ]; then echo 0; else echo 14; fi\n",
                encoding="utf-8",
            )
            date.chmod(0o755)

            result = self.run_bash(
                "./date-checker.sh",
                env={
                    "PATH": f"{tmp_dir}:{os.environ['PATH']}",
                    "DBT_SELECT_CRITERIA": "tag:monthly",
                },
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("contains 'tag:monthly'", result.stdout)


class EntryPointTest(ShellScriptTest):
    def copy_src_to_temp(self, tmp_dir: str) -> Path:
        dest = Path(tmp_dir) / "src"
        shutil.copytree(SRC_DIR, dest)
        return dest

    def test_dataset_check_with_unique_ids_exits_after_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            src = self.copy_src_to_temp(tmp_dir)
            log = Path(tmp_dir) / "calls.log"
            self.write_stub(
                src / "clone-create-a-derived-table.sh",
                f"#!/usr/bin/env bash\necho clone >> {log}\n",
            )
            self.write_stub(
                src / "create-a-derived-table.sh",
                f"#!/usr/bin/env bash\necho deploy >> {log}\n",
            )
            self.write_stub(
                src / "uv",
                "#!/usr/bin/env bash\n" f"printf '%s\\n' \"$*\" >> {log}\n" "exit 0\n",
            )

            result = self.run_bash(
                "./entrypoint.sh",
                cwd=src,
                env={
                    "PATH": f"{src}:{os.environ['PATH']}",
                    "IS_DATASET_CHECK": "True",
                    "UNIQUE_IDS": '{"daily": ["model.project.a"]}',
                },
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                log.read_text(encoding="utf-8").splitlines(),
                [
                    "clone",
                    'run check_run_results.py --unique-ids {"daily": ["model.project.a"]}',
                ],
            )

    def test_deployment_runs_when_dataset_check_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            src = self.copy_src_to_temp(tmp_dir)
            log = Path(tmp_dir) / "calls.log"
            self.write_stub(
                src / "clone-create-a-derived-table.sh",
                f"#!/usr/bin/env bash\necho clone >> {log}\n",
            )
            self.write_stub(
                src / "create-a-derived-table.sh",
                f"#!/usr/bin/env bash\necho deploy >> {log}\n",
            )

            result = self.run_bash("./entrypoint.sh", cwd=src)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                log.read_text(encoding="utf-8").splitlines(),
                ["clone", "deploy"],
            )


class CreateDerivedTableTest(ShellScriptTest):
    def test_run_dbt_returns_success_when_retry_artifacts_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_path = Path(tmp_dir) / "create-a-derived-table"
            project_path = repo_path / "project" / "target"
            bin_path = Path(tmp_dir) / "bin"
            log = Path(tmp_dir) / "calls.log"
            project_path.mkdir(parents=True)
            bin_path.mkdir()
            (project_path / "run_results.json").write_text("{}", encoding="utf-8")

            self.write_stub(
                bin_path / "dbt",
                "#!/usr/bin/env bash\n" f"echo dbt:$* >> {log}\n" "exit 1\n",
            )
            self.write_stub(bin_path / "sleep", "#!/usr/bin/env bash\nexit 0\n")

            result = self.run_bash(
                "source ./create-a-derived-table.sh; run_dbt",
                env={
                    "PATH": f"{bin_path}:{os.environ['PATH']}",
                    "ANALYTICAL_PLATFORM_DIRECTORY": tmp_dir,
                    "REPOSITORY_PATH": str(repo_path),
                    "MODE": "build",
                    "DBT_PROFILE_WORKGROUP": "workgroup",
                    "DBT_PROJECT": "project",
                    "DBT_SELECT_CRITERIA": "tag:daily",
                    "DEPLOY_ENV": "prod",
                    "WORKFLOW_NAME": "workflow",
                },
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("dbt command at least partially succeeded", result.stdout)
            self.assertTrue((project_path / "run_results_1.json").exists())

    def test_run_dbt_fails_when_no_retry_artifacts_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo_path = Path(tmp_dir) / "create-a-derived-table"
            (repo_path / "project" / "target").mkdir(parents=True)
            bin_path = Path(tmp_dir) / "bin"
            log = Path(tmp_dir) / "calls.log"
            bin_path.mkdir()

            self.write_stub(
                bin_path / "dbt",
                "#!/usr/bin/env bash\n" f"echo dbt:$* >> {log}\n" "exit 1\n",
            )
            self.write_stub(bin_path / "sleep", "#!/usr/bin/env bash\nexit 0\n")

            result = self.run_bash(
                "source ./create-a-derived-table.sh; run_dbt",
                env={
                    "PATH": f"{bin_path}:{os.environ['PATH']}",
                    "ANALYTICAL_PLATFORM_DIRECTORY": tmp_dir,
                    "REPOSITORY_PATH": str(repo_path),
                    "MODE": "build",
                    "DBT_PROFILE_WORKGROUP": "workgroup",
                    "DBT_PROJECT": "project",
                    "DBT_SELECT_CRITERIA": "tag:daily",
                    "DEPLOY_ENV": "prod",
                    "WORKFLOW_NAME": "workflow",
                },
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("dbt command failed after 3 attempts", result.stdout)


if __name__ == "__main__":
    unittest.main()
