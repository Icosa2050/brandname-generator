#!/usr/bin/env python3
"""Regression tests for automation lane environment bootstrap behavior."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


class AutomationLaneWithContractEnvBootstrapTest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.script = self.repo_root / "scripts/branding/run_automation_lane_with_contract.sh"

    def _make_fake_direnv(self, base: Path, allow_rc: int, allow_stderr: str) -> Path:
        fake_bin = base / "bin"
        fake_bin.mkdir(parents=True, exist_ok=True)
        fake_direnv = fake_bin / "direnv"
        fake_direnv.write_text(
            (
                "#!/usr/bin/env bash\n"
                "set -euo pipefail\n"
                "if [[ \"${1:-}\" == \"allow\" ]]; then\n"
                f"  echo {allow_stderr!r} >&2\n"
                f"  exit {allow_rc}\n"
                "fi\n"
                "echo \"unexpected direnv invocation: $*\" >&2\n"
                "exit 91\n"
            ),
            encoding="utf-8",
        )
        fake_direnv.chmod(0o755)
        return fake_bin

    def _run_validation_probe(self, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["zsh", str(self.script), "--lane", "validation"],
            cwd=self.repo_root,
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )

    def test_auto_mode_falls_back_to_dotenv_when_direnv_allow_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            fake_bin = self._make_fake_direnv(
                tmp, allow_rc=1, allow_stderr="direnv: error open allow hash: operation not permitted"
            )
            artifact_root = tmp / "artifacts"
            dotenv_file = tmp / ".env.test"
            dotenv_file.write_text("OPENROUTER_API_KEY=test-key\n", encoding="utf-8")

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"
            env["BRANDING_AUTOMATION_DATA_ROOT"] = str(artifact_root)
            env["BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE"] = "auto"
            env["BRANDING_AUTOMATION_DOTENV_FILE"] = str(dotenv_file)
            env.pop("BRANDING_AUTOMATION_REQUIRE_DIRENV", None)

            proc = self._run_validation_probe(env)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("warning: direnv allow failed; falling back to dotenv (.env)", proc.stderr)
            self.assertIn("missing pointer:", proc.stderr)

    def test_dotenv_mode_does_not_call_direnv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            fake_bin = self._make_fake_direnv(
                tmp, allow_rc=1, allow_stderr="direnv should not be called in dotenv mode"
            )
            artifact_root = tmp / "artifacts"
            dotenv_file = tmp / ".env.test"
            dotenv_file.write_text("OPENROUTER_API_KEY=test-key\n", encoding="utf-8")

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"
            env["BRANDING_AUTOMATION_DATA_ROOT"] = str(artifact_root)
            env["BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE"] = "dotenv"
            env["BRANDING_AUTOMATION_DOTENV_FILE"] = str(dotenv_file)

            proc = self._run_validation_probe(env)
            self.assertNotEqual(proc.returncode, 0)
            self.assertNotIn("direnv should not be called", proc.stderr)
            self.assertIn("missing pointer:", proc.stderr)

    def test_auto_mode_honors_require_direnv_strict_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            fake_bin = self._make_fake_direnv(
                tmp, allow_rc=1, allow_stderr="direnv: error open allow hash: operation not permitted"
            )
            artifact_root = tmp / "artifacts"

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"
            env["BRANDING_AUTOMATION_DATA_ROOT"] = str(artifact_root)
            env["BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE"] = "auto"
            env["BRANDING_AUTOMATION_REQUIRE_DIRENV"] = "1"

            proc = self._run_validation_probe(env)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("direnv allow failed and BRANDING_AUTOMATION_REQUIRE_DIRENV=1", proc.stderr)
            self.assertNotIn("missing pointer:", proc.stderr)

    def test_none_mode_skips_direnv_and_dotenv(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            fake_bin = self._make_fake_direnv(
                tmp, allow_rc=1, allow_stderr="direnv should not be called in none mode"
            )
            artifact_root = tmp / "artifacts"

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"
            env["BRANDING_AUTOMATION_DATA_ROOT"] = str(artifact_root)
            env["BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE"] = "none"

            proc = self._run_validation_probe(env)
            self.assertNotEqual(proc.returncode, 0)
            self.assertNotIn("direnv should not be called", proc.stderr)
            self.assertNotIn("dotenv fallback requested but missing file", proc.stderr)
            self.assertIn("missing pointer:", proc.stderr)

    def test_default_mode_is_none(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            fake_bin = self._make_fake_direnv(
                tmp, allow_rc=1, allow_stderr="direnv should not be called in default mode"
            )
            artifact_root = tmp / "artifacts"

            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}:{env.get('PATH', '')}"
            env["BRANDING_AUTOMATION_DATA_ROOT"] = str(artifact_root)
            env.pop("BRANDING_AUTOMATION_ENV_BOOTSTRAP_MODE", None)

            proc = self._run_validation_probe(env)
            self.assertNotEqual(proc.returncode, 0)
            self.assertNotIn("direnv should not be called", proc.stderr)
            self.assertNotIn("dotenv fallback requested but missing file", proc.stderr)
            self.assertIn("missing pointer:", proc.stderr)


if __name__ == "__main__":
    unittest.main()
