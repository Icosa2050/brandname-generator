from __future__ import annotations

import importlib.util
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT_DIR / "scripts" / "cleanup_github_actions_runs.py"

SPEC = importlib.util.spec_from_file_location("cleanup_github_actions_runs", SCRIPT_PATH)
assert SPEC is not None
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def build_run(
    *,
    run_id: int,
    workflow_id: int,
    name: str,
    days_old: int,
    status: str = "completed",
    conclusion: str = "success",
    branch: str = "main",
    event: str = "push",
) -> dict[str, object]:
    created_at = datetime(2026, 3, 23, tzinfo=timezone.utc) - timedelta(days=days_old)
    return {
        "id": run_id,
        "workflow_id": workflow_id,
        "name": name,
        "path": f".github/workflows/{name.casefold().replace(' ', '-')}.yml",
        "status": status,
        "conclusion": conclusion,
        "head_branch": branch,
        "event": event,
        "created_at": created_at.isoformat().replace("+00:00", "Z"),
    }


class CleanupGitHubActionsRunsTests(unittest.TestCase):
    def test_select_runs_keeps_latest_per_workflow(self) -> None:
        now = datetime(2026, 3, 23, tzinfo=timezone.utc)
        runs = [
            build_run(run_id=101, workflow_id=1, name="Branding CI", days_old=20),
            build_run(run_id=102, workflow_id=1, name="Branding CI", days_old=18),
            build_run(run_id=103, workflow_id=1, name="Branding CI", days_old=16),
            build_run(run_id=201, workflow_id=2, name="Canary", days_old=30),
            build_run(run_id=202, workflow_id=2, name="Canary", days_old=29),
        ]

        selected = MODULE.select_runs_to_delete(
            runs,
            now=now,
            older_than_days=14,
            keep_per_workflow=1,
            branch=None,
            event=None,
            workflows=[],
            conclusions=[],
        )

        self.assertEqual([run["id"] for run in selected], [201, 101, 102])

    def test_select_runs_applies_filters_before_grouping(self) -> None:
        now = datetime(2026, 3, 23, tzinfo=timezone.utc)
        runs = [
            build_run(run_id=301, workflow_id=1, name="Branding CI", days_old=20, branch="main"),
            build_run(run_id=302, workflow_id=1, name="Branding CI", days_old=21, branch="feature"),
            build_run(run_id=303, workflow_id=1, name="Branding CI", days_old=22, conclusion="failure"),
            build_run(run_id=304, workflow_id=2, name="Canary", days_old=23, event="schedule"),
            build_run(run_id=305, workflow_id=2, name="Canary", days_old=1),
            build_run(run_id=306, workflow_id=3, name="Queued", days_old=40, status="in_progress"),
        ]

        selected = MODULE.select_runs_to_delete(
            runs,
            now=now,
            older_than_days=14,
            keep_per_workflow=0,
            branch="main",
            event="push",
            workflows=["Branding CI", "1"],
            conclusions=["success"],
        )

        self.assertEqual([run["id"] for run in selected], [301])

    def test_infer_repo_from_origin_parses_common_remote_urls(self) -> None:
        cases = {
            "git@github.com:Icosa2050/brandname-generator.git": "Icosa2050/brandname-generator",
            "https://github.com/Icosa2050/brandname-generator.git": "Icosa2050/brandname-generator",
            "https://github.com/Icosa2050/brandname-generator": "Icosa2050/brandname-generator",
            "ssh://git@github.com/Icosa2050/brandname-generator.git": "Icosa2050/brandname-generator",
        }

        for remote, expected in cases.items():
            with self.subTest(remote=remote):
                self.assertEqual(expected, MODULE.parse_repo_from_remote_url(remote))


if __name__ == "__main__":
    unittest.main()
