#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

GITHUB_API_HEADERS = (
    "Accept: application/vnd.github+json",
    "X-GitHub-Api-Version: 2022-11-28",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Clean up old GitHub Actions workflow runs for a repository. "
            "Dry-run by default; pass --apply to delete runs."
        )
    )
    parser.add_argument(
        "--repo",
        help="GitHub repository in owner/name form. Defaults to the current git remote origin.",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=14,
        help="Only consider runs created at least this many days ago (default: 14).",
    )
    parser.add_argument(
        "--keep-per-workflow",
        type=int,
        default=10,
        help="Keep at least this many most recent completed runs per workflow (default: 10).",
    )
    parser.add_argument(
        "--branch",
        help="Only consider runs for this branch.",
    )
    parser.add_argument(
        "--event",
        help="Only consider runs triggered by this event.",
    )
    parser.add_argument(
        "--workflow",
        action="append",
        default=[],
        help=(
            "Only consider matching workflows. Repeatable. Matches workflow name, path, or workflow id."
        ),
    )
    parser.add_argument(
        "--conclusion",
        action="append",
        default=[],
        help=(
            "Only consider completed runs with this conclusion. Repeatable, e.g. "
            "--conclusion success --conclusion failure."
        ),
    )
    parser.add_argument(
        "--fetch-limit",
        type=int,
        default=1000,
        help="Maximum number of workflow runs to inspect (default: 1000, 0 = all).",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=25,
        help="Show at most this many candidate runs in dry-run/apply output (default: 25).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete selected workflow runs.",
    )
    return parser.parse_args()


def run_command(command: list[str], *, cwd: Path) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise SystemExit(f"required command not found: {command[0]}") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip()
        raise SystemExit(stderr or f"command failed: {' '.join(command)}") from exc
    return completed.stdout


def run_gh_json(args: list[str], *, cwd: Path) -> Any:
    stdout = run_command(["gh", *args], cwd=cwd)
    return json.loads(stdout)


def parse_repo_from_remote_url(remote: str) -> str | None:
    patterns = [
        r"^git@github\.com:(?P<owner>[^/]+)/(?P<name>[^/]+?)(?:\.git)?$",
        r"^https://github\.com/(?P<owner>[^/]+)/(?P<name>[^/]+?)(?:\.git)?/?$",
        r"^ssh://git@github\.com/(?P<owner>[^/]+)/(?P<name>[^/]+?)(?:\.git)?/?$",
    ]
    for pattern in patterns:
        match = re.match(pattern, remote)
        if match:
            return f"{match.group('owner')}/{match.group('name')}"
    return None


def infer_repo_from_origin(*, cwd: Path) -> str:
    remote = run_command(["git", "config", "--get", "remote.origin.url"], cwd=cwd).strip()
    parsed = parse_repo_from_remote_url(remote)
    if parsed:
        return parsed
    raise SystemExit(
        "could not infer owner/repo from remote.origin.url; pass --repo owner/name explicitly"
    )


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def fetch_runs(*, repo: str, fetch_limit: int, cwd: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    page = 1
    while True:
        if fetch_limit > 0:
            remaining = fetch_limit - len(runs)
            if remaining <= 0:
                break
            per_page = min(100, remaining)
        else:
            per_page = 100

        endpoint = f"/repos/{repo}/actions/runs?per_page={per_page}&page={page}"
        header_args: list[str] = []
        for header in GITHUB_API_HEADERS:
            header_args.extend(["-H", header])
        data = run_gh_json(
            [
                "api",
                *header_args,
                endpoint,
            ],
            cwd=cwd,
        )
        page_runs = data.get("workflow_runs", [])
        if not page_runs:
            break
        runs.extend(page_runs)
        if len(page_runs) < per_page:
            break
        page += 1
    return runs


def workflow_selector_values(run: dict[str, Any]) -> set[str]:
    values = set()
    for key in ("name", "path"):
        value = run.get(key)
        if value:
            values.add(str(value).casefold())
    workflow_id = run.get("workflow_id")
    if workflow_id is not None:
        values.add(str(workflow_id).casefold())
    return values


def workflow_group_key(run: dict[str, Any]) -> tuple[str, str]:
    workflow_id = str(run.get("workflow_id") or "")
    label = str(run.get("name") or run.get("path") or "unknown workflow")
    return workflow_id, label


def select_runs_to_delete(
    runs: Iterable[dict[str, Any]],
    *,
    now: datetime,
    older_than_days: int,
    keep_per_workflow: int,
    branch: str | None,
    event: str | None,
    workflows: Iterable[str],
    conclusions: Iterable[str],
) -> list[dict[str, Any]]:
    cutoff = now - timedelta(days=older_than_days)
    workflow_filters = {value.casefold() for value in workflows}
    conclusion_filters = {value.casefold() for value in conclusions}

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        if run.get("status") != "completed":
            continue
        created_at_raw = run.get("created_at")
        if not created_at_raw:
            continue
        created_at = parse_timestamp(str(created_at_raw))
        if created_at > cutoff:
            continue
        if branch and run.get("head_branch") != branch:
            continue
        if event and run.get("event") != event:
            continue
        conclusion = str(run.get("conclusion") or "").casefold()
        if conclusion_filters and conclusion not in conclusion_filters:
            continue
        if workflow_filters and workflow_selector_values(run).isdisjoint(workflow_filters):
            continue
        grouped[workflow_group_key(run)].append(run)

    selected: list[dict[str, Any]] = []
    for workflow_runs in grouped.values():
        workflow_runs.sort(
            key=lambda item: (
                parse_timestamp(str(item["created_at"])),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
        selected.extend(workflow_runs[max(keep_per_workflow, 0) :])

    selected.sort(
        key=lambda item: (
            parse_timestamp(str(item["created_at"])),
            int(item.get("id") or 0),
        )
    )
    return selected


def preview_lines(runs: list[dict[str, Any]], *, limit: int) -> list[str]:
    lines: list[str] = []
    for run in runs[: max(limit, 0)]:
        lines.append(
            "  - "
            f"{run.get('id')} | {run.get('name') or run.get('path') or 'unknown'} | "
            f"{run.get('conclusion') or 'n/a'} | "
            f"{run.get('head_branch') or '-'} | "
            f"{run.get('created_at')}"
        )
    return lines


def delete_runs(*, repo: str, runs: list[dict[str, Any]], cwd: Path) -> tuple[int, list[str]]:
    deleted = 0
    failures: list[str] = []
    for run in runs:
        run_id = str(run.get("id"))
        try:
            run_command(["gh", "run", "delete", run_id, "-R", repo], cwd=cwd)
        except SystemExit as exc:
            failures.append(f"{run_id}: {exc}")
            continue
        deleted += 1
    return deleted, failures


def print_summary(*, repo: str, fetched_runs: list[dict[str, Any]], selected_runs: list[dict[str, Any]]) -> None:
    by_workflow = Counter(str(run.get("name") or run.get("path") or "unknown workflow") for run in selected_runs)
    print(f"repo={repo}")
    print(f"fetched_runs={len(fetched_runs)}")
    print(f"selected_for_deletion={len(selected_runs)}")
    if by_workflow:
        print("selected_by_workflow:")
        for name, count in sorted(by_workflow.items(), key=lambda item: (-item[1], item[0])):
            print(f"  - {name}: {count}")


def main() -> int:
    args = parse_args()
    cwd = Path.cwd()
    repo = args.repo or infer_repo_from_origin(cwd=cwd)

    now = datetime.now(timezone.utc)
    runs = fetch_runs(repo=repo, fetch_limit=args.fetch_limit, cwd=cwd)
    selected_runs = select_runs_to_delete(
        runs,
        now=now,
        older_than_days=args.older_than_days,
        keep_per_workflow=args.keep_per_workflow,
        branch=args.branch,
        event=args.event,
        workflows=args.workflow,
        conclusions=args.conclusion,
    )

    print_summary(repo=repo, fetched_runs=runs, selected_runs=selected_runs)

    if selected_runs:
        print("preview:")
        for line in preview_lines(selected_runs, limit=args.preview_limit):
            print(line)
        remaining = len(selected_runs) - min(len(selected_runs), max(args.preview_limit, 0))
        if remaining > 0:
            print(f"  ... and {remaining} more")

    if not args.apply:
        print("mode=dry-run")
        print("next_step=rerun with --apply to delete the selected runs")
        return 0

    deleted, failures = delete_runs(repo=repo, runs=selected_runs, cwd=cwd)
    print(f"deleted_runs={deleted}")
    if failures:
        print("delete_failures:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
