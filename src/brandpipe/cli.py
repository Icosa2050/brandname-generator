from __future__ import annotations

import argparse
import json
from pathlib import Path

from .batch import run_batch
from .browser_profile import run_browser_profile_smoke, warm_browser_profile
from . import db
from .pipeline import export_ranked_csv, load_config, recheck_pending_web, recheck_tmview, run_pipeline
from .tmview import probe_names as probe_tmview_names, write_results_json as write_tmview_results_json


def _status_command(
    db_path: Path,
    run_id: int | None,
    limit: int,
    batch_id: str,
    show_metrics: bool,
) -> int:
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        if run_id is not None:
            row = db.get_run(conn, run_id=run_id)
            if row is None:
                raise SystemExit(f"run not found: {run_id}")
            print(
                f"id={row['id']} title={row['title']} status={row['status']} "
                f"step={row['current_step']} updated_at={row['updated_at']} "
                f"error_class={row['error_class']} error_message={row['error_message']}"
            )
            if show_metrics:
                print(f"metrics={row['metrics_json']}")
            return 0
        rows = db.list_runs(conn, limit=limit, batch_id=batch_id)
        for row in rows:
            print(
                f"id={row['id']} title={row['title']} status={row['status']} "
                f"step={row['current_step']} updated_at={row['updated_at']} "
                f"batch_id={row['batch_id']}"
            )
            if show_metrics:
                print(f"metrics={row['metrics_json']}")
        if batch_id:
            print(f"batch_id={batch_id} runs={len(rows)}")
    return 0


def _export_command(db_path: Path, run_id: int, out_csv: Path, top_n: int) -> int:
    with db.open_db(db_path) as conn:
        db.ensure_schema(conn)
        export_ranked_csv(conn=conn, run_id=run_id, out_path=out_csv.resolve(), limit=top_n)
    print(f"export_csv={out_csv.resolve()}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean-slate branding pipeline CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    init_db = sub.add_parser("init-db", help="Create or migrate the SQLite database")
    init_db.add_argument("--db", required=True, help="SQLite path")

    run = sub.add_parser("run", help="Run the linear pipeline from a TOML config")
    run.add_argument("--config", required=True, help="Run config TOML path")

    run_batch_parser = sub.add_parser("run-batch", help="Run a batch of briefs using one template config")
    run_batch_parser.add_argument("--template-config", required=True, help="Template run config TOML path")
    run_batch_parser.add_argument("--briefs-file", required=True, help="Batch briefs file (.toml, .json, .jsonl)")
    run_batch_parser.add_argument("--batch-id", default="", help="Optional explicit batch id")
    run_batch_parser.add_argument("--stop-on-error", action="store_true", help="Stop the batch on the first failed brief")

    status = sub.add_parser("status", help="Show run status")
    status.add_argument("--db", required=True, help="SQLite path")
    status.add_argument("--run-id", type=int, default=None, help="Optional single run id")
    status.add_argument("--limit", type=int, default=10, help="Number of runs to list")
    status.add_argument("--batch-id", default="", help="Optional batch id filter")
    status.add_argument("--show-metrics", action="store_true", help="Show stored funnel metrics JSON")

    export = sub.add_parser("export", help="Export ranked candidates for a run")
    export.add_argument("--db", required=True, help="SQLite path")
    export.add_argument("--run-id", required=True, type=int, help="Run id")
    export.add_argument("--out-csv", required=True, help="CSV path")
    export.add_argument("--top-n", type=int, default=25, help="Rows to export")

    recheck = sub.add_parser("recheck-web", help="Recheck pending web validations and rerank affected runs")
    recheck.add_argument("--db", required=True, help="SQLite path")
    recheck.add_argument("--run-id", type=int, default=None, help="Optional single run id")
    recheck.add_argument("--batch-id", default="", help="Optional batch id filter")
    recheck.add_argument("--limit", type=int, default=100, help="Maximum pending names to retry")
    recheck.add_argument("--no-export", action="store_true", help="Skip rewriting export CSVs")
    recheck.add_argument("--browser-profile-dir", default="", help="Dedicated browser profile directory for browser-backed web rechecks")
    recheck.add_argument("--browser-chrome-executable", default="", help="Optional Chrome executable override for browser-backed web rechecks")

    recheck_tm = sub.add_parser("recheck-tmview", help="Recheck candidate/watch names against TMView and rerank affected runs")
    recheck_tm.add_argument("--db", required=True, help="SQLite path")
    recheck_tm.add_argument("--profile-dir", required=True, help="Dedicated TMView browser profile directory")
    recheck_tm.add_argument("--chrome-executable", default="", help="Optional Chrome executable path override")
    recheck_tm.add_argument("--run-id", type=int, default=None, help="Optional single run id")
    recheck_tm.add_argument("--batch-id", default="", help="Optional batch id filter")
    recheck_tm.add_argument("--limit", type=int, default=25, help="Maximum candidate/watch names to probe")
    recheck_tm.add_argument("--force", action="store_true", help="Reprobe names even if a tmview result already exists")
    recheck_tm.add_argument("--no-export", action="store_true", help="Skip rewriting export CSVs")
    recheck_tm.add_argument("--headful", action="store_true", help="Run visible browser for debugging")
    recheck_tm.add_argument("--timeout-ms", type=int, default=20000, help="Navigation timeout")
    recheck_tm.add_argument("--settle-ms", type=int, default=2500, help="Post-load settle delay")

    browser_smoke = sub.add_parser("browser-profile-smoke", help="Create or reuse a dedicated Chrome profile and smoke-test it")
    browser_smoke.add_argument("--profile-dir", default="", help="Dedicated browser profile directory")
    browser_smoke.add_argument("--chrome-executable", default="", help="Optional Chrome executable path override")
    browser_smoke.add_argument("--url", default="", help="Explicit URL to open")
    browser_smoke.add_argument("--engine", choices=["google", "brave"], default="google", help="Search engine when --query is used")
    browser_smoke.add_argument("--query", default="", help="Optional search query for a search smoke")
    browser_smoke.add_argument("--headed", action="store_true", help="Run with a visible browser window")
    browser_smoke.add_argument("--timeout-ms", type=int, default=30000, help="Navigation timeout")
    browser_smoke.add_argument("--settle-ms", type=int, default=1500, help="Post-load settle delay")

    browser_warmup = sub.add_parser("browser-profile-warmup", help="Open the dedicated Chrome profile for manual consent/challenge solving")
    browser_warmup.add_argument("--profile-dir", default="", help="Dedicated browser profile directory")
    browser_warmup.add_argument("--chrome-executable", default="", help="Optional Chrome executable path override")
    browser_warmup.add_argument("--url", default="", help="Explicit URL to open")
    browser_warmup.add_argument("--engine", choices=["google", "brave"], default="google", help="Search engine when --query is used")
    browser_warmup.add_argument("--query", default="", help="Optional search query for a search warmup")
    browser_warmup.add_argument("--timeout-ms", type=int, default=30000, help="Navigation timeout")
    browser_warmup.add_argument("--settle-ms", type=int, default=1500, help="Post-load settle delay")

    tmview_probe = sub.add_parser("tmview-probe", help="Probe TMView/EUIPO with a dedicated logged-in browser profile")
    tmview_probe.add_argument("--names", required=True, help="Comma-separated names")
    tmview_probe.add_argument("--profile-dir", required=True, help="Dedicated TMView browser profile directory")
    tmview_probe.add_argument("--chrome-executable", default="", help="Optional Chrome executable path override")
    tmview_probe.add_argument("--timeout-ms", type=int, default=20000, help="Navigation timeout")
    tmview_probe.add_argument("--settle-ms", type=int, default=2500, help="Post-load settle delay")
    tmview_probe.add_argument("--headful", action="store_true", help="Run visible browser for debugging")
    tmview_probe.add_argument("--output-json", default="", help="Optional output JSON path")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "init-db":
        db_path = Path(args.db).expanduser().resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with db.open_db(db_path) as conn:
            db.ensure_schema(conn)
        print(f"db={db_path}")
        return 0
    if args.command == "run":
        config_path = Path(args.config).expanduser().resolve()
        load_config(config_path)
        run_pipeline(config_path)
        return 0
    if args.command == "run-batch":
        summary = run_batch(
            template_config_path=Path(args.template_config).expanduser().resolve(),
            briefs_file_path=Path(args.briefs_file).expanduser().resolve(),
            batch_id=args.batch_id,
            stop_on_error=bool(args.stop_on_error),
        )
        print(f"batch_id={summary['batch_id']}")
        print(f"requested={summary['requested']} succeeded={summary['succeeded']} failed={summary['failed']}")
        print(f"run_ids={','.join(str(item) for item in summary['run_ids'])}")
        if summary["failures"]:
            print(f"failures={json.dumps(summary['failures'], ensure_ascii=False)}")
        return 0
    if args.command == "status":
        return _status_command(
            Path(args.db).expanduser().resolve(),
            args.run_id,
            args.limit,
            args.batch_id,
            bool(args.show_metrics),
        )
    if args.command == "export":
        return _export_command(
            Path(args.db).expanduser().resolve(),
            args.run_id,
            Path(args.out_csv).expanduser(),
            args.top_n,
        )
    if args.command == "recheck-web":
        summary = recheck_pending_web(
            db_path=Path(args.db).expanduser().resolve(),
            run_id=args.run_id,
            batch_id=args.batch_id,
            limit=args.limit,
            rewrite_exports=not bool(args.no_export),
            browser_profile_dir=(Path(args.browser_profile_dir).expanduser().resolve() if args.browser_profile_dir else None),
            browser_chrome_executable=(
                Path(args.browser_chrome_executable).expanduser().resolve() if args.browser_chrome_executable else None
            ),
        )
        print(f"retried={summary['retried']} runs={summary['run_count']}")
        for item in summary["runs"]:
            print(
                f"run_id={item['run_id']} retried={item['retried']} "
                f"promoted_to_candidate={item['promoted_to_candidate']} "
                f"promoted_to_watch={item['promoted_to_watch']} "
                f"blocked={item['blocked']} unchanged={item['unchanged']} "
                f"pending_remaining={item['pending_remaining']}"
            )
        return 0
    if args.command == "recheck-tmview":
        summary = recheck_tmview(
            db_path=Path(args.db).expanduser().resolve(),
            profile_dir=Path(args.profile_dir).expanduser().resolve(),
            chrome_executable=(Path(args.chrome_executable).expanduser().resolve() if args.chrome_executable else None),
            run_id=args.run_id,
            batch_id=args.batch_id,
            limit=args.limit,
            rewrite_exports=not bool(args.no_export),
            force=bool(args.force),
            headless=not bool(args.headful),
            timeout_ms=args.timeout_ms,
            settle_ms=args.settle_ms,
        )
        print(f"retried={summary['retried']} runs={summary['run_count']}")
        for item in summary["runs"]:
            print(
                f"run_id={item['run_id']} retried={item['retried']} "
                f"promoted_to_candidate={item['promoted_to_candidate']} "
                f"promoted_to_watch={item['promoted_to_watch']} "
                f"blocked={item['blocked']} unchanged={item['unchanged']}"
            )
        return 0
    if args.command == "browser-profile-smoke":
        result = run_browser_profile_smoke(
            profile_dir=(Path(args.profile_dir).expanduser().resolve() if args.profile_dir else None),
            chrome_executable=(Path(args.chrome_executable).expanduser().resolve() if args.chrome_executable else None),
            url=args.url,
            engine=args.engine,
            query=args.query,
            headed=bool(args.headed),
            timeout_ms=args.timeout_ms,
            settle_ms=args.settle_ms,
        )
        print(
            f"profile_dir={result['profile_dir']} title={result['title']} "
            f"final_url={result['final_url']} cookies={result['cookies_count']}"
        )
        print(f"screenshot={result['screenshot_path']}")
        print(f"storage_state={result['storage_state_path']}")
        print(f"report={result['report_path']}")
        return 0
    if args.command == "browser-profile-warmup":
        result = warm_browser_profile(
            profile_dir=(Path(args.profile_dir).expanduser().resolve() if args.profile_dir else None),
            chrome_executable=(Path(args.chrome_executable).expanduser().resolve() if args.chrome_executable else None),
            url=args.url,
            engine=args.engine,
            query=args.query,
            timeout_ms=args.timeout_ms,
            settle_ms=args.settle_ms,
        )
        print(
            f"profile_dir={result['profile_dir']} title={result['title']} "
            f"final_url={result['final_url']} cookies={result['cookies_count']}"
        )
        print(f"storage_state={result['storage_state_path']}")
        print(f"report={result['report_path']}")
        return 0
    if args.command == "tmview-probe":
        names = [part.strip() for part in str(args.names).split(",") if part.strip()]
        results = probe_tmview_names(
            names=names,
            profile_dir=Path(args.profile_dir).expanduser().resolve(),
            chrome_executable=(Path(args.chrome_executable).expanduser().resolve() if args.chrome_executable else None),
            timeout_ms=args.timeout_ms,
            settle_ms=args.settle_ms,
            headless=not bool(args.headful),
        )
        for item in results:
            print(
                f"tmview_probe name={item.name} ok={int(item.query_ok)} exact={item.exact_hits} "
                f"near={item.near_hits} results={item.result_count} error={item.error or '-'}"
            )
        if args.output_json:
            out_path = write_tmview_results_json(args.output_json, results)
            print(f"output_json={out_path}")
        return 0
    raise SystemExit(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
