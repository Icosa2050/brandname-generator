# ruff: noqa: E402
from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe import validate_cli as MODULE
from brandpipe.models import ResultStatus


class ValidateCliTests(unittest.TestCase):
    def test_to_float_returns_default_for_invalid_values(self) -> None:
        self.assertEqual(MODULE._to_float("oops", 1.25), 1.25)
        self.assertEqual(MODULE._to_float(object(), 2.5), 2.5)

    def test_read_names_file_deduplicates_blank_lines_and_normalized_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "names.txt"
            path.write_text("\n Vantora \n vantora \nSet4You\n", encoding="utf-8")

            rows = MODULE._read_names_file(path)

        self.assertEqual([row.shortlist_rank for row in rows], [2, 4])
        self.assertEqual([row.name_display for row in rows], ["Vantora", "Set4You"])
        self.assertEqual([row.name_normalized for row in rows], ["vantora", "set4you"])
        self.assertTrue(all(row.shortlist_reason == "names_file_input" for row in rows))

    def test_read_review_csv_keep_mode_skips_non_keep_blank_and_duplicate_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "review.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["keep", "maybe", "drop", "name_display", "name_normalized"],
                )
                writer.writeheader()
                writer.writerow(
                    {"keep": "x", "maybe": "", "drop": "", "name_display": "Vantora", "name_normalized": "vantora"}
                )
                writer.writerow(
                    {"keep": "", "maybe": "x", "drop": "", "name_display": "Meridel", "name_normalized": "meridel"}
                )
                writer.writerow({"keep": "x", "maybe": "", "drop": "", "name_display": "", "name_normalized": ""})
                writer.writerow(
                    {
                        "keep": "x",
                        "maybe": "",
                        "drop": "",
                        "name_display": "Vantora Copy",
                        "name_normalized": "vantora",
                    }
                )
                writer.writerow(
                    {"keep": "x", "maybe": "", "drop": "", "name_display": "Set4You", "name_normalized": "set4you"}
                )

            rows = MODULE._read_review_csv(path, mode="keep")

        self.assertEqual([row.name_normalized for row in rows], ["vantora", "set4you"])
        self.assertEqual([row.shortlist_bucket for row in rows], ["keep", "keep"])

    def test_read_review_csv_all_mode_keeps_review_mark_rows_without_filtering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "review.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["keep", "maybe", "drop", "name_display", "name_normalized"],
                )
                writer.writeheader()
                writer.writerow(
                    {"keep": "", "maybe": "x", "drop": "", "name_display": "Meridel", "name_normalized": "meridel"}
                )
                writer.writerow(
                    {"keep": "", "maybe": "", "drop": "x", "name_display": "Certivo", "name_normalized": "certivo"}
                )

            rows = MODULE._read_review_csv(path, mode="all")

        self.assertEqual([row.name_normalized for row in rows], ["meridel", "certivo"])
        self.assertEqual([row.shortlist_bucket for row in rows], ["maybe", "drop"])

    def test_read_review_csv_uses_shortlist_selected_when_review_columns_are_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "review.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "shortlist_selected",
                        "name_display",
                        "name_normalized",
                        "shortlist_reason",
                        "recommendation",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "shortlist_selected": "yes",
                        "name_display": "Vantora",
                        "name_normalized": "vantora",
                        "shortlist_reason": "selected",
                        "recommendation": "Keep",
                    }
                )
                writer.writerow(
                    {
                        "shortlist_selected": "0",
                        "name_display": "Meridel",
                        "name_normalized": "meridel",
                        "shortlist_reason": "dropped",
                        "recommendation": "Drop",
                    }
                )
                writer.writerow(
                    {
                        "shortlist_selected": "",
                        "name_display": "Certivo",
                        "name_normalized": "certivo",
                        "shortlist_reason": "implicit include",
                        "recommendation": "Review",
                    }
                )

            rows = MODULE._read_review_csv(path, mode="keep_maybe")

        self.assertEqual([row.name_normalized for row in rows], ["vantora", "certivo"])
        self.assertTrue(all(row.shortlist_bucket == "selected" for row in rows))
        self.assertEqual([row.recommendation for row in rows], ["keep", "review"])

    def test_parse_result_details_handles_empty_and_non_mapping_payloads(self) -> None:
        self.assertEqual(MODULE._parse_result_details("", candidate_id=1, result_key="web"), {})
        self.assertEqual(
            MODULE._parse_result_details('["one", "two"]', candidate_id=1, result_key="web"),
            {"value": ["one", "two"]},
        )

    def test_load_candidate_result_rows_skips_blank_names(self) -> None:
        fake_conn = object()
        open_db_cm = mock.MagicMock()
        open_db_cm.__enter__.return_value = fake_conn
        open_db_cm.__exit__.return_value = False

        with (
            mock.patch.object(MODULE.db, "open_db", return_value=open_db_cm),
            mock.patch.object(MODULE.db, "ensure_schema"),
            mock.patch.object(
                MODULE.db,
                "list_candidates",
                return_value=[
                    {"id": 1, "display_name": "", "name": ""},
                    {"id": 2, "display_name": "Vantora", "name": "legacy-vantora"},
                ],
            ),
            mock.patch.object(
                MODULE.db,
                "fetch_results_for_candidate",
                return_value=[
                    {
                        "result_key": "domain",
                        "status": ResultStatus.PASS.value,
                        "score_delta": 0.0,
                        "reason": "",
                        "details_json": '{"available": true}',
                    }
                ],
            ) as fetch_results,
        ):
            candidate_lookup, result_map = MODULE._load_candidate_result_rows(Path("/tmp/brandpipe.db"), run_id=17)

        self.assertEqual(candidate_lookup, {"vantora": 2})
        self.assertEqual(len(result_map[2]), 1)
        self.assertEqual(result_map[2][0].details, {"available": True})
        fetch_results.assert_called_once_with(fake_conn, candidate_id=2)

    def test_load_rows_combines_names_file_and_names_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            names_file = Path(tmp_dir) / "names.txt"
            names_file.write_text("Meridel\n", encoding="utf-8")
            args = argparse.Namespace(
                input_csv="",
                names_file=str(names_file),
                names="vantora,,Meridel",
                mode="keep_maybe",
            )

            rows = MODULE._load_rows(args)

        self.assertEqual([row.name_normalized for row in rows], ["meridel", "vantora"])
        self.assertEqual([row.shortlist_rank for row in rows], [1, 2])

    def test_load_rows_raises_when_no_inputs_are_available(self) -> None:
        args = argparse.Namespace(input_csv="", names_file="", names="", mode="keep_maybe")

        with self.assertRaisesRegex(SystemExit, "no validation input rows found"):
            MODULE._load_rows(args)

    def test_run_validate_command_finalizes_failed_manifest_for_names_file_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            names_file = root / "names.txt"
            out_dir = root / "validated"
            names_file.write_text("Vantora\n", encoding="utf-8")
            args = argparse.Namespace(
                input_csv="",
                names_file=str(names_file),
                names="",
                mode="keep_maybe",
                out_dir=str(out_dir),
                checks="domain",
                concurrency=1,
                timeout_s=5.0,
                required_domain_tlds="",
                store_countries="de,ch,us",
                company_top=8,
                social_unavailable_fail_threshold=3,
                web_search_order="serper,brave",
                web_browser_profile_dir="",
                web_browser_chrome_executable="",
                tmview_profile_dir="",
                tmview_chrome_executable="",
                reset_state=True,
            )

            with (
                self.assertRaisesRegex(RuntimeError, "validation boom"),
                mock.patch.object(MODULE, "run_shortlist_validation", side_effect=RuntimeError("validation boom")),
            ):
                MODULE.run_validate_command(args)

            task_root = next(out_dir.iterdir())
            manifest = json.loads((task_root / "manifest.json").read_text(encoding="utf-8"))
            state_dir_exists = (task_root / "state").exists()

        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["metrics_summary"], {"input_count": 1})
        self.assertTrue(state_dir_exists)
        self.assertTrue(any(Path(path).name == "names.txt" for path in manifest["config_paths"]))


if __name__ == "__main__":
    unittest.main()
