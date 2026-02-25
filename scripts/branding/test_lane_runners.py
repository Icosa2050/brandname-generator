#!/usr/bin/env python3
"""Tests for creation/validation lane runners."""

from __future__ import annotations

import csv
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

import naming_db as ndb


class LaneRunnersTest(unittest.TestCase):
    def test_creation_lane_dry_run_prints_commands(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        script = repo_root / 'scripts/branding/run_creation_lane.py'

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            out_dir = tmp / 'campaign_out'
            out_dir.mkdir(parents=True, exist_ok=True)
            db_path = out_dir / 'naming_campaign.db'
            with ndb.open_connection(db_path) as conn:
                ndb.ensure_schema(conn)
                conn.commit()

            cfg_path = tmp / 'creation.toml'
            cfg_path.write_text(
                textwrap.dedent(
                    f"""
                    [creation]
                    out_dir = "{out_dir}"
                    db = "{db_path}"
                    pack_output_dir = "{tmp}/packs"
                    pack_prefix = "decision_pack"
                    review_tiers = "10,5"
                    run_generation = false
                    generation_command = []
                    include_unchecked = false
                    python_bin = "python3"
                    dry_run = true
                    """
                ).strip()
                + '\n',
                encoding='utf-8',
            )

            proc = subprocess.run(
                ['python3', str(script), '--config', str(cfg_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            self.assertIn('build_decision_pack.py', proc.stdout)
            self.assertIn('creation_lane_done', proc.stdout)

    def test_validation_lane_requires_manual_decisions(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        script = repo_root / 'scripts/branding/run_validation_lane.py'

        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            pack_dir = tmp / 'pack'
            pack_dir.mkdir(parents=True, exist_ok=True)
            review_csv = pack_dir / 'review_unique_top120.csv'
            with review_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        'rank',
                        'id',
                        'name_display',
                        'name_normalized',
                        'current_recommendation',
                        'score',
                        'risk',
                        'expensive_ok_types',
                        'expensive_bad_count',
                        'source_lane',
                        'keep',
                        'maybe',
                        'drop',
                        'decision_notes',
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        'rank': 1,
                        'id': 1,
                        'name_display': 'verasettle',
                        'name_normalized': 'verasettle',
                        'current_recommendation': 'strong',
                        'score': 80,
                        'risk': 20,
                        'expensive_ok_types': 5,
                        'expensive_bad_count': 0,
                        'source_lane': 'strict_strong',
                        'keep': '',
                        'maybe': '',
                        'drop': '',
                        'decision_notes': '',
                    }
                )

            cfg_path = tmp / 'validation.toml'
            cfg_path.write_text(
                textwrap.dedent(
                    """
                    [validation]
                    mode = "keep_maybe"
                    keep_top_n = 12
                    maybe_top_n = 12
                    final_top_n = 8
                    recommended_top_n = 6
                    scope = "global"
                    gate = "strict"
                    countries = "de,ch,it"
                    skip_live_screening = true
                    skip_legal_research = true
                    registry_top_n = 8
                    web_top_n = 8
                    print_top = 12
                    euipo_probe = false
                    swissreg_ui_probe = false
                    euipo_timeout_ms = 20000
                    euipo_settle_ms = 2500
                    swissreg_timeout_ms = 20000
                    swissreg_settle_ms = 2500
                    require_human_decisions = true
                    min_human_decisions = 1
                    python_bin = "python3"
                    dry_run = true
                    """
                ).strip()
                + '\n',
                encoding='utf-8',
            )

            fail_proc = subprocess.run(
                ['python3', str(script), '--config', str(cfg_path), '--pack-dir', str(pack_dir)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(fail_proc.returncode, 0)
            self.assertIn('insufficient manual decisions', fail_proc.stderr + fail_proc.stdout)

            with review_csv.open('r', encoding='utf-8', newline='') as handle:
                rows = list(csv.DictReader(handle))
            rows[0]['keep'] = 'x'
            with review_csv.open('w', encoding='utf-8', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            ok_proc = subprocess.run(
                ['python3', str(script), '--config', str(cfg_path), '--pack-dir', str(pack_dir)],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(ok_proc.returncode, 0, msg=ok_proc.stderr)
            self.assertIn('run_acceptance_tail.py', ok_proc.stdout)
            self.assertIn('validation_lane_done', ok_proc.stdout)


if __name__ == '__main__':
    unittest.main()
