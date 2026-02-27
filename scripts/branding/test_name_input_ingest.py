#!/usr/bin/env python3
"""Focused tests for source-ingest filtering helpers."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import name_input_ingest as nii


def _record(name: str) -> nii.SourceRecord:
    return nii.SourceRecord(
        name=name,
        language_hint='en',
        semantic_category='trust',
        confidence_weight=0.7,
        source_label='test',
        note='',
        provenance_tags=[],
        metadata={},
    )


class NameInputIngestFilteringTest(unittest.TestCase):
    def test_load_exclusion_names_supports_txt_csv_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            txt = base / 'exclude.txt'
            txt.write_text('Palantir\n# comment\nOpenAI\n', encoding='utf-8')

            csv_file = base / 'exclude.csv'
            csv_file.write_text('name,title\nAcme,\n,Globex\n', encoding='utf-8')

            json_file = base / 'exclude.json'
            json_file.write_text(
                json.dumps(
                    {
                        'items': [
                            {'name': 'Initech'},
                            {'label': 'Umbrella'},
                            'Wonka',
                        ]
                    }
                ),
                encoding='utf-8',
            )

            got = nii.load_exclusion_names([txt, csv_file, json_file])
            self.assertIn('palantir', got)
            self.assertIn('openai', got)
            self.assertIn('acme', got)
            self.assertIn('globex', got)
            self.assertIn('initech', got)
            self.assertIn('umbrella', got)
            self.assertIn('wonka', got)

    def test_filter_source_records_applies_exclusion_and_zipf_bounds(self) -> None:
        records = [_record('Atlas'), _record('Rareterm'), _record('The'), _record('Anchor')]

        def _mock_zipf(name: str, *, lang: str) -> float | None:
            table = {
                'atlas': 2.4,
                'rareterm': 0.5,
                'the': 7.6,
                'anchor': 3.2,
            }
            return table.get(name.lower())

        with mock.patch('name_input_ingest.maybe_zipf_frequency', side_effect=_mock_zipf):
            kept, summary = nii.filter_source_records(
                records,
                exclusion_names={'anchor'},
                zipf_min=1.0,
                zipf_max=6.0,
                zipf_lang='en',
            )

        kept_names = sorted(r.name.lower() for r in kept)
        self.assertEqual(kept_names, ['atlas'])
        self.assertEqual(int(summary.get('excluded_count') or 0), 1)
        self.assertEqual(int(summary.get('zipf_low_count') or 0), 1)
        self.assertEqual(int(summary.get('zipf_high_count') or 0), 1)
        self.assertEqual(int(summary.get('kept_count') or 0), 1)


if __name__ == '__main__':
    unittest.main()
