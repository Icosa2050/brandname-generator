#!/usr/bin/env python3
"""Unit tests for update_cheap_tm_blocklist."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import update_cheap_tm_blocklist as utb


class UpdateCheapTmBlocklistTest(unittest.TestCase):
    def test_reuses_existing_auto_added_header(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / 'blocklist.txt'
            path.write_text(
                '\n'.join(
                    [
                        '# seed',
                        'booking',
                        '',
                        '# auto-added tokens',
                        'alpha',
                    ]
                )
                + '\n',
                encoding='utf-8',
            )
            with mock.patch(
                'sys.argv',
                [
                    'update_cheap_tm_blocklist.py',
                    '--blocklist-file',
                    str(path),
                    '--tokens',
                    'beta',
                ],
            ):
                code = utb.main()
            self.assertEqual(code, 0)
            lines = path.read_text(encoding='utf-8').splitlines()
            self.assertEqual(lines.count('# auto-added tokens'), 1)
            self.assertIn('alpha', lines)
            self.assertIn('beta', lines)


if __name__ == '__main__':
    unittest.main()
