#!/usr/bin/env python3
"""Tests for euipo_esearch_probe."""

from __future__ import annotations

import unittest

import euipo_esearch_probe as probe


class EuipoEsearchProbeTest(unittest.TestCase):
    def test_probe_from_body_segments_detects_exact_and_near_hits(self) -> None:
        body_text = """
Graphic representation
Trade mark name
Application date
Goods and Services
Trade mark status
Trade mark office
Application number
Applicant name
-
EQUIDRAL
20/10/1995
5, 31
Expired
United Kingdom - UKIPO
UK00002042103
Dimminaco AG
Applicant name
Dimminaco AG
-
EQUIDRAL
17/08/1977
5
Registered
India - CGDPTM
327916
AARON PHARMACEUTICALS PVT. LTD.
Applicant name
AARON PHARMACEUTICALS PVT. LTD.
-
VEQUIDRAL
22/06/2005
5
Registered
Spain - OEPM
M2658694
RATIOPHARM GMBH
Applicant name
RATIOPHARM GMBH
"""
        (
            exact_hits,
            near_hits,
            samples,
            exact_samples,
            active_exact_hits,
            inactive_exact_hits,
            unknown_exact_hits,
        ) = probe._probe_from_body_segments('equidral', body_text)

        self.assertEqual(exact_hits, 2)
        self.assertEqual(near_hits, 1)
        self.assertEqual(active_exact_hits, 1)
        self.assertEqual(inactive_exact_hits, 1)
        self.assertEqual(unknown_exact_hits, 0)
        self.assertTrue(any('EQUIDRAL' in sample for sample in exact_samples))
        self.assertTrue(samples)

    def test_probe_from_body_segments_does_not_promote_substring_to_exact(self) -> None:
        body_text = """
Graphic representation
Trade mark name
Trade mark office
Applicant name
-
STABAFIL
27/10/1992
Expired
WIPO - WIPO
Zinggeler AG
Applicant name
Zinggeler AG
"""
        (
            exact_hits,
            near_hits,
            _samples,
            _exact_samples,
            active_exact_hits,
            inactive_exact_hits,
            unknown_exact_hits,
        ) = probe._probe_from_body_segments('stabafi', body_text)

        self.assertEqual(exact_hits, 0)
        self.assertEqual(near_hits, 1)
        self.assertEqual(active_exact_hits, 0)
        self.assertEqual(inactive_exact_hits, 0)
        self.assertEqual(unknown_exact_hits, 0)


if __name__ == '__main__':
    unittest.main()
