# ruff: noqa: E402
from __future__ import annotations

import http.client
import json
import os
import socket
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock
from urllib import error

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import brandpipe.ideation as ideation
from brandpipe.ideation import (
    _avoidance_fragment_hints,
    build_prompt,
    call_openai_compat_candidates,
    call_openrouter_candidates,
    filter_exemplar_echoes,
    format_avoidance_block,
    format_positive_anchor_block,
    generate_candidates,
    parse_candidate_payload,
    select_direct_seed_names,
    sanitize_positive_anchor_context,
)
from brandpipe.models import Brief, IdeationConfig, IdeationRoleConfig, LexiconBundle, PseudowordConfig, SeedCandidate


class _FakeHttpResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _RawHttpResponse:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def read(self) -> bytes:
        return self._raw

    def __enter__(self) -> "_RawHttpResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class IdeationTests(unittest.TestCase):
    def test_top_level_helpers_cover_prompt_context_and_limits(self) -> None:
        self.assertEqual(ideation.normalize_alpha_name(" Van-tora 2! "), "vantora")
        self.assertTrue(ideation.is_valid_candidate_name("vantora"))
        self.assertFalse(ideation.is_valid_candidate_name("ab"))
        self.assertEqual(ideation.render_context_lines({}), [])
        self.assertEqual(ideation.format_avoidance_block(None), "")
        self.assertEqual(ideation.format_positive_anchor_block(None), "")
        self.assertEqual(
            ideation._literal_fragment_hints(("priva", "PARCL", "xx", "cordnance"), limit=3),
            ["priv", "parc", "cordn"],
        )
        self.assertEqual(
            ideation._avoidance_terminal_families(
                {"external_terminal_families": ["ra", "RA", "x", "terra", "zen"]},
                limit=3,
            ),
            ("ra", "terra", "zen"),
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            prompt_path = root / "prompt.txt"
            empty_path = root / "empty.txt"
            read_error_path = root / "prompt-dir"
            prompt_path.write_text("  craft names boldly  \n", encoding="utf-8")
            empty_path.write_text("   \n", encoding="utf-8")
            read_error_path.mkdir()

            self.assertEqual(ideation.load_prompt_template(None), "")
            self.assertEqual(ideation.load_prompt_template(prompt_path), "craft names boldly")
            with self.assertRaisesRegex(ValueError, "prompt_template_not_found"):
                ideation.load_prompt_template(root / "missing.txt")
            with self.assertRaisesRegex(ValueError, "prompt_template_empty"):
                ideation.load_prompt_template(empty_path)
            with self.assertRaisesRegex(ValueError, "prompt_template_read_error"):
                ideation.load_prompt_template(read_error_path)

        brief = Brief(
            product_core="utility settlement",
            target_users=["operators"],
            trust_signals=["clarity"],
            forbidden_directions=["banking"],
            language_market="de",
            notes="keep it light",
        )
        context_packet = ideation._context_packet(brief)
        self.assertEqual(
            ideation.render_context_lines(context_packet),
            [
                "product_core: utility settlement",
                "target_users: operators",
                "trust_signals: clarity",
                "forbidden_directions: banking",
                "language_market: de",
                "notes: keep it light",
            ],
        )
        recomb_packet = ideation._role_context_packet(context_packet, "recombinator")
        self.assertEqual(recomb_packet["product_core"], "")
        self.assertEqual(recomb_packet["target_users"], [])
        self.assertIn("Recombine from lexicon cues and seed shapes.", str(recomb_packet["notes"]))
        hybrid_packet = ideation._role_context_packet(context_packet, "morpheme_hybridizer")
        self.assertEqual(hybrid_packet["product_core"], "")
        self.assertIn("Fuse shorter lexicon atoms", str(hybrid_packet["notes"]))
        contrarian_packet = ideation._role_context_packet(context_packet, "contrarian")
        self.assertEqual(contrarian_packet["target_users"], [])
        explorer_packet = ideation._role_context_packet(context_packet, "phonetic_explorer")
        self.assertEqual(explorer_packet["target_users"], [])
        self.assertIn("Push farther on opening and rhythm variation", str(explorer_packet["notes"]))
        ending_packet = ideation._role_context_packet(context_packet, "ending_diversifier")
        self.assertIn("Favor endings and terminal cadences", str(ending_packet["notes"]))
        self.assertEqual(ideation._role_context_packet(context_packet, "creative_divergence"), context_packet)

        self.assertEqual(
            ideation._round_seed_target(
                IdeationConfig(provider="fixture", candidates_per_round=3, round_seed_min=4, round_seed_max=6)
            ),
            4,
        )
        self.assertEqual(
            ideation._round_seed_target(
                IdeationConfig(provider="fixture", candidates_per_round=20, round_seed_min=3, round_seed_max=6)
            ),
            6,
        )
        lexicon_terms = ideation._prompt_lexicon_terms(
            LexiconBundle(
                core_terms=("core1", "core2"),
                modifiers=("mod1", "mod2"),
                associative_terms=("assoc1", "assoc2"),
                morphemes=("m1", "m2", "m3"),
            ),
            IdeationConfig(
                provider="fixture",
                lexicon_core_limit=1,
                lexicon_modifier_limit=1,
                lexicon_associative_limit=1,
                lexicon_morpheme_limit=2,
            ),
        )
        self.assertEqual(
            lexicon_terms,
            {
                "core_terms": ["core1"],
                "modifiers": ["mod1"],
                "associative_terms": ["assoc1"],
                "morphemes": ["m1", "m2"],
            },
        )

    def test_build_prompt_varies_scheme_by_role(self) -> None:
        base_context = {
            "product_core": "utility-cost settlement software",
            "target_users": ["property managers"],
            "trust_signals": ["clarity"],
            "forbidden_directions": [],
            "language_market": "de",
            "notes": "",
        }

        creative_prompt, creative_mode = build_prompt(
            scope="global",
            round_index=0,
            target_count=3,
            context_packet=base_context,
            role_name="creative_divergence",
        )
        recomb_prompt, recomb_mode = build_prompt(
            scope="global",
            round_index=0,
            target_count=3,
            context_packet=base_context,
            role_name="recombinator",
        )

        self.assertNotEqual(creative_mode, recomb_mode)
        self.assertIn("Scheme label: wildcard-open", creative_prompt)
        self.assertIn("Preferred endings: a, o, u, is, on", creative_prompt)
        self.assertIn("Scheme label: odd-familiar", recomb_prompt)
        self.assertIn("Preferred endings: o, a, er, um, en", recomb_prompt)
        self.assertNotIn("product_core: utility-cost settlement software", recomb_prompt)
        self.assertIn("Recombine from lexicon cues and seed shapes.", recomb_prompt)
        self.assertIn("maximize variation across openings, middles, endings, cadence, and stress", creative_prompt)

    def test_build_prompt_includes_literal_fragment_avoidance(self) -> None:
        prompt, _ = build_prompt(
            scope="global",
            round_index=0,
            target_count=3,
            context_packet={"product_core": "utility settlement"},
            literal_fragments=["priva", "parcl", "ledg"],
        )

        self.assertIn("Avoid clipped business fragments:", prompt)
        self.assertIn("priva-, parcl-, ledg-", prompt)

    def test_format_avoidance_block_includes_local_and_external_feedback(self) -> None:
        block = format_avoidance_block(
            {
                "local_examples": [
                    {"reason": "trigram_corpus_collision", "example": "precen:precerix"},
                ],
                "local_patterns": {
                    "prefixes": ["anch", "clar"],
                    "suffixes": ["ela", "len"],
                },
                "external_failures": {
                    "social_handle_crowded": ["baldex", "clarcel"],
                },
                "external_patterns": {
                    "prefixes": ["ren", "sta"],
                    "suffixes": ["dex", "ter"],
                },
                "external_terminal_families": ["la", "ra"],
                "external_lead_hints": ["samis", "parcl", "tenv"],
                "external_tail_hints": ["vela", "bela", "imen"],
                "external_fragment_hints": ["samis", "parcl", "tenv", "vela", "bela", "imen"],
                "external_reason_patterns": {
                    "web_near_collision": {
                        "examples": ["serevela", "hathera"],
                        "lead_hints": ["serev", "hathe"],
                        "tail_hints": ["vela", "hera"],
                        "terminal_families": ["la", "ra"],
                    }
                },
            }
        )

        self.assertIn("Crowded neighborhoods from recent collisions:", block)
        self.assertIn("precen:precerix", block)
        self.assertIn("anch-, clar-", block)
        self.assertIn("-ela, -len", block)
        self.assertIn("social handle crowded", block)
        self.assertIn("baldex, clarcel", block)
        self.assertIn("ren-, sta-", block)
        self.assertIn("-dex, -ter", block)
        self.assertIn("-la, -ra", block)
        self.assertIn("samis-, parcl-, tenv-", block)
        self.assertIn("-vela, -bela, -imen", block)
        self.assertIn("web near collision: names serevela, hathera", block)
        self.assertIn("lead serev-, hathe-", block)

    def test_avoidance_fragment_hints_normalizes_and_limits(self) -> None:
        hints = _avoidance_fragment_hints(
            {
                "external_fragment_hints": ["Samis", "parcl", "tenv", "samis", "  ", "CordN"],
                "external_tail_hints": ["Vela", "bela", "imen", "vela"],
            }
        )

        self.assertEqual(hints, ("samis", "parcl", "tenv", "cordn", "vela", "bela"))

    def test_format_positive_anchor_block_includes_recent_keepers(self) -> None:
        block = format_positive_anchor_block(
            {
                "names": ["planchiv", "covendel"],
                "endings": ["hiv", "del"],
            }
        )

        self.assertIn("Positive anchors from recent keepers:", block)
        self.assertIn("planchiv, covendel", block)
        self.assertIn("-hiv, -del", block)
        self.assertIn("Borrow their distinctiveness and distance from crowded namespace patterns", block)

    def test_sanitize_positive_anchor_context_does_not_backfill_from_seed_pool(self) -> None:
        sanitized = sanitize_positive_anchor_context(
            {
                "run_ids": [9],
                "names": ["vexlen", "splinter", "caldrea"],
                "endings": ["len", "ter", "rea"],
            },
            seed_pool=[
                SeedCandidate(name="anchora", archetype="transmute", source_score=0.9),
                SeedCandidate(name="clarita", archetype="transmute", source_score=0.9),
                SeedCandidate(name="habitan", archetype="transmute", source_score=0.9),
            ],
        )

        self.assertEqual(sanitized["run_ids"], [9])
        self.assertEqual(sanitized["names"], [])
        self.assertEqual(sanitized["endings"], [])

    def test_sanitize_positive_anchor_context_requires_multiple_strong_history_names(self) -> None:
        sanitized = sanitize_positive_anchor_context(
            {
                "run_ids": [12],
                "names": ["accorda", "anchan", "beacona", "caldrea"],
                "endings": ["rda", "han", "ona", "rea"],
            }
        )

        self.assertEqual(sanitized["names"], [])
        self.assertEqual(sanitized["endings"], [])

    def test_sanitize_positive_anchor_context_keeps_only_strong_validated_cluster(self) -> None:
        sanitized = sanitize_positive_anchor_context(
            {
                "run_ids": [14],
                "names": ["precela", "preceral", "baltera", "meridel", "beacona"],
                "endings": ["ela", "ral", "era", "del", "ona"],
            }
        )

        self.assertEqual(sanitized["names"], ["baltera", "meridel"])
        self.assertEqual(sanitized["endings"], ["era", "del"])

    def test_filter_exemplar_echoes_drops_close_remixes(self) -> None:
        kept, report = filter_exemplar_echoes(
            ["ancharlis", "charnlea", "veldora"],
            exemplars=["anchora", "charlea"],
            threshold=0.6,
        )

        self.assertEqual(kept, ["veldora"])
        self.assertEqual(report["dropped"], {"exemplar_echo": 2})
        self.assertEqual(len(report["examples"]), 2)

    def test_select_direct_seed_names_prefers_transmute_and_blend(self) -> None:
        names = select_direct_seed_names(
            [
                SeedCandidate(name="hardstopa", archetype="hardstop", source_score=0.9),
                SeedCandidate(name="anchora", archetype="transmute", source_score=0.9),
                SeedCandidate(name="meridel", archetype="blend", source_score=0.88),
                SeedCandidate(name="coinare", archetype="coined", source_score=0.95),
            ],
            limit=2,
        )

        self.assertEqual(names, ["anchora", "meridel"])

    def test_select_direct_seed_names_skips_flat_passes_below_stronger_floor(self) -> None:
        names = select_direct_seed_names(
            [
                SeedCandidate(name="cordastin", archetype="transmute", source_score=0.95),
                SeedCandidate(name="anchora", archetype="transmute", source_score=0.9),
            ],
            limit=2,
        )

        self.assertEqual(names, ["anchora"])

    def test_parse_candidate_payload_handles_braces_inside_json_strings(self) -> None:
        payload = """
        Here is the result:
        ```json
        {
          "candidates": [{"name": "Vantora"}],
          "notes": "keep the token {trust} in mind"
        }
        ```
        """
        self.assertEqual(parse_candidate_payload(payload), ["vantora"])

    def test_fixture_provider_reads_candidates_without_legacy_module(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            fixture_path = root / "fixture.json"
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "usage": {"prompt_tokens": 10, "completion_tokens": 4},
                      "candidates": [
                        {"name": "Vantora"},
                        {"name": " Certivo "},
                        {"name": "bad!"},
                        {"name": "rentiva"}
                      ]
                    }
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            names, report = generate_candidates(
                brief=Brief(product_core="utility-cost settlement software"),
                config=IdeationConfig(provider="fixture", fixture_input=fixture_path),
            )

            self.assertEqual(names, ["certivo", "rentiva", "vantora"])
            self.assertEqual(report["provider"], "fixture")
            self.assertEqual(report["usage"], {"prompt_tokens": 10, "completion_tokens": 4})

    def test_openai_compat_provider_uses_native_http_path(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(req, timeout=0):  # type: ignore[no-untyped-def]
            captured["url"] = req.full_url
            captured["timeout"] = timeout
            captured["headers"] = dict(req.header_items())
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeHttpResponse(
                {
                    "usage": {"prompt_tokens": 120, "completion_tokens": 30},
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "candidates": [
                                            {"name": "Vantora"},
                                            {"name": "Ledgero"},
                                            {"name": "Clarien"},
                                            {"name": "Taleris"},
                                        ]
                                    }
                                )
                            }
                        }
                    ],
                }
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            prompt_template = root / "prompt.txt"
            prompt_template.write_text(
                "Round {round_index} for {scope}. Need {target_count}. {context_block}",
                encoding="utf-8",
            )
            config = IdeationConfig(
                provider="openai_compat",
                model="local-test-model",
                rounds=1,
                candidates_per_round=3,
                timeout_ms=1500,
                prompt_template_file=prompt_template,
                openai_base_url="127.0.0.1:1234/v1",
                input_price_per_1k=0.5,
                output_price_per_1k=1.0,
                pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
            )
            brief = Brief(
                product_core="utility-cost settlement software",
                target_users=["property managers"],
                trust_signals=["clarity"],
            )

            with mock.patch(
                "brandpipe.ideation.generate_pseudoword_pool",
                return_value=(
                    ["zelnar", "trivex", "mirest", "cavlin", "rendax"],
                    {
                        "enabled": True,
                        "engine": "wuggy",
                        "language_plugin": "orthographic_english",
                        "generated_count": 5,
                        "warning": "",
                    },
                ),
            ):
                with mock.patch("brandpipe.ideation.request.urlopen", side_effect=fake_urlopen):
                    names, report = generate_candidates(brief=brief, config=config)

        self.assertTrue(any(name in names for name in {"beacona", "clarien"}))
        self.assertIn("taleris", names)
        self.assertIn("vantora", names)
        self.assertEqual(report["provider"], "openai_compat")
        self.assertEqual(report["model"], "local-test-model")
        self.assertGreaterEqual(report["candidate_count"], 3)
        self.assertEqual(report["errors"], [])
        self.assertAlmostEqual(float(report["cost_usd"]), 0.09, places=6)
        self.assertEqual(report["filtered_end_o"], 1)
        self.assertEqual(captured["url"], "http://127.0.0.1:1234/v1/chat/completions")
        assert isinstance(captured["body"], dict)
        self.assertEqual(captured["body"]["model"], "local-test-model")
        self.assertIn("response_format", captured["body"])
        self.assertIn("property managers", captured["body"]["messages"][0]["content"])
        self.assertIn("Round 1 for global", captured["body"]["messages"][0]["content"])

    def test_generate_candidates_supports_broadside_knobs(self) -> None:
        prompts: list[str] = []
        captured: dict[str, object] = {}
        bundle = LexiconBundle(
            core_terms=("ledger", "anchor", "signal"),
            modifiers=("steady", "lucid", "fair"),
            associative_terms=("harbor", "compass", "beacon"),
            morphemes=("talan", "meri", "nori", "vela"),
        )
        seed_pool = [
            SeedCandidate(name="talanor", archetype="coined", source_score=0.9),
            SeedCandidate(name="merivan", archetype="blend", source_score=0.88),
            SeedCandidate(name="noridel", archetype="transmute", source_score=0.86),
            SeedCandidate(name="velaris", archetype="coined", source_score=0.84),
            SeedCandidate(name="cavrien", archetype="blend", source_score=0.82),
            SeedCandidate(name="solveta", archetype="transmute", source_score=0.8),
        ]

        def fake_generate_seed_pool(*args, **kwargs):  # type: ignore[no-untyped-def]
            captured["seed_pool_total_limit"] = kwargs["total_limit"]
            return seed_pool, {"total": len(seed_pool), "blocked_fragments": []}

        def fake_filter_seed_candidates(candidates, *, avoid_terms, saturation_limit, **kwargs):  # type: ignore[no-untyped-def]
            captured["seed_saturation_limit"] = saturation_limit
            return list(candidates), {"kept": len(candidates), "saturation_limit": saturation_limit}

        def fake_filter_names(
            names,
            *,
            avoid_terms,
            saturation_limit,
            lead_fragment_limit,
            lead_fragment_length,
            lead_skeleton_limit,
            **kwargs,
        ):  # type: ignore[no-untyped-def]
            captured["name_saturation_limit"] = saturation_limit
            captured["lead_fragment_limit"] = lead_fragment_limit
            captured["lead_fragment_length"] = lead_fragment_length
            captured["lead_skeleton_limit"] = lead_skeleton_limit
            return list(names), {"kept": len(names), "saturation_limit": saturation_limit}

        def fake_call_provider_for_role(*, role_cfg, prompt, target_count, **kwargs):  # type: ignore[no-untyped-def]
            prompts.append(prompt)
            return (
                ["talanor", "merivan", "noridel", "velaris", "cavrien"],
                {
                    "resolved_model": role_cfg.model,
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                },
                "",
            )

        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            rounds=1,
            candidates_per_round=8,
            overgenerate_factor=2.5,
            round_seed_min=5,
            round_seed_max=7,
            seed_pool_multiplier=12,
            seed_saturation_limit=2,
            per_family_cap=4,
            lexicon_core_limit=2,
            lexicon_modifier_limit=2,
            lexicon_associative_limit=2,
            lexicon_morpheme_limit=3,
            local_filter_saturation_limit=2,
            local_filter_lead_fragment_limit=1,
            local_filter_lead_fragment_length=4,
            local_filter_lead_skeleton_limit=2,
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=6),
            roles=(
                IdeationRoleConfig(
                    model="local-test-model",
                    role="phonetic_explorer",
                    temperature=1.0,
                    weight=1,
                ),
            ),
        )

        with mock.patch("brandpipe.ideation.build_lexicon", return_value=(bundle, {"source": "test"})):
            with mock.patch(
                "brandpipe.ideation.generate_pseudoword_pool",
                return_value=(["zelnar", "mirest", "cavlin", "solven", "talmer", "noriva"], {"generated_count": 6}),
            ):
                with mock.patch("brandpipe.ideation.generate_seed_pool", side_effect=fake_generate_seed_pool):
                    with mock.patch("brandpipe.ideation.filter_seed_candidates", side_effect=fake_filter_seed_candidates):
                        with mock.patch("brandpipe.ideation.filter_names", side_effect=fake_filter_names):
                            with mock.patch("brandpipe.ideation.select_direct_seed_names", return_value=[]):
                                with mock.patch("brandpipe.ideation._call_provider_for_role", side_effect=fake_call_provider_for_role):
                                    names, report = generate_candidates(
                                        brief=Brief(product_core="utility-cost settlement software"),
                                        config=config,
                                    )

        self.assertEqual(names, ["talanor", "merivan", "noridel", "velaris", "cavrien"])
        self.assertEqual(captured["seed_pool_total_limit"], 96)
        self.assertEqual(captured["seed_saturation_limit"], 2)
        self.assertEqual(captured["name_saturation_limit"], 2)
        self.assertEqual(captured["lead_fragment_limit"], 1)
        self.assertEqual(captured["lead_fragment_length"], 4)
        self.assertEqual(captured["lead_skeleton_limit"], 2)
        self.assertEqual(report["pseudoword"]["used_per_round"], [5])
        self.assertEqual(report["broadside"]["per_family_cap"], 4)
        self.assertEqual(report["broadside"]["lexicon_morpheme_limit"], 3)
        self.assertEqual(report["broadside"]["local_filter_lead_fragment_limit"], 1)
        self.assertEqual(report["broadside"]["local_filter_lead_skeleton_limit"], 2)
        self.assertTrue(prompts)
        prompt = prompts[0]
        self.assertIn("core_terms: ledger, anchor", prompt)
        self.assertNotIn("core_terms: ledger, anchor, signal", prompt)
        self.assertIn("morphemes: talan, meri, nori", prompt)
        self.assertNotIn("morphemes: talan, meri, nori, vela", prompt)

    def test_openai_compat_does_not_retry_on_404(self) -> None:
        attempts = {"count": 0}

        def fake_post_json(**kwargs):  # type: ignore[no-untyped-def]
            attempts["count"] += 1
            return None, "http_404"

        with mock.patch("brandpipe.ideation._post_json", side_effect=fake_post_json):
            names, usage, err = call_openai_compat_candidates(
                api_key="ollama",
                base_url="127.0.0.1:1234/v1",
                model="missing-model",
                prompt="{}",
                timeout_ms=1000,
                strict_json=True,
            )

        self.assertEqual(names, [])
        self.assertEqual(usage, {})
        self.assertEqual(err, "http_404")
        self.assertEqual(attempts["count"], 1)

    def test_openai_compat_retries_transient_http_errors_with_backoff(self) -> None:
        responses = iter(
            [
                (None, "http_429"),
                (
                    {
                        "usage": {"prompt_tokens": 20, "completion_tokens": 10},
                        "choices": [
                            {
                                "message": {
                                    "content": json.dumps({"candidates": [{"name": "Vantora"}]})
                                }
                            }
                        ],
                    },
                    "",
                ),
            ]
        )

        def fake_post_json(**kwargs):  # type: ignore[no-untyped-def]
            return next(responses)

        with mock.patch("brandpipe.ideation._post_json", side_effect=fake_post_json) as post_mock:
            with mock.patch("brandpipe.ideation.time.sleep") as sleep_mock:
                names, usage, err = call_openai_compat_candidates(
                    api_key="ollama",
                    base_url="127.0.0.1:1234/v1",
                    model="local-model",
                    prompt="{}",
                    timeout_ms=1000,
                    strict_json=True,
                )

        self.assertEqual(names, ["vantora"])
        self.assertEqual(err, "")
        self.assertEqual(usage, {"prompt_tokens": 20, "completion_tokens": 10})
        self.assertEqual(post_mock.call_count, 2)
        sleep_mock.assert_called_once()

    def test_openrouter_caps_completion_tokens_and_disables_reasoning_for_kimi(self) -> None:
        captured: list[dict[str, object]] = []

        def fake_post_json(**kwargs):  # type: ignore[no-untyped-def]
            captured.append(kwargs["payload"])
            return (
                {
                    "usage": {"prompt_tokens": 30, "completion_tokens": 12},
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps({"candidates": [{"name": "Clarien"}]})
                            }
                        }
                    ],
                },
                "",
            )

        with mock.patch("brandpipe.ideation._post_json", side_effect=fake_post_json):
            names, usage, err = call_openrouter_candidates(
                api_key="test-key",
                model="moonshotai/kimi-k2.5",
                prompt="{}",
                target_count=2,
                timeout_ms=1000,
                strict_json=True,
                temperature=0.7,
            )

        self.assertEqual(names, ["clarien"])
        self.assertEqual(err, "")
        self.assertEqual(usage["attempt_count"], 1)
        self.assertEqual(usage["response_mode"], "json_schema")
        self.assertEqual(usage["max_completion_tokens"], 256)
        self.assertEqual(usage["response_preview"], '{"candidates": [{"name": "Clarien"}]}')
        self.assertEqual(len(captured), 1)
        payload = captured[0]
        self.assertEqual(payload["max_completion_tokens"], 256)
        self.assertEqual(payload["reasoning"], {"effort": "none", "exclude": True})
        self.assertIn("response_format", payload)

    def test_openrouter_prefers_json_object_mode_for_gemini_family(self) -> None:
        captured: list[dict[str, object]] = []

        def fake_post_json(**kwargs):  # type: ignore[no-untyped-def]
            captured.append(kwargs["payload"])
            return (
                {
                    "usage": {"prompt_tokens": 22, "completion_tokens": 10},
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps({"candidates": [{"name": "Taleris"}]})
                            }
                        }
                    ],
                },
                "",
            )

        with mock.patch("brandpipe.ideation._post_json", side_effect=fake_post_json):
            names, usage, err = call_openrouter_candidates(
                api_key="test-key",
                model="google/gemini-3.1-pro-preview",
                prompt="{}",
                target_count=1,
                timeout_ms=1000,
                strict_json=True,
                temperature=0.7,
            )

        self.assertEqual(names, ["taleris"])
        self.assertEqual(err, "")
        self.assertEqual(usage["attempt_count"], 1)
        self.assertEqual(usage["response_mode"], "json_object")
        self.assertEqual(len(captured), 1)
        payload = captured[0]
        self.assertEqual(payload["max_completion_tokens"], 1024)
        self.assertEqual(payload["response_format"], {"type": "json_object"})
        self.assertNotIn("reasoning", payload)

    def test_openrouter_parse_failure_keeps_response_preview(self) -> None:
        responses = iter(
            [
                (
                    {
                        "usage": {"prompt_tokens": 22, "completion_tokens": 12},
                        "choices": [
                            {
                                "message": {
                                    "content": '{"candidates":[{"name":"clarina"},{"'
                                }
                            }
                        ],
                    },
                    "",
                ),
                (
                    {
                        "usage": {"prompt_tokens": 22, "completion_tokens": 12},
                        "choices": [
                            {
                                "message": {
                                    "content": "Here is the JSON requested:\n```"
                                }
                            }
                        ],
                    },
                    "",
                ),
                (
                    {
                        "usage": {"prompt_tokens": 22, "completion_tokens": 12},
                        "choices": [
                            {
                                "message": {
                                    "content": None
                                }
                            }
                        ],
                    },
                    "",
                ),
            ]
        )

        def fake_post_json(**kwargs):  # type: ignore[no-untyped-def]
            return next(responses)

        with mock.patch("brandpipe.ideation._post_json", side_effect=fake_post_json):
            names, usage, err = call_openrouter_candidates(
                api_key="test-key",
                model="google/gemini-3.1-pro-preview",
                prompt="{}",
                target_count=2,
                timeout_ms=1000,
                strict_json=True,
                temperature=0.7,
            )

        self.assertEqual(names, [])
        self.assertEqual(err, "candidate_parse_failed")
        self.assertEqual(usage["attempt_count"], 3)
        self.assertEqual(usage["response_mode"], "plain")
        self.assertEqual(usage["max_completion_tokens"], 1024)
        self.assertEqual(usage["response_preview"], "Here is the JSON requested: ```")

    def test_payload_and_fixture_helpers_cover_fallbacks(self) -> None:
        raw = """
        preface
        ```
        {"candidates":[{"name":"Vantora"}]}
        ```
        suffix
        """
        self.assertEqual(
            ideation.extract_json_object(raw),
            '{"candidates":[{"name":"Vantora"}]}',
        )
        self.assertIsNone(ideation.extract_json_object("no dict here"))
        self.assertIsNone(ideation._load_candidate_payload(""))
        self.assertEqual(
            ideation._load_candidate_payload(raw),
            {"candidates": [{"name": "Vantora"}]},
        )
        self.assertEqual(ideation._candidate_source({"candidates": ["alpha"]}), ["alpha"])
        self.assertEqual(ideation._candidate_source({"names": ["beta"]}), ["beta"])
        self.assertEqual(ideation._candidate_source(["gamma"]), ["gamma"])
        self.assertEqual(ideation._candidate_source("nope"), [])
        self.assertEqual(
            ideation.extract_candidate_names(
                '{"names":["Alpha", {"candidate":"Beta"}, {"name":"Gamma"}, {"ignored":1}]}'
            ),
            ["Alpha", "Beta", "Gamma"],
        )
        self.assertEqual(
            ideation.parse_candidate_payload('["Vantora", "vantora", "ab", "Meridel"]'),
            ["meridel", "vantora"],
        )

        self.assertEqual(ideation.extract_response_content({}), ("", {}, "missing_choices"))
        self.assertEqual(
            ideation.extract_response_content(
                {
                    "usage": {"prompt_tokens": 4},
                    "choices": [{"message": {"content": [{"text": "Alpha"}, {"text": "Beta"}]}}],
                }
            ),
            ("Alpha\nBeta", {"prompt_tokens": 4}, ""),
        )
        self.assertEqual(
            ideation.extract_response_content({"choices": [{"message": {"content": 123}}]}),
            ("123", {}, ""),
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            self.assertEqual(
                ideation.load_fixture_candidates_with_usage(root / "missing.json"),
                ([], {}, "fixture_missing"),
            )

            read_error_path = root / "fixture_dir"
            read_error_path.mkdir()
            self.assertEqual(
                ideation.load_fixture_candidates_with_usage(read_error_path),
                ([], {}, "fixture_read_error"),
            )

            parse_fail_path = root / "parse_fail.json"
            parse_fail_path.write_text(
                json.dumps(
                    {
                        "usage": {"prompt_tokens": 9},
                        "choices": [{"message": {"content": "not useful"}}],
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                ideation.load_fixture_candidates_with_usage(parse_fail_path),
                ([], {"prompt_tokens": 9}, "candidate_parse_failed"),
            )

            fallback_path = root / "fallback.txt"
            fallback_path.write_text("- Vantora\n- meridel\n- ab\n", encoding="utf-8")
            self.assertEqual(
                ideation.load_fixture_candidates_with_usage(fallback_path),
                (["meridel", "vantora"], {}, ""),
            )

            empty_path = root / "empty.txt"
            empty_path.write_text("###\n--\n", encoding="utf-8")
            self.assertEqual(
                ideation.load_fixture_candidates_with_usage(empty_path),
                ([], {}, "fixture_no_candidates"),
            )

    def test_openai_compat_includes_pseudoword_seed_pool_in_prompt(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(req, timeout=0):  # type: ignore[no-untyped-def]
            captured["body"] = json.loads(req.data.decode("utf-8"))
            return _FakeHttpResponse(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {
                                        "candidates": [
                                            {"name": "Vantora"},
                                            {"name": "Clarien"},
                                            {"name": "Balanix"},
                                        ]
                                    }
                                )
                            }
                        }
                    ]
                }
            )

        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            rounds=1,
            candidates_per_round=4,
            timeout_ms=1500,
            openai_base_url="127.0.0.1:1234/v1",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
        )
        brief = Brief(product_core="utility-cost settlement software")

        # Replace mock seed candidates with plain objects carrying the expected shape.
        class _Seed:
            def __init__(self, name: str) -> None:
                self.name = name

        with mock.patch(
            "brandpipe.ideation.generate_pseudoword_pool",
            return_value=(
                ["zelnar", "trivex", "mirest", "cavlin", "rendax"],
                {
                    "enabled": True,
                    "engine": "wuggy",
                    "language_plugin": "orthographic_english",
                    "generated_count": 5,
                    "warning": "",
                },
            ),
        ):
            with mock.patch(
                "brandpipe.ideation.generate_seed_pool",
                return_value=(
                    [_Seed("zelnar"), _Seed("trivex"), _Seed("mirest"), _Seed("cavlin"), _Seed("rendax")],
                    {"total": 5, "archetypes": {"coined": 5}},
                ),
            ):
                with mock.patch("brandpipe.ideation.request.urlopen", side_effect=fake_urlopen):
                    names, report = generate_candidates(brief=brief, config=config)

        self.assertIn("clarien", names)
        self.assertIn("vantora", names)
        assert isinstance(captured["body"], dict)
        prompt = captured["body"]["messages"][0]["content"]
        self.assertIn("Phonotactic seed shapes:", prompt)
        self.assertIn("zelnar", prompt)
        self.assertIn("trivex", prompt)
        self.assertEqual(report["pseudoword"]["generated_count"], 5)
        self.assertEqual(report["pseudoword"]["used_per_round"], [3])
        self.assertEqual(report["seed_pool"]["archetypes"], {"coined": 5})

    def test_generate_candidates_promotes_direct_seed_names(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            rounds=1,
            candidates_per_round=2,
            openai_base_url="127.0.0.1:1234/v1",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
        )
        brief = Brief(product_core="utility-cost settlement software")

        with mock.patch(
            "brandpipe.ideation.generate_pseudoword_pool",
            return_value=(["zelnar"], {"enabled": True, "engine": "wuggy", "generated_count": 1, "warning": ""}),
        ):
            with mock.patch(
                "brandpipe.ideation.generate_seed_pool",
                return_value=(
                    [
                        SeedCandidate(name="anchora", archetype="transmute", source_score=0.9),
                        SeedCandidate(name="meridel", archetype="blend", source_score=0.88),
                    ],
                    {"total": 2, "archetypes": {"transmute": 1, "blend": 1}, "blocked_fragments": []},
                ),
            ):
                with mock.patch(
                    "brandpipe.ideation.call_openai_compat_candidates",
                    return_value=(["vantora"], {"prompt_tokens": 12, "completion_tokens": 6}, ""),
                ):
                    names, report = generate_candidates(brief=brief, config=config)

        self.assertIn("anchora", names)
        self.assertEqual(report["direct_seed_candidates"][0]["names"], ["anchora", "meridel"])

    def test_non_fixture_provider_requires_pseudoword_stage(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            openai_base_url="127.0.0.1:1234/v1",
        )

        with self.assertRaisesRegex(ValueError, "ideation.pseudoword is required"):
            generate_candidates(
                brief=Brief(product_core="utility-cost settlement software"),
                config=config,
            )

    def test_pseudoword_stage_failure_errors_out(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            openai_base_url="127.0.0.1:1234/v1",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
        )

        with mock.patch(
            "brandpipe.ideation.generate_pseudoword_pool",
            return_value=(
                [],
                {
                    "enabled": True,
                    "engine": "wuggy",
                    "language_plugin": "orthographic_english",
                    "generated_count": 0,
                    "warning": "wuggy_unavailable",
                    "error_message": "No module named wuggy",
                },
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "pseudoword seed stage failed: wuggy_unavailable"):
                generate_candidates(
                    brief=Brief(product_core="utility-cost settlement software"),
                    config=config,
                )

    def test_pseudoword_stage_allows_degraded_yield_when_names_exist(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            openai_base_url="127.0.0.1:1234/v1",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
        )

        with mock.patch(
            "brandpipe.ideation.generate_pseudoword_pool",
            return_value=(
                ["zelnar", "trivex", "mirest"],
                {
                    "enabled": True,
                    "engine": "wuggy",
                    "language_plugin": "orthographic_english",
                    "generated_count": 3,
                    "warning": "insufficient_pseudoword_yield",
                },
            ),
        ):
            with mock.patch(
                "brandpipe.ideation.generate_seed_pool",
                return_value=(
                    [
                        SeedCandidate(name="anchora", archetype="transmute", source_score=0.9),
                        SeedCandidate(name="meridel", archetype="blend", source_score=0.88),
                    ],
                    {"total": 2, "archetypes": {"transmute": 1, "blend": 1}, "blocked_fragments": []},
                ),
            ):
                with mock.patch(
                    "brandpipe.ideation.call_openai_compat_candidates",
                    return_value=(["vantora"], {"prompt_tokens": 12, "completion_tokens": 6}, ""),
                ):
                    names, report = generate_candidates(
                        brief=Brief(product_core="utility-cost settlement software"),
                        config=config,
                    )

        self.assertIn("vantora", names)
        self.assertEqual(report["pseudoword"]["warning"], "insufficient_pseudoword_yield")

    def test_multi_role_ideation_aggregates_successful_models(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            openai_base_url="127.0.0.1:1234/v1",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
            roles=(
                IdeationRoleConfig(model="model-a", role="creative_divergence", temperature=0.7, weight=2),
                IdeationRoleConfig(model="model-b", role="recombinator", temperature=0.6, weight=1),
            ),
        )

        def fake_call(**kwargs):  # type: ignore[no-untyped-def]
            model = kwargs["model"]
            if model == "model-a":
                return ["vantora", "clarien"], {"prompt_tokens": 10, "completion_tokens": 5}, ""
            return [], {}, "http_503"

        class _Seed:
            def __init__(self, name: str) -> None:
                self.name = name

        with mock.patch(
            "brandpipe.ideation.generate_pseudoword_pool",
            return_value=(["zelnar", "trivex"], {"enabled": True, "engine": "wuggy", "generated_count": 2, "warning": ""}),
        ):
            with mock.patch(
                "brandpipe.ideation.generate_seed_pool",
                return_value=(
                    [_Seed("zelnar"), _Seed("trivex"), _Seed("mirest")],
                    {"total": 3, "archetypes": {"coined": 3}},
                ),
            ):
                with mock.patch("brandpipe.ideation.call_openai_compat_candidates", side_effect=fake_call):
                    names, report = generate_candidates(
                        brief=Brief(product_core="utility-cost settlement software"),
                        config=config,
                    )

        self.assertCountEqual(names, ["clarien", "vantora"])
        self.assertEqual(len(report["roles"]), 2)
        self.assertTrue(any(item["error"] == "http_503" for item in report["roles"]))
        self.assertTrue(all("latency_ms" in item for item in report["roles"]))
        self.assertTrue(any(item["status"] == "error" for item in report["roles"]))

    def test_openrouter_404_uses_static_model_fallback(self) -> None:
        config = IdeationConfig(
            provider="openrouter_http",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
            roles=(
                IdeationRoleConfig(
                    model="mistralai/mistral-small-creative",
                    role="creative_divergence",
                    temperature=0.7,
                    weight=1,
                ),
            ),
        )
        calls: list[str] = []

        def fake_call(**kwargs):  # type: ignore[no-untyped-def]
            model = kwargs["model"]
            calls.append(model)
            if model == "mistralai/mistral-small-creative":
                return [], {}, "http_404"
            return ["clarien", "taleris"], {"prompt_tokens": 12, "completion_tokens": 6}, ""

        class _Seed:
            def __init__(self, name: str) -> None:
                self.name = name

        with mock.patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}, clear=False):
            with mock.patch(
                "brandpipe.ideation.generate_pseudoword_pool",
                return_value=(["zelnar", "trivex"], {"enabled": True, "engine": "wuggy", "generated_count": 2, "warning": ""}),
            ):
                with mock.patch(
                    "brandpipe.ideation.generate_seed_pool",
                    return_value=(
                        [_Seed("zelnar"), _Seed("trivex"), _Seed("mirest")],
                        {"total": 3, "archetypes": {"coined": 3}},
                    ),
                ):
                    with mock.patch("brandpipe.ideation.call_openrouter_candidates", side_effect=fake_call):
                        names, report = generate_candidates(
                            brief=Brief(product_core="utility-cost settlement software"),
                            config=config,
                        )

        self.assertCountEqual(names, ["clarien", "taleris"])
        self.assertEqual(calls, ["mistralai/mistral-small-creative", "moonshotai/kimi-k2.5"])
        self.assertEqual(report["errors"], [])
        self.assertEqual(report["roles"][0]["requested_model"], "mistralai/mistral-small-creative")
        self.assertEqual(report["roles"][0]["model"], "moonshotai/kimi-k2.5")
        self.assertEqual(report["roles"][0]["attempt_count"], 0)

    def test_openrouter_fallback_exhaustion_still_fails_cleanly(self) -> None:
        config = IdeationConfig(
            provider="openrouter_http",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
            roles=(IdeationRoleConfig(model="mistralai/mistral-small-creative"),),
        )
        calls: list[str] = []

        def fake_call(**kwargs):  # type: ignore[no-untyped-def]
            calls.append(kwargs["model"])
            return [], {}, "http_404"

        class _Seed:
            def __init__(self, name: str) -> None:
                self.name = name

        with mock.patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}, clear=False):
            with mock.patch(
                "brandpipe.ideation.generate_pseudoword_pool",
                return_value=(["zelnar", "trivex"], {"enabled": True, "engine": "wuggy", "generated_count": 2, "warning": ""}),
            ):
                with mock.patch(
                    "brandpipe.ideation.generate_seed_pool",
                    return_value=(
                        [_Seed("zelnar"), _Seed("trivex"), _Seed("mirest")],
                        {"total": 3, "archetypes": {"coined": 3}},
                    ),
                ):
                    with mock.patch("brandpipe.ideation.call_openrouter_candidates", side_effect=fake_call):
                        with self.assertRaisesRegex(RuntimeError, "ideation failed: creative_divergence:mistralai/mistral-small-creative:http_404"):
                            generate_candidates(
                                brief=Brief(product_core="utility-cost settlement software"),
                                config=config,
                            )

        self.assertEqual(calls, ["mistralai/mistral-small-creative", "moonshotai/kimi-k2.5"])

    def test_salvage_path_keeps_small_relaxed_set_when_diversity_wipes_out_names(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            model="local-test-model",
            openai_base_url="127.0.0.1:1234/v1",
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
        )

        class _Seed:
            def __init__(self, name: str) -> None:
                self.name = name

        with mock.patch(
            "brandpipe.ideation.generate_pseudoword_pool",
            return_value=(["zelnar", "trivex"], {"enabled": True, "engine": "wuggy", "generated_count": 2, "warning": ""}),
        ):
            with mock.patch(
                "brandpipe.ideation.generate_seed_pool",
                return_value=(
                    [_Seed("zelnar"), _Seed("trivex"), _Seed("mirest")],
                    {"total": 3, "archetypes": {"coined": 3}},
                ),
            ):
                with mock.patch(
                    "brandpipe.ideation.call_openai_compat_candidates",
                    return_value=(["clarien", "clariel"], {"prompt_tokens": 12, "completion_tokens": 6}, ""),
                ):
                    with mock.patch(
                        "brandpipe.ideation.filter_names",
                        return_value=([], {"input_count": 2, "kept": 0, "dropped": {"phonetic_duplicate": 2}}),
                    ):
                        with mock.patch(
                            "brandpipe.ideation.salvage_names",
                            return_value=(
                                ["clarien"],
                                {"input_count": 2, "kept": 1, "compression_ratio": 0.5, "dropped": {}, "mode": "salvage_exact_only"},
                            ),
                        ):
                            names, report = generate_candidates(
                                brief=Brief(product_core="utility-cost settlement software"),
                                config=config,
                            )

        self.assertEqual(names, ["clarien"])
        self.assertTrue(report["name_diversity"]["relaxed"])
        self.assertEqual(report["name_diversity"]["salvage"]["mode"], "salvage_exact_only")
        self.assertEqual(report["candidate_count"], 1)

    def test_openrouter_role_target_overgenerates_for_ideation_only(self) -> None:
        config = IdeationConfig(
            provider="openrouter_http",
            candidates_per_round=2,
            overgenerate_factor=2.0,
            pseudoword=PseudowordConfig(language_plugin="orthographic_english", seed_count=5),
            roles=(IdeationRoleConfig(model="moonshotai/kimi-k2.5"),),
        )
        captured_targets: list[int] = []

        def fake_call(**kwargs):  # type: ignore[no-untyped-def]
            captured_targets.append(int(kwargs["target_count"]))
            return ["clarien", "taleris"], {"prompt_tokens": 12, "completion_tokens": 8}, ""

        class _Seed:
            def __init__(self, name: str) -> None:
                self.name = name

        with mock.patch.dict(os.environ, {"OPENROUTER_API_KEY": "test-key"}, clear=False):
            with mock.patch(
                "brandpipe.ideation.generate_pseudoword_pool",
                return_value=(["zelnar", "trivex"], {"enabled": True, "engine": "wuggy", "generated_count": 2, "warning": ""}),
            ):
                with mock.patch(
                    "brandpipe.ideation.generate_seed_pool",
                    return_value=(
                        [_Seed("zelnar"), _Seed("trivex"), _Seed("mirest")],
                        {"total": 3, "archetypes": {"coined": 3}},
                    ),
                ):
                    with mock.patch("brandpipe.ideation.call_openrouter_candidates", side_effect=fake_call):
                        names, report = generate_candidates(
                            brief=Brief(product_core="utility-cost settlement software"),
                            config=config,
                        )

        self.assertEqual(captured_targets, [4])
        self.assertEqual(names, ["clarien", "taleris"])
        self.assertEqual(report["roles"][0]["desired_target"], 2)
        self.assertEqual(report["roles"][0]["requested_target"], 4)
        self.assertEqual(report["overgenerate_factor"], 2.0)

    def test_transport_and_provider_helpers_cover_edge_cases(self) -> None:
        self.assertEqual(ideation._normalize_openai_compat_base_url(""), "https://api.openai.com/v1")
        self.assertEqual(
            ideation._normalize_openai_compat_base_url("127.0.0.1:1234/v1"),
            "http://127.0.0.1:1234/v1",
        )
        self.assertEqual(
            ideation._normalize_openai_compat_base_url("https://example.com/v1/"),
            "https://example.com/v1",
        )
        self.assertEqual(
            ideation._normalize_openai_compat_base_url("bad url"),
            "https://api.openai.com/v1",
        )
        self.assertEqual(ideation._normalize_openrouter_http_referer(""), "")
        self.assertEqual(
            ideation._normalize_openrouter_http_referer('"example.com/brandpipe"'),
            "https://example.com/brandpipe",
        )
        self.assertEqual(
            ideation._normalize_openrouter_http_referer("https://app.example/path"),
            "https://app.example/path",
        )
        self.assertEqual(ideation._normalize_openrouter_http_referer("bad referer value"), "")
        self.assertEqual(ideation._temperature("oops"), 0.8)
        self.assertEqual(ideation._temperature(-1.0), 0.0)
        self.assertEqual(ideation._temperature(9.0), 2.0)
        self.assertEqual(ideation._retry_delay_seconds(0), 0.25)
        self.assertEqual(ideation._retry_delay_seconds(10), 2.0)
        self.assertEqual(ideation._max_completion_tokens("google/gemini-3.1-pro", 4), 1024)
        self.assertEqual(ideation._max_completion_tokens("openai/gpt-test", 2), 256)
        self.assertEqual(
            ideation._openrouter_reasoning_payload("moonshotai/kimi-k2.5"),
            {"effort": "none", "exclude": True},
        )
        self.assertIsNone(ideation._openrouter_reasoning_payload(""))
        self.assertEqual(
            ideation._openrouter_response_modes("google/gemini-3.1-pro"),
            ("json_object", "json_schema", "plain"),
        )
        self.assertEqual(
            ideation._openrouter_response_modes("openai/gpt-test"),
            ("json_schema", "json_object", "plain"),
        )
        self.assertEqual(ideation._response_preview("  too \n many \t spaces  ", limit=8), "too many")
        self.assertEqual(ideation._response_preview("", limit=8), "")

        self.assertEqual(
            ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=0),
            (None, "invalid_timeout"),
        )
        with mock.patch("brandpipe.ideation.request.urlopen", return_value=_FakeHttpResponse({"ok": True})):
            response, err = ideation._post_json(
                url="https://example.com",
                headers={"X-Test": "1"},
                payload={"hello": "world"},
                timeout_ms=1000,
            )
        self.assertEqual(response, {"ok": True})
        self.assertEqual(err, "")
        with mock.patch("brandpipe.ideation.request.urlopen", return_value=_RawHttpResponse(b"not-json")):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "response_json_decode_error"),
            )
        with mock.patch("brandpipe.ideation.request.urlopen", return_value=_RawHttpResponse(b"[]")):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "response_invalid_root"),
            )
        with mock.patch(
            "brandpipe.ideation.request.urlopen",
            side_effect=error.HTTPError("https://example.com", 503, "boom", hdrs=None, fp=None),
        ):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "http_503"),
            )
        with mock.patch("brandpipe.ideation.request.urlopen", side_effect=error.URLError(socket.timeout("slow"))):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "timeout"),
            )
        with mock.patch("brandpipe.ideation.request.urlopen", side_effect=error.URLError(OSError("no route"))):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "network_error"),
            )
        with mock.patch("brandpipe.ideation.request.urlopen", side_effect=socket.timeout("slow")):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "timeout"),
            )
        with mock.patch("brandpipe.ideation.request.urlopen", side_effect=http.client.HTTPException("bad")):
            self.assertEqual(
                ideation._post_json(url="https://example.com", headers={}, payload={}, timeout_ms=1000),
                (None, "network_error"),
            )

        explicit_roles = (
            IdeationRoleConfig(model="alpha", role="creative_divergence", temperature=0.3),
            IdeationRoleConfig(model="beta", role="recombinator", temperature=0.5),
        )
        self.assertEqual(
            ideation._ideation_roles(IdeationConfig(provider="openrouter_http", roles=explicit_roles)),
            explicit_roles,
        )
        model_role = ideation._ideation_roles(
            IdeationConfig(provider="openrouter_http", model="llama", temperature=0.6)
        )
        self.assertEqual(len(model_role), 1)
        self.assertEqual(model_role[0].model, "llama")
        self.assertEqual(model_role[0].role, "creative_divergence")
        self.assertEqual(model_role[0].temperature, 0.6)
        self.assertEqual(ideation._ideation_roles(IdeationConfig(provider="openrouter_http")), ())
        self.assertTrue(ideation._candidate_schema(True)["strict"])
        self.assertFalse(ideation._candidate_schema(False)["strict"])
        self.assertEqual(
            ideation.estimate_usage_cost_usd(
                usage={"cost": "1.234567891"},
                in_price_per_1k=0.0,
                out_price_per_1k=0.0,
            ),
            1.23456789,
        )
        self.assertEqual(
            ideation.estimate_usage_cost_usd(
                usage={"prompt_tokens": 500, "completion_tokens": 250},
                in_price_per_1k=2.0,
                out_price_per_1k=4.0,
            ),
            2.0,
        )

        role_cfg = IdeationRoleConfig(model="mistralai/mistral-small-creative", temperature=0.7)
        config = IdeationConfig(provider="openrouter_http", api_key_env="BRANDPIPE_TEST_KEY", timeout_ms=500)
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "missing env BRANDPIPE_TEST_KEY"):
                ideation._call_provider_for_role(
                    provider="openrouter_http",
                    config=config,
                    role_cfg=role_cfg,
                    prompt="prompt",
                    target_count=3,
                )
        with (
            mock.patch.dict(os.environ, {"BRANDPIPE_TEST_KEY": "secret"}, clear=True),
            mock.patch(
                "brandpipe.ideation.call_openrouter_candidates",
                side_effect=[
                    ([], {}, "http_404"),
                    (["vantora"], {"prompt_tokens": 11}, ""),
                ],
            ) as call_openrouter_mock,
        ):
            names, usage, err = ideation._call_provider_for_role(
                provider="openrouter_http",
                config=config,
                role_cfg=role_cfg,
                prompt="prompt",
                target_count=3,
            )
        self.assertEqual(names, ["vantora"])
        self.assertEqual(err, "")
        self.assertEqual(usage["fallback_from"], "mistralai/mistral-small-creative")
        self.assertEqual(usage["resolved_model"], "moonshotai/kimi-k2.5")
        self.assertEqual(call_openrouter_mock.call_count, 2)

        compat_config = IdeationConfig(provider="openai_compat", openai_base_url="http://127.0.0.1:1234/v1")
        compat_role = IdeationRoleConfig(model="llama")
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch(
                "brandpipe.ideation.call_openai_compat_candidates",
                return_value=(["meridel"], {"prompt_tokens": 3}, ""),
            ) as call_openai_mock,
        ):
            names, usage, err = ideation._call_provider_for_role(
                provider="openai_compat",
                config=compat_config,
                role_cfg=compat_role,
                prompt="prompt",
                target_count=2,
            )
        self.assertEqual(names, ["meridel"])
        self.assertEqual(err, "")
        self.assertEqual(usage, {"prompt_tokens": 3})
        self.assertEqual(call_openai_mock.call_args.kwargs["api_key"], "ollama")

    def test_diversity_helpers_cover_family_extension_branches(self) -> None:
        self.assertEqual(ideation._ending_family("solaria"), "aria")
        self.assertEqual(ideation._ending_family("qq"), "qq")

        current_names: list[str] = []
        seen: set[str] = set()
        family_counts = {"a": 1}
        filtered_end_o, filtered_family = ideation._extend_diverse_names(
            current_names=current_names,
            seen=seen,
            family_counts=family_counts,
            round_names=["baltera", "neo", "meridel", "meridel", "solaria"],
            per_family_cap=1,
        )
        self.assertEqual(filtered_end_o, 1)
        self.assertEqual(filtered_family, 2)
        self.assertEqual(current_names, ["meridel", "solaria"])
        self.assertEqual(family_counts["el"], 1)
        self.assertEqual(family_counts["aria"], 1)
        self.assertEqual(seen, {"meridel", "solaria"})


if __name__ == "__main__":
    unittest.main()
