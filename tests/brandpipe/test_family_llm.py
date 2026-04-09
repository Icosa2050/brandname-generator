# ruff: noqa: E402
from __future__ import annotations

import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from brandpipe.family_llm import (
    _accept_candidate,
    _call_provider_for_family,
    _fixture_surface_candidates,
    _parse_surface_candidate_payload,
    _render_prompt,
    _resolve_prompt_path,
    _role_configs,
    generate_family_candidates,
)
from brandpipe.models import Brief, IdeationConfig, IdeationRoleConfig, NameFamily


class FamilyLlmTests(unittest.TestCase):
    def test_role_configs_prefers_fixture_provider(self) -> None:
        config = IdeationConfig(
            provider="fixture",
            model="ignored-model",
            roles=(IdeationRoleConfig(model="custom", role="recombinator", temperature=0.3, weight=2),),
            temperature=0.61,
        )

        resolved = _role_configs(config)

        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolved[0].model, "fixture")
        self.assertEqual(resolved[0].role, "creative_divergence")
        self.assertEqual(resolved[0].temperature, 0.61)

    def test_role_configs_uses_roles_then_model_then_empty(self) -> None:
        explicit_roles = (
            IdeationRoleConfig(model="alpha", role="creative_divergence", temperature=0.9, weight=2),
            IdeationRoleConfig(model="beta", role="recombinator", temperature=0.4, weight=1),
        )
        config_with_roles = IdeationConfig(provider="openrouter_http", roles=explicit_roles)
        config_with_model = IdeationConfig(provider="openrouter_http", model="gpt-family", temperature=0.27)
        config_empty = IdeationConfig(provider="openrouter_http")

        self.assertEqual(_role_configs(config_with_roles), explicit_roles)

        model_only = _role_configs(config_with_model)
        self.assertEqual(len(model_only), 1)
        self.assertEqual(model_only[0].model, "gpt-family")
        self.assertEqual(model_only[0].role, "creative_divergence")
        self.assertEqual(model_only[0].temperature, 0.27)

        self.assertEqual(_role_configs(config_empty), ())

    def test_parse_surface_candidate_payload_dedupes_multiple_key_shapes(self) -> None:
        raw_payload = textwrap.dedent(
            """
            {
              "candidates": [
                {"display_name": "Vantoro"},
                {"name": "vantoro"},
                {"candidate": "Meridel"},
                {"display_name": "MERIDEL"},
                {"display_name": "Baltera"}
              ]
            }
            """
        )

        self.assertEqual(_parse_surface_candidate_payload(raw_payload), ["Vantoro", "Meridel", "Baltera"])

    def test_fixture_surface_candidates_handles_parse_failure_and_read_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            empty_fixture = root / "empty.json"
            empty_fixture.write_text("{}", encoding="utf-8")

            names, usage, error = _fixture_surface_candidates(empty_fixture)
            self.assertEqual((names, usage, error), ([], {}, "candidate_parse_failed"))

            missing_names, missing_usage, missing_error = _fixture_surface_candidates(root / "missing.json")
            self.assertEqual((missing_names, missing_usage, missing_error), ([], {}, "fixture_read_error"))

    def test_render_prompt_fills_all_context_blocks(self) -> None:
        prompt = _render_prompt(
            template=(
                "family={family_name}\n"
                "count={target_count}\n"
                "role={role_name}\n"
                "hint={role_instructions}\n"
                "anchors={positive_anchor_block}\n"
                "avoid={avoidance_block}\n"
                "retry={retry_feedback_block}\n"
            ),
            brief=Brief(
                product_core="settlement software",
                target_users=["property managers"],
                trust_signals=["clarity"],
                language_market="de",
                notes="lean and calm",
            ),
            family=NameFamily.SMOOTH_BLEND,
            target_count=4,
            role_cfg=IdeationRoleConfig(model="fixture", role="recombinator", temperature=0.5, weight=1),
            success_context={"top_names": ["Meridel", "Baltera"]},
            avoidance_context={
                "external_avoid_names": ["Vantora", "Clarcel"],
                "external_fragment_hints": ["vant", "clar"],
            },
            retry_feedback="Do not repeat Meridel",
        )

        self.assertIn("family=smooth_blend", prompt)
        self.assertIn("count=4", prompt)
        self.assertIn("role=recombinator", prompt)
        self.assertIn("Keep the divergence energy of these prior positives", prompt)
        self.assertIn("Avoid these recent names or very close neighbors: Vantora, Clarcel", prompt)
        self.assertIn("Move away from crowded fragments such as: vant, clar", prompt)
        self.assertIn("retry=Do not repeat Meridel", prompt)

    def test_accept_candidate_enforces_family_specific_surface_rules(self) -> None:
        self.assertEqual(
            _accept_candidate(NameFamily.LITERAL_TLD_HACK, "brand.example"),
            (True, ""),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.LITERAL_TLD_HACK, "brandexample"),
            (False, "missing_namespace_marker"),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.SMOOTH_BLEND, "vantora"),
            (True, ""),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.SMOOTH_BLEND, "van-tora"),
            (False, "unexpected_surface_marker"),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.MASCOT_MUTATION, "Vantoro"),
            (True, ""),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.MASCOT_MUTATION, "Brndx"),
            (False, "too_harsh"),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.RUNIC_FORGE, "SØLKRIN"),
            (True, ""),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.RUNIC_FORGE, "Solkrin"),
            (False, "missing_structural_marker"),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.CONTRARIAN_DICTIONARY, "Harbor"),
            (True, ""),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.BRUTALIST_UTILITY, "ToolGrid ZX"),
            (True, ""),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.BRUTALIST_UTILITY, "Tool.Grid"),
            (False, "unexpected_dot"),
        )
        self.assertEqual(
            _accept_candidate(NameFamily.BRUTALIST_UTILITY, "Meridel Suite"),
            (False, "corporate_cliche"),
        )

    def test_resolve_prompt_path_prefers_family_override(self) -> None:
        override_path = Path("/tmp/custom-family-prompt.txt")
        config = IdeationConfig(
            provider="fixture",
            family_prompt_template_files={NameFamily.MASCOT_MUTATION.value: override_path},
        )

        resolved = _resolve_prompt_path(config, NameFamily.MASCOT_MUTATION)

        self.assertEqual(resolved, override_path)

    def test_call_provider_for_family_requires_openrouter_api_key(self) -> None:
        config = IdeationConfig(provider="openrouter_http", api_key_env="FAMILY_LLM_TEST_MISSING")
        role_cfg = IdeationRoleConfig(model="model-a")

        with mock.patch.dict(os.environ, {"FAMILY_LLM_TEST_MISSING": ""}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "missing env FAMILY_LLM_TEST_MISSING"):
                _call_provider_for_family(
                    provider="openrouter_http",
                    config=config,
                    role_cfg=role_cfg,
                    prompt="prompt",
                    target_count=3,
                )

    def test_call_provider_for_family_uses_openai_compat_default_api_key(self) -> None:
        config = IdeationConfig(
            provider="openai_compat",
            api_key_env="FAMILY_LLM_TEST_OPTIONAL",
            openai_base_url="http://127.0.0.1:9999/v1",
        )
        role_cfg = IdeationRoleConfig(model="model-a", role="creative_divergence", temperature=0.25, weight=1)

        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch(
                "brandpipe.family_llm._call_openai_compat_surface_candidates",
                return_value=(["Vantoro"], {"usage": 1}, ""),
            ) as call_mock,
        ):
            result = _call_provider_for_family(
                provider="openai_compat",
                config=config,
                role_cfg=role_cfg,
                prompt="prompt-body",
                target_count=2,
            )

        self.assertEqual(result, (["Vantoro"], {"usage": 1}, ""))
        self.assertEqual(call_mock.call_args.kwargs["api_key"], "ollama")
        self.assertEqual(call_mock.call_args.kwargs["base_url"], "http://127.0.0.1:9999/v1")

    def test_generate_family_candidates_with_fixture_provider_filters_and_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            template_path = root / "mascot_prompt.txt"
            fixture_path = root / "fixture.json"
            template_path.write_text(
                textwrap.dedent(
                    """
                    Family: {family_name}
                    Count: {target_count}
                    Role: {role_name}
                    Anchors: {positive_anchor_block}
                    Avoidance: {avoidance_block}
                    Retry: {retry_feedback_block}
                    """
                ).strip(),
                encoding="utf-8",
            )
            fixture_path.write_text(
                textwrap.dedent(
                    """
                    {
                      "candidates": [
                        {"display_name": "Brndx"},
                        {"display_name": "Vantoro"},
                        {"display_name": "Vantoro"}
                      ]
                    }
                    """
                ),
                encoding="utf-8",
            )
            config = IdeationConfig(
                provider="fixture",
                fixture_input=fixture_path,
                family_llm_retry_limit=0,
                family_prompt_template_files={NameFamily.MASCOT_MUTATION.value: template_path},
            )

            accepted, report = generate_family_candidates(
                family=NameFamily.MASCOT_MUTATION,
                brief=Brief(product_core="settlement software"),
                config=config,
                quota=2,
                success_context={"top_names": ["Meridel"]},
                avoidance_context={"external_avoid_names": ["Vantora"]},
            )

        self.assertEqual(accepted, ["Vantoro"])
        self.assertEqual(report["family"], NameFamily.MASCOT_MUTATION.value)
        self.assertEqual(report["provider"], "fixture")
        self.assertEqual(report["accepted"], 1)
        self.assertEqual(report["attempts"], 1)
        self.assertEqual(report["prompt_template_file"], str(template_path))
        self.assertEqual(report["errors"], [])
        self.assertEqual(len(report["role_reports"]), 1)
        self.assertEqual(report["role_reports"][0]["accepted"], 1)
        self.assertEqual(report["role_reports"][0]["rejected"], 1)
        self.assertTrue(any("Brndx" in item for item in report["rejected_examples"]))


if __name__ == "__main__":
    unittest.main()
