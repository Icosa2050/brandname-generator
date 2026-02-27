#!/usr/bin/env python3
"""Canonical repository paths for branding pipeline scripts."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

DOCS_BRANDING_DIR = REPO_ROOT / 'docs' / 'branding'
ARCHIVE_BRANDING_DIR = REPO_ROOT / 'docs' / 'archive' / 'branding'
RESOURCES_BRANDING_DIR = REPO_ROOT / 'resources' / 'branding'
TEST_OUTPUTS_BRANDING_DIR = REPO_ROOT / 'test_outputs' / 'branding'
ARTIFACTS_BRANDING_DIR = REPO_ROOT / 'artifacts' / 'branding'


# Static inputs and examples.
SOURCE_INPUTS_V2 = RESOURCES_BRANDING_DIR / 'inputs' / 'source_inputs_v2.csv'
SOURCE_INPUTS_CORE_V3 = RESOURCES_BRANDING_DIR / 'inputs' / 'source_inputs_core_v3.csv'
SOURCE_INPUTS_EXPANSION_V3 = RESOURCES_BRANDING_DIR / 'inputs' / 'source_inputs_expansion_v3.csv'
SOURCE_EXCLUSIONS_SEED_V1 = RESOURCES_BRANDING_DIR / 'inputs' / 'source_exclusions_seed_v1.txt'
FALSE_FRIEND_LEXICON_V1 = RESOURCES_BRANDING_DIR / 'lexicon' / 'naming_false_friend_lexicon_v1.md'
LLM_CONTEXT_EXAMPLE = RESOURCES_BRANDING_DIR / 'llm' / 'llm_context.example.json'
LLM_MODELS_EXAMPLE = RESOURCES_BRANDING_DIR / 'llm' / 'llm_models.example.toml'
LLM_PROMPT_UTILITY_SPLIT_V1 = RESOURCES_BRANDING_DIR / 'llm' / 'llm_prompt.utility_split_v1.txt'
USER_TEST_RESULTS_TEMPLATE = RESOURCES_BRANDING_DIR / 'templates' / 'naming_user_test_results.csv'


# Mutable defaults.
NAMING_PIPELINE_DB = TEST_OUTPUTS_BRANDING_DIR / 'naming_pipeline.db'
NAMING_PIPELINE_V1_DB = TEST_OUTPUTS_BRANDING_DIR / 'naming_pipeline_v1.db'
NAME_GENERATOR_RUNS_JSONL = TEST_OUTPUTS_BRANDING_DIR / 'name_generator_runs.jsonl'
GENERATED_CANDIDATES_DIR = TEST_OUTPUTS_BRANDING_DIR
