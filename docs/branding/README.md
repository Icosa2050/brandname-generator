# Branding Docs Index

This folder now contains only active documentation.

## Active documentation
- `docs/branding/name_generator_guide.md`: operational guide and command reference.
- `docs/branding/naming_brief.md`: product/problem framing for naming.
- `docs/branding/naming_rubric.md`: scoring rubric.
- `docs/branding/naming_input_corpus_spec_v2.md`: source-input schema/spec.
- `docs/branding/corpus_strategy_execution_plan.md`: phased corpus strategy, checkpoints, and rollout criteria.
- `docs/branding/continuous_pipeline_test_plan.md`: mostly-automated validation plan for 24/7 loop.
- `docs/branding/continuous_pipeline_deferred_backlog.md`: intentionally deferred architecture/tuning backlog.

## Non-doc locations
- Repository dependency manifests:
  - `requirements.txt`
  - `requirements-dev.txt`
- `resources/branding/`: static inputs, templates, prompt/context examples.
  - `resources/branding/inputs/source_inputs_v2.csv`
  - `resources/branding/inputs/source_inputs_core_v3.csv`
  - `resources/branding/inputs/source_inputs_expansion_v3.csv`
  - `resources/branding/inputs/source_exclusions_seed_v1.txt`
  - `resources/branding/lexicon/naming_false_friend_lexicon_v1.md`
  - `resources/branding/configs/creation_lane.default.toml`
  - `resources/branding/configs/creation_lane.creative_hybrid.toml`
  - `resources/branding/configs/validation_lane.default.toml`
  - `resources/branding/configs/validation_lane.legal_heavy.toml`
  - `resources/branding/llm/llm_context.example.json`
  - `resources/branding/llm/llm_models.example.toml`
  - `resources/branding/llm/llm_prompt.utility_split_v1.txt`
  - `resources/branding/llm/llm_prompt.creative_longer_names_v1.txt`
  - `resources/branding/llm/llm_prompt.brand_market_template_v1.txt`
  - recommended custom prompt location:
    - `resources/branding/llm/prompts/<brand_slug>/<market_slug>.txt`
    - `resources/branding/llm/prompts/README.md`
  - `resources/branding/templates/naming_user_test_results.csv`
- `scripts/branding/`: operational scripts.
  - `scripts/branding/build_corpus_strategy_baseline.py`
- `test_outputs/branding/`: mutable run outputs and working DBs.
- `artifacts/branding/legacy/2026-02/`: historical output artifacts moved out of docs.

## Archive
- Obsolete plans/analyses and superseded decision docs are under:
  - `docs/archive/branding/2026/`
