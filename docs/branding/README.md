# Branding Docs Index

This folder now contains only active documentation.

## Active documentation
- `docs/branding/validation_workflow.md`: canonical shortlist validation flow based on `run_brandpipe_validate.py`.
  - includes the queue-backed blocking validator behavior, resume model, and `--reset-state` handling
- `docs/branding/local_remote_ai_workflow.md`: practical workflow for local-only, remote-only, and hybrid ideation modes.
- `docs/branding/campaign_configuration_reference.md`: grouped configuration reference and high-impact runner options.
- `docs/branding/background_daemon_setup.md`: cross-platform background daemon setup and operating guidance.
- `docs/branding/creative_search_redesign_plan.md`: creativity-first redesign plan for wider name search in tight markets.
- `docs/branding/creative_generation_stack_strategy.md`: strategy for combining pseudoword science, datasets, and multi-model widening without browser-driven production dependencies.
- `docs/branding/creative_tactics_tight_markets.md`: concrete creativity tactics, multi-LLM broadening patterns, and scientific naming references.
- `docs/branding/naming_algorithms_and_libraries.md`: algorithmic naming methods, key papers, and reusable libraries.
- `docs/branding/science_based_creativity_recovery_plan.md`: concrete recovery plan for replacing collision-survivor naming with more attractive, science-guided generation and ranking.
- `docs/branding/science_based_creativity_implementation_plan.md`: file-level implementation blueprint for creativity recovery, including Wuggy’s bounded role and helper-library choices.
- `docs/branding/name_generator_guide.md`: current operational guide and command reference.
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
- `scripts/branding/archive_run_documents.sh`: manual archive/zip helper for non-review run documents.
- `resources/branding/`: static inputs, templates, prompt/context examples.
  - `resources/branding/inputs/source_inputs_v2.csv`
  - `resources/branding/inputs/source_inputs_core_v3.csv`
  - `resources/branding/inputs/source_inputs_expansion_v3.csv`
  - `resources/branding/inputs/source_exclusions_seed_v1.txt`
  - `resources/branding/lexicon/naming_false_friend_lexicon_v1.md`
  - `resources/branding/llm/llm_context.example.json`
  - `resources/branding/llm/llm_models.example.toml`
  - `resources/branding/llm/llm_prompt.utility_split_v1.txt`
  - `resources/branding/llm/llm_prompt.creative_longer_names_v1.txt`
  - `resources/branding/llm/llm_prompt.brand_market_template_v1.txt`
  - recommended custom prompt location:
    - `resources/branding/llm/prompts/<brand_slug>/<market_slug>.txt`
    - `resources/branding/llm/prompts/README.md`
  - `resources/branding/templates/naming_user_test_results.csv`
- `resources/brandpipe/`: brandpipe briefs and lane inputs for the supported attack runner.
- `scripts/branding/`: operational scripts.
  - `scripts/branding/run_brandpipe_attack.py`
  - `scripts/branding/run_brandpipe_validate.py`
    - active shortlist validator with persisted queue state under each out-dir
  - `scripts/branding/build_corpus_strategy_baseline.py`
- `test_outputs/branding/`: mutable run outputs and working DBs.
- `artifacts/branding/legacy/2026-02/`: historical output artifacts moved out of docs.

## Archive
- Obsolete plans/analyses and superseded decision docs are under:
  - `docs/archive/branding/2026/`
- Archived operational guide snapshot:
  - `docs/archive/branding/2026/name_generator_guide_legacy_pre_dual_validation_2026-03-26.md`
- Archived legacy lane wrappers/configs:
  - `artifacts/branding/legacy/2026-03/`
