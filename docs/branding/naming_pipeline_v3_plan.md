# Naming Pipeline V3 Runbook

## Goal
Run a wider naming surface (multi-engine + morphology + LLM-safe ingest) while controlling collision/confusion risk through tiered gates and deterministic shortlist diversity.

## Entry Points
- V2 baseline/regression: `/Users/bernhard/Development/kostula/scripts/branding/test_naming_pipeline_v2.sh`
- V3 dedicated runner: `/Users/bernhard/Development/kostula/scripts/branding/test_naming_pipeline_v3.sh`
- DB contract assertion: `/Users/bernhard/Development/kostula/scripts/branding/naming_db.py --db <path> assert-contract`

## Execution Workflow
1. Ingest source atoms with morphology derivation:
   - `python3 /Users/bernhard/Development/kostula/scripts/branding/name_input_ingest.py --derive-morphology ...`
2. Generate candidates with V3 flags:
   - `--pipeline-version=v3 --enable-v3 --use-engine-interfaces --use-tiered-validation`
3. Cheap gate on all generated candidates:
   - similarity/adversarial/gibberish/false-friend scoring + static cheap trademark pre-screen (`tm_cheap`).
4. Expensive gate only on finalists:
   - domain/web/app-store/package/social checks on top finalists only.
5. Diversity shortlist reranking:
   - bucket + prefix + phonetic fingerprint quotas.
6. Persist:
   - candidates + lineage + score snapshots + shortlist decisions.
7. Optional async validator pass (cheap-only smoke, full in non-smoke).
   - cheap-tier checks reuse recent results via DB cache (`--cheap-cache`, TTL-controlled).
8. Assert contract/provenance via `assert-contract`.

## Acceptance Metrics
- Candidate volume:
  - `candidate_count >= 50` in non-smoke runs.
- Diversity:
  - shortlist has at least `20` selected names and multiple buckets (`shortlist_buckets > 5`).
- Tiered gating:
  - cheap gate drop-off and expensive gate drop-off emitted in `stage_event=` lines.
  - expensive checks run on finalists only (`finalist_count << evaluated_count`).
- Provenance integrity:
  - `assert-contract` passes (engine_id, parent_ids, lineage, score snapshots, shortlist rows present).
- Observability:
  - stage events include `generation`, `cheap_gate`, `finalist_selection`, `expensive_gate`, `shortlist`, `complete`.

## Artifact Locations
- Smoke defaults:
  - DB: `/tmp/naming_pipeline_v3_smoke.db`
  - CSV: `/tmp/candidate_batch_v3_smoke.csv`
  - JSON: `/tmp/candidate_batch_v3_smoke.json`
  - Run log: `/tmp/name_generator_runs_v3_smoke.jsonl`
  - Validator log: `/tmp/naming_validator_v3_smoke.log`
- Full defaults:
  - DB: `/Users/bernhard/Development/kostula/docs/branding/naming_pipeline_v1.db`
  - CSV: `/Users/bernhard/Development/kostula/docs/branding/candidate_batch_v3.csv`
  - JSON: `/Users/bernhard/Development/kostula/docs/branding/candidate_batch_v3.json`
  - Run log: `/Users/bernhard/Development/kostula/docs/branding/name_generator_runs_v3.jsonl`
  - Validator log: `/Users/bernhard/Development/kostula/docs/branding/naming_validator_v3.log`

## Kill-Switch / Rollback
1. Disable V3 behavior by running V2 pipeline flags:
   - generator: `--pipeline-version=v2` and no V3 flags.
   - validator: `--pipeline-version=v2 --validation-tier=all`.
2. Run explicit regression mode:
   - `/Users/bernhard/Development/kostula/scripts/branding/test_naming_pipeline_v2.sh kill-switch`
3. Confirm:
   - output contains `kill_switch_regression=pass`.
   - validator log indicates `v3_enabled=false`.
4. If V3 run degrades quality/yield, switch CI/manual ops to V2 runner until thresholds are re-tuned.

## Manual Legal Review Decision Points
- Always manually review top shortlist against DPMA/IGE/EUIPO before selecting finalists.
- Treat automated checks as pre-screen only; no legal adoption from pipeline output alone.
- If top names are in crowded semantic clusters, increase shortlist diversity quotas before final legal review.
