# Broadside Variation Plan

Date: 2026-03-21

## Goal

Increase the system's effective variation enough to surface 25-50 materially different, review-worthy names per wave.

Important clarification:
- the target is not 25-50 fully validated legal survivors after expensive checks;
- the target is 25-50 worthwhile names after ideation, taste, cheap collision filtering, and clustering;
- final expensive trademark/web screening should still stay strict.

## Why The Current System Still Collapses

The outer campaign layer already requests substantial volume:
- creative lane currently runs 6 rounds x 14 candidates per round;
- hybrid runner already exposes 2-8 rounds and 12-24 candidates per round.

The inner funnel is still narrow:
- round seed selection is capped at 3-6 names per round in `src/brandpipe/ideation.py`;
- prompt lexicon slices only pass 6 core terms, 6 modifiers, 6 associative terms, and 8 morphemes;
- `_extend_diverse_names()` uses `per_family_cap=2`;
- seed diversity uses `saturation_limit=1`;
- final review tiers are still modest for a broadside search.

Result:
- we ask for many names outside the core loop;
- we only allow a small amount of actual structural variation inside the core loop;
- the generator then collapses into a few phonetic neighborhoods and fights itself.

## Design Principles

1. Broaden variation structurally, not just numerically.
2. Keep hard ugliness and hard collision guardrails.
3. Loosen internal throttles that destroy useful variation too early.
4. Split search into isolated lanes so the system does not average everything into one safe mush.
5. Stage validation so creative exploration is not prematurely dominated by expensive legal checks.

## External Grounding

- Creativity research supports a variation-plus-selection workflow, and idea fluency correlates with judged creativity:
  [Quantity yields quality when it comes to creativity](https://pmc.ncbi.nlm.nih.gov/articles/PMC4479710/)
- Standard search/decoding tends to collapse into near-duplicate outputs:
  [Diverse Beam Search](https://arxiv.org/abs/1610.02424)
- Semantic diversification improves Best-of-N coverage beyond lexical diversity alone:
  [SemDiD](https://arxiv.org/abs/2506.23601)
- Name-confusion review should weight beginnings of names heavily:
  [FDA POCA update](https://www.fda.gov/drugs/medication-errors-related-cder-regulated-drug-products/update-phonetic-and-orthographic-computer-analysis-tool)
- Strong marks still need distinctiveness, memorability, and pronounceability:
  [USPTO Strong Trademarks](https://www.uspto.gov/trademarks/basics/strong-trademarks)

## Ranked Implementation Plan

### 1. Add A Broadside Mode To Core Ideation

Objective:
- make the inner loop capable of supporting 25-50 materially different names instead of silently clamping the search.

Files:
- `src/brandpipe/models.py`
- `src/brandpipe/ideation.py`

Exact changes:
- extend `IdeationConfig` with broadside-friendly knobs:
  - `round_seed_min`
  - `round_seed_max`
  - `seed_pool_multiplier`
  - `seed_saturation_limit`
  - `per_family_cap`
  - `lexicon_core_limit`
  - `lexicon_modifier_limit`
  - `lexicon_associative_limit`
  - `lexicon_morpheme_limit`
  - `local_filter_saturation_limit`
- keep defaults equivalent to current behavior so existing runs do not change.

Target broadside values:
- `round_seed_min = 6`
- `round_seed_max = 12`
- `seed_pool_multiplier = 12`
- `seed_saturation_limit = 2`
- `per_family_cap = 4`
- `lexicon_core_limit = 10`
- `lexicon_modifier_limit = 10`
- `lexicon_associative_limit = 10`
- `lexicon_morpheme_limit = 16`
- `local_filter_saturation_limit = 2`

Required code edits:
- replace `min(6, max(3, int(config.candidates_per_round) // 2))` with config-driven round-seed sizing;
- replace the hard-coded `[:6] / [:6] / [:6] / [:8]` prompt lexicon slices with config-driven limits;
- replace `per_family_cap=2` with `config.per_family_cap`;
- replace the final `filter_names(... saturation_limit=1)` with `config.local_filter_saturation_limit`.

Why first:
- this is the narrowest point in the funnel;
- without this, broader lanes and higher round counts mostly produce more rejected near-duplicates.

### 2. Expand The Search Into Isolated Broadside Lanes

Objective:
- create clearly different phonetic and structural neighborhoods on purpose.

Files:
- `resources/branding/configs/creation_lane.creative_hybrid.toml`
- `resources/branding/configs/creation_lane.default.toml`
- new files under `resources/branding/configs/`
- `scripts/branding/run_hybrid_lmstudio_mistral.sh`
- new orchestration script under `scripts/branding/`

Create three new creation-lane configs:

1. `creation_lane.broadside_short.toml`
- short/coined lane
- 8 rounds
- 20 candidates per round
- strong contrarian plus coined-heavy prompting
- length bias 6-9
- cheap-first validation posture

2. `creation_lane.broadside_balanced.toml`
- balanced lane
- 6 rounds
- 18 candidates per round
- near-real, blend, pragmatic-metaphor bias
- local share around 0.35

3. `creation_lane.broadside_expressive.toml`
- expressive long lane
- 6 rounds
- 16 candidates per round
- metaphor, evocative, longer-shape bias
- remote-heavy model mix

Add a new orchestration script:
- `scripts/branding/run_broadside_hybrid.sh`

Script responsibilities:
- run the three creation lanes in sequence or parallel;
- keep separate output directories;
- merge review CSVs after cheap screening;
- dedupe before decision-pack build;
- emit one combined summary.

Why lanes instead of one giant run:
- one blended prompt tends to average toward a middle style;
- isolated lanes preserve distinct clusters;
- failure analysis becomes much easier.

### 3. Expand Role Diversity Instead Of Only Raising Counts

Objective:
- use more differentiated model behavior per round.

Files:
- `src/brandpipe/ideation.py`
- new or updated TOML configs

Exact changes:
- add role hints and offsets for three new role types:
  - `phonetic_explorer`
  - `morpheme_hybridizer`
  - `ending_diversifier`
- keep existing roles:
  - `creative_divergence`
  - `recombinator`
  - `contrarian`

Broadside lane role target:
- 5-6 roles per lane, not 1-3

Suggested role setup:
- `creative_divergence`: weight 2, temp 0.95
- `recombinator`: weight 2, temp 0.75
- `contrarian`: weight 1, temp 1.00
- `phonetic_explorer`: weight 1, temp 1.05
- `morpheme_hybridizer`: weight 1, temp 0.85
- `ending_diversifier`: weight 1, temp 0.90

Implementation note:
- do not change default role behavior for legacy configs;
- define explicit role arrays inside the new broadside TOMLs.

Why this matters:
- more roles only help if the roles are actually distinct;
- otherwise extra calls just reproduce the same family with minor spelling drift.

### 4. Broaden The Seed Pool And Prompt Surface

Objective:
- give the generator more raw material before the LLM widens it.

Files:
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/ideation.py`
- `resources/branding/llm/llm_prompt.creative_longer_names_v1.txt`
- `resources/branding/llm/llm_prompt.constrained_pronounceable_de_en_v3.txt`
- new prompt templates under `resources/branding/llm/`

Exact changes:
- change seed-pool sizing in `generate_candidates()` from `candidates_per_round * 8` to `candidates_per_round * config.seed_pool_multiplier`;
- increase broadside pseudoword seed count from 18 to 28-32 in broadside configs;
- pass more lexicon terms into prompts as defined in step 1;
- create two new prompt templates:
  - `llm_prompt.broadside_short_coined_v1.txt`
  - `llm_prompt.broadside_expressive_long_v1.txt`

Prompt intent:
- short/coined template should reward compact, ownable, punchy names without falling into harsh pseudo-tech;
- expressive template should reward metaphor and richer cadence without falling into soft-latin sludge.

Do not do:
- do not simply append more banned prefixes and suffixes;
- do not keep adding longer negative lists to every prompt.

### 5. Tighten Less In The Homogenizers, Keep Hard Taste Strict

Objective:
- preserve more useful variation without reopening obviously bad creative pockets.

Files:
- `src/brandpipe/scoring.py`
- `src/brandpipe/diversity.py`
- `src/brandpipe/taste.py`

Keep strict:
- hard taste rejects for bad clusters, clipped business fragments, and generic-safe openings;
- high-signal avoidance from recent external failures;
- final expensive TM/web screening.

Tighten less:
- broaden attractiveness length sweet spot from a narrow 7-9 window to a wider 6-11 window;
- reduce the dominance of liquid-support and open-syllable rewards;
- remove the penalty for zero liquids;
- slightly loosen local family quotas and trigram collision thresholds in broadside mode only.

Recommended broadside-only targets:
- `terminal_bigram_quota`: raise floor from 2 to 4
- `trigram_threshold`: test 0.62 baseline vs 0.66 and 0.70
- `per_family_cap`: raise from 2 to 4
- `seed_saturation_limit`: raise from 1 to 2

Important caution:
- do not remove `generic_safe_opening` or literal-fragment penalties;
- those are not generic anti-diversity heuristics, they are direct anti-sludge guards.

### 6. Widen Review Surfaces And Stage Validation

Objective:
- let broadside mode explore widely without paying full legal cost on everything.

Files:
- `resources/branding/configs/creation_lane.default.toml`
- `resources/branding/configs/validation_lane.default.toml`
- new validation broadside config
- `scripts/branding/run_hybrid_lmstudio_mistral.sh`

Exact changes:
- create `validation_lane.broadside.toml`
- raise creation review tiers from `120,50` or `160,80` to `240,120,60`
- broadside validation defaults:
  - `keep_top_n = 24`
  - `maybe_top_n = 24`
  - `final_top_n = 12`
  - `recommended_top_n = 8`
- raise expensive finalist limit from the current low-20s into the 48-60 range for broadside mode
- keep cheap validation first;
- run expensive checks only after clustering and human keep/maybe marking.

Why:
- the broadside target is a wider review surface;
- expensive checks should support curation, not dominate ideation.

## Suggested First Three Broadside Runs

### Run 1: Balanced Broadside

Purpose:
- confirm the inner-funnel widening works without blowing up quality.

Settings:
- 6 rounds
- 18 candidates per round
- 5 roles
- `seed_pool_multiplier = 12`
- `seed_saturation_limit = 2`
- `per_family_cap = 4`
- review tiers `200,120,60`

Expected result:
- 25+ cheap-screen survivors
- at least 8 materially different prefix/shape clusters in top 120

### Run 2: Split Extremes

Purpose:
- test whether isolated lanes outperform one blended lane.

Settings:
- run short/coined lane and expressive lane separately;
- keep balanced lane off for this run;
- cheap validation only for both lanes before merge.

Expected result:
- more cluster variety than run 1 even if absolute keeper rate is lower

### Run 3: Full Broadside

Purpose:
- run all three lanes and confirm the orchestration holds.

Settings:
- short lane: 8 x 20
- balanced lane: 6 x 18
- expressive lane: 6 x 16
- remote-heavy on expressive lane only
- expensive finalist limit 60

Expected result:
- top review surface with 30-50 worthwhile names
- significantly lower single-family dominance

## Stop / Go Metrics

Go:
- at least 25 names survive cheap validation and clustering;
- top review set contains at least 8-10 materially different lead/shape neighborhoods;
- no single ending family exceeds 20 percent of the top 60 review set;
- no single lead fragment exceeds 20 percent of the top 60 review set.

Stop and retune:
- broadside volume rises but distinct clusters do not;
- the top 60 is still dominated by one soft-latin family;
- validator job volume grows faster than unique-cluster count;
- cheap-screen survivors still collapse into names that only differ by tail.

## Acceptable Risks

- 2-4x higher ideation cost per wave
- more noisy or unavailable names during cheap screening
- longer run times
- more human review burden in the 120-240 range

These are acceptable in broadside mode because the purpose is exploration, not daily steady-state generation.

## Unacceptable Risks

- disabling hard taste rejections
- disabling external avoidance feedback
- replacing structured lanes with one giant averaged prompt
- treating 25-50 as a final legal-survivor target

## Recommended Implementation Order

1. Add broadside knobs to `IdeationConfig` and wire them through `generate_candidates()`.
2. Replace hard-coded round-seed, lexicon-slice, and per-family-cap values with config values.
3. Add new roles and prompt templates.
4. Add three broadside creation-lane TOMLs and one broadside validation TOML.
5. Add `run_broadside_hybrid.sh`.
6. Run the first three experiments and compare cluster counts, cheap-screen survivors, and family dominance against baseline.

## Bottom Line

The next step is not another pocket ban.

The next step is to create a deliberate broadside mode:
- wider seed pool
- wider role ensemble
- isolated lanes
- looser internal throttles
- strict hard taste
- staged validation

That is the highest-probability path to 25-50 actually different names without reopening the ugly pockets we just killed.
