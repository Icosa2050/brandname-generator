---
owner: product
status: draft
last_validated: 2026-03-19
---

# Science-Based Creativity Implementation Plan

## Purpose
This document turns the recovery strategy into an implementation sequence for `brandpipe`.

It answers:
1. Which algorithms and libraries are worth using now?
2. Where can Wuggy help, and where should custom code take over?
3. Which files should change first?

This is deliberately practical.
It is not a research survey and not a future-state architecture fantasy.

Related docs:
- `docs/branding/science_based_creativity_recovery_plan.md`
- `docs/branding/naming_algorithms_and_libraries.md`
- `docs/branding/creative_generation_stack_strategy.md`

## Current Code Surface

Current relevant modules:
- `src/brandpipe/pseudowords.py`
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/ideation.py`
- `src/brandpipe/diversity.py`
- `src/brandpipe/ranking.py`
- `src/brandpipe/pipeline.py`
- `src/brandpipe/models.py`

Current practical state:
- Wuggy is already integrated as a seed generator.
- The current generator pool is operational but drifts into ugly synthetic families.
- Ranking is still dominated by validation outcomes, not attractiveness.
- The browser/TMView/legal recheck path is strong enough and should not be the focus of the next implementation cycle.

## Web-Validated Technical Inputs

### Wuggy is viable, but only as a bounded component
What current upstream docs show:
- Wuggy is an actively published Python package on PyPI, version `1.1.2`, released on `2024-02-20`, requiring Python `>=3.8`.
- The official docs recommend starting with `classic` generation mode.
- The Python API exposes:
  - `WuggyGenerator`
  - `generate_classic(...)`
  - `generate_advanced(...)`
  - `download_language_plugin(...)`
  - `supported_official_language_plugin_names`
  - custom language plugins
  - evaluators including `ld1nn`

Primary sources:
- Wuggy docs: <https://wuggycode.github.io/wuggy/>
- Wuggy PyPI: <https://pypi.org/project/wuggy/>
- Wuggy paper: <https://pubmed.ncbi.nlm.nih.gov/20805584/>

Local verification on 2026-03-19:
- `WuggyGenerator` imports successfully in this environment.
- `generate_advanced` exists.
- `download_language_plugin` exists.
- `ld1nn` exists.

### Other algorithmic helpers that are likely useful
Primary or official references:
- `pronouncing` for CMUdict pronunciations, phones, and stress:
  - <https://pronouncing.readthedocs.io/en/latest/tutorial.html>
- `RapidFuzz` for fast fuzzy string matching:
  - <https://rapidfuzz.github.io/RapidFuzz/>
- `Abydos` for phonetic encoders and phonetic similarity:
  - <https://abydos.readthedocs.io/en/latest/abydos.phonetic.html>
- `wordfreq` for token familiarity / frequency:
  - <https://pypi.org/project/wordfreq/>

Useful but not first-wave:
- `panphon`:
  - <https://pypi.org/project/panphon/>
- `phonemizer`:
  - <https://pypi.org/project/phonemizer/>

Local environment check on 2026-03-19:
- installed: `rapidfuzz`
- not installed: `pronouncing`, `abydos`, `wordfreq`, `panphon`, `phonemizer`

## Algorithm Decision Summary

### Keep now
- Wuggy for structure-preserving pseudoword seeds
- custom phonotactic ugliness filters
- custom near-real-word transmutations
- custom overlap-aware blend generation
- `RapidFuzz` for orthographic similarity
- `pronouncing` for syllables, phones, and stress
- `wordfreq` for familiarity / stem quality
- `Abydos` for phonetic-code similarity

### Defer
- `panphon`
- `phonemizer`
- embedding-based semantic distance
- custom Wuggy language plugins
- `generate_advanced(...)` as the default path

### Do not do in this cycle
- replace Wuggy with browser automation or an external service
- make Wuggy the orchestrator
- build a heavy phonology service

## Wuggy: What It Should And Should Not Do

### Wuggy should do
- generate phonotactically plausible nonword seeds from real-word templates
- preserve coarse shape:
  - syllable count
  - orthographic pattern
  - subsyllabic legality
- widen the search space before LLM ideation and deterministic mutation
- provide candidate pools that are less repetitive than straight prompt-only ideation

### Wuggy should not do
- decide what sounds beautiful
- generate blends or portmanteaus
- handle semantic anchors or metaphor directly
- become the main scoring engine
- replace deterministic taste gates

### Wuggy integration rule
Use Wuggy as one seed source among several, not the main creative authority.

Operationally, that means:
- keep `generate_classic(...)` as the main path now
- keep Wuggy behind `src/brandpipe/pseudowords.py`
- persist more provenance from Wuggy output
- only experiment with `generate_advanced(...)` after the hard taste gate exists

### Wuggy-specific optional experiment
The `ld1nn` evaluator is not a first-wave feature, but it is promising as a later audit tool.
Possible use:
- compare a sample of anchor words to Wuggy outputs
- detect whether generated nonwords are too word-like or too bizarre
- use as one offline calibration metric, not a hot-path score

## Detailed Implementation Sequence

## Phase 0: Dependency And Data Preparation
Timeline:
- 0.5 to 1 day

Goal:
- prepare the minimum library and data substrate without starting a broad refactor.

Changes:
1. Add a new optional dependency group in `pyproject.toml`:
   - `creative = ["Wuggy", "pronouncing", "wordfreq", "abydos>=0.3,<0.4"]`
2. Keep `panphon` and `phonemizer` out of the first dependency group.
3. Create small calibration artifacts under `resources/brandpipe/`:
   - `positive_name_anchors_v1.txt`
   - `negative_name_anchors_v1.txt`
   - `banned_suffix_families_v1.txt`
   - `banned_cluster_patterns_v1.txt`
4. Add one minimal README note on how to install the creative extras.

Files:
- `pyproject.toml`
- `resources/brandpipe/`
- optional small note in `docs/branding/name_generator_guide.md`

Exit criteria:
- creative extras install in one command
- anchor and ban lists exist in versioned files, not only in code

## Phase 1: Hard Taste Gate
Timeline:
- 2 to 3 days

Goal:
- stop obviously ugly names before they consume more generation and validation effort.

New module:
- `src/brandpipe/taste.py`

Responsibilities:
- rule-based rejection
- rule-based penalty scoring
- explainable rejection reasons

Core rules:
1. banned suffix families
2. banned consonant clusters
3. max consecutive consonants
4. vowel/consonant balance
5. open-syllable ratio proxy
6. repeated-character rule
7. stitched-fragment seam rule
8. product-word mashup rule

Key implementation detail:
Every rejection must emit a machine-readable reason, for example:
- `banned_suffix_family`
- `cluster_overload`
- `stitched_fragment_seam`
- `low_open_syllable_ratio`

Integration points:
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/diversity.py`
- `src/brandpipe/models.py`

Data model additions:
- `TasteRuleHit`
- `TasteReport`
- `SeedCandidate` should carry either:
  - `taste_penalty`
  - or `taste_reasons`

Tests:
- add `tests/brandpipe/test_taste.py`
- add regression cases from the ugly survivors:
  - `krelixen`
  - `porthvenix`
  - `deptrixen`
  - `blentrex`
  - `tenurblen`
  - `beacparcel`

Exit criteria:
- these names are blocked before final validation
- top raw batches are no longer dominated by `-xen/-trix/-ex/-venix`

## Phase 2: Generator Retune
Timeline:
- 3 to 5 days

Goal:
- generate better raw material before ranking tries to judge beauty.

### 2A. Improve Wuggy seed handling
Update `src/brandpipe/pseudowords.py`:
1. persist seed provenance more fully:
   - source anchor word
   - language plugin
   - dropped-seed reason
2. expose a small number of per-seed controls:
   - min/max seed length
   - max generated names per source anchor
3. add a thin experiment path for `generate_advanced(...)`, but keep it off by default
4. keep `generate_classic(...)` as the production default

Why:
- Wuggy is most useful as a structured seed expander
- we need more visibility into which anchors create ugly outputs

### 2B. Add a near-real-word transmutation generator
New module:
- `src/brandpipe/transmute.py`

Responsibilities:
- generate candidates by small edits to anchor words and lexicon stems
- preserve human-credible shapes

Operations:
- vowel substitution
- consonant softening / hardening
- one-step deletion
- one-step insertion
- suffix substitution from a curated set
- suffix removal and reclosure

Constraints:
- edit distance should stay small
- names should remain pronounceable under the Phase 1 taste gate
- direct copies of obvious dictionary words should be disallowed

Why:
- current output is too synthetic
- near-real-word transmutation is a direct answer to that

### 2C. Add a real blend generator
New module:
- `src/brandpipe/blend.py`

Responsibilities:
- overlap-aware portmanteau generation
- phonotactically plausible splice points

Algorithm basis:
- overlap-aware blending inspired by:
  - Frenemy
  - CharManteau

Practical implementation:
- use orthographic overlap first
- use `pronouncing` phones later if available
- score splice points by:
  - overlap quality
  - resulting length
  - seam smoothness

Why:
- `generator_pool.py` currently uses simple overlap joining
- that is too crude for good blends in tight markets

### 2D. Reduce ugly archetype weight
Update `src/brandpipe/generator_pool.py`:
1. reduce or remove current harsh archetypes that reliably produce ugly survivors
2. rebalance budget toward:
   - transmutation
   - cleaner coined forms
   - smoother blends
3. treat “hardstop” generation as optional, not central

Exit criteria:
- raw ideation output visibly contains more names that feel like plausible words
- Phase 1 no longer deletes most of the batch

## Phase 3: Attractiveness Scoring
Timeline:
- 2 to 4 days

Goal:
- finalists must be both clear enough and attractive enough.

New module:
- `src/brandpipe/scoring.py`

Dependencies:
- `pronouncing`
- `wordfreq`
- `RapidFuzz`
- `Abydos`

Feature set for v1:
1. syllable count score
2. stress-shape plausibility
3. vowel/consonant smoothness
4. substring familiarity score using `wordfreq`
5. phonetic distance to recent losers using `Abydos`
6. orthographic distance to recent losers using `RapidFuzz`
7. semantic anchor score:
   - does the name preserve one meaningful stem or morpheme?

Important non-goal:
- do not try to build a universal beauty model
- build a transparent proxy score that can be tuned

Integration points:
- `src/brandpipe/ranking.py`
- `src/brandpipe/pipeline.py`
- possibly `src/brandpipe/models.py`

Ranking rule change:
- a name cannot become `candidate` purely because it has zero blockers
- it must also clear `min_attractiveness_score`

Possible model additions:
- `AttractivenessScore`
- `attractiveness_score` on `RankedCandidate`
- `attractiveness_components` for explainability

Tests:
- `tests/brandpipe/test_scoring.py`
- fixture-based comparisons where:
  - ugly survivor < cleaner transmutation
  - harsh synthetic form < smoother alternative

Exit criteria:
- top 20 contains multiple names that feel usable before any human override

## Phase 4: Human Calibration Loop
Timeline:
- 1 to 2 days after Phase 3

Goal:
- calibrate the proxy score against actual taste instead of intuition drift.

Artifacts:
- `test_outputs/brandpipe/taste_calibration_v1.jsonl`

Method:
1. show paired names
2. force a preference choice
3. record:
   - winner
   - loser
   - optional tag:
     - `beautiful`
     - `usable`
     - `technically clean but ugly`
     - `dead on arrival`

Use:
- tune scoring weights
- tune banned suffixes and cluster rules

Do not:
- build a UI
- build a training service

Exit criteria:
- the next serious shortlist is clearly better on first read than the discarded synthetic shortlist

## Helpful Algorithms Beyond Wuggy

### Helpful now

#### 1. Sonority-inspired cluster checks
Use:
- simple hand-built consonant class maps
- not a full phonology engine

Why:
- catches ugly cluster sequences cheaply
- directly addresses the current “pharma / firmware” sound

Where:
- `src/brandpipe/taste.py`

#### 2. Near-neighbor exclusion
Use:
- `RapidFuzz`
- `Abydos`

Why:
- avoid regenerating names too close to recent ugly survivors or incumbent marks
- one method catches look-alikes, the other catches sound-alikes

Where:
- `src/brandpipe/diversity.py`
- `src/brandpipe/scoring.py`

#### 3. Frequency-weighted stem selection
Use:
- `wordfreq`

Why:
- avoid anchors and fragments that are either too obscure or too common
- prefer stems that are familiar enough to feel sayable but not generic

Where:
- `src/brandpipe/lexicon.py`
- `src/brandpipe/transmute.py`

#### 4. Wuggy `ld1nn` as an offline audit
Use:
- batch evaluation of candidate pools only

Why:
- check whether pseudoword pools are becoming too word-like or too weird
- not appropriate as a hot-path score in the first implementation cycle

Where:
- separate offline evaluation helper later

### Helpful later

#### 5. PanPhon feature distance
Potential use:
- finer phonetic smoothness and substitution penalties

Reason to defer:
- complexity is high compared with the current gap
- first-wave heuristics plus `pronouncing` should be enough

#### 6. Phonemizer
Potential use:
- fallback grapheme-to-phoneme for names not covered by CMUdict

Reason to defer:
- current plan can start with orthographic heuristics plus `pronouncing`
- novel-name G2P can wait until the first score pass proves too weak

## Exact File-Level Change Plan

### Wave 1
- `pyproject.toml`
- `src/brandpipe/models.py`
- `src/brandpipe/taste.py` (new)
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/diversity.py`
- `tests/brandpipe/test_taste.py` (new)

### Wave 2
- `src/brandpipe/pseudowords.py`
- `src/brandpipe/transmute.py` (new)
- `src/brandpipe/blend.py` (new)
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/ideation.py`
- `tests/brandpipe/test_pseudowords.py`
- `tests/brandpipe/test_transmute.py` (new)
- `tests/brandpipe/test_blend.py` (new)

### Wave 3
- `src/brandpipe/scoring.py` (new)
- `src/brandpipe/ranking.py`
- `src/brandpipe/pipeline.py`
- `tests/brandpipe/test_scoring.py` (new)
- `tests/brandpipe/test_pipeline.py`

### Wave 4
- `test_outputs/brandpipe/taste_calibration_v1.jsonl`
- light docs update if needed

## Success Criteria

### Mechanical
- the new taste gate removes the rejected synthetic families early
- generation still yields enough names after the gate
- ranking is explainable and testable

### Product
- a human scan of the top 20 no longer triggers “they are all horrible”
- more top names feel speakable, ownable, and warm
- the next shortlist fails because of market crowding only after it first feels desirable

## Recommended Next Action
Implement Wave 1 first.

Reason:
- it is the cheapest way to stop obvious ugliness
- it creates the constraints needed to judge whether generator retuning is actually working
- it does not depend on new browser or validation work

## References
- Wuggy docs: <https://wuggycode.github.io/wuggy/>
- Wuggy PyPI: <https://pypi.org/project/wuggy/>
- Wuggy paper: <https://pubmed.ncbi.nlm.nih.gov/20805584/>
- Ozbal & Strapparava, ACL 2012: <https://aclanthology.org/P12-1074/>
- Brand Pitt corpus: <https://aclanthology.org/L12-1395/>
- Generating Appealing Brand Names: <https://arxiv.org/abs/1706.09335>
- Pronouncing docs: <https://pronouncing.readthedocs.io/en/latest/tutorial.html>
- RapidFuzz docs: <https://rapidfuzz.github.io/RapidFuzz/>
- Abydos phonetic docs: <https://abydos.readthedocs.io/en/latest/abydos.phonetic.html>
- wordfreq package: <https://pypi.org/project/wordfreq/>
- PanPhon package: <https://pypi.org/project/panphon/>
- Phonemizer package: <https://pypi.org/project/phonemizer/>
