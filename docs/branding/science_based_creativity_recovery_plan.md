---
owner: product
status: draft
last_validated: 2026-03-19
---

# Science-Based Creativity Recovery Plan

## Purpose
This plan describes how to pivot `brandpipe` from:
- operationally reliable name screening;
- technically clean but aesthetically weak finalists;

to:
- more attractive, more pronounceable, more ownable name generation in tight markets;
- without rebuilding the whole pipeline or adding new infrastructure layers.

This is a recovery plan for the current failure mode, not a blank-sheet architecture rewrite.

Related docs:
- `docs/branding/creative_search_redesign_plan.md`
- `docs/branding/creative_tactics_tight_markets.md`
- `docs/branding/naming_algorithms_and_libraries.md`

## Current Diagnosis
The current pipeline is now operationally competent:
- browser-backed web rechecks work;
- TMView/EUIPO rechecks work;
- reranking and export refresh work;
- stale `web_check_pending` names no longer disappear silently.

But the most recent technically clean survivors looked like:
- `krelixen`
- `porthvenix`
- `deptrixen`
- `blentrex`
- `tenurblen`
- `beacparcel`

This is not a screening failure.
It is a creativity-objective failure.

The pipeline is currently better at finding names that do not fail than names a human would actually want.

## Consensus Snapshot
This plan was pressure-tested with PAL using:
- `anthropic/claude-opus-4.6`
- `google/gemini-3.1-pro-preview`
- `qwen/qwen3-coder-next`

Shared consensus:
1. The operational pipeline should be preserved.
2. The creative objective is mis-specified.
3. The current ranking logic rewards absence of negatives more than presence of positives.
4. The generation stack is drifting into high-entropy synthetic naming space to escape crowded markets.
5. The fastest fix is not more validation. It is better generation constraints and better positive scoring.
6. The first scoring change should not come before the generator is retuned, or the system will simply learn to rank the "least bad" ugly names.
7. The recovery plan needs both negative anchors and positive anchors.

Important nuance:
- Opus stressed phonetic fluency, suffix monotony, and missing semantic anchors.
- Gemini stressed availability overfitting, generation retuning before beauty scoring, and explicit volume scaling because nicer names are harder to clear.
- Qwen stressed atomic freezing of tuning knobs and the need for executable, repeatable taste gates instead of vibes.

## Science-Based Principles

### 1. Naming should combine semantics, phonetics, and morphology
The ACL paper on creative naming argues that automation should combine semantic, phonetic, lexical, and morphological knowledge rather than random combination or naive word mixing:
- Gozde Ozbal and Carlo Strapparava, "A Computational Approach to the Automation of Creative Naming" (ACL 2012)
- Source: https://aclanthology.org/P12-1074/

Implication:
- `brandpipe` should not treat collision survival as the dominant creative strategy.
- Candidate generation should be explicitly shaped by semantics and sound.

### 2. Good naming systems use multiple naming devices, not one dominant surface form
The Brand Pitt corpus work identified common and latent naming devices in real-world creative names:
- Gozde Ozbal, Carlo Strapparava, Marco Guerini, "Brand Pitt: A Corpus to Explore the Art of Naming" (LREC 2012)
- Source: https://aclanthology.org/L12-1395/

Implication:
- We should stop producing one narrow family of synthetic pseudo-tech names.
- The generator should maintain explicit diversity across naming devices:
  - near-real-word transmutations;
  - evocative/metaphorical forms;
  - clean coined names;
  - compounds only when elegant.

### 3. Readability, pronounceability, memorability, and uniqueness should all be scored
The 2017 brand-name generation paper used these dimensions together, rather than uniqueness alone:
- Hiranandani, Maneriker, Jhamtani, "Generating Appealing Brand Names" (arXiv 2017)
- Source: https://arxiv.org/abs/1706.09335

Implication:
- uniqueness is necessary but not sufficient;
- a name should not become a finalist unless it also clears a lightweight attractiveness score.

### 4. Pseudowords should obey phonotactic constraints
Wuggy's core contribution is generating pseudowords that match subsyllabic structure and transition frequencies:
- Keuleers and Brysbaert, "Wuggy: a multilingual pseudoword generator" (Behavior Research Methods, 2010)
- Source: https://pubmed.ncbi.nlm.nih.gov/20805584/

Implication:
- pseudoword generation should continue;
- but outputs should be filtered by stronger phonotactic and orthographic fluency gates before finalist consideration.

### 5. Sound symbolism can carry meaning
Sound symbolism research shows that phonetic form communicates associations such as speed, strength, weight, or size:
- Richard R. Klink, "Creating Brand Names With Meaning: The Use of Sound Symbolism"
- Source: https://www.researchgate.net/publication/225886900_Creating_Brand_Names_With_Meaning_The_Use_of_Sound_Symbolism

Implication:
- sound-shape is not cosmetic;
- the pipeline should intentionally choose phonetic profiles that fit the brief instead of accepting arbitrary synthetic harshness.

### 6. Some linguistic features improve memory
Brand-name memory work found that specific linguistic properties correlate with memory:
- Lowrey, Shrum, Dubitsky, "The Relation Between Brand-name Linguistic Characteristics and Brand-name Memory"
- DOI reference surfaced via: https://polipapers.upv.es/index.php/rdlyla/article/download/7711/10273

Implication:
- memorability should become part of ranking;
- we should explicitly penalize names that are hard to say, hard to hear, or hard to reconstruct on first exposure.

### 7. Non-semantic names can be good, but only when they generate curiosity rather than ugliness
Recent work suggests non-semantic names can improve evaluation by stimulating curiosity:
- "Meaningless brand names can spark consumer curiosity and improve brand evaluations" (Journal of Business Research, 2026)
- Source: https://www.sciencedirect.com/science/article/pii/S0148296325005909

Implication:
- we do not need to force fully meaningful names;
- but non-semantic names should feel intriguing, not synthetic and dead.

## Core Strategic Shift
Current system tendency:
- generate safe-looking synthetic forms;
- pass them through strong operational filters;
- accept survivors by lack of failure.

Target system tendency:
- generate attractive, human-credible names first;
- gate them with taste and phonotactic constraints;
- let the existing operational pipeline do legal/web screening afterward.

In short:
- clearance stays a gate;
- attractiveness becomes a requirement.

Two practical constraints must shape the sequence:
- a stricter taste gate can starve the pipeline if raw ideation volume stays low;
- an attractiveness scorer built on an ugly candidate pool will mostly learn how to pick the least bad ugly names.

## Detailed Plan

### Phase 0: Freeze The Wrong Objective And Build Ground Truth
Timeline:
- 1 day

Goal:
- stop treating ugly survivors as progress;
- establish the current shortlist as a failure case for tuning;
- define what "good" should mean before code starts moving again.

Actions:
1. Freeze the current tuning knobs so later changes are causally attributable:
   - prompt variants;
   - temperature / sampling settings;
   - per-role output counts;
   - ranking weights.
2. Mark the recent synthetic shortlist as calibration failure examples.
3. Build two small ground-truth sets:
   - negative anchors: the ugly survivors and their families;
   - positive anchors: 20 to 50 real names the team considers genuinely strong for the target tone.
4. Keep the current operational recheck and export path unchanged.
5. Capture a small "bad survivor lexicon" from recent outputs:
   - suffix families: `-xen`, `-ixen`, `-ex`, `-trix`, `-lex`, `-venix`;
   - ugly compound seams;
   - over-synthetic stitched product-word fragments.

Implementation targets:
- `src/brandpipe/ideation.py`
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/diversity.py`
- `resources/brandpipe/` for anchor examples if needed

Success metric:
- the team stops debating whether the current shortlist is acceptable;
- there is a shared positive and negative reference set for later tuning.

### Phase 1: Add A Hard Taste Gate
Timeline:
- 2 to 3 days

Goal:
- prevent obviously ugly names from ever reaching finalist status.

Actions:
1. Add immediate bans for dead suffix families.
2. Add a hard reject for severe synthetic forms with executable, repeatable rules:
   - too many harsh consonant clusters;
   - overlong technical-looking endings;
   - stitched domain compounds with poor phonetic bridges.
3. Add a small set of phonotactic penalties and thresholds:
   - cluster length;
   - vowel/consonant balance;
   - open-syllable ratio proxy;
   - awkward onset/coda patterns;
   - ambiguous stress shape proxy;
   - banned consonant cluster families from recent failures.
4. Keep the gate transparent:
   - every rejection should be attributable to a rule or a small set of rules;
   - avoid subjective "felt ugly" logic at this layer.

Implementation targets:
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/diversity.py`

Do not:
- add a new service;
- add a heavyweight phonetics library first;
- touch browser/TMView infrastructure.

Success metrics:
- top 20 no longer clusters into synthetic `-xen/-trix/-ex` families;
- batch-level ugliness is visibly reduced before validation.

### Phase 2: Retune Generation Toward Human-Credible Forms
Timeline:
- 3 to 5 days

Goal:
- steer generation toward better archetypes instead of letting crowded-market pressure force synthetic ugliness.

Actions:
1. Retune prompts and deterministic generation toward:
   - near-real-word transmutations;
   - cleaner coined names with one anchor morpheme;
   - metaphor/evocative names;
   - softer, more speakable vowel-consonant shapes.
2. Reduce or ban prompt patterns that encourage:
   - synthetic pharma-like endings;
   - pseudo-enterprise technical forms;
   - direct product-word mashups unless elegant.
3. Increase raw ideation volume enough to survive both the taste gate and later clearance pressure.
4. Keep archetype diversity explicit at batch level.
5. Use the positive anchor set from Phase 0 as a style reference, not as names to imitate.

Implementation targets:
- `src/brandpipe/ideation.py`
- `src/brandpipe/generator_pool.py`
- `src/brandpipe/pipeline.py`

Success metrics:
- raw ideation output visibly spans multiple style families;
- shorter and more natural names appear before screening;
- the taste gate no longer starves the pipeline.

### Phase 3: Add Positive Attractiveness Scoring
Timeline:
- 2 to 4 days

Goal:
- require positive quality, not just lack of failure.

Actions:
1. Add a lightweight attractiveness proxy to ranking.
2. Score each candidate on:
   - pronounceability;
   - phonetic smoothness;
   - brevity;
   - rhythm/stress plausibility;
   - semantic anchor quality;
   - category congruence;
   - competitive namespace distance.
3. Make a finalist eligible only if:
   - it passes operational checks;
   - and it clears a minimum attractiveness threshold.
4. Keep the scorer parallel to clearance, but do not let high attractiveness overrule hard legal/web failures.

Implementation targets:
- `src/brandpipe/ranking.py`
- `src/brandpipe/pipeline.py`

Success metrics:
- human review of top 20 produces multiple "interesting / usable" reactions instead of immediate dismissal;
- fewer names survive purely because they are collision-sparse.

### Phase 4: Add A Minimal Human Taste Calibration Loop
Timeline:
- 2 to 3 days after Phase 3

Goal:
- stop tuning only against automated filters.

Actions:
1. Introduce a tiny hand-labeled review set:
   - `beautiful`
   - `usable`
   - `technically clean but ugly`
   - `dead on arrival`
2. Prefer forced ranking or pairwise comparisons over simple thumbs up / thumbs down.
3. Use this set to validate the attractiveness gate and ranking weights.
4. Re-run one batch and compare:
   - pre-taste-gate top 20;
   - post-taste-gate top 20.

Implementation targets:
- small artifact under `test_outputs/brandpipe/`
- optional prompt examples under `resources/brandpipe/`

Success metrics:
- top 20 is no longer dominated by names the team dislikes on first read;
- hit rate of "would actually consider this" improves materially.

## Non-Goals
Do not do these during this recovery plan:
- no new orchestration layer;
- no new long-running browser service;
- no large-scale infra redesign;
- no fine-tuning project;
- no complex phonetic microservice;
- no broader legal automation expansion until creativity improves.

## Decision Gates
Move from one phase to the next only if:

### Exit gate for Phase 1
- the obvious ugly families are largely absent from raw top batches.

### Exit gate for Phase 2
- fresh raw batches remain large enough after the hard taste gate;
- the system is producing better-shaped material for scoring.

### Exit gate for Phase 3
- at least 5 names in a fresh top-20 batch are considered "genuinely usable" by fast human review.

### Exit gate for Phase 4
- the shortlist no longer feels like collision survivors only;
- operational yield remains acceptable after the stronger taste gate.

## Recommended Implementation Order
The smallest effective order is:
1. freeze knobs and define positive/negative anchors;
2. hard bans and ugly-form filters;
3. prompt/generation retune toward cleaner forms and higher ideation volume;
4. attractiveness proxy in reranking;
5. small human calibration loop;
6. only then generate a new serious shortlist.

## Short Version
Do not repair this by making the pipeline more powerful.
Repair it by making the pipeline more selective about beauty.

The current system already knows how to reject risky names.
It now needs to learn how to reject ugly safe ones.

## References
- ACL 2012, Ozbal and Strapparava, "A Computational Approach to the Automation of Creative Naming"
  - https://aclanthology.org/P12-1074/
- LREC 2012, Ozbal, Strapparava, Guerini, "Brand Pitt: A Corpus to Explore the Art of Naming"
  - https://aclanthology.org/L12-1395/
- Hiranandani et al., "Generating Appealing Brand Names"
  - https://arxiv.org/abs/1706.09335
- Keuleers and Brysbaert, "Wuggy: a multilingual pseudoword generator"
  - https://pubmed.ncbi.nlm.nih.gov/20805584/
- Klink, "Creating Brand Names With Meaning: The Use of Sound Symbolism"
  - https://www.researchgate.net/publication/225886900_Creating_Brand_Names_With_Meaning_The_Use_of_Sound_Symbolism
- Lowrey, Shrum, Dubitsky, "The Relation Between Brand-name Linguistic Characteristics and Brand-name Memory"
  - DOI reference surfaced via https://polipapers.upv.es/index.php/rdlyla/article/download/7711/10273
- "Meaningless brand names can spark consumer curiosity and improve brand evaluations"
  - https://www.sciencedirect.com/science/article/pii/S0148296325005909
