---
owner: product
status: draft
last_validated: 2026-03-14
---

# Creative Tactics For Tight Markets

## Purpose
This document lists concrete tactics for making the naming pipeline more creative in markets where:
- obvious semantic space is saturated;
- short clean names are mostly taken;
- validation cost is high enough that creative width must improve before more screening helps.

This is a tactics document, not the full architecture plan.

Related plan:
- `docs/branding/creative_search_redesign_plan.md`

## Consensus Snapshot
This document was pressure-tested with PAL using:
- `qwen/qwen3-coder-next`
- `anthropic/claude-opus-4.6`
- `google/gemini-3.1-pro-preview`
- `moonshotai/kimi-k2.5`

Shared consensus:
1. Creativity should come from a wider input space and better structural generation, not more generic prompt variants.
2. The first pipeline should use deterministic generation and deterministic filters much more aggressively.
3. Multiple LLMs can help, but only in a shallow parallel-divergence pattern.
4. A long serial chain of LLM roles is too expensive and too slow for the ideation loop.
5. Sound, structure, and semantic distance all matter in tight markets.
6. Preference learning should not be the first answer.

## The Core Creative Problem
Current naming systems often fail in tight markets because they keep searching the same neighborhood:
- same suffixes;
- same trust/clarity/ledger/property vocabulary;
- same category-adjacent metaphors;
- same phonetic shapes.

The result is not necessarily terrible names.
The result is names that feel plausible but dismissible.

## Concrete Tactics

### 1. Use A Constraint Budget
Do not treat all brief requirements as equally binding.

Split them into:
- hard constraints;
- soft constraints;
- stretch directions.

Examples:
- hard: lowercase only, pronounceable, avoid direct landlord words;
- soft: feel trustworthy, somewhat European, concise;
- stretch: allow sharper consonants, more abstract metaphors, slightly stranger roots.

Why this matters:
- over-constrained briefs kill creativity before generation starts;
- a tight market often requires relaxing the least important assumptions on purpose.

Implementation note:
- output a `constraint_budget` from brief decomposition and let generators explicitly relax soft constraints when diversity drops.

### 2. Map Lateral Domains, Not Synonyms
Do not expand from category synonyms only.

Use lateral domains that share structural or emotional logic with the target brief.

Examples:
- trust/stability -> bridge engineering, cartography, metallurgy
- speed/clarity -> optics, falconry, fluid dynamics
- fairness/balance -> scales, music intervals, orbital mechanics

Why this matters:
- synonym expansion returns saturated territory;
- remote domains create better raw material for ownable names.

Implementation note:
- the lexicon stage should output a small number of explicit lateral domains plus why they map back to the brief.

### 3. Build Morpheme And Phonestheme Inventories
Do not work only at the full-word level.

Collect:
- roots;
- prefixes;
- suffixes;
- infixes;
- phonesthetic clusters;
- taboo fragments.

Why this matters:
- good coined names are often built from meaningful fragments, not full words;
- phonesthetic clusters help shape the feel of a name.

Examples:
- brighter/lighter feeling: front vowels, sharper stops, lighter endings;
- weight/stability feeling: broader vowels, heavier codas, slower rhythm.

Implementation note:
- treat morphemes and phonesthetic clusters as first-class generation inputs, not just prompt hints.

### 4. Use Three Primary Archetypes First
Start with:
- `compound`
- `coined`
- `evocative`

Why this matters:
- too many archetypes early create operational noise;
- these three cover the commercially useful space well enough to learn from.

Important nuance:
- blend, portmanteau, clipping, and altered spelling should exist as tactics inside the toolkit from day one;
- they do not need to be separate generators in v1.

If the three-archetype setup still clusters too tightly, promote `blend/constructed` to a distinct archetype later.

### 5. Add A Shared Structural Toolkit
The generators should use a shared deterministic toolkit for:
- compounding;
- blending;
- portmanteau formation;
- clipping;
- mutation;
- phonetic smoothing.

Why this matters:
- the LLM should not invent everything from scratch;
- deterministic structure search is one of the best ways to escape crowded language.

Implementation note:
- the toolkit should emit provenance so every candidate can be traced to roots and operations.

### 6. Add A Fluency Floor And A Novelty Ceiling
Creative names should be surprising, but not broken.

Use:
- a fluency floor:
  - basic pronounceability;
  - shape constraints;
  - orthographic clarity;
  - acceptable syllable count.
- a novelty ceiling:
  - enough deviation from saturated patterns;
  - not so much deviation that recall collapses.

Why this matters:
- processing fluency and memorability matter;
- pure novelty without fluency creates junk;
- fluency without novelty returns generic clutter.

Implementation note:
- score moderate deviation from common phonotactic patterns as better than zero deviation or extreme deviation.

### 7. Enforce Diversity At Batch Level
Creativity is a batch property, not just a candidate property.

Enforce diversity across:
- ending families;
- phonetic clusters;
- edit-distance clusters;
- root overlap;
- semantic neighborhoods.

Why this matters:
- one generator can flood the batch with twenty variations of the same idea;
- the goal is not just more names, but broader coverage.

Implementation note:
- cluster first, then cap cluster size.

### 8. Add A Mutator For Near-Misses
Do not let the critic only reject.

When a good idea is too crowded or too generic, mutate it.

Mutation tactics:
- vowel shifts;
- consonant substitutions;
- clipping;
- morpheme swaps;
- phonetic smoothing;
- alternate stress shapes.

Why this matters:
- tight markets kill near-misses constantly;
- a mutator salvages conceptually strong candidates instead of losing the idea entirely.

### 9. Add A Fast-Fail Availability And Saturation Screen
Use cheap knockouts before expensive validation.

Stages:
1. local/offline heuristics:
   - banned stems;
   - known-incumbent overlap;
   - frequency or density penalties;
   - obvious domain dead patterns.
2. cheap network checks:
   - exact domain signal;
   - exact availability proxy;
   - lightweight collision checks.

Why this matters:
- tight markets waste enormous energy on obviously dead names;
- the cheap filter should kill those early.

Implementation note:
- run local heuristics before any network calls.

### 10. Use Negative Space Deliberately
Do not only generate names for the desired brand promise.

Also generate from:
- the opposite of the category norm;
- what incumbents over-signal;
- taboo metaphors that should be inverted rather than copied.

Examples:
- if the market is full of “clear / ledger / balance / settle”, try names from tension, flow, edge, orbit, hinge, seam, span.

Why this matters:
- creativity in tight markets often appears when the system explores what the market systematically ignores.

## Scientific Approaches To Bake In

### Remote Association
Use remote association deliberately rather than hoping the model finds it.

Practical translation:
- connect the brief to lateral domains with moderate distance, not direct synonyms and not random noise;
- treat those domains as root material for the lexicon builder.

### Conceptual Blending
Blend structures from two distant domains into one name concept.

Practical translation:
- do not only join words;
- blend frames, roles, and morphemes.

### Sound Symbolism
Target sound patterns intentionally.

Practical translation:
- choose sound profiles that match the brief:
  - faster/lighter names often use tighter rhythms and brighter sounds;
  - heavier/stabler names often use broader sounds and stronger codas.

### Fluency And Memory
Names should be memorable because they are easy enough to process, not because they are plain.

Practical translation:
- reward moderate novelty under a fluency floor.

### Curiosity Through Partial Non-Semantics
Not all good names should be fully transparent.

Practical translation:
- allow partially non-semantic coined names that still have one semantic anchor or one strong sound cue.

## Multiple OpenRouter LLMs: Broadening Pattern

### Recommended Pattern
Use multiple models in parallel for divergence, then converge with deterministic and selective filtering.

Good pattern:
1. decompose the brief once;
2. run parallel model roles;
3. merge model outputs into one lexicon or candidate pool;
4. run deterministic filters and diversity enforcement;
5. use one selector only after the pool is already reduced.

Avoid:
- long serial chains where four models each wait on the previous model;
- putting all creativity through a single “critic” model.

### Suggested Multi-Model Role Split

| Role | Good Model Fit | Job |
|---|---|---|
| Domain mapper | `anthropic/claude-opus-4.6`, `google/gemini-3.1-pro-preview` | map lateral domains, extract tension pairs, define avoid-stems |
| Structural generator | `qwen/qwen3-coder-next`, `moonshotai/kimi-k2.5`, `mistralai/mistral-small-creative` | propose roots, blends, mutations, coined variants |
| Provocateur / anti-cliche pass | `anthropic/claude-opus-4.6`, `qwen/qwen3-coder-next` | attack generic stems, propose opposites, force negative-space variants |
| Final selector | `google/gemini-3.1-pro-preview`, `anthropic/claude-opus-4.6` | choose a small final batch after deterministic narrowing |

### Recommended Interaction Pattern

#### Pattern A: Parallel Divergence
Run 3-4 models in parallel on the same structured brief, each with a different role.

Example:
- Opus -> lateral domains and avoid-zones
- Gemini -> structural analogies and tension pairs
- Qwen -> morpheme/blend suggestions
- Kimi -> anti-cliche and mutation proposals

Then:
- merge outputs deterministically;
- build one combined lexicon bundle;
- generate candidates from that combined space.

#### Pattern B: Generator + Provocateur
Run one generator and one provocateur in parallel.

Generator:
- produces names or roots.

Provocateur:
- attacks cliches;
- proposes more remote alternatives;
- flags saturated stems.

Then:
- keep only candidates that survive both breadth and anti-cliche pressure.

#### Pattern C: Parallel Candidate Pools, Single Selector
Let different models generate separate pools, but force convergence in Python first.

Flow:
- model pools;
- deterministic filters;
- diversity enforcement;
- one selector pass.

This is safer than selector-first orchestration.

### When To Avoid Multi-Model Ideation
Avoid multi-model orchestration when:
- the candidate pool is still tiny;
- you do not yet measure diversity per batch;
- network cost is already dominating the loop;
- you have not built deterministic filters yet.

## Suggested Experiments

### Experiment 1: Constraint Relaxation
Compare:
- strict brief
- brief with ranked hard/soft constraints

Measure:
- cluster count;
- top-batch novelty;
- fast-fail knockout rate.

### Experiment 2: Lateral Domains
Compare:
- direct category lexicon only
- direct category + lateral domains

Measure:
- semantic spread;
- avoid-stem reuse;
- shortlist freshness.

### Experiment 3: Single Model vs Parallel Divergence
Compare:
- one model generating names
- four role-separated models generating lexicon inputs in parallel

Measure:
- distinct clusters per batch;
- repeated family collapse;
- final shortlist variety.

### Experiment 4: Fluency Floor
Compare:
- no fluency scoring
- fluency floor + moderate novelty preference

Measure:
- pronunciation quality by human spot-check;
- memory/recall proxy;
- genericity.

## Operational Notes
- Keep all deterministic filters ahead of any network-heavy screening.
- Keep exact cheap knockouts ahead of any semantic selector.
- Do not allow preference learning to narrow the space before the system proves it can widen the space.
- Track diversity as a first-class metric from the first implementation slice.

## Research References
- Mednick, S. A. (1962). *The associative basis of the creative process*. [doi:10.1037/h0048850](https://doi.org/10.1037/h0048850)
- Finke, R. A., Ward, T. B., and Smith, S. M. (1992/1995). *Creative cognition* / *The creative cognition approach*. [doi:10.7551/mitpress/7722.001.0001](https://doi.org/10.7551/mitpress/7722.001.0001), [doi:10.7551/mitpress/2205.001.0001](https://doi.org/10.7551/mitpress/2205.001.0001)
- Fauconnier, G., and Turner, M. (2001). *Conceptual blending*. [doi:10.1016/B0-08-043076-7/00363-6](https://doi.org/10.1016/B0-08-043076-7/00363-6)
- Klink, R. R. (2000). *Creating Brand Names With Meaning: The Use of Sound Symbolism*. [doi:10.1023/A:1008184423824](https://doi.org/10.1023/A:1008184423824)
- Lowrey, T. M., Shrum, L. J., and Dubitsky, T. M. (2003). *The Relation Between Brand-name Linguistic Characteristics and Brand-name Memory*. [doi:10.1080/00913367.2003.10639137](https://doi.org/10.1080/00913367.2003.10639137)
- Özbal, G., and Strapparava, C. (2018). *Generating Appealing Brand Names*. [doi:10.1007/978-3-319-77116-8_45](https://doi.org/10.1007/978-3-319-77116-8_45)
- *Meaningless brand names can spark consumer curiosity and improve brand evaluations* (2026). [doi:10.1016/j.jbusres.2025.115767](https://doi.org/10.1016/j.jbusres.2025.115767)

## Immediate Recommendation
If only three tactics are implemented next, they should be:
1. constraint budget + brief decomposition;
2. lateral-domain lexicon + avoid-stems;
3. deterministic toolkit + diversity enforcement.

Those three create the largest creativity gain before any expensive orchestration work begins.
