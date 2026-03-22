---
owner: product
status: draft
last_validated: 2026-03-14
---

# Creative Search Redesign Plan

## Objective
Increase the odds of finding ownable brand names in tight markets by widening the creation search space before validation.

Primary outcome:
- produce batches that explore meaningfully different semantic and phonetic territory instead of recycling one narrow SaaS/property-finance naming neighborhood.

Secondary outcomes:
- reduce repetitive ending/stem collapse during ideation;
- reduce wasted validation work on obviously generic or structurally weak candidates;
- make creation behavior traceable via provenance and stage metrics.

## Why This Exists
The current clean-slate app (`src/brandpipe/`) is more truthful than the legacy lane stack, but it still under-searches the name space.

Current creation constraints:
- `src/brandpipe/ideation.py` still uses a single main generation loop.
- The loop rotates through six static prompt schemes, but still asks one LLM workflow to do most of the creative search.
- Diversity controls happen late and mostly at the ending-family level.
- There is no upstream lexicon-expansion stage, no explicit archetype routing, no deterministic recombination toolkit, and no batch-level diversity enforcement before validation.

Result:
- generated batches are cleaner than before, but still cluster in familiar naming territory;
- in tight markets, that means many names feel plausible but not ownable.

## Design Principles
1. Creativity comes from wider semantic territory, not just more prompt variants.
2. LLMs should handle semantic expansion and selective refinement, not all structural generation.
3. Deterministic generation and deterministic filters should carry more of the pipeline than they do now.
4. The pipeline should prefer a smaller number of well-defined archetypes over many overlapping generators.
5. Search/SERP should enrich ideation inputs later, not become a hot-path dependency at the start.
6. Every candidate should be explainable: where it came from, what roots it used, what mutations were applied, and why it survived.

## Scope
In scope:
- redesign of the creation path inside `brandpipe`;
- new data models for ideation provenance;
- new pre-validation stages that widen and filter creative search space.

Out of scope:
- backward compatibility with the legacy lane system;
- manual review workflows;
- heavy legal/trademark architecture;
- preference learning in the first implementation wave.

## Consensus Summary
This plan was reviewed against internal analysis and challenged with PAL using:
- `anthropic/claude-opus-4.6`
- `google/gemini-3.1-pro-preview`

Shared conclusions:
- the creation bottleneck is real;
- the most important creativity lever is better input space, especially lateral domains and avoid-stems;
- the first version should use three main archetypes only:
  - `compound`
  - `coined`
  - `evocative`
- altered spelling should be a mutation pass, not a full generator;
- a heavy LLM critic should not be the first answer;
- deterministic filters and diversity enforcement should arrive early;
- SERP/search should be added later as optional lexicon enrichment, not as the foundation.

## Target Pipeline Shape

```text
brief
  -> brief decomposition
  -> lexicon builder
  -> parallel archetype generation
  -> deterministic filters
  -> diversity enforcer
  -> loose semantic screen
  -> validation
  -> ranking/export
```

## Proposed Architecture

### 1. Brief Decomposition
Add a stage that converts the raw naming brief into a structured `BriefProfile`.

Expected outputs:
- target attributes;
- emotional register;
- phonetic direction;
- taboo zones;
- saturated competitor language;
- desired tension pairs (for example `trust + energy`, `precision + warmth`).

Why:
- current prompts infer all of this implicitly from the raw brief every time;
- structured decomposition gives later stages a stable contract.

### 2. Lexicon Builder
Add a stage that produces a `LexiconBundle` from the `BriefProfile`.

The lexicon builder should prioritize:
- lateral domains;
- morpheme inventory;
- phonesthetic clusters;
- avoid-stems;
- selected metaphors;
- structural patterns.

The lexicon builder should not become a giant synonym dump.

Good output shape:
- 40-60 roots/morphemes;
- 10-20 avoid-stems;
- a small number of selected lateral domains;
- a compact pattern inventory.

### 3. Archetype Generators
Start with three explicit archetypes.

#### Compound
Examples of structure:
- root + root
- root + suffix
- metaphor + functional stem

Use case:
- high clarity with better odds of ownable variation than direct category words.

#### Coined
Examples of structure:
- novel forms from morpheme recombination;
- controlled portmanteaus;
- clipped and blended forms.

Use case:
- highest upside for tight markets;
- should rely heavily on deterministic construction.

#### Evocative
Examples of structure:
- names built from lateral-domain language;
- near-metaphor names that imply category values without literal category words.

Use case:
- helps escape saturated semantic space.

### 4. Shared Deterministic Recombination Toolkit
Build a shared toolkit used by the archetype generators rather than one monolithic generator stage.

Capabilities:
- compound assembly;
- morpheme recombination;
- portmanteau blending;
- clipping;
- later mutation pass for altered spelling.

Important:
- this toolkit should generate structure, not final truth;
- its outputs must carry provenance.

### 5. Deterministic Filters
Apply cheap filters immediately after generation.

First-pass filters:
- invalid character/length rejection;
- rough syllable/shape limits;
- simple pronounceability heuristics;
- banned stems and taboo fragments;
- repeated ending-family limits;
- excessive cluster repetition;
- obvious exact availability knockouts when cheap enough.

Do not start with heavyweight linguistic modeling.

### 6. Diversity Enforcer
Add a batch-level diversity stage before validation.

Enforce diversity across:
- ending families;
- edit-distance clusters;
- phonetic similarity clusters;
- root overlap;
- semantic neighborhood overlap.

Goal:
- prevent one generator from flooding the batch with minor variants of the same idea.

### 7. Loose Semantic Screen
After diversity enforcement, run a permissive semantic-fit screen.

Purpose:
- reject clear misses;
- keep surprising but viable names alive.

This should not overfit the batch back into safe/generic territory.

### 8. Optional Later Enrichment
Only after the new creation path proves itself:
- add SERP/search enrichment to the lexicon builder;
- mine competitor snippets, glossary pages, customer language, and adjacent-category metaphors;
- keep this cached and outside the request hot path.

## Data Model Additions

### `BriefProfile`
Fields should include:
- target attributes;
- emotional register;
- phonetic direction;
- taboo zones;
- category pressure terms;
- lateral-domain seeds.

### `LexiconBundle`
Fields should include:
- roots;
- morphemes;
- metaphors;
- phonesthetic clusters;
- patterns;
- avoid-stems;
- bundle provenance.

### `NameCandidate`
Fields should include:
- name;
- archetype;
- source roots;
- transformations applied;
- generator provenance;
- filter outcomes.

### `GenerationTrace`
Fields should include:
- stage timings;
- counts in/out per stage;
- cluster distribution;
- reasons for rejection.

## Phased Plan

### Phase 1: Creation Backbone
Build:
- `BriefProfile`
- `LexiconBundle`
- `NameCandidate`
- `GenerationTrace`

Also:
- add a brief-decomposition step;
- add a lexicon builder that works from the brief only.

Goal:
- establish the contracts that the later creativity stages rely on.

### Phase 2: First Wide Generation Path
Build:
- `compound` generator;
- `coined` generator;
- `evocative` generator;
- shared recombination toolkit.

Goal:
- produce wider batches than the current single-loop ideation without yet adding search enrichment.

### Phase 3: Deterministic Funnel
Build:
- simple pronounceability heuristics;
- banned-stem filters;
- family repetition limits;
- diversity enforcement.

Goal:
- narrow cheaply and early before expensive validation.

### Phase 4: Loose Semantic Screen
Add a permissive semantic-fit layer after diversity enforcement.

Goal:
- remove obvious misses without collapsing creative width.

### Phase 5: Optional Search Enrichment
Add cached SERP/search-powered lexicon enrichment.

Goal:
- improve lateral-domain discovery and avoid-stem accuracy after the core pipeline is already working.

### Phase 6: Preference Learning
Add feedback-driven steering only after the pipeline already produces meaningfully wider and better batches.

Goal:
- bias later batches toward validated creative directions instead of fixing a narrow generator.

## First Implementation Slice
The first slice should be intentionally modest but meaningful.

Build now:
1. `BriefProfile`
2. `LexiconBundle`
3. `NameCandidate`
4. brief decomposition
5. brief-only lexicon builder
6. three archetypes:
   - `compound`
   - `coined`
   - `evocative`
7. simple deterministic filters
8. diversity enforcer

Do not build yet:
- SERP enrichment;
- preference learning;
- heavyweight semantic critic;
- full phonotactic modeling.

## Success Metrics
Judge the redesign by creation metrics, not just final validation output.

Primary metrics:
- more distinct phonetic/semantic clusters per batch;
- lower ending-family collapse;
- lower avoid-stem reuse;
- more candidates surviving cheap knockout checks;
- more finalists outside the immediate incumbent naming neighborhood.

Secondary metrics:
- lower percentage of batches dominated by one structural family;
- improved ratio of `interesting survivors` to total generated names;
- stable runtime and cost despite wider generation.

## Failure Signals
The redesign is not working if:
- output volume rises but cluster diversity does not;
- lateral domains still collapse back into the same few SaaS/property stems;
- the coined generator mostly produces unpronounceable junk;
- the diversity enforcer kills most of the batch because upstream generators are still too similar;
- validation throughput worsens without better finalist quality.

## Implementation Notes
- Keep prompt assets out of the main Python file once archetypes are added.
- Keep provenance mandatory from the first implementation slice.
- Prefer simple deterministic heuristics over speculative NLP complexity.
- Treat altered spelling as a mutation pass after initial archetype generation, not as an independent generator.

## Immediate Next Step
Implement Phase 1 and Phase 2 together as the first proof point:
- brief decomposition;
- brief-only lexicon builder;
- three archetype generators with shared recombination utilities;
- deterministic filters plus diversity enforcement.

This is the smallest slice that can prove the core hypothesis:
- wider creative search space produces better ownable candidates in tight markets than the current single-loop ideation design.
