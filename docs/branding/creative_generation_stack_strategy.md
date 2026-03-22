---
owner: product
status: draft
last_validated: 2026-03-14
---

# Creative Generation Stack Strategy

## Purpose
This document turns the naming-algorithm research into a build strategy for `brandpipe`.

It answers:
1. How should we exploit Wuggy, UniPseudo, OpenLexicon, and LLMs together?
2. Is the Lexique UniPseudo Shiny app worth automating?
3. What should we build first to get more creative outputs in tight markets?

## Decision Summary

### Hard decisions
- Do **not** make third-party Shiny UI automation part of the production generation pipeline.
- Do use OpenLexicon and Lexique data as source material.
- Do adopt a native pseudoword engine early.
- Do treat pseudoword generation as a **broadening stage before LLM ideation**, not as a replacement for all ideation.
- Do keep orchestration in Python inside `brandpipe`, not in browser automation.

### Bounded exception
Lexique UniPseudo web automation is acceptable only for:
- one-off benchmarking;
- generating a reference corpus;
- acceptance-testing our own native generator against an external baseline.

If UniPseudo turns out to be uniquely valuable beyond benchmarking, the right next step is:
- port the logic;
- or wrap an internal R backend behind an API.

It is **not**:
- a production dependency;
- a hot-path generation service;
- a stability layer.

## Why The Shiny App Should Not Be Core Infrastructure

The UniPseudo app is useful research software, but a poor operational dependency.

Direct inspection on 2026-03-14 showed:
- session-bound Shiny traffic;
- SockJS/WebSocket activity;
- transient session download URLs;
- repeated reactive POSTs tied to browser state.

That means:
- selectors and flows are brittle;
- throughput is poor compared with native generation;
- failures are likely to be operationally noisy and hard to diagnose;
- a third-party UI change can silently break our creation pipeline.

For a system that needs large candidate volumes, this is the wrong layer to bet on.

## Consensus From Opus, Gemini, Qwen, And Kimi

All four models converged on the same core conclusion:
- do not automate the Shiny UI in production;
- use it at most as a research benchmark;
- put pseudoword science **before** the current LLM ideation loop;
- widen the search space with deterministic generators and phonotactic constraints;
- keep availability and collision triage early.

Shared consensus:
- `wuggy` is the best immediate Python-native entry point;
- OpenLexicon datasets and scripts are more valuable than the hosted UI;
- a production system should prefer native code or an internal service contract over browser automation;
- the real goal is not “more names”, but “more disjoint, pronounceable, non-obvious names”.

Where they differed slightly:
- Opus emphasized Wuggy first, then a simple Markov or n-gram generator for diversity.
- Gemini pushed hardest on replacing UI automation with native Python immediately.
- Qwen placed pseudoword generation explicitly before the current ideation loop as a diversity engine.
- Kimi was the most open to an internal containerized R service, but only **after** rejecting browser automation.

## Architecture Strategy

### Current shape
Today `brandpipe` is roughly:

`config -> ideation -> validation -> ranking -> export`

with one main LLM ideation loop and no explicit lexicon or pseudoword stage.

### Target shape

`brief -> lexicon -> generator pool -> diversity filter -> LLM widening -> cheap knockout -> validation -> ranking`

### Generator pool
The generator pool should have multiple families:
- `compound`: deterministic word-part combination;
- `blend`: overlap-aware portmanteau generation;
- `coined`: pseudoword generation with phonotactic constraints;
- `evocative`: metaphor and lateral-domain names;
- `llm`: model-guided mutation and reinterpretation, not raw monopoly over creation.

The key change is that the LLM stops being the only origin of names.

## Recommended Use Of Each External Resource

### 1. OpenLexicon / Lexique
Use for:
- lexical frequency data;
- phonological forms;
- syllable structure;
- scripts and examples for offline use.

This should feed:
- phonotactic legality checks;
- seed-word selection;
- candidate scoring;
- language-specific filters.

### 2. Wuggy
Use as the first native pseudoword engine.

Role:
- generate pronounceable nonwords from real-word templates;
- produce candidate pools that are broader than direct LLM imitation;
- supply structure-preserving coined names for crowded markets.

### 3. UniPseudo
Use in three possible ways, in this order:

1. benchmark source;
2. rule source for porting;
3. optional internal service if a port is not worth it and the algorithm clearly outperforms alternatives.

Do **not** use the hosted app as a core dependency.

### 4. OpenRouter LLMs
Use multiple models for **parallel divergence**, not long serial roleplay chains.

Good roles:
- one model mutates pseudowords into brand-like candidates;
- one model pushes semantic distance and metaphor;
- one model tightens fluency and removes obvious junk;
- one model critiques saturation and overused patterns.

Bad role:
- making LLM-to-LLM conversation the center of the pipeline.

The orchestration should stay in Python:
- generate seeds;
- fan out prompts;
- merge outputs;
- cluster;
- filter;
- score.

## Multi-Model Broadening Pattern

The most practical multi-model pattern is:

1. deterministic generators create a seed pool;
2. several models mutate the same seed pool in parallel using different creative instructions;
3. Python merges and deduplicates outputs;
4. a cheap diversity pass removes cluster collapse;
5. cheap availability and collision checks remove obvious dead names;
6. only then do we run deeper validation.

This keeps the models useful without turning the system into opaque orchestration theater.

## Implementation Phases

### Phase 1: Native pseudoword entry point
Build:
- `src/brandpipe/pseudowords.py`

Responsibilities:
- wrap `wuggy` if available;
- provide a fallback local trigram or syllable generator if not;
- output candidate names plus provenance.

Add new models:
- `LexiconBundle`
- `SeedCandidate`
- `GenerationTrace`

Goal:
- insert a reproducible pseudoword seed stage ahead of the current ideation loop.

### Phase 2: Lexicon and phonotactic data
Build:
- `src/brandpipe/lexicon.py`

Responsibilities:
- brief decomposition;
- avoid-stem list creation;
- lateral-domain expansion;
- phonotactic stats from local datasets;
- optional Lexique/OpenLexicon ingestion.

Goal:
- stop generating from an empty semantic field.

### Phase 3: Generator pool
Build:
- `compound_generator.py`
- `blend_generator.py`
- `coined_generator.py`
- `evocative_generator.py`

Goal:
- stop asking one model to discover every name type by itself.

### Phase 4: Diversity and cheap knockout
Build:
- `diversity.py`
- `collision.py`

Responsibilities:
- phonetic clustering;
- edit-distance family caps;
- repeated ending-family caps;
- competitor and incumbent near-match triage;
- cheap domain and web knockout before expensive validation.

Goal:
- widen the batch while reducing obvious waste.

### Phase 5: Multi-model widening
Extend ideation so that:
- deterministic seeds feed multiple OpenRouter models in parallel;
- each model has a narrow job;
- outputs are merged by provenance, not by trust in one model.

Suggested first role split:
- `creative divergence`: `mistralai/mistral-small-creative`
- `semantic stretch`: `google/gemini-3.1-pro-preview`
- `cleanup / critique`: `anthropic/claude-opus-4.6` or `anthropic/claude-sonnet-4.5`

Goal:
- get broader lexical search without letting one model’s habits dominate the whole batch.

### Phase 6: Optional UniPseudo internalization
Only do this if Phase 1-5 show that:
- Wuggy plus our fallback generator are not enough;
- UniPseudo consistently produces better coined names for our market.

Preferred options:
1. port the needed logic to Python;
2. wrap a local R backend with a stable API.

Avoid:
- browser automation in the hot path.

## What We Should Explicitly Not Build
- a production dependency on third-party browser automation;
- a long multi-agent LLM conversation loop for each batch;
- a system where validation is the main creativity engine;
- another shell wrapper stack around the current pipeline.

## Success Metrics

The strategy is working if we see:
- more distinct phonetic clusters per batch;
- fewer repeated suffix families;
- higher cheap-knockout survival rates;
- more candidates that are pronounceable but not semantically obvious;
- fewer finalists sitting directly next to incumbents in naming space.

## Immediate Next Step
The best next slice is:
- add `pseudowords.py`;
- integrate `wuggy`;
- insert `pseudoword seed -> current ideation loop`;
- log provenance and cluster diversity;
- keep UniPseudo web automation out of production.

## References
- [UniPseudo Shiny app](http://www.lexique.org/shiny/unipseudo/)
- [OpenLexicon GitHub](https://github.com/chrplr/openlexicon)
- [Lexique home](https://www.lexique.org/)
- [Lexique 3.83 downloads](https://www.lexique.org/databases/Lexique383/)
- [Open lexical databases index](https://openlexicon.fr/datasets-info/)
- [UniPseudo paper](https://www.pallier.org/papers/Unipseudo.pdf)
- [Wuggy PyPI](https://pypi.org/project/wuggy/)
- [Wuggy GitHub](https://github.com/WuggyCode/wuggy)
- [A Computational Approach to the Automation of Creative Naming](https://aclanthology.org/P12-1074/)
- [Brand Pitt: A Corpus to Explore the Art of Naming](https://aclanthology.org/L12-1395/)
- [Generating Appealing Brand Names](https://arxiv.org/abs/1706.09335)
- [How to Make a Frenemy](https://aclanthology.org/N15-1021/)
- [CharManteau](https://aclanthology.org/D17-1315/)
- [Wuggy: a multilingual pseudoword generator](https://pubmed.ncbi.nlm.nih.gov/20805584/)
- [UniPseudo: A universal pseudoword generator](https://pubmed.ncbi.nlm.nih.gov/36891822/)
