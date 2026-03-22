---
owner: product
status: draft
last_validated: 2026-03-14
---

# Naming Algorithms And Libraries

## Purpose
This document summarizes the technical literature and reusable libraries that are actually relevant to *creating* new names.

It answers three questions:
1. What algorithmic patterns exist for generating better names?
2. Which parts are already implemented by existing tools or Python libraries?
3. What still needs custom code in `brandpipe`?

## Short Answer
There is **no single mature Python library** that does end-to-end brand naming well.

What does exist:
- direct research on computational brand naming;
- strong subproblem research for pseudoword generation and portmanteau generation;
- solid Python libraries for phonetics, similarity, embeddings, and frequency data;
- a few practical hobby/utility packages for simple name generation and availability checks.

So the realistic approach is:
- reuse libraries for subproblems;
- build the brand-specific orchestration and scoring ourselves.

## Core Algorithmic Patterns

### 1. Naming-Device Taxonomy
One of the clearest technical ideas in the literature is that names are not all generated the same way.

In [A Computational Approach to the Automation of Creative Naming](https://aclanthology.org/P12-1074/) and [Brand Pitt: A Corpus to Explore the Art of Naming](https://aclanthology.org/L12-1395/), Özbal, Strapparava, and colleagues treat naming as a set of linguistic creativity devices.

Useful device families:
- phonetic:
  - rhyme
  - reduplication
  - punning / homophonic play
- orthographic:
  - acronyms
  - palindromes
  - letter shifts
- morphological:
  - affixation
  - blending
  - clipped formation
- semantic:
  - metaphor
  - analogy
  - category transfer

Why this matters:
- one generic “generate names” prompt hides the fact that different name devices require different algorithms.

Implementation implication:
- `brandpipe` should treat “compound”, “blend”, “coined”, and “evocative/metaphor” as distinct generation patterns, not just different prompt moods.

### 2. Property-Driven Expansion
The same ACL paper is also useful because it frames naming as:
- identify the target category;
- identify desired emphasized properties;
- enlarge the ingredient list with common-sense and semantically related material;
- combine semantic and phonetic reasoning to create neologisms.

That is much closer to a usable system than random word mixing.

Implementation implication:
- explicit brief decomposition and lexicon expansion are not optional extras;
- they are the front half of the algorithm.

### 3. Multi-Objective Scoring
[Generating Appealing Brand Names](https://arxiv.org/abs/1706.09335) is important because it does not stop at generation.

It explicitly scores generated names for:
- readability
- pronounceability
- memorability
- uniqueness

Then it recommends a *diverse* set rather than just the highest raw score.

Implementation implication:
- our generator should not optimize only for creativity;
- it should optimize for a bundle of partially conflicting objectives.

### 4. Portmanteau / Blend Generation
Blend generation is a real algorithmic subproblem with dedicated papers:
- [How to Make a Frenemy: Multitape FSTs for Portmanteau Generation](https://aclanthology.org/N15-1021/)
- [CharManteau: Character Embedding Models For Portmanteau Creation](https://aclanthology.org/D17-1315/)

The useful lesson is not that we need their exact model architecture.

The useful lesson is:
- blends should be treated as a separate generator;
- character-level and overlap-aware operations work better than naive concatenation.

Implementation implication:
- build a dedicated blend/portmanteau generator with overlap-aware candidate creation;
- do not expect the generic LLM prompt to discover good blends reliably.

### 5. Pseudoword / Coined-Word Generation
The psycholinguistics literature is especially useful here.

Important references:
- [Wuggy: a multilingual pseudoword generator](https://pubmed.ncbi.nlm.nih.gov/20805584/)
- [UniPseudo: A universal pseudoword generator](https://pubmed.ncbi.nlm.nih.gov/36891822/)
- [Pseudo](https://waltervanheuven.net/pseudo/)

Common technical patterns:
- preserve syllabic or subsyllabic structure;
- preserve transition frequencies;
- control bigram/trigram legality;
- optionally preserve morphological class cues;
- ensure output is nonword-like but still word-shaped.

Implementation implication:
- coined names should be generated with phonotactic constraints, not just character sampling or raw LLM output.

### 6. Sound Symbolism And Memory
There is also relevant evidence on *what kinds of names work better* once generated:
- [Creating Brand Names With Meaning: The Use of Sound Symbolism](https://doi.org/10.1023/A:1008184423824)
- [The Relation Between Brand-name Linguistic Characteristics and Brand-name Memory](https://doi.org/10.1080/00913367.2003.10639137)

Useful principles:
- sounds can imply size, speed, sharpness, softness, energy;
- names that are easier to process are often remembered better;
- but full transparency is not always ideal.

Recent evidence:
- [Meaningless brand names can spark consumer curiosity and improve brand evaluations](https://doi.org/10.1016/j.jbusres.2025.115767)

Implementation implication:
- scoring should include a fluency floor and a novelty/curiosity component.

## Practical Library Map

### Libraries That Look Immediately Useful

| Library | What it helps with | Notes |
|---|---|---|
| [`wuggy`](https://pypi.org/project/wuggy/) | research-grade pseudoword generation | closest off-the-shelf Python package for phonotactically controlled coined names |
| [`pronouncing`](https://pronouncing.readthedocs.io/en/latest/tutorial.html) | English pronunciations and stress via CMUdict | English only, but excellent for fast fluency/stress checks |
| [`phonemizer`](https://pypi.org/project/phonemizer/) | grapheme-to-phoneme conversion | multilingual; useful for candidate phoneme forms |
| [`epitran`](https://pypi.org/project/epitran/) | transliteration / grapheme-to-phoneme | strong for multilingual phonetic normalization |
| [`panphon`](https://pypi.org/project/panphon/) | IPA feature vectors | useful if we want phonetic feature scoring, not just string matching |
| [`pyphen`](https://pyphen.org/) | hyphenation / rough syllable boundaries | simple and cheap approximation |
| [`RapidFuzz`](https://rapidfuzz.github.io/RapidFuzz/) | fast string distances and fuzzy matching | good for near-duplicate and saturation checks |
| [`Abydos`](https://abydos.readthedocs.io/en/latest/abydos.phonetic.html) | phonetic encoders and string metrics | useful for Metaphone / phonetic clustering |
| [`wordfreq`](https://pypi.org/project/wordfreq/) | word frequency and tokenization | already relevant in this repo; good for avoiding over-common stems |
| [`sentence-transformers`](https://sbert.net/) | semantic embeddings | useful for lateral-domain mapping and semantic density checks |
| [`Faiss`](https://faiss.ai/) | fast vector similarity search | useful for saturation maps and “semantic dead zone” detection |
| [`Pincelate`](https://pincelate.readthedocs.io/en/latest/) | spelling/pronunciation modeling for English | useful if we want orthography<->phonology mutation support |

### Libraries / Tools That Are Useful But Not A Full Fit

| Tool | What it does | Limitation |
|---|---|---|
| [`brand`](https://pypi.org/project/brand/) | simple name generation and availability checks | pragmatic, but not scientifically grounded or brand-specific enough |
| [`namemaker`](https://github.com/Rickmsd/namemaker) | Markov-chain fake name generation | useful baseline, but too imitation-heavy for tight commercial markets |
| [`Pseudo`](https://waltervanheuven.net/pseudo/) | GUI/Java pseudoword tool with bigram/trigram constraints | useful algorithmically, but not a Python library we can directly slot in |
| [`UniPseudo`](https://pubmed.ncbi.nlm.nih.gov/36891822/) | universal pseudoword generation approach | paper/tool reference more than drop-in Python dependency |

## Pattern -> Library Mapping

### Pattern: Coherent Coined Names
Goal:
- make new names that are not real words but still feel like possible words.

Best current building blocks:
- `wuggy`
- `pronouncing`
- `phonemizer`
- `epitran`
- `pyphen`

Likely `brandpipe` shape:
- use `wuggy` or a Wuggy-style generator for structural candidate creation;
- use `pronouncing` or `phonemizer` for phonetic checks;
- use `pyphen` for cheap syllable heuristics.

### Pattern: Blends And Portmanteaus
Goal:
- create ownable hybrids from multiple semantic anchors.

Best current building blocks:
- no dominant Python library found that directly implements research-grade blend generation end-to-end;
- use the papers as design guidance;
- use `RapidFuzz`/custom overlap logic for candidate generation.

Likely `brandpipe` shape:
- custom blend generator;
- custom overlap scoring;
- optional character-level reranking.

### Pattern: Fluency And Pronounceability
Goal:
- avoid junk and awkward nonwords.

Best current building blocks:
- `pronouncing`
- `phonemizer`
- `epitran`
- `panphon`
- `pyphen`

Likely `brandpipe` shape:
- cheap orthographic/syllabic filters first;
- phonetic feature scoring second if needed.

### Pattern: Semantic Distance And White Space
Goal:
- avoid generating into already saturated semantic neighborhoods.

Best current building blocks:
- `sentence-transformers`
- `Faiss`
- `wordfreq`

Likely `brandpipe` shape:
- embed incumbent names and crowded stems;
- map “dead zones”;
- steer generation toward moderately remote semantic space.

### Pattern: Saturation / Near-Duplicate Detection
Goal:
- kill names that are too close to each other or to known crowded patterns.

Best current building blocks:
- `RapidFuzz`
- `Abydos`

Likely `brandpipe` shape:
- edit-distance clustering;
- phonetic-code clustering;
- ending-family clustering.

## What No Library Really Solves Yet
No library we found gives us all of this in one package:
- brief decomposition for naming;
- lateral-domain discovery;
- explicit naming-device orchestration;
- blend + coined + metaphor + compound generation in one system;
- brand-specific multi-objective scoring;
- cheap availability knockouts plus later legal checks;
- batch diversity enforcement for commercial naming.

So the missing layer is the actual `brandpipe` orchestration.

## Recommended Build Stack

### Reuse Directly
- `wuggy` for coined-name generation experiments
- `pronouncing` for English phoneme/stress checks
- `wordfreq` for over-common stem penalties
- `RapidFuzz` for similarity clustering
- `sentence-transformers` + `Faiss` for semantic saturation maps

### Evaluate Carefully
- `phonemizer`
- `epitran`
- `panphon`
- `Abydos`
- `Pincelate`

These are promising, but we should keep them behind small adapter modules rather than baking them into the whole app immediately.

### Build Ourselves
- brief decomposition
- lateral-domain lexicon builder
- avoid-stem extractor
- blend / portmanteau generator
- brand-specific multi-objective scorer
- diversity enforcer
- mutator for near-misses

## Suggested `brandpipe` Architecture Implications

```text
brief
  -> brief decomposition
  -> naming-device routing
  -> lexicon expansion
  -> generators
     - compound
     - coined (Wuggy-style / pseudoword)
     - blend (custom)
     - evocative/metaphor
  -> fluency + similarity filters
  -> semantic density screen
  -> cheap availability knockout
  -> final validation
```

## Recommended Experiments

### Experiment 1: Coined Names With `wuggy`
Question:
- does Wuggy-style generation produce more viable coined names than the current LLM-only path?

Measure:
- pronounceability;
- novelty;
- cheap knockout survival.

### Experiment 2: Blend Generator
Question:
- does a dedicated blend generator outperform generic LLM prompts for hybrid names?

Measure:
- human plausibility;
- near-duplicate rate;
- shortlist quality.

### Experiment 3: Semantic Density Maps
Question:
- does steering away from saturated embedding clusters produce more ownable batches?

Measure:
- cluster spread;
- incumbent-neighborhood overlap;
- final shortlist freshness.

## References
- Özbal, G., and Strapparava, C. (2012). *A Computational Approach to the Automation of Creative Naming*. ACL. [https://aclanthology.org/P12-1074/](https://aclanthology.org/P12-1074/)
- Özbal, G., Strapparava, C., and Guerini, M. (2012). *Brand Pitt: A Corpus to Explore the Art of Naming*. LREC. [https://aclanthology.org/L12-1395/](https://aclanthology.org/L12-1395/)
- Hiranandani, G., Maneriker, P., and Jhamtani, H. (2017). *Generating Appealing Brand Names*. [https://arxiv.org/abs/1706.09335](https://arxiv.org/abs/1706.09335)
- Deri, A., and Knight, K. (2015). *How to Make a Frenemy: Multitape FSTs for Portmanteau Generation*. [https://aclanthology.org/N15-1021/](https://aclanthology.org/N15-1021/)
- Gangal, V., Jhamtani, H., Neubig, G., Hovy, E., and Nyberg, E. (2017). *CharManteau: Character Embedding Models For Portmanteau Creation*. [https://aclanthology.org/D17-1315/](https://aclanthology.org/D17-1315/)
- Keuleers, E., and Brysbaert, M. (2010). *Wuggy: a multilingual pseudoword generator*. [https://pubmed.ncbi.nlm.nih.gov/20805584/](https://pubmed.ncbi.nlm.nih.gov/20805584/)
- New, B., Bourgin, J., Barra, J., and Pallier, C. (2024). *UniPseudo: A universal pseudoword generator*. [https://pubmed.ncbi.nlm.nih.gov/36891822/](https://pubmed.ncbi.nlm.nih.gov/36891822/)
- Klink, R. R. (2000). *Creating Brand Names With Meaning: The Use of Sound Symbolism*. [https://doi.org/10.1023/A:1008184423824](https://doi.org/10.1023/A:1008184423824)
- Lowrey, T. M., Shrum, L. J., and Dubitsky, T. M. (2003). *The Relation Between Brand-name Linguistic Characteristics and Brand-name Memory*. [https://doi.org/10.1080/00913367.2003.10639137](https://doi.org/10.1080/00913367.2003.10639137)
- *Meaningless brand names can spark consumer curiosity and improve brand evaluations* (2026). [https://doi.org/10.1016/j.jbusres.2025.115767](https://doi.org/10.1016/j.jbusres.2025.115767)

## Immediate Recommendation
If we want the fastest algorithmic gain:
1. test `wuggy` for coined-name generation;
2. add `RapidFuzz` + phonetic clustering for diversity and collision control;
3. add `sentence-transformers` + `Faiss` for semantic saturation mapping;
4. keep the brand-specific orchestration layer custom.
