---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 719.1
---

# Naming Input Corpus Spec V2

## Goal
Define high-signal naming inputs that produce human-sounding, distinctive, low-confusion candidate names for DE/CH-first go-to-market with global portability.

## Audience and Trust Context
- Primary: German and Swiss property managers + landlords.
- Required signals: reliability, clarity, professionalism, legal defensibility.
- Avoid playful/consumer-gadget tone.

## Approved Source Families
1. Positive trust roots:
- Meaning fields: clarity, fairness, stability, order, confidence, verification.
- Typical categories: `trust`, `clarity`, `stability`, `finance`.

2. Domain-adjacent but non-generic roots:
- Meaning fields: place, portfolio, structure, residence, operations.
- Must avoid direct generic legal compounds (for example `Nebenkosten*`).

3. Multilingual inspiration roots (latin alphabet only):
- Romance/Latin family roots for broad pronounceability.
- Carefully selected non-European roots (for example Swahili-inspired stems) when:
  - pronunciation remains simple for German/English,
  - negative associations are screened,
  - semantic confidence is documented.

4. Human-reviewed coinable morphemes:
- Short atoms that blend naturally and remain pronounceable.
- Must pass anti-gibberish and false-friend gates.

## Required Metadata Per Source Atom
- `name`
- `language_hint`
- `semantic_category`
- `confidence_weight` in range `[0.0, 1.0]`
- `source_label`
- optional `note`

## Blocked Classes
1. Over-descriptive legal compounds:
- `neben`, `umlage`, `abrechnung`, `betriebskosten`, `heizkosten`, `mietkosten`.

2. High-collision market prefixes used as dominant identity:
- `immo*` unless paired with strong uniqueness evidence.

3. Low-humanity pattern signatures:
- long consonant clusters,
- repeated trigram loops,
- many near-identical suffix chains in one batch,
- pseudo-words that cannot be read out loud consistently.

4. Semantic/brand risk tokens:
- negative connotations in DE/CH context,
- misleading regulated promises (for example `certified` equivalents) when unsupported.

## Scoring Weights For Input Reliability
- `0.85 - 1.00`: high-confidence trusted roots from curated lexical review.
- `0.70 - 0.84`: good-quality roots with broad readability.
- `0.55 - 0.69`: exploratory roots; allowed with extra review.
- `< 0.55`: excluded from default generation unless explicitly enabled.

## Diversity Targets
Per 100-candidate generation target:
- max 35% from a single generator family,
- at least 4 semantic categories represented,
- at least 3 language-hint clusters represented,
- no dominant suffix pattern above 20% of output.

## Quality Gate Before External Checks
Candidates must pass:
- anti-gibberish filters,
- false-friend and negative-association filters,
- diversity controls (phonetic/morphological clustering).

Only then continue to expensive external checks (domain/store/web/package/social and trademark pre-screen).

## Notes
This corpus spec is a generation-quality control artifact. It is not legal advice and does not replace trademark counsel clearance.
