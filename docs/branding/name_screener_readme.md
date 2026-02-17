---
owner: engineering
status: draft
last_validated: 2026-02-17
---

# Name Screener README

The naming pipeline has four executable components:
- `scripts/branding/name_generator.py`: generates + screens candidate batches.
- `scripts/branding/naming_db.py`: SQLite candidate lake and run history.
- `scripts/branding/name_ideation_ingest.py`: ingests AI/manual batches with provenance.
- `scripts/branding/naming_validate_async.py`: parallel validation jobs persisted to DB.

Use `docs/branding/name_generator_guide.md` for all command examples.

## Deterministic Fixture Gate (Smoke)
```zsh
python3 -m py_compile \
  scripts/branding/name_generator.py \
  scripts/branding/naming_db.py \
  scripts/branding/name_ideation_ingest.py \
  scripts/branding/naming_validate_async.py

rm -f docs/branding/naming_pipeline_v1.db
python3 scripts/branding/name_ideation_ingest.py \
  --db docs/branding/naming_pipeline_v1.db \
  --names "Utilaro,Saldaro,Kostula"

python3 scripts/branding/naming_validate_async.py \
  --db docs/branding/naming_pipeline_v1.db \
  --state-filter="new" \
  --checks="adversarial,psych,descriptive,domain,web,app_store,package,social" \
  --candidate-limit=3 \
  --concurrency=4

python3 scripts/branding/naming_db.py --db docs/branding/naming_pipeline_v1.db stats
```

## Progress Reporting
`naming_validate_async.py` now emits progress snapshots during long runs:
- start line: candidate/job count + check set
- periodic lines: completed/total, success/fail, throughput, ETA
- final line: 100% completion summary

Tune with:
- `--progress-every=<N>`
- `--progress-interval-s=<seconds>`
- `--no-progress`

## Legal Notice
This tooling is internal screening support. It is not legal advice and does not replace trademark counsel.
