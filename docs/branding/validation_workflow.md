# Validation Workflow

This repository has two valid post-review validation lanes because they answer different questions:

- Acceptance-tail lane:
  - script: `scripts/branding/run_acceptance_tail.py`
  - purpose: rerun strict live screening on manually reviewed `keep` and `maybe` names, merge survivors, and run legal/brand precheck
  - best for: final human-triaged shortlist, live collision checks, and counsel handoff preparation
- Async publish validator:
  - script: `scripts/branding/naming_validate_async.py`
  - purpose: bucket shortlist-selected names into `survivor`, `review`, `rejected`, and `pending_coverage`
  - best for: scalable publish/readiness screening, coverage tracking, and wide shortlist filtering

They are not duplicates.

- Acceptance-tail is a curated, human-in-the-loop last-mile filter.
- Async validation is a broader publish-bucket pipeline that can run during campaign loops and on reviewed inputs.

## Straight workflow

Use `scripts/branding/run_validation_lane.py` as the stable entrypoint after manual review.

Default behavior now:

- validate the reviewed CSV has real `keep/maybe/drop` decisions
- run acceptance-tail first
- run async publish validation second
- write both result sets into one validation output directory

Recommended command:

```zsh
direnv exec . python3 scripts/branding/run_validation_lane.py \
  --config resources/branding/configs/validation_lane.default.toml \
  --pack-dir <decision_pack_dir>
```

Legal-heavier variant:

```zsh
direnv exec . python3 scripts/branding/run_validation_lane.py \
  --config resources/branding/configs/validation_lane.legal_heavy.toml \
  --pack-dir <decision_pack_dir>
```

## Workflow modes

Validation configs support `workflow`:

- `dual`
  - recommended
  - runs `run_review_validation_bundle.py` underneath
  - acceptance-tail first, async validation second
- `acceptance_only`
  - fallback mode when only the live/legal pass is wanted
  - runs `run_acceptance_tail.py` directly

The default configs now use `workflow = "dual"`.

## Outputs

When `workflow = "dual"` and no explicit `validation_out_dir` is set, outputs stay in the decision pack directory:

- `acceptance_tail/`
- `postrank/`
- `combined_validation_results.csv`
- `combined_validation_summary.md`
- `selected_review_names.txt`
- `naming_campaign.db`

This makes the reviewed CSV, live/legal results, async buckets, and combined summary live together.

## Low-level entrypoints

Use the lower-level scripts only when you need to bypass the normal wrapper:

- `scripts/branding/run_acceptance_tail.py`
  - manual acceptance-tail only
- `scripts/branding/naming_validate_async.py`
  - direct async publish validation against an existing DB/run
- `scripts/branding/run_review_validation_bundle.py`
  - direct dual-run helper when you already have a compatible reviewed CSV and want one-off control

## Why the old split felt confusing

Historically:

- `run_validation_lane.py` sounded like the whole validation lane
- but it only ran the acceptance-tail path
- the actual dual-run helper existed separately
- docs mostly pointed to the split command

That mismatch is what caused the “do we have two validation lanes?” confusion.

The current rule is:

- after manual review, run `run_validation_lane.py`
- let the config decide whether that means `dual` or `acceptance_only`
