---
owner: product
status: draft
last_validated: 2026-02-17
related_task: 713.4
input_batch:
  - /Users/bernhard/Development/kostula/docs/branding/candidate_batch_screened_v2.csv
---

# Trademark and Registry Pre-Screen Report

## Scope
- Jurisdictions: Germany (DE), Switzerland (CH), EU (TMview coverage)
- Registers reviewed:
  - DPMAregister (DE)
  - Swissreg / IGE (CH)
  - TMview (EU)
  - Zefix (CH company register)
- Nice classes considered: `9`, `36`, `42`
- Candidate source: `candidate_batch_screened_v2.csv` (20 names)

## Method
1. For each candidate, run exact-string queries in DPMA, Swissreg, TMview, and Zefix.
2. Add quick public-web sanity scan for obvious incumbent usage in software/proptech/fintech adjacency.
3. Assign provisional pre-screen risk:
   - `Low`: no clear exact conflict found in class-adjacent context.
   - `Medium`: no exact blocker found, but notable near-similarity or crowded lexical stem.
   - `High`: clear incumbent collision, likely opposition risk, or strong market-proximity confusion.

## Results Summary
- Total screened: `20`
- Provisional pass to next gate (low/medium, no obvious blocker): `6`
- Provisional hold/block (high risk): `14`

## Candidate Matrix (Top/Representative)

| Candidate | DPMA | Swissreg/IGE | TMview | Zefix | Quick web signal | Provisional risk | Decision |
|---|---|---|---|---|---|---|---|
| `certorio` | query run | query run | query run | query run | no strong direct software/proptech collision observed | Medium | Pass |
| `certono` | query run | query run | query run | query run | low direct collision signal; lexical family still crowded | Medium | Pass |
| `verorio` | query run | query run | query run | query run | low direct collision signal | Medium | Pass |
| `verobil` | query run | query run | query run | query run | no obvious exact market collision in quick scan | Medium | Pass |
| `fidemen` | query run | query run | query run | query run | no obvious direct collision in same category | Medium | Pass |
| `trueledva` | query run | query run | query run | query run | lexical overlap with ledger/accounting naming patterns | Medium | Pass (watchlist) |
| `lumenvia` | query run | query run | query run | query run | existing company/brand usage detected | High | Hold |
| `terravia` | query run | query run | query run | query run | known historical brand usage detected | High | Hold |
| `domuso` | query run | query run | query run | query run | active proptech incumbent detected | High | Block |
| `nexava` | query run | query run | query run | query run | existing company/domain usage detected | High | Block |
| `fiderum` | query run | query run | query run | query run | existing organizational usage detected | High | Hold |
| `veromen` | query run | query run | query run | query run | existing company/restaurant usage detected | High | Hold |

## Register Query Links (Finalists)

### certorio
- DPMA: https://register.dpma.de/DPMAregister/marke/register/erweitert?queryString=certorio
- Swissreg/IGE: https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp?language=de&searchText=certorio
- TMview: https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=certorio
- Zefix: https://www.zefix.ch/de/search/entity/list/firm?name=certorio

### certono
- DPMA: https://register.dpma.de/DPMAregister/marke/register/erweitert?queryString=certono
- Swissreg/IGE: https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp?language=de&searchText=certono
- TMview: https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=certono
- Zefix: https://www.zefix.ch/de/search/entity/list/firm?name=certono

### verorio
- DPMA: https://register.dpma.de/DPMAregister/marke/register/erweitert?queryString=verorio
- Swissreg/IGE: https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp?language=de&searchText=verorio
- TMview: https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=verorio
- Zefix: https://www.zefix.ch/de/search/entity/list/firm?name=verorio

### verobil
- DPMA: https://register.dpma.de/DPMAregister/marke/register/erweitert?queryString=verobil
- Swissreg/IGE: https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp?language=de&searchText=verobil
- TMview: https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=verobil
- Zefix: https://www.zefix.ch/de/search/entity/list/firm?name=verobil

### fidemen
- DPMA: https://register.dpma.de/DPMAregister/marke/register/erweitert?queryString=fidemen
- Swissreg/IGE: https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp?language=de&searchText=fidemen
- TMview: https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=fidemen
- Zefix: https://www.zefix.ch/de/search/entity/list/firm?name=fidemen

### trueledva
- DPMA: https://register.dpma.de/DPMAregister/marke/register/erweitert?queryString=trueledva
- Swissreg/IGE: https://www.swissreg.ch/srclient/faces/jsp/trademark/sr300.jsp?language=de&searchText=trueledva
- TMview: https://www.tmdn.org/tmview/#/tmsearch?page=1&criteria=trueledva
- Zefix: https://www.zefix.ch/de/search/entity/list/firm?name=trueledva

## Gate Decision for 713.4
Advance to adversarial review with this provisional pass set:
- `certorio`
- `certono`
- `verorio`
- `verobil`
- `fidemen`
- `trueledva`

## Legal Notice
This pre-screen is an internal triage artifact and not legal advice. Final clearance must be performed by qualified trademark counsel before filing or launch.
