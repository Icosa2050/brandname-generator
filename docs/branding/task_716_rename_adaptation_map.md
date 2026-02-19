---
owner: tbd
status: active
last_validated: 2026-02-19
task: 716
---

# Task 716 Rename Preparation Adaptation Map

Scope: prepare a rename-ready map for runtime strings, localization, database naming compatibility, and PDF-visible brand copy so the next rename is primarily config-driven.

## Summary

This document is the implementation handoff for `task 716` and covers subtasks `716.1` through `716.12`.

Key finding: `lib/app_config.dart` expects `appName` while `assets/config/app_config.json` currently defines `app_name`, so the configured app name is not consumed today (`lib/app_config.dart:40`, `assets/config/app_config.json:2`).

## 716.1, 716.9, 716.10: Database Name Decoupling and Migration Matrix

### Current state

- Hardcoded DB basename: `kostula_db` in `lib/database/connection_impl_flutter.dart:12`.
- Drift file path construction uses `<dbName>.sqlite` and sidecars (`-wal`, `-shm`, `-journal`) in `lib/database/drift_native_options_io.dart:7`.
- Existing migration behavior only moves same basename from documents dir to application support dir (`lib/database/drift_native_options_io.dart:23` to `lib/database/drift_native_options_io.dart:45`).

### Target design

- Introduce a stable internal DB identifier in config, separate from marketing name:
  - Example internal basename: `settlement_core_db`.
  - Keep this stable across brand renames.
- Maintain explicit legacy basename list for idempotent migration:
  - first entry is active basename,
  - following entries are old basenames.

### Legacy migration matrix (idempotent behavior)

| Condition | Action |
| --- | --- |
| Active basename exists in support dir | Use it; no migration. |
| Active basename missing, legacy basename exists in support dir | Rename/copy legacy basename + sidecars to active basename in support dir. |
| Active and legacy missing in support dir, legacy exists in documents dir | Migrate documents legacy basename + sidecars to support active basename. |
| Multiple legacy candidates exist | Choose first according to ordered list; log choice once. |
| Migration copy/rename fails for optional sidecars | Continue; only primary sqlite file is required. |
| Migration fails for primary sqlite file | Keep original path, emit warning, do not delete source. |

### Concrete implementation targets

- `lib/database/connection_impl_flutter.dart`
  - source basename from typed config accessor, not hardcoded const.
- `lib/database/drift_native_options_io.dart`
  - accept ordered candidate basenames and target basename,
  - migrate sidecars per basename pair,
  - keep operation idempotent.

## 716.2 and 716.8: Runtime/App-Surface Name Inventory and Centralization Plan

### Primary runtime hardcoded strings

| Surface | Current source |
| --- | --- |
| Logger namespace | `lib/main.dart:47` (`Logger('Kostula')`) |
| Startup log line | `lib/main.dart:95` (`Kostula main() started`) |
| Desktop window title | `lib/main.dart:224` (`title: 'Kostula'`) |
| Material app title | `lib/main.dart:613` (`title: 'Kostula'`) |
| Config fallback app name | `lib/app_config.dart:27`, `lib/app_config.dart:40` |

### Additional runtime touchpoints to include in rename rollout

| Surface | Current source |
| --- | --- |
| Splash title hardcoded text | `lib/widgets/splash/app_splash_screen.dart:45` |
| macOS shortcut dialog literals | `lib/widgets/keyboard/shortcuts_help_dialog.dart:300`, `lib/widgets/keyboard/shortcuts_help_dialog.dart:302` |
| Navigation fallback app label | `lib/widgets/navigation/adaptive_navigation.dart:686` |
| macOS app/help menu labels via l10n keys | `lib/platform/macos/platform_menu_manager.dart:101`, `lib/platform/macos/platform_menu_manager.dart:553` |

### Centralization checklist

1. Route app display name and short app name through typed brand config accessors.
2. Keep technical identifiers (logger category, class names) stable unless explicit refactor is required.
3. Remove hardcoded UI strings that bypass l10n or config.
4. Ensure config key naming is consistent (`appName` vs `app_name`) before wiring.

## 716.3, 716.11, 716.12: EN ARB Baseline and Parameterization Plan

### EN ARB keys with brand literals

| Key | Current line | Notes |
| --- | --- | --- |
| `aboutKostula` | `lib/l10n/app_en.arb:5` | App title phrasing. |
| `errorReportingSubtitle` | `lib/l10n/app_en.arb:1491` | Brand mention. |
| `errorReportingOnboardingSubtitle` | `lib/l10n/app_en.arb:1493` | Brand mention. |
| `errorReportingConsentBody` | `lib/l10n/app_en.arb:1497` | Brand mention. |
| `kostula` | `lib/l10n/app_en.arb:1717` | Core app label used in menus/navigation. |
| `kostulaLicenseTerms` | `lib/l10n/app_en.arb:1719` | Legal title. |
| `mitLicenseText` | `lib/l10n/app_en.arb:2171` | Legal body includes brand term. |
| `welcomeToKostula` | `lib/l10n/app_en.arb:3672` | Onboarding. |
| `startUsingKostula` | `lib/l10n/app_en.arb:3726` | Onboarding CTA. |
| `menuKostulaHelp` | `lib/l10n/app_en.arb:8381` | macOS menu. |
| `menuHideKostula` | `lib/l10n/app_en.arb:8405` | macOS menu. |
| `demoDatasetPromptBody` | `lib/l10n/app_en.arb:9322` | Demo prompt. |
| `paywallDescPdfExport` | `lib/l10n/app_en.arb:10174` | Mentions branded PDF footer. |
| `freeTierPdfFooter` | `lib/l10n/app_en.arb:10196` | Footer text with app/domain. |
| `paywallOnboardingLimitsBody` | `lib/l10n/app_en.arb:10202` | Tier + brand phrase. |

### Proposed placeholder baseline (EN first)

- `{brandName}` for app display name.
- `{brandDomain}` for public domain string.
- `{freeTierName}` for tier display label.
- Optional `{legalTermsName}` where legal labels diverge from app display name.

### Propagation notes

- Update ARB source only; do not edit generated localization files directly.
- Keep key IDs stable initially; parameterize values first to reduce translation churn.
- Regeneration and checks after ARB changes:
  - `dart run tool/dev.dart arb detect --summary-only`
  - `dart run tool/dev.dart arb analyze`
  - `dart run tool/dev.dart arb validate --enhanced`

## 716.4 and 716.12: PDF-Visible Brand Text Map

| Source | Consumer | Exposure |
| --- | --- | --- |
| `FreeTierBranding.defaultFooterMessage` (`lib/services/pdf/branding/free_tier_branding.dart:8`) | `appendFreeTierNotice` fallback (`lib/services/pdf/branding/free_tier_branding.dart:21`) | Direct PDF footer text when localized message is absent. |
| `freeTierPdfFooter` ARB key (`lib/l10n/app_en.arb:10196`) | `_currentServiceLocalizations().freeTierPdfFooter` in `lib/services/pdf/core/simplified_pdf_engine.dart:3002`, `lib/services/pdf/core/simplified_pdf_engine.dart:3344` | Primary localized free-tier footer copy in generated PDFs. |
| `paywallDescPdfExport` ARB key (`lib/l10n/app_en.arb:10174`) | Paywall UI in `lib/widgets/paywall/paywall_dialog.dart:511` and `lib/widgets/paywall/paywall_banner.dart:160` | Must stay semantically aligned with actual PDF footer wording. |
| `l10n.kostula` (`lib/l10n/app_en.arb:1717`) | PDF metadata author in `lib/services/pdf/core/simplified_pdf_engine.dart:1277` | Embedded PDF metadata; not always user-visible, but part of exported artifact identity. |

### PDF rename rule

- Single source of truth for footer copy must be localized `freeTierPdfFooter`.
- `defaultFooterMessage` should be treated as emergency fallback only and mirror the same template contract.

## 716.5, 716.6, 716.7: Reusable Brand/Tier Config Seam and Contract

### Current config mismatch to fix first

- JSON key uses `app_name` (`assets/config/app_config.json:2`).
- Runtime getter expects `appName` (`lib/app_config.dart:40`).
- Result: fallback path is used, not config-sourced app name.

### Proposed canonical JSON contract

```json
{
  "brand": {
    "id": "kostula",
    "display_name": "Kostula",
    "short_name": "Kostula",
    "domain": "kostula.com",
    "legal_terms_name": "Kostula License Terms"
  },
  "tiers": {
    "active_tier_id": "free",
    "catalog": [
      {
        "id": "free",
        "display_name": "Kostula Free",
        "pdf_footer_template": "Generated with {freeTierName} - Upgrade at {brandDomain}"
      },
      {
        "id": "premium",
        "display_name": "Kostula Premium"
      }
    ]
  },
  "storage": {
    "database": {
      "basename": "settlement_core_db",
      "legacy_basenames": ["kostula_db"]
    }
  }
}
```

### Typed accessor policy

- Accessors live behind one resolver (for example in `lib/app_config.dart` with shared constants in `lib/config/app_config.dart`).
- All call sites read typed values, never map keys directly.
- Fallback policy:
  - Missing required field: use hardcoded safe default and log warning.
  - Unknown `active_tier_id`: fallback to `free`.
  - Missing `legacy_basenames`: fallback to empty list.
  - Empty domain: suppress domain substitution rather than emitting malformed text.

## Dependency-Aligned Rollout Sequence

1. Land schema + typed accessors (`716.5` -> `716.6` -> `716.7`).
2. Centralize runtime titles/log text reads (`716.8`) using the new accessors.
3. Decouple DB basename + migration matrix implementation (`716.9` -> `716.10`).
4. Parameterize EN ARB brand/tier literals and map all usages (`716.11` -> `716.12`).
5. Propagate to non-EN ARB files and regenerate localization outputs.

## Verification Checklist for Implementation Phase

- `python3 ./tools/testing/flutter_errors_lib_only.py`
- `dart run tool/dev.dart arb detect --summary-only`
- `dart run tool/dev.dart arb analyze`
- `dart run tool/dev.dart arb validate --enhanced`
- Targeted PDF export smoke test (free and premium) to validate footer copy behavior.
- Database migration smoke test with pre-existing legacy filename fixture.

## Deliverable Coverage by Subtask

- `716.1`: DB rename plan and compatibility strategy documented.
- `716.2`: runtime name inventory and replacement checklist documented.
- `716.3`: EN ARB rename baseline documented.
- `716.4`: PDF-visible source map documented.
- `716.5`: reusable brand config seam documented.
- `716.6`: canonical schema contract documented.
- `716.7`: typed accessor and fallback policy documented.
- `716.8`: runtime centralization plan documented.
- `716.9`: DB basename decoupling strategy documented.
- `716.10`: legacy DB migration matrix documented.
- `716.11`: EN ARB brand literal parameterization plan documented.
- `716.12`: EN ARB tier/PDF wording parameterization plan documented.
