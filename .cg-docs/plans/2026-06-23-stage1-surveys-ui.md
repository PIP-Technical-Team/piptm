---
title: "Stage 1: Surveys UI endpoint"
date: "2026-06-23"
status: active
deviation-policy: ask
completed-phases: []
current-phase: 1
## Completion Contract
Outcome: Add a new API endpoint `/surveys-ui` that returns a UI-friendly list of surveys for the Table Maker Step 1 grid. Verification: unit test that validates output shape for a fixture manifest.
---

## Phase 1: Implement

### 1.1 Inspect existing manifest + registry APIs
- Use `piptm_manifest()` and variable registry to assemble UI rows.

### 1.2 Implement `piptm_surveys_ui()` helper
- Exported function returning list of surveys with `pip_id`, `country_code`, `country_label`, `welfare_type`, `welfare_label`, `year`, `dimensions` (list of `{varname,label}`), `welfare_vars`, and `ppp_sort`.

### 1.3 Add `/surveys-ui` plumber route
- Route uses existing `capture_with_warnings()` and `api_response()` envelopes.

### 1.4 Tests and docs
- Add a `testthat` unit test that writes a fixture manifest, calls `piptm_surveys_ui()`, and validates output shape.

## Verification Surface
- Unit test `tests/testthat/test-surveys-ui.R` passing for the new behavior.

