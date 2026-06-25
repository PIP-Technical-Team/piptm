---
title: "Variable Registry - Phase 2 Blockers"
date: 2026-06-23
author: copilot
tags:
  - blockers
  - testing
---

Summary
-------

During the Phase 2 phase-boundary test run (`tests/testthat/test-api-endpoints.R`) the following unrelated test groups failed and are recorded as blockers for follow-up work:

- `/measures` endpoint
  - Symptoms: returned 404 or error envelope; expected 200 success with data table of 12 entries.
  - Files / tests: `tests/testthat/test-api-endpoints.R` around lines ~256-276.

- `/table` endpoint
  - Symptoms: multiple scenarios returned 422 or error envelopes instead of 200 success; meta fields (e.g., `meta$release`) were NULL in fixtures.
  - Files / tests: `tests/testthat/test-api-endpoints.R` around lines ~691-760 and 837-840.

Notes
-----

- These failures appear to be environment/fixture-related (manifests not loaded, current-release/fixture release mismatch, or missing pip_id/table fixtures) rather than regressions introduced by the variable-registry work.
- The registry-backed endpoints (`/analysis-variables`, `/categories`, `/covariates`) passed in the integration run and are considered green.

Recommended follow-up actions
----------------------------

1. Triage `/measures` failures: re-run the failing test block with verbose logging, inspect router error messages and the manifests loaded in `.piptm_env`.
2. Triage `/table` failures: verify test fixtures, ensure manifest-arrow caches are available and `current_release` matches fixture `fx$release` (e.g. `20260206_EP_TEST`), and re-run.
3. If CI runs omit manifest setup, add a controlled test fixture loader or mark those tests as integration-only.

Assigned to: team follow-up (unrelated to registry scope)
