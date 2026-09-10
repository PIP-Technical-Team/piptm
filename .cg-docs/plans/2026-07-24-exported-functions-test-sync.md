---
date: 2026-07-24
title: "Sync testthat suite to current exported functions"
status: active
completed-phases: []
current-phase: 1
failing-steps: []
deviation-policy: ask
execution-report: .cg-docs/work-reports/2026-07-24-exported-functions-test-sync.md
scope: Standard
language: R
---

# Plan: Sync tests with current exported functions

## Objective

Bring `tests/testthat` in sync with the current implementation in `R/` for exported functions, treating current function code as ground truth.

## Phase 1: Baseline and audit

### 1. Capture exported API and current test mapping
- Read `NAMESPACE` and exported function definitions in `R/`.
- Map each exported function to existing tests.
- Identify missing coverage and stale expectations.

### 2. Run baseline tests and baseline coverage
- Run full `testthat` suite.
- Run `covr` and capture total percent coverage.
- Record raw pass/fail output and failing test details.

## Phase 2: Align tests to current behavior

### 3. Update tests for exported functions that still exist and changed behavior
- Rewrite expectations to match current function behavior.
- Add focused tests for exported functions currently untested.

### 4. Flag obsolete tests before deletion
- For tests that no longer match any valid current behavior, list each candidate with concise rationale.
- Pause for user confirmation before deleting/rewriting those tests.

## Completion Contract

### Outcome
- Exported-function tests reflect current behavior in `R/`.
- Baseline and post-change suite status is visible with actual failing/passing test output.
- Coverage percentage is reported using `covr`.

### Verification Surface
- `devtools::test()` output captured and reported with real failing/passing lines.
- `covr::package_coverage()` and `covr::percent_coverage()` executed and reported.
- Candidate obsolete tests listed with short reason and explicit user confirmation gate before removal.
