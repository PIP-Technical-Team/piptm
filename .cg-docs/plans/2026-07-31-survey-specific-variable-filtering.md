---
date: 2026-07-31
title: "Implement survey-specific variable filtering endpoints"
status: active
completed-phases: []
current-phase: 1
failing-steps: []
deviation-policy: ask
execution-report: .cg-docs/work-reports/2026-07-31-survey-specific-variable-filtering.md
scope: Lightweight
language: R
---

# Plan: Implement survey-specific variable filtering endpoints

## Objective

Update API endpoints so Step 2 UI can pass selected `pip_id` values and receive only variables available across all selected surveys.

## Steps

### 1. Add optional `pip_id` support to `/categories`
- Keep current behavior when `pip_id` is omitted.
- When `pip_id` is provided, filter returned categories to the intersection of manifest `dimensions` across matched surveys.
- Preserve response envelope and include helpful metadata.

### 2. Add optional `pip_id` support to `/covariates`
- Keep current behavior when `pip_id` is omitted.
- When `pip_id` is provided, filter returned covariates to the same intersection logic.
- Preserve response envelope and include helpful metadata.

### 3. Validate behavior with targeted tests/checks
- Verify both endpoints still return full catalogs with no `pip_id`.
- Verify both endpoints return filtered catalogs with one or more `pip_id`.
- Verify empty/no-match handling returns empty collections without crashing.

## Completion Contract

### Outcome
- `/categories` and `/covariates` accept optional `pip_id` and return intersection-filtered results when supplied.
- Existing clients without `pip_id` continue to receive full catalogs.

### Verification Surface
- Focused endpoint tests or runtime calls demonstrate:
  - no-`pip_id` path unchanged,
  - filtered path works,
  - no-match path is safe and deterministic.
