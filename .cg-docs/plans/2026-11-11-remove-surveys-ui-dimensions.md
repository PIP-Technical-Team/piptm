---
date: 2026-11-11
title: "Remove Unused dimensions Field from surveys-ui Endpoint"
status: completed
completed-date: 2026-11-11
scope: "Lightweight"
brainstorm: null
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-11-11-remove-surveys-ui-dimensions.md"
tags: [api, plumber, surveys-ui, manifest]
---

# Plan: Remove Unused `dimensions` Field from `surveys-ui` Endpoint

## Objective

Remove the unused `dimensions` field (and the unused variable registry lookup) from `piptm_surveys_ui()` and the `GET /surveys-ui` endpoint response so the Step 1 survey selection catalog does not return unnecessary per-survey dimension arrays.

## Implementation Steps

### 1. Update `piptm_surveys_ui()` and documentation
- In `R/manifest.R` inside `piptm_surveys_ui()`:
  - Remove `dims` / `dims_list` extraction and the `dimensions = dims_list` entry from each survey record, retaining `pip_id`, `country_code`, `country_label`, `year`, `welfare_type`, `welfare_label`, and `version`.
  - Remove the unused `registry <- tryCatch(piptm_variable_registry(release), error = function(e) NULL)` call.
  - Update the roxygen2 docstring of `piptm_surveys_ui()` and `man/piptm_surveys_ui.Rd` so they no longer mention dimension metadata.

### 2. Update `GET /surveys-ui` Plumber OpenAPI comments
- In `inst/plumber/plumber.R`:
  - Update the `GET /surveys-ui` description comment (around line 1164) to reflect the actual fields returned (`pip_id`, `country_code`, `country_label`, `year`, `welfare_type`, `welfare_label`, `version`) without mentioning `dimensions`.

### 3. Update unit and endpoint tests
- In `tests/testthat/test-surveys-ui.R` (and `tests/testthat/test-api-endpoints.R` if applicable):
  - Assert that the expected fields (`pip_id`, `country_code`, `country_label`, `year`, `welfare_type`, `welfare_label`, `version`) are present and explicitly verify that `"dimensions"` is NOT in `names(srv)`.

## Completion Contract

### Outcome

`piptm_surveys_ui()` and `GET /surveys-ui` return survey records containing only `pip_id`, `country_code`, `country_label`, `year`, `welfare_type`, `welfare_label`, and `version` (with no `dimensions` field), with updated documentation and passing `testthat` tests.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | `test-surveys-ui.R` passes and verifies `"dimensions"` is absent from `piptm_surveys_ui()` output | `testthat::test_file("tests/testthat/test-surveys-ui.R")` | yes |
| V2 | API endpoint tests in `test-api-endpoints.R` pass | `testthat::test_file("tests/testthat/test-api-endpoints.R")` | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | `piptm_manifest()` and `GET /surveys` continue to retain `dimensions` for backend intersection and developer inspection | Existing manifest and `/categories` / `/covariates` tests pass |

### Boundaries

- **In scope**: `R/manifest.R` (`piptm_surveys_ui`), `man/piptm_surveys_ui.Rd`, `inst/plumber/plumber.R` (`GET /surveys-ui` doc comment), `tests/testthat/test-surveys-ui.R`, `tests/testthat/test-api-endpoints.R`.
- **Out of scope**: `piptm_manifest()`, `GET /surveys`, `GET /dimensions`, `GET /categories`, `GET /covariates`.

### Blocked-Stop Conditions

- Required verification fails after allowed recovery attempts.
- A required deviation is discovered under policy `ask` without user approval.
