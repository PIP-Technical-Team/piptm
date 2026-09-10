---
date: 2026-05-29
title: "Refactor PPP handling: default to 2021, skip surveys missing welfare column"
status: completed
completed-date: 2026-05-29
scope: Lightweight
brainstorm: null
language: R
estimated-effort: small
tags: [refactor, load-data, ppp]
phases: 2
completed-phases: [1, 2]
---

# Plan: Refactor PPP handling in load_surveys() and load_survey_microdata()

## Objective

Remove the `ppp_sort` inference/fallback mechanism from both `load_survey_microdata()` and `load_surveys()`. Default `ppp` to `2021L` (configurable via the argument default). In `load_surveys()`, when a survey lacks the welfare column for the requested PPP, skip that survey with a warning instead of erroring — return the remaining surveys that do have it.

## Context

Currently, when `ppp = NULL`:
- `load_survey_microdata()` falls back to `ppp_sort` from the manifest entry.
- `load_surveys()` requires all entries to share the same `ppp_sort` value and errors if they diverge.

This is fragile because surveys are not guaranteed to share the same `ppp_sort`. The new behavior:
- Default `ppp = 2021L` in both functions (hard-coded default argument, easily changed later).
- `load_survey_microdata()`: if the requested PPP column is missing, error (single survey — no partial result makes sense).
- `load_surveys()`: if some surveys lack the PPP column, skip them with a warning and return the rest. Error only if ALL surveys lack it.

## Requirements

| ID  | Requirement                          | Source |
|-----|--------------------------------------|--------|
| R1  | Default `ppp` argument to `2021L` in both functions | user |
| R2  | Remove `ppp_sort` fallback logic entirely | user |
| R3  | `load_surveys()`: skip surveys missing the PPP welfare column with a warning | user |
| R4  | `load_surveys()`: error if no surveys remain after skipping | implied |
| R5  | `load_survey_microdata()`: error if the single survey lacks the PPP column (existing behavior, unchanged) | implied |

## Implementation Steps

## Phase 1: Core implementation

### 1. Refactor PPP handling in load_data.R
- **Requirements**: R1, R2, R3, R4, R5
- **Files**: `R/load_data.R`
- **Details**:
  - Change function signature: `ppp = NULL` → `ppp = 2021L` in both `load_survey_microdata()` and `load_surveys()`.
  - In `load_survey_microdata()`:
    - Remove the `if (is.null(ppp))` branch that reads `ppp_sort_val` and does fallback. Since `ppp` always has a value now, go straight to the `!is.null(ppp)` path (find candidates via `.find_welfare_col(welfare_vars, ppp)`). The existing error when candidates is empty is correct — keep it.
    - Remove unused references to `ppp_sort_val` in the column-pruning section (step 4a) — simplify to always use `ppp`.
  - In `load_surveys()`:
    - Remove the entire `if (!is.null(ppp)) { ... } else { ... }` block that resolves `effective_year` from `ppp_sort`. Replace with `effective_year <- ppp` (always has a value).
    - Replace the `bad_mask` error with skip-with-warning: filter `entries_dt` to only rows that have a matching welfare column, issue `cli::cli_warn()` listing skipped `pip_id`s, error if none remain.
    - Remove the `ppp_sort` requirement from the `stopifnot()` column check (it's no longer used).
  - Update roxygen `@param ppp` documentation in both functions to reflect the new default and remove references to `ppp_sort`.
- **Test Scenarios**:
  - ✅ Both functions work with explicit `ppp = 2017L` (unchanged)
  - ✅ Both functions work with default `ppp = 2021L` when welfare column exists
  - 🛑 `load_surveys()` skips surveys missing the PPP column and warns
  - ❌ `load_surveys()` errors when ALL surveys lack the column
- **Acceptance criteria**: Functions use the new default; `ppp_sort` logic is gone; partial-batch loading works.

## Phase 2: Tests

### 2. Update tests in test-load-data.R
- **Requirements**: R1, R2, R3, R4
- **Files**: `tests/testthat/test-load-data.R`
- **Details**:
  - Update existing test `"load_survey_microdata() ppp=NULL uses ppp_sort from manifest"` → change to verify the new default (2021) behavior. Will need a fixture with a `welfare_ppp_2021_*` column.
  - Update `"load_survey_microdata() with ppp=NULL and ppp_sort=NA errors informatively"` → this scenario no longer applies (ppp is always provided). Remove or repurpose.
  - Update `"load_surveys() ppp=NULL uses uniform ppp_sort across surveys"` → verify default 2021 behavior.
  - Update `"load_surveys() errors when surveys have inconsistent ppp_sort and ppp=NULL"` → remove (no longer relevant).
  - Update `"load_surveys() errors when a survey lacks the requested ppp column"` → change expectation from error to warning + partial result (only the survey with the column is returned).
  - Add new test: `"load_surveys() warns and skips surveys missing the PPP welfare column"`.
  - Add new test: `"load_surveys() errors when ALL surveys lack the requested PPP column"`.
  - Update `make_ppp_fixtures()` to include a `welfare_ppp_2021_*` column so default-PPP tests work.
- **Acceptance criteria**: All tests pass; new skip-with-warning behavior is covered.

### 3. Update roxygen documentation
- **Requirements**: R1, R2
- **Files**: `R/load_data.R`
- **Details**:
  - Remove `ppp_sort` from `@param` `entries_dt` column list in `load_surveys()` docs (or note it's unused).
  - Update file-level comment block to reflect new PPP strategy.
- **Acceptance criteria**: `devtools::document()` runs clean; no stale references to `ppp_sort` fallback in docs.

## Testing Strategy

- Unit tests with fixture Parquet files (existing pattern).
- Key new scenario: batch with heterogeneous PPP availability → partial load + warning.

## Documentation Checklist
- [x] Function documentation (roxygen2) — updated in step 3
- [ ] README updates — not needed (internal behavior change)
- [x] Inline comments for complex logic
- [x] Usage examples updated in roxygen

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Downstream callers rely on `ppp = NULL` + `ppp_sort` fallback | The default `2021L` is a drop-in replacement; callers passing `ppp` explicitly are unaffected |
| Tests using `ppp_sort`-based fixtures break | Update fixtures to include 2021 columns |
| `table_maker()` passes `ppp = NULL` | Verify call sites; if found, update to omit or pass explicitly |

## Out of Scope

- Removing `ppp_sort` from the manifest schema or manifest-generation code (that's in {pipdata}).
- Changing `load_survey_microdata()` to skip-with-warning (single survey — error is appropriate).
- Adding multiple-PPP loading (load welfare for >1 PPP year simultaneously).
