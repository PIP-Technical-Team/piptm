---
date: 2026-11-09
title: "Name Specific Covariates in Table Exclusion Error Messages"
status: completed
completed-date: 2026-11-09
scope: "Lightweight"
brainstorm: null
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-11-09-covariate-exclusion-error-wording.md"
artifact-schema-version: 1
tags: [api, error-handling, table_maker, ux-wording]
---

# Plan: Name Specific Covariates in Table Exclusion Error Messages

## Objective

Replace the two generic, jargon-mixing `user_message` strings in
`table_maker()`'s dimension-exclusion paths with messages that name the
actual covariate(s) the user must remove, using their UI-facing labels.
Replace "breakdown" wording with "covariate" throughout.

## Context

`R/table_maker.R` has two places where, if every requested survey is
excluded for lacking requested dimensions, `table_maker()` raises a
`piptm_abort()` with a generic `user_message`:

- Line ~794-801 (`by`-dimension exclusion, variable `by_check` in scope):
  `"None of the selected surveys support all of the requested breakdown
  variables. Try fewer breakdown dimensions or different surveys."`
- Line ~727-734 (`filter_base`-dimension exclusion, variable `filter_vars`
  in scope): `"None of the selected surveys contain the variables needed
  for your sample filters. Try removing a filter or selecting different
  surveys."`

Both are vague and mix "variables"/"dimensions" terminology. The registry
(`piptm_variable_registry()`, backing `piptm_layout_covariates()` and
`piptm_filter_categories()`) already maps internal `varname`s (e.g. `"area"`)
to UI labels (e.g. `"Area"`), keyed by varname.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Add an internal helper that resolves internal variable names to UI labels via the release's variable registry, falling back to the raw varname if no label is found, and never erroring. | User request item 3 |
| R2 | Update the `by`-dimension exclusion `user_message` (`R/table_maker.R` ~line 800) to name the specific missing covariate(s) by label, use "covariate"/"covariates" (not "breakdown"), and pluralize naturally for 2+. | User request item 1 |
| R3 | Update the `filter_base`-dimension exclusion `user_message` (`R/table_maker.R` ~line 732) with the same wording style, naming the actual `filter_base` covariate(s). | User request item 2 |
| R4 | Update existing tests asserting the old wording; add a 2+-covariate case to check pluralization/joining. | User request item 4 |
| R5 | Update `docs/api-contract.md`'s representative-messages table to the new wording. | User request item 5 |
| R6 | Developer-facing `conditionMessage()` text (the `cli_abort` message argument) stays unchanged; only `user_message` changes. | User request constraint |

## Implementation Steps

### 1. Add `.piptm_covariate_labels()` helper in `R/errors.R`
- **Requirements**: R1
- **Files**: `R/errors.R`, `tests/testthat/test-errors.R`
- **Details**: `.piptm_covariate_labels(varnames, release)` looks up
  `piptm_variable_registry(release)` inside a `tryCatch`, maps each
  `varnames` entry to `registry[[v]]$ui_label` when present and non-empty,
  else falls back to `v` itself. Returns a character vector the same length
  and order as `varnames`. Never raises.
- **Test Scenarios**: happy path (known varnames resolve to labels); unknown
  varname falls back to itself; registry lookup failure (e.g. no release
  set) falls back to raw varnames for all inputs without erroring.
- **Tests**: `tests/testthat/test-errors.R`
- **Acceptance criteria**: Helper returns correct labels/fallbacks in all
  three scenarios.

### 2. Update the two `user_message`s in `R/table_maker.R`
- **Requirements**: R2, R3, R6
- **Files**: `R/table_maker.R`
- **Details**:
  - `filter_base` site (~line 727-734): build
    `labels <- .piptm_covariate_labels(filter_vars, release)` and set
    `user_message` to, for 1 label: `sprintf("None of the selected surveys
    have data for the selected covariate: %s. Remove this covariate or
    choose a different survey.", labels)`; for 2+: `sprintf("None of the
    selected surveys have data for the selected covariates: %s. Remove one
    or more of these covariates or choose a different survey.",
    paste(labels, collapse = ", "))`.
  - `by`-dimension site (~line 794-801): same pattern using
    `labels <- .piptm_covariate_labels(by_check, release)`.
  - Leave the `cli_abort`/developer message text (`"All requested surveys
    were excluded: none have all of the requested dimensions ({.val
    {by_check}})."` etc.) unchanged.
- **Test Scenarios**: single missing covariate (1 survey or all surveys
  excluded) names it by label; 2 missing covariates joins both labels with
  "and"/", " and pluralizes "covariates".
- **Tests**: `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: API 422 responses for both exclusion paths show
  the new wording with correct label(s).

### 3. Update tests asserting the old wording
- **Requirements**: R4
- **Files**: `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`
- **Details**: Update the existing assertions in
  `"table_maker() empty pip_id and filter_base errors carry user_message"`
  (in `test-errors.R`) and the `GET /table` 422-message test in
  `test-api-endpoints.R` (the `by`-exclusion and `filter_base`-exclusion
  subcases) to the new wording. Add one new case with 2 covariates
  requested where neither survey has both, to check the pluralized/joined
  message.
- **Tests**: `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: All updated/added tests pass.

### 4. Update `docs/api-contract.md`
- **Requirements**: R5
- **Files**: `docs/api-contract.md`
- **Details**: Update the two rows in the representative-messages table
  (breakdown-dimension and sample-filter-dimension exclusion) to the new
  wording, using a placeholder label like `Area` in the example.
- **Tests**: none (documentation only).
- **Acceptance criteria**: Table rows match actual message format.

## Testing Strategy

Run `tests/testthat/test-errors.R` and `tests/testthat/test-api-endpoints.R`
targeted first, then the full `devtools::test()` suite as the commit gate.

## Documentation Checklist

- [ ] `docs/api-contract.md` representative-messages table rows updated to the new wording

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| `.piptm_covariate_labels()` registry lookup raises if no release is set or registry is malformed | Medium — would turn a message-wording change into a new crash path | Low | Wrap the registry lookup in `tryCatch` and fall back to raw varnames on any error (R1/C2). |
| Updated tests miss an existing assertion still pinned to the old wording elsewhere in the suite | Low — stray test failure after merge | Low | Run the full test suite (V2) before completion, not just the targeted files. |
| Pluralization/joining logic diverges from the style of existing multi-value messages (e.g. PPP "Available PPP years: X, Y") | Low — minor inconsistency | Low | Reuse the same `paste(..., collapse = ", ")` pattern already used elsewhere in `R/table_maker.R`/`R/load_data.R`. |

## Out of Scope

- The `/description` "Excluded surveys" `reason` column wording (raw
  internal string; explicitly deferred by the user to a future task).
- Any other error message wording not covered by R2/R3.

## Completion Contract

### Outcome
`table_maker()`'s two dimension-exclusion `user_message`s name the specific
covariate(s) (by UI label) the requester must remove, worded consistently
with "covariate"/"covariates", and all related tests and docs are updated.

### Verification Surface
| ID | Evidence Required | Command/Artifact | Required |
|----|--------------------|-------------------|----------|
| V1 | `test-errors.R` and `test-api-endpoints.R` pass with new wording assertions, including the 2-covariate case | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-errors.R'); testthat::test_file('tests/testthat/test-api-endpoints.R')"` | yes |
| V2 | Full test suite passes with 0 failures | `Rscript -e "devtools::test()"` | yes |

### Constraints
| ID | Constraint | Check |
|----|------------|-------|
| C1 | Developer-facing `conditionMessage()` text for these two abort sites is unchanged | Diff review of `cli_abort()` message arguments in `R/table_maker.R` |
| C2 | `.piptm_covariate_labels()` never raises, even with no release set or unknown varnames | Covered by Step 1 test scenarios |

### Boundaries
- Allowed: `R/errors.R`, `R/table_maker.R`, `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`, `docs/api-contract.md`.
- Out of scope: any other file; `/description` rendering wording.

### Iteration Policy
1. Follow active `deviation-policy: ask` — pause and ask before any wording or scope deviation.

### Blocked-Stop Conditions
- Required verification cannot be run.
- Any required evidence item (V1, V2) fails after allowed recovery attempts.
