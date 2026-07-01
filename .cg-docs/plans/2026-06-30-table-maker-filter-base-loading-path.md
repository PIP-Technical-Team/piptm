---
date: 2026-06-30
title: "Table Maker sample-base filtering (filter_base) in loading path"
status: active
scope: "Standard"
brainstorm: null
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
phases: 2
completed-phases: []
current-phase: 2
failing-steps:
  - V10: focused suite does not fully pass due pre-existing failures in `test-table-maker.R` and `test-api-endpoints.R`
  - V3: `filter_base` table-maker prefilter assertions are currently blocked by existing `meta[result, on = "pip_id"]` failure path
execution-report: ".cg-docs/work-reports/2026-06-30-table-maker-filter-base-loading-path.md"
tags: [table-maker, api, arrow, filtering, sample-base, load-surveys]
---

# Plan: Table Maker sample-base filtering (filter_base) in loading path

## Objective
Add a `filter_base` feature that restricts the sample entering computation by filtering microdata in the loading path only (API endpoint → `table_maker()` → `load_surveys()`), with Arrow-side filtering applied before `collect()`.

## Context
`table_maker()` currently computes on full samples (optionally disaggregated by `by`). The UI now needs sample-base restriction semantics: AND across variables, IN within each variable’s selected codes. This must occur pre-collect for performance and determinism, and without touching compute-layer logic.

## Requirements
| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Existing `/table` endpoint accepts optional `filter_base` query parameter | user |
| R2 | Endpoint parses `filter_base` JSON with `jsonlite::fromJSON()` when non-NULL | user |
| R3 | Parsed `filter_base` is passed through to `table_maker()` | user |
| R4 | `table_maker()` adds `filter_base = NULL` parameter | user |
| R5 | `table_maker()` validates `filter_base` variable names against `pip_optional_dims()` and aborts clearly on invalid names | user |
| R6 | `table_maker()` excludes manifest entries lacking all `filter_base` variables and warns with excluded `pip_id`s | user |
| R7 | `table_maker()` includes all `filter_base` variables in `needed_cols` before calling `load_surveys()` | user |
| R8 | `table_maker()` passes `filter_base` to `load_surveys()` | user |
| R9 | `load_surveys()` adds `filter_base = NULL` parameter | user |
| R10 | `load_surveys()` applies filter on Arrow dataset after `open_dataset()` and before `collect()` | user |
| R11 | Filter semantics are AND across variables and IN within each variable’s value set | user |
| R12 | When `cols` is non-NULL, `load_surveys()` forcibly includes all `filter_base` columns in selected columns | user |
| R13 | Backward compatibility: behavior unchanged when `filter_base` is NULL/omitted | user |
| R14 | No compute-layer modifications (`compute_measures()` and all `compute_*`) | user |
| R15 | After `collect()`, `load_surveys()` drops `filter_base` columns that were added only for filtering (not originally user-requested in `cols`) | user |

## Phase 1: Core implementation

### 1. Extend `/table` endpoint to receive and pass `filter_base`
- **Requirements**: R1, R2, R3, R13
- **Files**: `inst/plumber/plumber.R` (and `inst/plumber/helpers.R` only if input validation extension is needed)
- **Details**:
  - Update the existing `GET|POST /table` handler signature to include `filter_base = NULL`.
  - Parse non-NULL `filter_base` using `jsonlite::fromJSON()` inside the handler before calling `piptm::table_maker()`.
  - Preserve current validation/error envelope behavior; malformed JSON should surface as existing 422 domain error through `capture_with_warnings()`.
  - Pass parsed object as named list to `table_maker(filter_base = parsed_filter_base)`.
- **Test Scenarios**: happy path (valid JSON), error path (invalid JSON), compat path (no `filter_base`)
- **Tests**: `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: `/table` accepts omitted or provided `filter_base`, and parsed payload reaches `table_maker()`.

### 2. Add `filter_base` handling in `table_maker()`
- **Requirements**: R4, R5, R6, R7, R8, R13, R14
- **Files**: `R/table_maker.R`
- **Details**:
  - Add function parameter `filter_base = NULL`.
  - Validate structure expectations for named list semantics; enforce variable allowlist with `pip_optional_dims()`.
  - Abort with explicit invalid-variable message when any filter variable is not in allowed optional dimensions.
  - Add manifest pre-filter analogous to existing `by` pre-filter: keep only entries whose `dimensions` contain all filter vars; warn with excluded `pip_id` and missing vars.
  - If all entries are excluded, abort loudly with actionable message.
  - Add filter variable names into `needed_cols` so Arrow select path includes them.
  - Pass `filter_base` through to `load_surveys(entries, ..., cols = needed_cols, filter_base = filter_base)`.
  - Do not alter any call or behavior in compute-layer functions.
- **Test Scenarios**: invalid variable abort, partial survey exclusion warning, all-excluded abort, `needed_cols` propagation, NULL compatibility
- **Tests**: `tests/testthat/test-table-maker.R`
- **Acceptance criteria**: `table_maker()` validates and pre-filters correctly, and forwards `filter_base` plus required columns to loader.

### 3. Apply Arrow-side `filter_base` in `load_surveys()` pre-collect
- **Requirements**: R9, R10, R11, R12, R13, R15
- **Files**: `R/load_data.R`
- **Details**:
  - Add parameter `filter_base = NULL` to `load_surveys()` signature and docs.
  - Preserve `original_cols <- cols` before forcing filter columns into the Arrow select path.
  - For `cols != NULL`, force union with filter variable columns prior to `select()` safety intersection.
  - After `arrow::open_dataset()` and after optional `select()`, apply one filter clause per variable:
    - coerce allowed values to integer codes
    - `ds <- dplyr::filter(ds, .data[[var]] %in% allowed_vals)`
  - Ensure combined semantics naturally implement AND across variables and IN within each variable vector.
  - After `collect()`, compute `filter_only_cols <- setdiff(names(filter_base), original_cols)` and drop those columns from `dt`.
  - Keep filter columns when they are legitimately requested (already present in `cols`, e.g., via `by`/requested output columns).
  - Keep existing PPP column handling, integrity checks, and output shaping unchanged.
- **Test Scenarios**: one-variable filter, multi-variable AND filter, filter with `cols` subset, filter-only column cleanup, NULL compatibility
- **Tests**: `tests/testthat/test-load-data.R`
- **Acceptance criteria**: loader returns only rows matching `filter_base`, drops filter-only columns post-collect, and still supports existing NULL behavior.

## Phase 2: Tests and verification

### 4. Add endpoint tests for `filter_base` parse and pass-through
- **Requirements**: R1, R2, R3, R13
- **Files**: `tests/testthat/test-api-endpoints.R`
- **Details**:
  - Add success test for `/table?...&filter_base={...}` with valid JSON.
  - Add malformed JSON test expecting existing structured error response.
  - Confirm omitted `filter_base` remains successful and unchanged.
- **Test Scenarios**: valid parse, parse error, omitted compatibility
- **Tests**: `devtools::test(filter = "api-endpoints")`
- **Acceptance criteria**: endpoint behavior is stable and `filter_base` does not break existing contract.

### 5. Add `table_maker()` tests for validation and manifest pre-filtering
- **Requirements**: R5, R6, R7, R8, R13, R14
- **Files**: `tests/testthat/test-table-maker.R`
- **Details**:
  - Add invalid filter variable test (`expect_error`).
  - Add exclusion warning test when selected surveys lack some filter variables.
  - Add all-excluded test (`expect_error`).
  - Add test ensuring filter vars are included in requested columns path (behavioral assertion via fixture columns).
- **Test Scenarios**: invalid var, partial exclusion warning, all excluded, needed-cols inclusion
- **Tests**: `devtools::test(filter = "table-maker")`
- **Acceptance criteria**: pre-load filtering gate in `table_maker()` is fully covered.

### 6. Add `load_surveys()` tests for Arrow pre-collect filtering and `cols` interaction
- **Requirements**: R10, R11, R12, R13, R15
- **Files**: `tests/testthat/test-load-data.R`
- **Details**:
  - Add fixture-based tests where filter selects strict subset by integer codes.
  - Add two-variable filter test to confirm AND semantics.
  - Add `cols`-subset test where filter column is not user-requested but must still be present for filter execution.
  - Add cleanup test verifying post-collect removal of filter-only columns (`setdiff(names(filter_base), original_cols)`), while keeping filter columns that are legitimately requested.
  - Add null `filter_base` compatibility test proving unchanged output.
- **Test Scenarios**: single filter, multi-filter AND, cols+filter interaction, filter-only cleanup, null compatibility
- **Tests**: `devtools::test(filter = "load-data")`
- **Acceptance criteria**: filtering is demonstrably pre-collect and robust under column pruning.

## Testing Strategy
- Start focused: run `test-api-endpoints`, `test-table-maker`, and `test-load-data` filters only.
- Add regression check for existing `/table` and `table_maker()` non-filter paths.
- If fixtures need extension, keep changes minimal and local to test helpers.
- Confirm no compute tests need changes since compute layer is untouched.

## Documentation Checklist
- [ ] Update roxygen for `table_maker()` with `@param filter_base` and semantics.
- [ ] Update roxygen for `load_surveys()` with `@param filter_base` and pre-collect filtering note.
- [ ] Update `/table` endpoint param docs in `inst/plumber/plumber.R` comments.
- [ ] Ensure examples/notes distinguish `by` vs `filter_base` semantics.

## Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|------------|
| JSON parsing shape inconsistencies from query string | Endpoint passes malformed structure downstream | Normalize/validate parsed `filter_base` structure at entry; test malformed and scalar cases |
| Column pruning drops filter columns | Arrow filter fails or silently no-ops | Force inclusion of filter vars in `needed_cols` and loader `physical_cols`; add explicit tests |
| Manifest `dimensions` mismatch across surveys | Unexpected over-exclusion or under-exclusion | Reuse existing pre-filter pattern and include detailed warning messages with missing vars |
| Integer code coercion mismatch | Filter selects wrong rows | Coerce explicitly and test with known integer-coded fixture values |
| Backward compatibility regressions | Existing clients break | Keep `filter_base` optional default NULL and add compatibility tests |

## Out of Scope
- Any changes to `compute_measures()`.
- Any changes to `compute_*` helper functions.
- UI/front-end behavior and state management.
- New endpoints beyond modifying existing `/table`.
- Roadmap/issue management changes.

## Completion Contract

### Outcome
The Table Maker loading pipeline supports optional sample-base filtering through `filter_base`, with filtering executed in Arrow before `collect()`, and all legacy non-filter usage preserved.

### Verification Surface
| ID | Phase | Evidence Required | Command/Artifact | Required |
|----|:-----:|-------------------|------------------|:--------:|
| V1 | 1 | Endpoint accepts and parses `filter_base` | `tests/testthat/test-api-endpoints.R` | yes |
| V2 | 1 | Invalid filter variables abort in `table_maker()` | `tests/testthat/test-table-maker.R` | yes |
| V3 | 1 | Manifest pre-filter excludes/warns by missing filter dims | `tests/testthat/test-table-maker.R` | yes |
| V4 | 1 | `needed_cols` includes filter vars before load call | `tests/testthat/test-table-maker.R` | yes |
| V5 | 1 | `load_surveys()` filters dataset pre-collect with AND/IN semantics | `tests/testthat/test-load-data.R` | yes |
| V6 | 1 | `cols` + `filter_base` includes filter columns in Arrow select path | `tests/testthat/test-load-data.R` | yes |
| V7 | 2 | Post-collect cleanup drops filter-only columns while preserving requested columns | `tests/testthat/test-load-data.R` | yes |
| V8 | 2 | Backward compatibility for `filter_base = NULL` in endpoint/table/load | targeted tests in three files | yes |
| V9 | final | No compute-layer files changed | changed-file audit | yes |
| V10 | final | Focused tests pass | `devtools::test(filter = "api-endpoints|table-maker|load-data")` | yes |

### Constraints
| ID | Constraint | Check |
|----|------------|-------|
| C1 | Filtering is pre-collect in Arrow path | `R/load_data.R` review + tests |
| C2 | `filter_base` optional; omitted behavior unchanged | compatibility tests |
| C3 | `filter_base` variable names restricted to `pip_optional_dims()` | validation tests |
| C4 | Manifest-level exclusion occurs before file loading | `R/table_maker.R` flow + tests |
| C5 | Filter columns forced into both `needed_cols` and loader select path | table/load tests |
| C6 | Post-collect cleanup removes only filter-only columns (not user-requested columns) | load-data tests |
| C7 | No compute-layer edits | changed-file audit |

### Boundaries
- Allowed: edits in endpoint, loader orchestration, and tests for these paths.
- Allowed: minimal helper validation adjustments needed to support endpoint parsing.
- Out of scope: compute function logic and result calculation rules.

### Iteration Policy
1. Implement endpoint parse + pass-through.
2. Implement `table_maker()` validation and manifest pre-filter.
3. Implement loader pre-collect filtering and `cols` forcing.
4. Add targeted tests for each gate.
5. Run focused suites, fix scoped failures only.
6. Halt and report if fixture constraints prevent reliable verification.

### Blocked-Stop Conditions
- Endpoint contract clarification: `/table` is the only API route; `table_maker()` is the internal R function used by that route.
- Existing fixtures cannot represent required filter dimensions/codes without broader unrelated fixture rewrites.
- Any required behavior would force compute-layer changes (out-of-scope hard stop).
