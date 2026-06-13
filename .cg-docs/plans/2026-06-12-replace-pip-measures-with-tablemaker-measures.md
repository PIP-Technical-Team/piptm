---
date: 2026-06-12
title: "Replace pip_measures() with pip_tablemaker_measures() and update /measures endpoint"
status: completed
completed-date: 2026-06-13
scope: "Standard"
brainstorm: null
language: "R"
estimated-effort: "small"
tags: [api, measures, endpoint, catalogue, ui]
phases: 2
completed-phases: [1, 2]
---

# Plan: Replace pip_measures() with pip_tablemaker_measures() and update /measures endpoint

## Objective

Remove `pip_measures()` and replace it with `pip_tablemaker_measures()` — a richer,
static catalogue of analysis variables for the Table Maker Step 2 UI.  Update the GET
`/measures` plumber endpoint to serve the new catalogue.  Update all affected tests.

**Computation logic, query logic, and binary variable resolution are out of scope** — this
plan covers the function definition and endpoint only.

## Context

`pip_measures()` currently returns a named character vector of 21 measure identifiers
mapped to their computation family (`poverty`/`inequality`/`welfare`).  GET `/measures`
wraps this in a column-oriented data.table and serialises it as
`{"measure": [...], "family": [...]}`.

The new `pip_tablemaker_measures()` encodes UI decisions — analysis variable types,
available statistics per type, human-readable labels, and poverty line slider visibility
— as a list of 12 named lists, one per analysis variable.  The GET `/measures` endpoint
returns this list directly via `api_response()`.

The internal `.MEASURE_REGISTRY` and all computation-layer functions (`.classify_measures()`,
`compute_measures()`, `table_maker()`, etc.) are **not touched**.

The sister function `pip_tablemaker_categories()` in `R/categories.R` is the structural
and documentation template.

## Requirements

| ID  | Requirement | Source |
|-----|-------------|--------|
| R1  | `pip_measures()` is removed from `R/measures.R` and from NAMESPACE | Spec |
| R2  | `pip_tablemaker_measures()` is added, takes no arguments, returns a list directly serialisable via `jsonlite::toJSON()` | Spec |
| R3  | Function is fully static — no data sources, Parquet files, or computation at runtime | Spec |
| R4  | Stat-type lists are defined as local variables within the function body; `continuous_stats <- welfare_stats` (reference, not duplication) | Spec |
| R5  | `pop_share` and `obs_share` appear exclusively in `binary_stats`; absent from welfare, poverty, inequality, and continuous stats | Spec |
| R6  | `poverty_line_slider` is `TRUE` only for `type == "poverty"`; `FALSE` for all other types | Spec |
| R7  | Catalogue contains exactly 12 entries in the specified order | Spec |
| R8  | GET `/measures` endpoint calls `pip_tablemaker_measures()` and returns the result directly via `api_response()` — no data.table wrapper | Spec |
| R9  | Roxygen description on the `/measures` endpoint is updated to reflect the new purpose | Spec |
| R10 | `test-measures.R` — `pip_measures()` tests removed; `pip_tablemaker_measures()` contract tests added | Spec |
| R11 | `test-api-endpoints.R` — two `/measures` endpoint tests updated for the new response structure | Spec |
| R12 | `devtools::document()` run; NAMESPACE updated; man pages regenerated | Project standard |

---

## Implementation Steps

phases: 2  # convenience hint — always recount from ## Phase headers

## Phase 1: Implementation

### 1. Remove pip_measures(); add pip_tablemaker_measures() in R/measures.R

- **Requirements**: R1, R2, R3, R4, R5, R6, R7
- **Files**: `R/measures.R`
- **Details**:
  - **Delete** the `pip_measures()` function definition and its roxygen block (currently
    the first exported function in the "Exported helpers" section).  Do **not** touch
    `.MEASURE_REGISTRY`, `.classify_measures()`, `.validate_by()`,
    `.validate_poverty_lines()`, or `.bin_age()`.
  - **Add** `pip_tablemaker_measures()` after the remaining exported helpers.  Follow the
    structure of `pip_tablemaker_categories()` in `R/categories.R` as the documentation
    and style template.
  - Internal stat-type lists are **local variables** defined at the top of the function
    body (not package-level constants):
    - `welfare_stats` — list of 11 named lists, each `list(measure = <id>, label = <label>)`:
      - `mean` → `"Mean"`, `median` → `"Median"`, `sd` → `"Standard deviation"`,
        `var` → `"Variance"`, `min` → `"Minimum"`, `max` → `"Maximum"`, `sum` → `"Sum"`,
        `p10` → `"10th percentile"`, `p25` → `"25th percentile"`,
        `p75` → `"75th percentile"`, `p90` → `"90th percentile"`
    - `poverty_stats` — list of 5:
      - `headcount` → `"Poverty rate"`, `poverty_gap` → `"Poverty gap"`,
        `severity` → `"Poverty severity"`, `watts` → `"Watts index"`,
        `pop_poverty` → `"Poor population"`
    - `inequality_stats` — list of 2:
      - `gini` → `"Gini index"`, `mld` → `"Mean log deviation"`
    - `continuous_stats <- welfare_stats` (assign by reference — no duplication)
    - `binary_stats` — list of 2:
      - `pop_share` → `"Share of population"`, `obs_share` → `"Share of observations"`
  - **Return** `list(...)` of exactly 12 entries in this order:
    1.  `varname="welfare"`, `label="Welfare"`, `type="welfare"`, `poverty_line_slider=FALSE`, `available_stats=welfare_stats`
    2.  `varname="poverty"`, `label="Poverty status"`, `type="poverty"`, `poverty_line_slider=TRUE`, `available_stats=poverty_stats`
    3.  `varname="inequality"`, `label="Inequality"`, `type="inequality"`, `poverty_line_slider=FALSE`, `available_stats=inequality_stats`
    4.  `varname="age"`, `label="Age"`, `type="continuous"`, `poverty_line_slider=FALSE`, `available_stats=continuous_stats`
    5.  `varname="primary_completed"`, `label="Primary education completed"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    6.  `varname="secondary_completed"`, `label="Secondary education completed"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    7.  `varname="higher_than_secondary"`, `label="Higher than secondary education"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    8.  `varname="university"`, `label="University education"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    9.  `varname="imp_wat_rec"`, `label="Access to improved water"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    10. `varname="imp_san_rec"`, `label="Access to improved sanitation"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    11. `varname="electricity"`, `label="Access to electricity"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
    12. `varname="employed"`, `label="Employed"`, `type="binary"`, `poverty_line_slider=FALSE`, `available_stats=binary_stats`
  - Add roxygen: `@family measures`, `@export`, `@description`, `@return`, `@examples`
    (following `pip_tablemaker_categories()` verbatim style).

- **Test scenarios**: happy path (12 entries, correct structure); stat isolation (pop_share
  absent from non-binary entries); slider invariant (only poverty entry TRUE)
- **Tests**: `tests/testthat/test-measures.R` (written in Step 4)
- **Acceptance criteria**: `piptm::pip_tablemaker_measures()` callable and returns list of
  12; `pip_measures` no longer defined in the package

---

### 2. Update GET /measures endpoint in plumber.R

- **Requirements**: R8, R9
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - Replace the current `/measures` endpoint block (lines ~234–246) with:
    ```r
    # ── GET /measures ─────────────────────────────────────────────────────────────

    #* Return the full catalogue of analysis variables for the Table Maker Step 2 UI
    #*
    #* Static endpoint — returns all analysis variables with their type, available
    #* statistics, and poverty line slider visibility.  Used by the UI to render the
    #* analysis variable dropdown and configure the statistics panel per variable.
    #*
    #* @serializer json list(na = "null")
    #* @get /measures
    function() {
      api_response(piptm::pip_tablemaker_measures())
    }
    ```
  - Remove the `data.table::data.table(measure = names(m), family = unname(m))` wrapper
    entirely — `pip_tablemaker_measures()` is returned directly.
  - Route path stays `/measures`.  No new route.

- **Test scenarios**: GET /measures 200; data is a 12-row data.frame (after fromJSON
  simplification); `measure`/`family` columns no longer present
- **Tests**: `tests/testthat/test-api-endpoints.R` (updated in Step 5)
- **Acceptance criteria**: `plumb()` succeeds; GET /measures returns new structure

---

### 3. Regenerate documentation

- **Requirements**: R1, R2, R12
- **Files**: `NAMESPACE`, `man/pip_measures.Rd` (delete), `man/pip_tablemaker_measures.Rd` (create)
- **Details**:
  - Run `devtools::document()` from the package root.
  - Verify `export(pip_measures)` removed from NAMESPACE.
  - Verify `export(pip_tablemaker_measures)` added to NAMESPACE.
  - Verify `man/pip_tablemaker_measures.Rd` was created.
  - **Important**: roxygen2 does not auto-delete orphaned `.Rd` files when a function is
    removed.  If `man/pip_measures.Rd` still exists after `devtools::document()`, delete
    it manually (R CMD check raises a WARNING for documented-but-absent exports).
  - Run `devtools::check(document = FALSE)` to verify no NAMESPACE or documentation errors.

- **Test scenarios**: `R CMD check` passes cleanly (no NOTE or WARNING about pip_measures)
- **Acceptance criteria**: NAMESPACE updated; no orphaned `pip_measures.Rd`

---

## Phase 2: Tests and polish

### 4. Rewrite test-measures.R for pip_tablemaker_measures()

- **Requirements**: R10
- **Files**: `tests/testthat/test-measures.R`
- **Details**:
  - Remove the entire `pip_measures()` test section (6 tests: "returns a named character
    vector", "contains all 21 measure names", "values are valid family names", "maps
    poverty measures", "maps inequality measures", "maps welfare measures to 'welfare'
    family").
  - Add a new `pip_tablemaker_measures()` section with these 12 tests:
    1. Returns a list of length 12
    2. Each entry has exactly the 5 required fields: `varname`, `label`, `type`,
       `poverty_line_slider`, `available_stats`
    3. All `varname` values are unique character scalars
    4. All `type` values are in `c("welfare", "poverty", "inequality", "continuous", "binary")`
    5. `poverty_line_slider` is logical; `TRUE` only for the `type == "poverty"` entry
    6. `available_stats` is a non-empty list in each entry; each element has `measure` and
       `label` character scalar fields
    7. `pop_share` and `obs_share` appear only in entries where `type == "binary"`
    8. Entries with `type %in% c("welfare", "continuous")` have the same 11 measure
       identifiers in `available_stats`
    9. Poverty entry has exactly 5 stats
    10. Inequality entry has exactly 2 stats
    11. Each binary entry has exactly 2 stats (`pop_share`, `obs_share`)
    12. Catalogue order: entry 1 is `"welfare"`, entry 2 is `"poverty"`, entry 3 is
        `"inequality"`, entry 4 is `"age"` (spot-check)
  - **Retain unchanged**: all `pip_age_bins()`, `.classify_measures()`, `.validate_by()`,
    `.validate_poverty_lines()`, and `.bin_age()` tests.

- **Test scenarios**: all 12 contract properties verified; regression — existing internal
  tests still pass
- **Acceptance criteria**: all new tests pass; no reference to `pip_measures()` in the file

---

### 5. Update test-api-endpoints.R /measures endpoint tests

- **Requirements**: R11
- **Files**: `tests/testthat/test-api-endpoints.R`
- **Details**:
  - Locate the two `/measures` tests in Block 1 (Discovery endpoints).
  - **Note on serialisation**: `parse_api_res(res, simplify = TRUE)` uses
    `jsonlite::fromJSON()` with `simplifyVector = TRUE`.  A JSON array of 12 objects with
    uniform keys is simplified to a `data.frame` with 12 rows.  Use `nrow()`, not
    `length()`, to count entries.
  - Replace test 1 (`"GET /measures returns 200 and all measure names"`) with:
    ```r
    test_that("GET /measures returns 200 with success status and 12 entries", {
      skip_if_not_installed("plumber")
      skip_if(is.null(.ep_router), "Router could not be created")
      res  <- .ep_router$call(make_api_req("GET", "/measures"))
      expect_equal(res$status, 200L)
      body <- parse_api_res(res)
      expect_equal(body$status, "success")
      expect_equal(nrow(body$data), 12L)
    })
    ```
  - Replace test 2 (`"GET /measures data has measure and family columns with valid families"`) with:
    ```r
    test_that("GET /measures data has required fields, valid types, and one poverty slider", {
      skip_if_not_installed("plumber")
      skip_if(is.null(.ep_router), "Router could not be created")
      body <- parse_api_res(.ep_router$call(make_api_req("GET", "/measures")))
      expect_true(all(c("varname", "label", "type", "poverty_line_slider") %in%
                        names(body$data)))
      expect_true(all(body$data$type %in%
                        c("welfare", "poverty", "inequality", "continuous", "binary")))
      # poverty_line_slider is TRUE for exactly one entry (the poverty variable)
      expect_equal(sum(body$data$poverty_line_slider), 1L)
      expect_true(body$data$poverty_line_slider[body$data$type == "poverty"])
    })
    ```
  - All other Block 1 tests (`/health`, `/releases`, `/dimensions`) are unchanged.

- **Test scenarios**: GET /measures 200; new column names present; old `measure`/`family`
  columns not asserted
- **Acceptance criteria**: both replacement tests pass; no reference to `pip_measures()` in
  the file

---

## Testing Strategy

| After step | Run |
|------------|-----|
| Step 1     | `testthat::test_file("tests/testthat/test-measures.R")` — confirm `pip_measures()` undefined error (red), then re-run after Step 4 |
| Step 2     | `testthat::test_file("tests/testthat/test-api-endpoints.R")` — /measures tests fail (red) |
| Step 3     | `devtools::check(document = FALSE)` — clean |
| Step 4     | `testthat::test_file("tests/testthat/test-measures.R")` — all green |
| Step 5     | `testthat::test_file("tests/testthat/test-api-endpoints.R")` — all green |
| Final      | `devtools::test()` — full suite, no regressions |

## Documentation Checklist

- [ ] `pip_tablemaker_measures()` has complete roxygen2 docs (`@description`, `@details`, `@return`, `@family measures`, `@examples`, `@export`)
- [ ] GET `/measures` plumber roxygen comment updated (description and detail lines)
- [ ] `man/pip_measures.Rd` deleted (if not auto-removed by `devtools::document()`)
- [ ] `man/pip_tablemaker_measures.Rd` generated

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| `man/pip_measures.Rd` not auto-deleted (roxygen2 does not remove orphans) | High | Low — R CMD check WARNING | Delete manually in Step 3; verify with `devtools::check(document = FALSE)` |
| `fromJSON` simplification depth differs from expectation — `nrow()` vs `length()` confusion in tests | Low | Medium | Use `nrow(body$data)` explicitly; add `simplify = FALSE` spot-check if needed |
| `available_stats` nested list serialises as empty array `[]` for some entries due to NULL-handling | Low | Medium | Run `jsonlite::toJSON(pip_tablemaker_measures(), auto_unbox = TRUE)` manually before writing tests to confirm shape |
| External callers of `pip_measures()` (scripts, other packages) break silently | Low | Low — breaking API change is intentional | Note in commit message; no deprecation shim needed per spec |
| `nobs` appears in `.MEASURE_REGISTRY` but is absent from `welfare_stats` in the new catalogue — test mismatch if `.classify_measures()` tests are inadvertently tightened | Low | Low | `.classify_measures()` tests are unchanged; clearly separate concerns |

## Out of Scope

- Computation logic for binary variables (e.g. `primary_completed` derivation from `educat7`)
- Query-time binary variable resolution from source columns
- Changes to `.MEASURE_REGISTRY`, `.classify_measures()`, or any compute function
- New plumber routes
- Modifications to `table_maker()`, `compute_measures()`, or family-level compute functions
- Deprecation wrapper or backward-compatibility shim for `pip_measures()`
- `nobs` measure — absent from the UI catalogue by design (not listed in spec)
