---
date: 2026-10-26
title: "/table/download Approach C stateless flattening (Shape A)"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-10-26-table-download-approach-c-flattening.md"
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
execution-report: .cg-docs/work-reports/2026-10-26-table-download-approach-c-flattening.md
artifact-schema-version: 1
completed-phases: [1]
current-phase: 2
phases: 2
tags: [api, plumber, table-download, approach-c, csv, rds, serialization, breaking-change]
---

# Plan: `/table/download` Approach C Stateless Flattening (Shape A)

## Objective

Update `POST /table/download` so it accepts the **Approach C** `data[]` payload
returned by `/table` (per-survey objects with `layout` + `cells[]`) and
statelessly flattens it into a rectangular **Shape A** table (one row per
observed cell, measures as columns) before exporting as CSV or RDS. Reject
legacy flat-row payloads and malformed inputs with HTTP `400` in the standard
JSON error envelope (replacing the temporary HTTP `500` characterization
behavior).

## Context

In `.cg-docs/plans/2026-10-26-table-layout-cells-response-format.md`
(completed), `/table` (with `include_metadata = false`) was reshaped to return
Approach C: `data[]` is an array of per-survey objects, each containing 6
survey header fields (`pip_id`, `country_code`, `surveyid_year`,
`welfare_type`, `poverty_line`, `analysis_var`), `measures[]`, a 4-key `layout`
block (`columns`, `rows`, `superColumns`, `superRows`), and `cells[]` where
each cell has `superRow`, `row`, `superColumn`, `column` (`{code, label}` or
`null`) and a `measures` object keyed by measure name with formatted display
strings (`"4.50"`, `"NR"`, or `null`).

`/table/download` (`inst/plumber/plumber.R:247-343`) still assumes `body$data`
is a flat array of row objects and calls `data.table::rbindlist(body$data,
fill = TRUE)`. Passing an Approach C payload currently causes an unhandled
`500 Internal server error`.

The design decision was captured in
`.cg-docs/brainstorms/2026-10-26-table-download-approach-c-flattening.md`:
implement **Approach 1 — stateless server-side flattening into Shape A**:

1. **6 leading survey header columns (always present)**:
   `pip_id` (character), `country_code` (character), `surveyid_year` (integer),
   `welfare_type` (character), `analysis_var` (character), `poverty_line`
   (numeric; `NA_real_` when `null`).
2. **Active dimension columns (variable names + category labels only)**:
   For each active (non-`null`) slot in `layout`, ordered `superRows` → `rows`
   → `superColumns` → `columns` (matching cell sort order), emit one character
   column named after `layout[[slot]]$varname` (e.g. `educat4`, `area`,
   `gender`) populated with the cell's category `label` (`NA_character_` if
   missing). Unused slots (`null` in `layout`) are omitted; when no slots are
   active (national table), 0 dimension columns are emitted.
3. **Measure columns**:
   One character column per measure in the survey's `measures[]` order,
   preserving formatted display strings verbatim (`"4.50"`, `"NR"`,
   `NA_character_` when `null` — never coerced via `as.numeric()`).
4. **Approach C only**:
   Legacy flat-row payloads, empty `data`, or structurally invalid Approach C
   objects are rejected with HTTP `400` in the standard JSON error envelope.

**Relevant learnings from Brain:**
- `.cg-docs/solutions/bugs/2026-10-22-table-download-format-contract-leakage.md`:
  never coerce formatted measure strings (`"NR"`, scaled percentages) via
  `as.numeric()`.
- `.cg-docs/solutions/bugs/2026-10-26-data-table-by-named-vector-silent-bug.md`:
  always `unname()` programmatically constructed column-name vectors passed to
  `data.table`.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Export pure helper `flatten_table_response(data)` in `R/table_response.R` that accepts an Approach C `data[]` payload and returns a Shape A `data.table` with one row per cell | Brainstorm Next Step 1 |
| R2 | Leading columns of the flattened table are always `pip_id`, `country_code`, `surveyid_year`, `welfare_type`, `analysis_var`, `poverty_line` (numeric, `NA_real_` when `null`) in that exact order | Brainstorm Req 3 |
| R3 | Active slot columns follow the 6 header columns in `superRows` → `rows` → `superColumns` → `columns` order, named after `layout[[slot]]$varname`, populated with each cell's category `label` (character); unused (`null`) slots are omitted; national tables have 0 dimension columns | Brainstorm Req 4 |
| R4 | Measure columns follow the dimension columns in `measures[]` order, preserving formatted display strings verbatim (`"4.50"`, `"NR"`, `NA_character_` when `null`); never coerce measure columns to numeric | Brainstorm Req 2 + 2026-10-22 learning |
| R5 | Multi-survey `data[]` payloads stack in `data[]` order, preserving each survey's `cells[]` order; if surveys in the same payload have incompatible active slot varnames or measure sets, abort with a clear validation error | Brainstorm Req 5 |
| R6 | `flatten_table_response()` and `/table/download` reject empty `data`, legacy flat-row payloads (e.g. objects lacking `layout`/`cells`), and malformed survey/layout/cell objects; `/table/download` returns HTTP `400` with the standard JSON error envelope (never `500`) | Brainstorm Req 6 |
| R7 | `POST /table/download` parses JSON with `simplifyVector = FALSE` (or handles both list and simplified forms), calls `flatten_table_response()`, removes the dead `population` coercion loop, and writes `tbl` to CSV (`data.table::fwrite()`) or RDS (`saveRDS(as.data.frame(tbl))`) | Brainstorm Next Step 2 |
| R8 | `tests/testthat/test-api-table-download.R` replaces the temporary 500 characterization test with 200 round-trip tests (`/table` → `/table/download` for CSV and RDS), updates existing download unit tests to Approach C payloads, and tests 400 rejection of legacy flat-row payloads | Brainstorm Next Step 3 |
| R9 | `docs/api-contract.md` (`/table/download` section updated to Shape A; temporary incompatibility note removed) and `NEWS.md` are updated; `man/` regenerated via `devtools::document()` | Brainstorm Next Step 4 |

## Implementation Steps

## Phase 1: Pure Flattener & Plumber Endpoint Wire-In

### 1. Implement and export `flatten_table_response()` in `R/table_response.R`
- **Requirements**: R1, R2, R3, R4, R5, R6
- **Files**: `R/table_response.R`, `tests/testthat/test-table-response.R`
- **Details**:
  - Implement `flatten_table_response(data)` accepting a non-empty list of
    per-survey Approach C objects (as produced by `serialize_table_response()`
    or `jsonlite::fromJSON(raw_body, simplifyVector = FALSE)$data`). Also
    gracefully accept a `data.frame` if a caller parsed JSON with
    `simplifyVector = TRUE`, or standardize `/table/download` to parse with
    `simplifyVector = FALSE`.
  - Validate each survey object contains required header fields (`pip_id`,
    `country_code`, `surveyid_year`, `welfare_type`, `analysis_var`,
    `measures`), a `layout` list with keys `columns`, `rows`, `superColumns`,
    `superRows`, and a non-empty `cells` list where each cell has `superRow`,
    `row`, `superColumn`, `column`, and `measures`. Abort via
    `cli::cli_abort()` with a clear message if `data` is empty, is a legacy
    flat-row structure (e.g. contains `measure`/`value` without `layout` and
    `cells`), or is missing required keys.
  - Determine active slots in canonical visual order:
    `layout_order <- c(superRow = "superRows", row = "rows", superColumn = "superColumns", column = "columns")`.
    For each active slot where `!is.null(survey$layout[[layout_key]])`, extract
    `varname <- as.character(survey$layout[[layout_key]]$varname)` (using
    `unname()`). Verify all surveys in `data` share the same active slot
    varnames and `measures` vector.
  - For each cell, extract the category `label` (`as.character(...)`, or
    `NA_character_` if `NULL`/`NA`) for each active slot, and extract each
    measure value from `cell$measures[[m]]` as `character` (`NA_character_` if
    `NULL`/`NA`).
  - Assemble a `data.table` with columns:
    `pip_id`, `country_code`, `surveyid_year` (`as.integer`), `welfare_type`,
    `analysis_var`, `poverty_line` (`as.numeric`, `NA_real_` when `NULL`/`NA`),
    active slot varname columns, and measure columns.
  - Add complete roxygen2 documentation (`@param`, `@return`, `@section
    Errors:`, `@family api`, `@export`, `@examples`) and run
    `devtools::document()`.
- **Test Scenarios**:
  - Happy path: 3-covariate fixture (`three_covariates_post_format.json`)
    flattens to 20 rows × (`6 + 3 + 1 = 10`) columns (`pip_id`, `country_code`,
    `surveyid_year`, `welfare_type`, `analysis_var`, `poverty_line`, `educat4`,
    `area`, `gender`, `mean`), with category labels (`"None"`, `"Rural"`,
    `"Female"`, `"Unknown"`) and string measure values (`"4.50"`).
  - Edge cases: national table (all slots `NULL` → 6 header cols + measure
    cols, 1 row); multi-measure cell (`mean` + `headcount` → 2 measure
    columns); `"NR"` sentinel and `null` (`NA_character_`) measure values
    preserved verbatim; multi-survey payload stacks rows in survey + cell
    order; `poverty_line = 2.15` vs `NULL` (`NA_real_`).
  - Error paths: empty `data` (`list()`), legacy flat-row list, survey missing
    `layout` or `cells`, inconsistent slot varnames across surveys in a
    multi-survey payload.
- **Tests**: `tests/testthat/test-table-response.R`
- **Acceptance criteria**: All unit tests for `flatten_table_response()` pass.

### 2. Wire `flatten_table_response()` into `POST /table/download`
- **Requirements**: R6, R7
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - In `POST /table/download` (`inst/plumber/plumber.R:288-338`), parse
    `raw_body` with `jsonlite::fromJSON(raw_body, simplifyVector = FALSE)` so
    `body$data` is a clean nested list matching `serialize_table_response()`
    output.
  - Call `flatten_table_response(body$data)` inside `tryCatch()`, returning
    HTTP `400` via `err(conditionMessage(e), 400L)` (or a clean validation
    message) whenever `body$data` is missing, empty, legacy flat-row shaped, or
    structurally invalid.
  - Remove the obsolete `for (col in intersect(c("population", "poverty_line"),
    names(tbl)))` loop (`flatten_table_response()` already types
    `poverty_line` as `numeric` and `population` no longer exists).
  - Keep the existing 25 MB size guard (413), `format` validation (`csv` /
    `rds`, default `csv`), `data.table::fwrite(tbl, tmp)` for CSV (`na = ""`
    default), and `saveRDS(as.data.frame(tbl), tmp)` for RDS.
- **Test Scenarios**:
  - Happy path: `POST /table/download` with `format = "csv"` and `format =
    "rds"` on Approach C payloads returns HTTP 200 with valid CSV/RDS.
  - Error path: legacy flat-row payload returns HTTP 400 (not 500) with
    `status = "error"` JSON envelope.
- **Tests**: `tests/testthat/test-api-table-download.R`
- **Acceptance criteria**: `/table/download` returns 200 for Approach C payloads
  and 400 for invalid/legacy payloads.

## Phase 2: Endpoint Round-Trip Tests & Documentation

### 3. Rewrite `test-api-table-download.R` for Approach C & round-trip verification
- **Requirements**: R8
- **Files**: `tests/testthat/test-api-table-download.R`
- **Details**:
  - Replace `.dl_rows` fixture at the top of `test-api-table-download.R` with
    a canonical Approach C `.dl_data` fixture (including at least 1 active slot,
    multiple measures, an `"NR"` masked value, a `null` measure value, and a
    numeric `poverty_line`).
  - Update existing CSV, RDS, default-format, 413, and `"NR"`/scaled-percentage
    preservation tests to use the Approach C payload and assert Shape A columns
    and types (`character` measure columns in RDS, `numeric` `poverty_line`,
    category labels in slot varname columns).
  - Replace the temporary `characterization: /table/download cannot consume
    Approach C nested layout+cells payload` test with:
    1. A full round-trip test: call `/table` (or load
       `three_covariates_post_format.json`) → pass its `data` directly to
       `POST /table/download` for both `csv` and `rds` → assert HTTP 200,
       20 rows, exact columns `c("pip_id", "country_code", "surveyid_year",
       "welfare_type", "analysis_var", "poverty_line", "educat4", "area",
       "gender", "mean")`, and exact values.
    2. An explicit HTTP 400 test asserting that legacy flat-row payloads
       (`list(list(pip_id = "AAA_2020", measure = "mean", value = "1.50"))`)
       are rejected with 400 and a clear error message.
- **Test Scenarios**: CSV & RDS round-trip on 3-covariate fixture, national
  (no-slot) download, multi-survey download, `"NR"` and `null` preservation,
  400 on legacy flat rows, 400 on empty/malformed bodies, 413 on oversized body.
- **Tests**: `tests/testthat/test-api-table-download.R`
- **Acceptance criteria**: All tests in `test-api-table-download.R` pass with 0
  failures.

### 4. Update `docs/api-contract.md`, `NEWS.md`, and run full verification
- **Requirements**: R9
- **Files**: `docs/api-contract.md`, `NEWS.md`, `man/flatten_table_response.Rd`,
  `NAMESPACE`
- **Details**:
  - In `docs/api-contract.md` (`POST /table/download` section):
    - Document that `data` in the request body is the Approach C `data[]` array
      returned by `/table` (`include_metadata = false`).
    - Document the exported Shape A column structure: 6 survey header columns
      (`pip_id`, `country_code`, `surveyid_year`, `welfare_type`,
      `analysis_var`, `poverty_line`), active slot columns named after
      `layout[[slot]]$varname` in `superRows` → `rows` → `superColumns` →
      `columns` order containing category labels, and measure columns in
      `measures[]` order containing formatted display strings (`"12.50"`,
      `"NR"`, or empty/`NA`).
    - Remove the temporary "Payload compatibility note" that stated
      `/table/download` was temporarily unsupported for Approach C payloads.
  - Add a bullet in `NEWS.md` documenting `flatten_table_response()` and the
    updated `/table/download` Shape A export format.
  - Run `devtools::document()` and the full test suite (`devtools::test()`).
- **Test Scenarios**: Full package test suite regression check; `git diff`
  inspection confirming no compute-layer files outside `R/table_response.R`
  were modified.
- **Tests**: `devtools::test()`
- **Acceptance criteria**: Full test suite passes with 0 failures; docs and
  `man/` are clean and up to date.

## Testing Strategy

- **Unit tests** (`tests/testthat/test-table-response.R`): test
  `flatten_table_response()` directly across 0-slot (national), 1-slot,
  3-slot (`three_covariates_post_format.json`), multi-measure, multi-survey,
  `"NR"`/missing values, and invalid-input error cases.
- **Endpoint integration tests** (`tests/testthat/test-api-table-download.R`):
  test `POST /table/download` via the Plumber router for CSV and RDS exports,
  verifying HTTP status codes (200, 400, 413), headers (`Content-Type`,
  `Content-Disposition`), column names, column types, and cell values.

## Documentation Checklist

- [ ] Roxygen2 block on `flatten_table_response()` with `@examples` and
      `@section Errors:`
- [ ] `man/flatten_table_response.Rd` and `NAMESPACE` updated via
      `devtools::document()`
- [ ] `docs/api-contract.md` `POST /table/download` updated to Shape A and
      temporary incompatibility note removed
- [ ] `NEWS.md` updated

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| `jsonlite::fromJSON()` default simplification turns `body$data` into a nested `data.frame` with list-columns, complicating extraction | Medium — brittle indexing across 1-cell vs multi-cell payloads | Parse `raw_body` in `/table/download` with `simplifyVector = FALSE` so `body$data` is always a uniform list-of-lists matching `serialize_table_response()` |
| Formatted measure values (`"4.50"`, `"NR"`) get coerced to numeric by `data.table::fread()` in tests or during RDS export | High — destroys `"NR"` disclosure-control sentinel | Keep all measure columns explicitly `character` in `flatten_table_response()`; RDS stores `character` columns; CSV tests read back with `colClasses = "character"` where checking exact string preservation |
| Multi-survey payload in `body$data` has surveys with mismatched active slot variables or measure sets | Low — `/table` produces uniform layout/measures per request, but hand-crafted payloads could differ | Validate in `flatten_table_response()` that all surveys in `data` share identical active slot varnames and measure names; abort with a clear error (mapped to HTTP 400) if they diverge |
| Named character vectors from `vapply()` silently rename `data.table` columns | Medium — caused a bug in Phase 2 of the prior plan | Always wrap programmatically extracted column name vectors in `unname()` before building the `data.table` |

## Out of Scope

- Modifying `table_maker()`, `format_table_output()`, `serialize_table_response()`,
  or the `/table` and `/description` endpoints.
- Supporting legacy flat-row payloads on `/table/download` (clean break to
  Approach C only, per brainstorm decision).
- Reshaping `/table` when `include_metadata = true`.
- Adding new download formats beyond `csv` and `rds`.

## Completion Contract

### Outcome
`POST /table/download` accepts the Approach C `data[]` payload produced by
`/table` and returns a rectangular Shape A table (`csv` or `rds`) with one row
per observed cell: 6 leading survey header columns (`pip_id`, `country_code`,
`surveyid_year`, `welfare_type`, `analysis_var`, `poverty_line`), active slot
columns named after `layout[[slot]]$varname` in `superRows` → `rows` →
`superColumns` → `columns` order containing category labels, and one column per
requested measure in `measures[]` order with formatted display strings preserved
verbatim. Legacy flat-row payloads and malformed `data` structures are rejected
with HTTP `400` in the standard JSON error envelope (replacing the current
unhandled 500).

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | `flatten_table_response()` flattens Approach C `data[]` into the Shape A `data.table` with exact column order (`pip_id`, `country_code`, `surveyid_year`, `welfare_type`, `analysis_var`, `poverty_line`, active slot varnames in `superRows→rows→superColumns→columns` order with labels, then `measures[]` in order) | `tests/testthat/test-table-response.R` | 1 | yes |
| V2 | Formatted measure strings (`"4.50"`, `"NR"`, `NA_character_` for `null`) survive verbatim in both CSV and RDS without `as.numeric()` coercion; `poverty_line` is numeric (`NA_real_` when `null`); national table (all slots `null`) emits 0 dimension columns and 1 row per survey | `test-table-response.R` + `test-api-table-download.R` | 1 | yes |
| V3 | `POST /table/download` round-trips a real `/table` Approach C response (including the 3-covariate fixture and multi-survey payloads) into downloadable CSV (`text/csv`) and RDS (`application/octet-stream`) with HTTP 200 | `tests/testthat/test-api-table-download.R` | 2 | yes |
| V4 | Legacy flat-row payloads, empty `data`, missing required survey/layout/cell fields, or surveys with inconsistent layout/measure schemas are rejected with HTTP 400 and the standard JSON error envelope (never 500) | `tests/testthat/test-api-table-download.R` | 2 | yes |
| V5 | `docs/api-contract.md` (`/table/download` section updated to Shape A; temporary incompatibility note removed), `NEWS.md`, and `man/` (`devtools::document()`) are updated and consistent | `git diff docs/ NEWS.md man/` | 2 | yes |
| V6 | Full package test suite passes with 0 failures | `devtools::test()` | final | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | `table_maker()` and all compute-layer files remain untouched; `/table/download` remains stateless with zero Arrow or registry access | `git diff R/` touches only `R/table_response.R` | final |
| C2 | Deterministic output: identical `body$data` always produces byte-identical CSV/RDS column and row order | V1, V3 | 2 |
| C3 | Formatted display values (`"NR"`, scaled percentages) are never coerced to numeric | V2 | 1 |
| C4 | No new package dependencies | `DESCRIPTION` unchanged | final |

### Boundaries
- Allowed: `R/table_response.R` (export `flatten_table_response()`),
  `inst/plumber/plumber.R` (`POST /table/download` handler),
  `tests/testthat/test-table-response.R`,
  `tests/testthat/test-api-table-download.R`, `docs/api-contract.md`,
  `NEWS.md`, `NAMESPACE`, `man/`.
- Out of scope: Any changes to `/table`, `table_maker()`,
  `format_table_output()`, or `/description`; supporting legacy flat-row
  payloads on `/table/download`; adding new export formats.

### Iteration Policy
1. On test failure, fix `flatten_table_response()` or the `/table/download`
   validation path rather than weakening test assertions.
2. Parse `/table/download` request JSON with `simplifyVector = FALSE` so the
   Plumber endpoint and direct R callers pass the same list structure into
   `flatten_table_response()`.

### Blocked-Stop Conditions
- Flattening requires consulting the release registry or Arrow data (violates
  statelessness / C1).
- Achieving Shape A requires modifying `/table` or `table_maker()`.

### Deviation policy
`ask` (default).
