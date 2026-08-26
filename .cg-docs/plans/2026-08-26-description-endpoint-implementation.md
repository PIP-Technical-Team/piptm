---
date: 2026-08-26
title: "/description Endpoint — Dynamic Table Description Generation"
status: active
scope: "Deep"
brainstorm: ".cg-docs/brainstorms/2026-08-26-description-endpoint-stress-test.md"
language: "R"
estimated-effort: "large"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 5
current-phase: 2
completed-phases: [0, 1]
execution-report: ".cg-docs/work-reports/2026-08-26-description-endpoint-implementation.md"
tags: [description-endpoint, table_maker, api, cell-definition, metadata, renderer]
---

# Plan: /description Endpoint — Dynamic Table Description Generation

## Objective

Implement a `/description` endpoint that generates factual, dynamically constructed natural-language descriptions of tabular results produced by `table_maker()`. The endpoint serves Step 3 of the PIP UI experience, providing researchers, economists, and data scientists with a human-readable overview of table contents, provenance, input specifications, execution details, and cell definitions.

## Context

This plan stems from the stress-tested specification at `docs/description-endpoint-spec.md`. Key architectural decisions confirmed during brainstorming and plan review:

- **Approach A (Inline Metadata Assembly)**: `table_maker()` assembles `description_metadata` internally when `include_metadata = TRUE`
- **Backward-compatible return**: `include_metadata = FALSE` (default) returns `data.table` unchanged; `include_metadata = TRUE` returns `list(data = <data.table>, description_metadata = <list>)`
- **No recomputation**: `/description` consumes metadata from `/table?include_metadata=true` (fast path); param-based recomputation is a rare fallback only
- **All four worked examples are v1 requirements** (spec §4.3, cleaned — no interpretation blocks)
- **English only**, strictly factual/definitional

The cell definition algorithm (spec Section IV) is the highest-risk component: it must generate mathematically accurate descriptions that adapt to all permutations of filters, analysis variables, measures, and covariates. Tests for this component must serve as **safety mechanisms** that catch incorrect cell content before it reaches users.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `include_metadata = FALSE` (default) returns `data.table` — current behavior preserved | Plan review P1.1 |
| R2 | `include_metadata = TRUE` returns `list(data = <data.table>, description_metadata = <list>)` | Spec §2.1 |
| R3 | `description_metadata` captures all 9 parameter groups and 4 execution log categories | Spec §2.1 |
| R4 | `build_description_model()` produces structured model with conditional visibility | Spec §3.1 |
| R5 | Cell definition algorithm handles all measure families × filter × covariate permutations | Spec §4.2 |
| R6 | All 4 worked examples produce mathematically correct descriptions (no interpretation blocks) | Spec §4.3, Plan review P2.1 |
| R7 | `render_description_markdown()` produces valid, readable Markdown | Spec §3, Phase 3 |
| R8 | `/table` endpoint supports `include_metadata` query parameter | Brainstorm decision 1 |
| R9 | `/description` POST endpoint accepts metadata and returns Markdown | Brainstorm decision 2 |
| R10 | `/description` fallback mode accepts params and recomputes | Brainstorm decision 3 |
| R11 | Registry function return structures match spec §5.1 | Plan review P2.2 |
| R12 | Warning capture does not interfere with error propagation | Plan review P1.2 |
| R13 | `devtools::check()` clean — 0 errors, 0 warnings | Spec §VII |

## Phase 0: Pre-flight — Registry Structure Validation

### 1. Validate registry function return structures

- **Requirements**: R11
- **Files**: `tests/testthat/test-registry-structure.R` (new)
- **Details**:
  - Call each of the 4 registry functions with `release = piptm_current_release()`:
    - `piptm_variable_registry(release)` — verify returns named list with `varname`, `ui_label`, `tm_type`, `roles`, `stat_groups`, `n_categories`, `categories` fields
    - `piptm_stat_groups(release)` — verify returns list of groups, each with `group`, `group_label`, `measures` (list of `measure`, `label`)
    - `piptm_filter_categories(release)` — verify returns list of entries with `varname`, `label`, `subcategories` (list of `code`, `label`)
    - `piptm_layout_covariates(release)` — verify returns list of entries with `varname`, `label`, `n_categories`
  - Document any deviations from spec §5.1 schema
  - If structure mismatches, **block** until schema issues are resolved
- **Test Scenarios**:
  - Happy path: All 4 functions return structures matching spec §5.1
  - Edge case: `measure_spec` embedded in registry has correct `stat_groups` structure
  - Error path: Missing registry for release → clear error message
- **Tests**: `tests/testthat/test-registry-structure.R`
- **Acceptance criteria**: All 4 functions return structures matching spec §5.1; any deviations documented

## Phase 1: Backend Enhancements — Metadata Capture (No Return-Type Change)

### 2. Add `include_metadata` parameter to `table_maker()`

- **Requirements**: R1, R2
- **Files**: `R/table_maker.R`
- **Details**:
  - Add `include_metadata = FALSE` to function signature (line 187)
  - Validate: `assert(is.logical(include_metadata) && length(include_metadata) == 1L)`
  - **When `FALSE` (default)**: return `result` directly as `data.table` — **no change to existing behavior**
  - **When `TRUE`**: return `list(data = result, description_metadata = .build_description_metadata(...))`
  - Update roxygen2 `@return` documentation to document both return paths
  - **No existing call sites need updating** — default behavior is unchanged
- **Test Scenarios**:
  - Happy path: `include_metadata = FALSE` returns `data.table` (identical to current)
  - Happy path: `include_metadata = TRUE` returns list with `data` and `description_metadata` fields
  - Error path: `include_metadata = "yes"` (non-logical) errors
  - Edge case: `include_metadata = logical(0)` (empty logical) errors
- **Tests**: `tests/testthat/test-table-maker-metadata.R` — new file
- **Acceptance criteria**: `table_maker(pip_id, ..., include_metadata = FALSE)` returns `data.table` identical to current output; `include_metadata = TRUE` returns list

### 3. Instrument execution log capture

- **Requirements**: R3, R12
- **Files**: `R/table_maker.R`
- **Details**:
  - **Surveys loaded**: Capture from `entries` after manifest join (line 276)
  - **Surveys excluded**: Capture in **two locations**:
    - **Filter-base exclusions** (lines 309-329): After `dropped_entries` and `dropped_info` are computed, capture `data.table(pip_id = dropped_entries$pip_id, reason = dropped_info)` into `excluded_surveys` tracker
    - **Dimension exclusions** (lines 366-384): Same pattern — capture `data.table(pip_id = dropped_entries$pip_id, reason = dropped_info)` and rbind to `excluded_surveys`
    - Initialize `excluded_surveys = data.table(pip_id = character(0), reason = character(0))` before Step 2b
  - **Filters applied**: Resolve `filter_base` keys/values via `piptm_filter_categories()` into labels
  - **Suppression events**: Capture `suppressed` data.table from pop_share_threshold block (line 534)
  - **Warnings**: Implement selective `withCallingHandlers` wrapper:
    - Wrap **only the core computation block** (after all validation passes, before metadata join), NOT the entire function
    - Capture only `cli_warn()` messages, not all warnings
    - Use `invokeRestart("muffleWarning")` for captured warnings only
    - Only activate when `include_metadata = TRUE` to avoid overhead in default path
  - **Abort-path safety**: Add explicit test verifying `cli_abort()` messages are unchanged when `include_metadata = TRUE`
- **Test Scenarios**:
  - Happy path: Warnings are captured, not displayed to console
  - Edge case: No warnings → `warnings` field is `NULL`
  - Edge case: Multiple warnings → all captured in order
  - Edge case: Suppression triggered → `suppression$triggered = TRUE`, `suppressed_cells` populated
  - **Abort-path safety**: Invalid parameters with `include_metadata = TRUE` produce same error messages as `FALSE`
- **Tests**: `tests/testthat/test-table-maker-metadata.R`
- **Acceptance criteria**: `description_metadata$execution$warnings` captures all `cli_warn` messages; `description_metadata$surveys$excluded` has correct structure with `pip_id` and `reason` columns; abort messages unchanged

### 4. Build `.build_description_metadata()` helper

- **Requirements**: R2, R3
- **Files**: `R/table_maker.R` (internal helper, or new `R/description-metadata.R`)
- **Details**:
  - Populate all fields per spec §2.1 schema
  - `params`: Echo function arguments directly
  - `provenance`: `piptm_current_release()`, `ppp` param, `Sys.time()`
  - `surveys`: Join result `pip_id` with manifest metadata for loaded; use `excluded_surveys` tracker for excluded
  - `resolved_labels`: Call `piptm_variable_registry()`, `piptm_stat_groups()`, `piptm_filter_categories()`, `piptm_layout_covariates()` — structures validated in Phase 0
  - `execution`: Populated from capture instruments (Step 3)
  - Add internal assertions: `stopifnot(is.list(meta$params), is.data.table(meta$surveys$loaded))`
- **Test Scenarios**:
  - Happy path: All fields populated correctly for a standard request
  - Edge case: `by = NULL` → `resolved_labels$covariates` is empty data.table
  - Edge case: `filter_base = NULL` → `resolved_labels$filters` is `NULL`
  - Edge case: No excluded surveys → `surveys$excluded` is empty data.table
  - Edge case: Both filter-base and dimension exclusions → `surveys$excluded` has rows from both
- **Tests**: `tests/testthat/test-description-metadata.R` — new file
- **Acceptance criteria**: `str(description_metadata)` matches spec §2.1 schema exactly

## Phase 2: Description Data Model Builder

### 5. Implement `build_description_model()`

- **Requirements**: R4
- **Files**: `R/description_builder.R` (new)
- **Details**:
  - Input: `description_metadata` list + original `table_maker()` params
  - Output: Structured model per spec §3.1 with 9 sections
  - Each section has `visible`, `title`, `content` fields
  - Apply conditional visibility rules per spec §3.2:
    - `filters_applied`: `!is.null(filter_base)`
    - `layout_configuration`: `!is.null(by) && length(by) > 0`
    - `warnings`: `length(execution$warnings) > 0`
  - Delegate cell definition to `build_cell_definition()`
  - Resolve all labels via registry functions (label resolution happens here, not in metadata)
- **Test Scenarios**:
  - Happy path: All 9 sections present with correct `visible` flags
  - Edge case: `filter_base = NULL` → `filters_applied$visible = FALSE`
  - Edge case: `by = NULL` → `layout_configuration$visible = FALSE`
  - Edge case: No warnings → `warnings$visible = FALSE`
- **Tests**: `tests/testthat/test-description-builder.R` — new file
- **Acceptance criteria**: `build_description_model(metadata, params)` produces a list with all 9 section keys, each with `visible`, `title`, `content`

### 6. Implement `build_cell_definition()` — the core algorithm

- **Requirements**: R5, R6
- **Files**: `R/description_builder.R`
- **Details**:
  - Implements spec §4.2 decision tree exactly:
    1. Determine `base_pop` from `filter_base` (NULL → "total weighted population"; non-NULL → filtered description)
    2. Layer `group_qualifier` from `by` (NULL → empty; non-NULL → "within each X group")
    3. Apply measure-specific semantics per family:
       - `summary_stats`: "The {label} of {analysis_var} for {pop}."
       - `inequality`: "The {label} of {analysis_var} among {pop}."
       - `poverty`: "The {label} at ${line}/day (PPP {year}) for {pop}."
       - `shares/pop_share`: "The share of the total weighted survey population represented by {pop}."
       - `shares/target_within_group_share`: "The share of {denominator_pop} for whom {label} is true."
       - `shares/target_survey_share`: "The share of the total weighted survey population represented by {numerator_pop}."
    4. Handle `pov_status` as derived covariate (special sentence with threshold annotation)
  - Use `piptm_stat_groups()` for measure labels, `piptm_variable_registry()` for analysis var label
  - Handle `welfare_type_labels <- c(INC = "Income", CON = "Consumption")` mapping
  - Handle `slot_labels <- c(columns = "Columns", rows = "Rows", super_columns = "Super Columns", super_rows = "Super Rows")` mapping
- **Test Scenarios** (SAFETY CRITICAL — these tests are the primary defense against incorrect cell content):
  - **Example 1**: `analysis_var="welfare", measures=c("mean"), filter_base=NULL, by=NULL` → exact string match: "The mean of Welfare for the total weighted population of the survey."
  - **Example 2**: `analysis_var="welfare", measures=c("headcount","poverty_gap"), poverty_line=2.15, ppp=2021L, filter_base=list(age_group=c(1L,2L)), by=c("gender","area")` → exact string match for both measure sentences
  - **Example 3**: `analysis_var="imp_wat_rec", measures=c("target_within_group_share","target_survey_share"), filter_base=list(age_group=1L), by=c("area")` → exact string match for both share sentences (NO interpretation blocks)
  - **Example 4**: `analysis_var="welfare", measures=c("mean","gini"), poverty_line=6.85, by=c("pov_status")` → exact string match including "Poverty status group" annotation (NO interpretation blocks)
  - **Negative test**: Ensure `target_within_group_share` with `by=NULL` produces identical text to `target_survey_share` with `by=NULL` (both collapse to survey-level)
  - **Negative test**: Ensure poverty sentence includes PPP year and dollar sign
  - **Negative test**: Ensure inequality measures use "among" not "for"
  - **Negative test**: Ensure summary stats use "of" not "among"
  - **Edge case**: Multiple filters → "AND" separator in filter conditions
  - **Edge case**: 3+ covariates → "X × Y × Z" join in covariate description
  - **Edge case**: Binary analysis var with non-share measures → no error (measures are gated at UI level, but builder should handle gracefully)
- **Tests**: `tests/testthat/test-cell-definition.R` — new file, **golden-file style assertions** with exact expected strings
- **Acceptance criteria**: All 4 worked examples produce **byte-identical** output to cleaned spec §4.3 (no interpretation blocks); negative tests confirm no incorrect phrasing

### 7. Implement helper functions

- **Requirements**: R4, R5
- **Files**: `R/description_builder.R`
- **Details**:
  - `resolve_filter_labels(filter_base, release)`: Join filter codes with `piptm_filter_categories()` subcategories
  - `resolve_measure_labels(measures, release)`: Join measure keys with `piptm_stat_groups()` to get labels and stat groups
  - `format_covariate_description(by, release)`: Join covariate varnames with `piptm_layout_covariates()`, return "X × Y" string
  - `format_welfare_type(welfare_type_code)`: Map `INC`→`"Income"`, `CON`→`"Consumption"`
  - `format_slot_label(slot)`: Map internal slot names to display labels
- **Test Scenarios**:
  - Each helper tested independently with mock/real registry data
  - `resolve_filter_labels`: NULL input → NULL output; single filter → 1-row data.table; multiple filters → multi-row
  - `format_covariate_description`: single var → "Gender"; two vars → "Gender × Area"; three vars → "Gender × Area × Education"
- **Tests**: `tests/testthat/test-description-helpers.R` — new file
- **Acceptance criteria**: Each helper returns expected output for all documented input shapes

## Phase 3: Markdown Renderer

### 8. Implement `render_description_markdown()`

- **Requirements**: R7
- **Files**: `R/description_renderer.R` (new)
- **Details**:
  - Input: Structured model from `build_description_model()`
  - Output: Character scalar (Markdown text)
  - Iterate sections in order; skip `visible = FALSE`
  - Render `## {title}` headings
  - Dispatch content rendering:
    - `data.table` → Markdown table with `|` separators
    - `list` with named fields → Bullet list or paragraph
    - `character` vector → Numbered list
    - `NULL` content → skip section
  - Cell definition section: render `population_scope` as paragraph, `measure_interpretation` as numbered list
- **Test Scenarios**:
  - Happy path: Full model renders to valid Markdown
  - Edge case: All sections visible → complete output
  - Edge case: Only `overview` + `cell_definition` visible → minimal output
  - Edge case: Empty `measure_interpretation` → no numbered list
- **Tests**: `tests/testthat/test-description-renderer.R` — new file, **parsed Markdown assertions** (semantic structure) with 1-2 byte-identical regression tests
- **Acceptance criteria**: Rendered Markdown is valid, readable, and matches expected semantic structure for all 4 worked examples

### 9. Implement `.render_section_content()` internal dispatcher

- **Requirements**: R7
- **Files**: `R/description_renderer.R`
- **Details**:
  - Type-dispatch based on content structure
  - `data.table` with `variable`/`selected_categories` columns → filter table renderer
  - `data.table` with `measure_label`/`stat_group` columns → statistics table renderer
  - `data.table` with `country_name`/`year`/`welfare_type_label` → survey list table renderer
  - `list` with scalar fields → key-value paragraph renderer
  - `character` vector → numbered list renderer
- **Test Scenarios**:
  - Each content type renders correctly in isolation
  - Mixed content types within a single model
- **Tests**: `tests/testthat/test-description-renderer.R`
- **Acceptance criteria**: Each content type produces correct Markdown syntax

## Phase 4: API Integration

### 10. Update `/table` endpoint with `include_metadata` support

- **Requirements**: R8
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - Add `include_metadata` parameter to `/table` endpoint (default `"false"`)
  - Parse as logical: `include_meta <- identical(tolower(include_metadata), "true")`
  - Pass to `table_maker(..., include_metadata = include_meta)`
  - When `TRUE`, include `description_metadata` in the `api_response()` `meta` field
  - Update Plumber annotations to document the new parameter
- **Test Scenarios**:
  - Happy path: `GET /table?include_metadata=true` returns `meta.description_metadata`
  - Happy path: `GET /table` (no param) returns no `description_metadata` in response
  - Edge case: `include_metadata=invalid` → defaults to `false`, no error
- **Tests**: `tests/testthat/test-api-description.R` — new file, using `plumber` test harness
- **Acceptance criteria**: `/table?include_metadata=true` returns metadata; default behavior unchanged

### 11. Implement `/description` POST endpoint

- **Requirements**: R9, R10
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - **Request body schemas**:
    - Fast path: `{"description_metadata": <metadata object>}` — render-only, no recomputation
    - Fallback path: `{"pip_id": [...], "analysis_var": "...", "measures": [...], "poverty_line": ..., "by": [...], "filter_base": {...}, "ppp": ..., "release": "...", "pop_share_threshold": ...}` — recomputes via `table_maker(include_metadata = TRUE)`
  - **Distinguish**: Check if `description_metadata` field is present in request body
  - **Validation**: If both `description_metadata` and param fields are present, abort with 400
  - Add `#* @parser json` Plumber annotation
  - **Fast path logic**:
    - Parse `description_metadata` from body
    - Call `build_description_model(metadata, params)`
    - Call `render_description_markdown(model)`
    - Return as `@serializer text`
  - **Fallback path logic**:
    - Parse params from body (same validation as `/table`)
    - Call `table_maker(..., include_metadata = TRUE)`
    - Proceed as fast path with returned metadata
  - Wrap in `capture_with_warnings()` for consistent error handling
- **Test Scenarios**:
  - Happy path: POST with metadata → returns Markdown text
  - Happy path: POST with params (no metadata) → recomputes and returns Markdown
  - Edge case: Empty metadata body → 400 error
  - Edge case: Invalid metadata structure → 422 error
  - Edge case: Both `description_metadata` and params present → 400 error
- **Tests**: `tests/testthat/test-api-description.R`
- **Acceptance criteria**: Both paths produce identical Markdown for the same logical inputs; request body schemas documented

### 12. Update Plumber router annotations and helpers

- **Requirements**: R8, R9
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**:
  - Add `/description` to the endpoint list in router header comment
  - Add validation helper `validate_description_input()` in `helpers.R` for the fallback path
  - Ensure CORS headers apply to `/description` POST
  - Update `capture_with_warnings()` if needed for text serializer
- **Test Scenarios**:
  - `/description` endpoint appears in router introspection
  - CORS preflight works for `/description`
- **Tests**: `tests/testthat/test-api-description.R`
- **Acceptance criteria**: `/description` is listed in router endpoints; CORS works

## Testing Strategy

### Cell Definition Safety Net (R5, R6)

The cell definition algorithm is the highest-risk component. Tests must serve as **safety mechanisms** that catch incorrect cell content before it reaches users. Strategy:

1. **Golden-file assertions**: Each worked example has an exact expected output string (cleaned — no interpretation blocks). Tests compare byte-for-byte. Any deviation in phrasing, formatting, or mathematical content fails the test.

2. **Negative phrasing tests**: Explicit assertions that the WRONG phrasing is NOT produced:
   - Inequality measures must use "among", not "for"
   - Summary stats must use "for", not "among"
   - Poverty measures must include dollar sign and PPP year
   - `target_within_group_share` must reference the group denominator, not the survey total
   - `target_survey_share` must reference the survey total, not the group

3. **Permutation matrix tests**: Parameterized tests across all measure subfamilies × filter states × covariate states. The `shares` family has 3 distinct measures with different semantics, so each is counted separately:
   - `summary_stats` (1) × 3 filter states × 3 covariate states = 9
   - `inequality` (1) × 3 × 3 = 9
   - `poverty` (1) × 3 × 3 = 9
   - `shares/pop_share` (1) × 3 × 3 = 9
   - `shares/target_within_group_share` (1) × 3 × 3 = 9
   - `shares/target_survey_share` (1) × 3 × 3 = 9
   - **Total: 54 combinations**
   
   Each cell asserts:
   - `population_scope` sentence is non-empty and contains expected keywords
   - `measure_interpretation` has exactly one entry per measure
   - No raw codes appear in output (all labels resolved)

4. **Regression guard**: A dedicated test that runs `build_cell_definition()` with a "known-good" fixture and compares against a saved golden file. If the algorithm changes, the golden file must be explicitly updated.

### Test File Inventory

| File | Purpose | Phase |
|------|---------|-------|
| `test-registry-structure.R` | Validate registry function return structures match spec §5.1 | 0 |
| `test-table-maker-metadata.R` | Return type, metadata structure, warning capture, abort-path safety | 1 |
| `test-description-metadata.R` | `.build_description_metadata()` field correctness | 1 |
| `test-description-builder.R` | `build_description_model()` section visibility and content | 2 |
| `test-cell-definition.R` | **Cell definition algorithm — safety-critical tests (54 combos)** | 2 |
| `test-description-helpers.R` | Helper functions (label resolution, formatting) | 2 |
| `test-description-renderer.R` | Markdown rendering, parsed Markdown assertions | 3 |
| `test-api-description.R` | `/table` metadata param, `/description` endpoint, POST body schemas | 4 |

## Documentation Checklist

- [ ] `man/table_maker.Rd` updated: new `include_metadata` param, updated `@return` (documenting both paths)
- [ ] `man/build_description_model.Rd` created
- [ ] `man/build_cell_definition.Rd` created
- [ ] `man/render_description_markdown.Rd` created
- [ ] `vignettes/description-endpoint.Rmd` — architectural overview, cell definition walkthrough, worked examples
- [ ] Roxygen2 `@export` on public functions: `build_description_model()`, `render_description_markdown()`
- [ ] Internal functions marked `@keywords internal`: `build_cell_definition()`, `.render_section_content()`, helpers

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Cell definition logic errors produce incorrect descriptions | **High** | Golden-file tests, negative phrasing tests, 54-combination permutation matrix — all must pass |
| Warning capture interferes with error propagation | **Medium** | Only wrap core computation (after validation); selective `cli_warn()` capture; explicit abort-path tests |
| Registry data inconsistencies cause wrong labels | **Medium** | Phase 0 structure validation; tests use real registry data |
| Spec examples contain interpretation blocks that shouldn't be in output | **Medium** | Clean spec §4.3 before Phase 2; golden files based on cleaned examples |
| Markdown rendering produces malformed output | **Low** | Parsed Markdown assertions; visual inspection of canonical examples |
| Performance degradation from metadata assembly | **Low** | Metadata assembly is opt-in (`include_metadata = TRUE`); benchmark after Phase 1 |

## Out of Scope

- Internationalization (English only for v1)
- HTML/JSON renderers (extensibility hook only)
- Server-side caching of descriptions
- UI implementation (Step 3 frontend)
- Performance benchmarking of `/description` endpoint latency
- `tm_measure_spec.yaml` `definition` field enhancement (optional per spec §5.2)
- Changing `table_maker()` return type for `include_metadata = FALSE` (backward compatibility preserved)

## Completion Contract

### Outcome
`table_maker()` gains an opt-in `include_metadata` parameter that returns a list with `description_metadata` when `TRUE`, preserving the existing `data.table` return when `FALSE`. A new `build_description_model()` function transforms metadata into a structured model with conditional section visibility and mathematically correct cell definitions for all measure families. The renderer produces valid Markdown. The `/table` endpoint exposes metadata via `include_metadata=true`, and the `/description` endpoint renders it via POST.

### Verification Surface

| Phase | ID | Evidence Required | Command/Artifact | Required |
|-------|----|-------------------|------------------|----------|
| 0 | V0 | Registry functions return structures matching spec §5.1 | `test-registry-structure.R` | yes |
| 1 | V1 | `include_metadata = FALSE` returns `data.table` (unchanged) | `test-table-maker-metadata.R` | yes |
| 1 | V2 | `include_metadata = TRUE` returns `list(data, description_metadata)` | `test-table-maker-metadata.R` | yes |
| 1 | V3 | All 414+ existing tests pass unchanged | `devtools::test()` | yes |
| 1 | V4 | Abort messages unchanged when validation fails with `include_metadata = TRUE` | `test-table-maker-metadata.R` | yes |
| 2 | V5 | `build_description_model()` produces correct model for all 4 worked examples | `test-description-builder.R` | yes |
| 2 | V6 | Cell definition algorithm passes 54-combination permutation matrix | `test-cell-definition.R` | yes |
| 2 | V7 | Negative phrasing tests confirm no incorrect cell content | `test-cell-definition.R` | yes |
| 3 | V8 | Markdown output matches golden files for all 4 examples | `test-description-renderer.R` | yes |
| 4 | V9 | `/table?include_metadata=true` returns metadata in JSON | `test-api-description.R` | yes |
| 4 | V10 | `/description` POST returns Markdown from metadata | `test-api-description.R` | yes |
| final | V11 | `devtools::check()` clean — 0 errors, 0 warnings | R CMD check | yes |

### Constraints

| Phase | ID | Constraint | Check |
|-------|----|------------|-------|
| 0 | C0 | Registry function return structures match spec §5.1 | Structure validation tests |
| 1 | C1 | `include_metadata = FALSE` output identical to current `data.table` | Regression test |
| 1 | C2 | Abort messages unchanged when `include_metadata = TRUE` | Explicit abort-path tests |
| 2 | C3 | All 4 worked examples byte-identical to cleaned spec §4.3 | Golden-file assertions |
| 2 | C4 | Cell definition permutation matrix (54 combos) all pass | Parameterized tests |
| 3 | C5 | Markdown output valid and readable | Parsed Markdown assertions |
| 4 | C6 | API responses follow `{status, data, warnings, errors, meta}` envelope | Integration tests |
| 4 | C7 | POST body schemas documented and validated | Schema validation tests |

### Boundaries
- Allowed: New files `R/description_builder.R`, `R/description_renderer.R`; modifications to `R/table_maker.R`, `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- Out of scope: Internationalization, HTML/JSON renderers, server-side caching, UI, performance benchmarking, changing `table_maker()` return type for default path

### Iteration Policy
1. Each phase must complete and pass all tests before starting the next
2. Phase 0 is a gate — block if registry structures don't match spec
3. Phase 1 is the foundation — all downstream phases depend on it
4. Phases 2 and 3 can be developed in parallel after Phase 1 completes
5. Phase 4 depends on Phases 1 + 2 + 3
6. Within each phase, implement steps sequentially

### Blocked-Stop Conditions
- Phase 0: If any registry function returns a structure that doesn't match spec §5.1, halt and document the deviation
- Phase 1: If warning capture breaks any `cli_abort()` test, halt and redesign the capture boundary
- Phase 1: If `include_metadata = FALSE` return type differs from current `data.table`, halt
- Phase 2: If golden-file tests fail for any of the 4 worked examples, halt — the algorithm has a bug
- Phase 4: If Plumber serialization produces malformed JSON, halt and investigate

### Deviation policy
`ask` (default)
