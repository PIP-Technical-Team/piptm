---
date: 2026-08-20
title: "Step 3 Description File - Implementation Plan"
status: completed
completed-date: 2026-08-21
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-08-20-step3-description-architecture.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
tags: [ui, api, documentation, step-3, metadata, architecture]
phases: 2
completed-phases: [1, 2]
---

# Plan: Step 3 Description File - Implementation

## Objective

Implement a structured description document for Step 3 table results. The description explains what a generated table represents so users can share, cite, and understand it later. The architecture uses a sidecar `with_meta = TRUE` variant of `table_maker()` to capture execution truth, a `description_model` intermediate representation, and a markdown renderer.

## Context

Users reach Step 3 after making multiple decisions across Step 1 (survey selection) and Step 2 (filters, statistics, layout). The resulting table may be complex — especially with share measures, sample-base filters, or multi-slot disaggregation. The table displays numbers but does not explain what each number represents, how the table is structured, what happened during computation, or what methodological choices were made.

This plan implements the architecture decided in the Aug 20 brainstorm, which revises the original Aug 13 approach to avoid duplicating `table_maker()` logic.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `table_maker()` default return unchanged (data.table) | Brainstorm Decision 1 |
| R2 | `table_maker(with_meta = TRUE)` returns `list(data, specification, execution, provenance, warnings)` | Brainstorm Decision 1 |
| R3 | `specification` includes labeled inputs from registry (not raw codes) | Brainstorm Decision 2 |
| R4 | `execution` includes included/excluded surveys with reasons, applied filters, suppression effects | Brainstorm Decision 2 |
| R5 | `build_description_model(table_result)` returns structured list with conditional sections | Brainstorm Decision 2 |
| R6 | `render_description_markdown(model)` returns markdown string | Brainstorm Decision 2 |
| R7 | `build_table_description()` convenience function wraps the pipeline | Brainstorm Decision 2 |
| R8 | `GET /description` endpoint returns `{model, markdown}` | Brainstorm Decision 3 |
| R9 | Existing 414+ tests pass without modification | Brainstorm constraint |
| R10 | `/table` endpoint JSON contract unchanged | Brainstorm constraint |

## Implementation Steps

### Phase 1: Core Implementation

### 1. Implement `with_meta` flag in `table_maker()`

- **Requirements**: R1, R2, R3, R4, R9, R10
- **Files**: `R/table_maker.R`
- **Details**:
  - Add `with_meta = FALSE` parameter to `table_maker()` signature
  - When `with_meta = FALSE`: existing behavior, return data.table (zero change)
  - When `with_meta = TRUE`:
    - Initialize `meta_state` list before computation begins
    - Harvest `specification` from function arguments + registry lookups:
      - `pip_id` (requested)
      - `analysis_var` with label from `piptm_analysis_variables()`
      - `measures` with labels from `piptm_stat_groups()` and families from `.MEASURE_REGISTRY`
      - `poverty_line`, `ppp`, `pop_share_threshold` (as-is)
      - `by` with labels from `piptm_layout_covariates()` and role assignments
      - `filter_base` with labels from `piptm_filter_categories()`
    - Harvest `execution` metadata during existing pipeline:
      - `included_surveys`: from `entries` after manifest join (before dimension pre-filter)
      - `excluded_surveys`: from dimension pre-filter and filter-base pre-filter warnings
      - `filters_applied`: the `normalized_filter_base` that was actually applied
      - `measures_computed`: from `classified` (the families actually dispatched)
      - `suppression`: threshold + count of suppressed cells from Step 10
      - `ppp_used`: resolved PPP column name
    - Harvest `provenance`: `piptm_current_release()`, `utils::packageVersion("piptm")`
    - Harvest `warnings`: capture `cli_warn()` messages via `withCallingHandlers()`
    - Return `list(data = result, specification = spec, execution = exec, provenance = prov, warnings = warns)` instead of bare `result`
  - Add `@param with_meta` to roxygen documentation
  - Update `@return` documentation to describe both paths
- **Test Scenarios**:
  - Happy path: `with_meta = TRUE` returns list with all 5 fields
  - Backward compat: `with_meta = FALSE` (default) returns data.table
  - `specification` labels match registry output
  - `execution$excluded_surveys` captures dimension-filter exclusions
  - `execution$suppression` captures suppression count
- **Tests**: `tests/testthat/test-table-maker-with-meta.R`
- **Acceptance criteria**: All existing tests pass; new tests verify `with_meta = TRUE` return shape and content

### 2. Create `build_description_model()` function

- **Requirements**: R5
- **Files**: `R/description.R` (new file)
- **Details**:
  - Create `R/description.R` with `build_description_model(table_result)` function
  - Accept the `with_meta = TRUE` return from `table_maker()`
  - Build structured list with these sections:
    - `metadata`: from `provenance` (release, package_version, ppp from specification)
    - `surveys`: from `execution$included_surveys` (table of country/year/type/pip_id) + `execution$excluded_surveys` (table of pip_id/reason, shown only if non-empty)
    - `sample`: from `specification$filter_base` (variable labels + kept categories) or "full survey sample" text
    - `statistics`: analysis_var label + measures with display labels from `specification$measures`
    - `poverty_line`: from `specification$poverty_line` (only if non-NULL)
    - `layout`: from `specification$by` mapped to roles (rows/columns/super-rows/super-columns) with variable labels and category labels
    - `cell_definition`: generated from `by` + `analysis_var` + `measures` using programmatic sentence construction
    - `suppression`: from `execution$suppression` (threshold, n_suppressed_cells)
    - `warnings`: from `execution$warnings` (only if non-empty)
  - All sections use labeled data from registry (not raw codes)
  - Return the `description_model` list
- **Test Scenarios**:
  - Simple: single survey, welfare, 2 dimensions, no filters → all sections populated correctly
  - Complex: 3 surveys, poverty, 4 dimensions, filters, exclusions → all conditional sections present
  - Edge: `by = NULL` → layout section absent, cell definition uses "full survey sample"
  - Edge: no poverty line → poverty_line section absent
  - Edge: no exclusions → excluded_surveys section absent
  - Edge: no warnings → warnings section absent
- **Tests**: `tests/testthat/test-description-model.R`
- **Acceptance criteria**: Model structure matches brainstorm spec; all conditional sections behave correctly

### 3. Create `render_description_markdown()` function

- **Requirements**: R6
- **Files**: `R/description.R` (same file as Step 2)
- **Details**:
  - Add `render_description_markdown(model)` to `R/description.R`
  - Walk each section in the model and emit markdown
  - Section rendering:
    - `metadata`: `**Generated:** {date} · **Release:** {release} · **PPP year:** {ppp}`
    - `surveys`: markdown table (Country | Year | Welfare type | Survey ID) + excluded surveys sub-table if non-empty
    - `sample`: "The full survey sample was used (no filters applied)." or "Filtered to:" + bullet list of filter variables with kept categories
    - `statistics`: bullet list with analysis variable and measures
    - `poverty_line`: "Poverty line: $X.XX per day (PPP year)"
    - `layout`: bullet list with role assignments (Super Rows, Rows, Super Columns, Columns) + cell narrative sentence
    - `cell_definition`: standalone paragraph
    - `suppression`: threshold message + "No cells were suppressed" or "X cells were suppressed"
    - `warnings`: bullet list of warning messages
  - Return single character string (complete markdown document)
- **Test Scenarios**:
  - Output is valid markdown (no syntax errors)
  - Output matches mock from brainstorm for simple scenario
  - Output matches mock from brainstorm for complex scenario
  - Empty model sections produce appropriate fallback text
- **Tests**: `tests/testthat/test-description-renderer.R`
- **Acceptance criteria**: Output matches brainstorm mocks; valid markdown

### Phase 2: Integration and Testing

### 4. Create `build_table_description()` convenience function

- **Requirements**: R7
- **Files**: `R/description.R` (same file)
- **Details**:
  - Add `build_table_description()` as a convenience wrapper
  - Accept same parameters as `table_maker()`: `pip_id`, `analysis_var`, `measures`, `poverty_line`, `by`, `filter_base`, `ppp`, `release`, `pop_share_threshold`
  - Internally: call `table_maker(with_meta = TRUE)`, then `build_description_model()`, then `render_description_markdown()`
  - Return markdown string directly
  - Add roxygen documentation with `@export` tag
  - Add `@family api` tag for grouping
- **Test Scenarios**:
  - Happy path: returns markdown string for simple input
  - Edge: returns markdown for `by = NULL` (aggregate mode)
  - Integration: end-to-end from parameters to markdown
- **Tests**: `tests/testthat/test-description-convenience.R`
- **Acceptance criteria**: Function exported, documented, works end-to-end

### 5. Add `GET /description` API endpoint

- **Requirements**: R8, R10
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**:
  - Add `GET /description` endpoint to `plumber.R`
  - Query parameters: same as `/table` (`pip_id`, `analysis_var`, `measures`, `poverty_line`, `by`, `filter_base`, `ppp`, `pop_share_threshold`, `release`)
  - Reuse `validate_table_input()` from `helpers.R` for parameter validation
  - Internally: call `piptm::table_maker(with_meta = TRUE)`, then `piptm:::build_description_model()`, then `piptm:::render_description_markdown()`
  - Return: `{status, data: {model, markdown}, warnings, errors, meta}`
  - Add plumber annotations for documentation
  - Add to Insomnia collection (`insomnia-collection.json`)
- **Test Scenarios**:
  - Happy path: returns `{model, markdown}` for valid input
  - Error: returns 400 for invalid parameters (reuses existing validation)
  - Error: returns 422 for domain errors (reuses existing error handling)
  - Warnings: captured from `table_maker()` and included in response
- **Tests**: `tests/testthat/test-api-description-endpoint.R`
- **Acceptance criteria**: Endpoint works, returns correct shape, validates input

### 6. Documentation and export updates

- **Requirements**: R7, R8
- **Files**: `R/description.R`, `NAMESPACE`, `README.md`
- **Details**:
  - Run `devtools::document()` to regenerate NAMESPACE with new exports
  - Verify `build_table_description` appears in NAMESPACE
  - Update README.md endpoint table with `/description` entry
  - Add roxygen examples to `build_table_description()`
  - Verify `devtools::check()` passes (or document any new notes)
- **Test Scenarios**:
  - NAMESPACE includes `export(build_table_description)`
  - README has `/description` in endpoint table
  - `devtools::check()` produces no new errors
- **Tests**: Manual verification via `devtools::document()` and `devtools::check()`
- **Acceptance criteria**: Package builds cleanly, exports are correct

## Testing Strategy

- **Unit tests**: Each new function gets dedicated test file using existing fixture patterns (`write_fixture_parquet_tm`, `write_fixture_manifest_tm`)
- **Integration tests**: End-to-end from `table_maker()` parameters to markdown output
- **Regression**: All existing 414+ tests must pass unchanged (the `with_meta = FALSE` default ensures this)
- **Fixture approach**: Reuse `helper-fixtures.R` patterns; create new fixtures for description-specific scenarios (excluded surveys, poverty line, filters)

## Documentation Checklist

- [ ] roxygen for `build_table_description()` with `@export`, `@family api`, `@examples`
- [ ] roxygen for `build_description_model()` with `@keywords internal`
- [ ] roxygen for `render_description_markdown()` with `@keywords internal`
- [ ] `@param with_meta` added to `table_maker()` roxygen
- [ ] `@return` updated for both `with_meta` paths
- [ ] README.md endpoint table updated
- [ ] Insomnia collection updated

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| `with_meta` harvests add overhead to `table_maker()` | Medium | Low | Harvests are lightweight (list construction, not recompute); measure before/after |
| Registry lookup failures in harvest path | Medium | Low | Wrap in `tryCatch()` with fallback to raw codes |
| `/table` endpoint breaks due to signature change | High | Very Low | Default `with_meta = FALSE` ensures identical path; existing tests validate |
| Markdown output doesn't match mocks | Low | Medium | TDD approach: write tests with expected output first |
| Plumер serializer drops `model` field | Medium | Low | Test with actual plumber response; verify `jsonlite::toJSON()` serialization |

## Out of Scope

- S3 `table_result` class wrapping the sidecar list
- Structured warning codes (refactoring `cli_warn()` calls)
- Server-side PDF generation (HTML template migration)
- Reproducibility code snippets
- Multiple languages (internationalization)
- Survey documentation links
- Interpretation notes section
- UI integration (deferred to UI team)

## Completion Contract

### Outcome
`table_maker()` gains an opt-in `with_meta = TRUE` sidecar returning structured metadata. A new `build_table_description()` function and `GET /description` endpoint produce markdown descriptions of table results, using the execution truth from `table_maker()`.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | All existing tests pass | `devtools::test()` — 414+ pass, 0 fail | yes |
| V2 | `with_meta = FALSE` returns data.table | `test-table-maker-with-meta.R` — default path unchanged | yes |
| V3 | `with_meta = TRUE` returns list with 5 fields | `test-table-maker-with-meta.R` — specification, execution, provenance, warnings populated | yes |
| V4 | `build_description_model()` produces correct sections | `test-description-model.R` — all conditional branches covered | yes |
| V5 | `render_description_markdown()` matches brainstorm mocks | `test-description-renderer.R` — simple + complex scenarios | yes |
| V6 | `build_table_description()` works end-to-end | `test-description-convenience.R` — parameters to markdown | yes |
| V7 | `/description` endpoint returns `{model, markdown}` | `test-api-description-endpoint.R` — happy path + error cases | yes |
| V8 | NAMESPACE exports `build_table_description` | `NAMESPACE` file inspection | yes |
| V9 | README updated with `/description` endpoint | `README.md` inspection | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | Default `table_maker()` return must be data.table | V2 — `is.data.table(table_maker(...))` is TRUE |
| C2 | `/table` endpoint JSON contract unchanged | V1 — all existing API tests pass |
| C3 | No new hard dependencies | DESCRIPTION unchanged (no new Imports) |
| C4 | Existing 414+ tests pass without modification | V1 — zero test changes |

### Boundaries
- Allowed: new file `R/description.R`, modifications to `R/table_maker.R`, `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, new test files
- Out of scope: UI integration, S3 wrapper, structured warning codes, PDF generation

### Iteration Policy
1. Implement Steps 1-3 first (core R functions) — verify with unit tests
2. Implement Steps 4-6 (convenience, endpoint, docs) — verify with integration tests
3. Run full test suite after each phase to catch regressions early

### Blocked-Stop Conditions
- If existing tests fail after Step 1 — halt, investigate, fix before proceeding
- If `with_meta = TRUE` harvests add >50ms overhead to 15-survey benchmark — halt, optimize
- If markdown output doesn't match mocks after 2 iterations — halt, revisit mock design
