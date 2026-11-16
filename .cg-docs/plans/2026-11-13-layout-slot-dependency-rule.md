---
date: 2026-11-13
title: "Relax layout-slot validation to parent-child dependency rule"
status: completed
completed-date: 2026-11-13
scope: "Standard"
brainstorm: null
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-11-13-layout-slot-dependency-rule.md"
completed-phases: [1, 2]
artifact-schema-version: 1
phases: 2
tags: [api, plumber, table-endpoint, layout-slots, validation, error-messages]
---

# Plan: Relax Layout-Slot Validation to a Parent-Child Dependency Rule

## Objective

Replace the strict "fill column -> row -> superColumn -> superRow with no gaps"
validation rule for the `/table`, `/table/download`, and `/metadata` layout
slots with a parent-child dependency rule: `column` and `row` are independent
and may be used alone or together in any request order; `superColumn`
requires `column`; `superRow` requires `row`; `superColumn` and `superRow` are
independent of each other. All layout-slot error messages must remain, or
become, user-friendly — the PIP UI surfaces these API error strings directly
to end users without rewriting them, so every message must read as a
complete, plain-language sentence naming the offending parameter(s) and the
fix, with no internal jargon, slot-key-only fragments, or implementation
details.

## Context

This revises the locked decision in
`.cg-docs/plans/2026-10-22-table-explicit-slot-parameters-revised.md`
("fill order is `column`, `row`, `superColumn`, `superRow`; gaps are HTTP
400"). That plan intentionally deferred the dependency question; this plan
resolves it. `R/table_response.R:70-99` (`build_table_layout()`) and
`R/table_flatten.R` already treat each slot independently with no
contiguous-prefix assumption, which suggests this is primarily a
validation-rule, docs, and test change. Phase 1 confirms that before any
other code is touched.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `column` and `row` may each be used alone or together, in any request argument order | User |
| R2 | `superColumn` requires `column` to be filled; omitted `column` + filled `superColumn` is HTTP 400 | User |
| R3 | `superRow` requires `row` to be filled; omitted `row` + filled `superRow` is HTTP 400 | User |
| R4 | `superColumn` and `superRow` are independent of each other | User |
| R5 | Request argument order never affects the validation outcome | User |
| R6 | All other per-slot checks (type, arity, unknown dimension, duplicate variable, `analysis_var` overlap, `poverty_line` requirement for `pov_status`) are unchanged | Existing behavior |
| R7 | A slot that fails its own check (type/arity/etc.) must not also trigger a dependency error on its child slot | Existing behavior (P1.3-style isolation) |
| R8 | Every layout-slot error message is a complete, user-friendly sentence naming the parameter(s) involved and the required fix, suitable for direct display in the PIP UI with no rewriting | User (this turn) |
| R9 | `build_table_layout()`, the flattener, and the description builder produce correct output for row-only and row+superRow tables (no `column`) | Derived from R1 |

## Implementation Steps

## Phase 1: Research

### 1. Verify downstream slot-order independence
- **Requirements**: R9
- **Files**: `R/table_response.R`, `R/table_flatten.R`, `R/description_builder.R` (`.build_layout_content`), `R/cell_explanation.R`
- **Details**: Trace the code path from the validated `slots` list through
  `build_table_layout()`, `serialize_table_response()`,
  `flatten_table_response()`, `.build_layout_content()`, and the
  cell-explanation renderer. Confirm each treats `column`/`row`/`superColumn`/
  `superRow` as independent named slots (not a contiguous prefix). Document
  any place that assumes `columns` is always populated before `rows`, or that
  the UI/description renderer breaks for a row-only or row+superRow table.
  Do not fix structural issues yet — report findings first (see Blocked-Stop
  Conditions).
- **Test Scenarios**: n/a (research step)
- **Tests**: n/a
- **Acceptance criteria**: Written finding stating either "no order
  assumption found" or a precise list of locations that assume order, each
  with a proposed fix.

## Phase 2: Implement, document, and test

### 2. Implement the parent-dependency validation rule
- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8
- **Files**: `inst/plumber/helpers.R` (validation helper, currently lines
  ~260-414)
- **Details**: Remove the prefix/gap check. Add two independent checks:
  `superColumn` filled requires `column` filled; `superRow` filled requires
  `row` filled. Each check runs only when the slot in question did not already
  fail its own per-slot check (reuse the existing `invalid_names`/arity
  exclusion pattern). Word every new error message as a complete sentence
  naming both the slot and its required parent in display terms the UI can
  show verbatim, e.g. "Add a Columns selection before Super Columns can be
  used." / "Add a Rows selection before Super Rows can be used." Avoid raw
  parameter identifiers alone; pair the internal name with the user-facing
  slot label already used elsewhere (Columns/Rows/Super Columns/Super Rows).
  Review the existing per-slot error strings (arity, type, duplicate, unknown
  dimension, `analysis_var`, `poverty_line`) for the same user-friendliness
  bar; rewrite any that are terse, parameter-only, or exposes internal
  mechanics, while preserving their trigger conditions exactly.
- **Test Scenarios**: happy path (row alone, column alone, row+column in
  either order), edge case (superColumn+column+row valid; superColumn+row
  only, no column, invalid), error path (superColumn alone; column+superRow
  without row)
- **Tests**: `tests/testthat/test-api-helpers.R`
- **Acceptance criteria**: All combinations in the table below validate as
  specified; every error string reads as a complete, jargon-free sentence.

  | Request | Result |
  |---|---|
  | `row` | valid |
  | `column` | valid |
  | `row` + `column` (either order) | valid |
  | `superColumn` alone | 400, dependency error |
  | `column` + `superRow` (no `row`) | 400, dependency error |
  | `column` + `superColumn` + `row` | valid |
  | `row` + `superRow` | valid |
  | `column` + `row` + `superRow` (no `superColumn`) | valid |

### 3. Update API documentation strings
- **Requirements**: R1, R2, R3, R4, R8
- **Files**: `inst/plumber/plumber.R` (doc blocks and `@param` descriptions
  around the `/table`, `/table/download`, and `/metadata` endpoints,
  including the request-schema `superColumn`/`superRow` descriptions), any
  `dev/` notes describing the old "strict order" rule
- **Details**: Replace "requires `column` and `row` first" / "filled in
  strict order" language with the parent-dependency description: `column`
  requires nothing; `row` requires nothing; `superColumn` requires `column`;
  `superRow` requires `row`. Keep wording consistent with the UI-facing error
  text from Step 2 so docs and runtime errors do not contradict each other.
- **Test Scenarios**: n/a (docs)
- **Tests**: n/a
- **Acceptance criteria**: No remaining doc string describes the old
  prefix/gap rule; `grep -r "strict order"` and `"filled in order"` return no
  matches referring to layout slots.

### 4. Update and extend tests
- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8, R9
- **Files**: `tests/testthat/test-api-helpers.R`, `test-api-metadata.R`,
  `test-api-endpoints.R`, `test-table-response.R`,
  `test-description-builder.R` (and `test-description-renderer*.R` if Step 1
  found renderer assumptions)
- **Details**: Update or remove the test asserting the old gap/prefix error
  (`test-api-metadata.R`, "rejects gapped or duplicate layout slots"). Add
  cases for every row of the Step 2 acceptance table, for both the validation
  helper directly and through `/table`/`/metadata` HTTP calls (400 vs 200).
  Add a case proving request argument order does not change the outcome. Add
  a case asserting the new error message text is a complete sentence (not
  just that an error occurred). Add row-only and row+superRow cases through
  `build_table_layout()`/flattener/description builder per Step 1 findings.
- **Test Scenarios**: happy path, edge case, error path (see Step 2 table)
- **Tests**: run each affected file individually via `execution_subagent` +
  `. tests\Run-Tests.ps1 -File <test-name>`; never run the full directory or
  `Invoke-Pester` directly
- **Acceptance criteria**: All targeted test files pass; full-suite gate run
  once at phase boundary per `cg-skill-pester-safety`.

## Testing Strategy

Unit-level: validation helper tested directly with all slot combinations.
Integration-level: `/table`, `/table/download`, `/metadata` HTTP round trips
for representative valid/invalid combinations. Regression: existing per-slot
error tests (type, arity, duplicate, unknown dimension, `analysis_var`,
`poverty_line`) re-run unmodified in trigger condition, only message wording
may differ.

## Documentation Checklist

- [ ] `inst/plumber/plumber.R` doc blocks and `@param` descriptions
- [ ] OpenAPI/request-schema descriptions for `superColumn`/`superRow`
- [ ] `dev/` notes referencing the old prefix rule, if any
- [ ] This plan references and supersedes the locked "fill order" decision in
      `2026-10-22-table-explicit-slot-parameters-revised.md`

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Flattener/renderer assumes `columns` precedes `rows` | Phase 1 research step verifies before any downstream code is touched; blocked-stop if a non-trivial rewrite is needed |
| New error messages drift from UI copy conventions used elsewhere in the API | Reuse existing user-facing slot labels (Columns/Rows/Super Columns/Super Rows) already present in other error strings; have a reviewer read each string as if displayed raw in the UI |
| Per-slot error isolation regresses (a failing slot wrongly also reports a dependency error) | Explicit test case per failure-isolation requirement (R7) |
| Existing tests pinned to old error text break silently | Full grep for the old error string literal before removing it |

## Out of Scope

- Changing the `cells[]`/`layout` response shape
- Changing `table_maker()` computation or any file under `R/` beyond what
  Step 1 proves is strictly required
- Adding new layout slots or changing slot count
- PIP platform UI changes (this plan only changes what the API returns)

## Completion Contract

### Outcome

`/table`, `/table/download`, and `/metadata` accept `column`/`row`
independently and in either order, require `superColumn` only when `column`
is filled and `superRow` only when `row` is filled, and reject other
combinations with a clear, complete, user-friendly 400 error message fit for
direct display in the PIP UI — replacing the current strict prefix/no-gap
rule.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----|
| V1 | Validation helper accepts `row` alone, `column` alone, `row+column` in any order | `test-api-helpers.R` | 2 | yes |
| V2 | `superColumn` alone and `column+superRow` (no row) rejected with dependency-specific, user-friendly message | `test-api-helpers.R` | 2 | yes |
| V3 | `column+superColumn+row`, `row+superRow`, `column+row+superRow` accepted | `test-api-helpers.R` | 2 | yes |
| V4 | Per-slot errors (arity, type, duplicate, unknown dim, analysis_var, poverty_line) unchanged in trigger condition and do not co-fire a dependency error for an already-failed slot | `test-api-helpers.R` | 2 | yes |
| V5 | `/table`, `/table/download`, `/metadata` endpoints return 400/200 consistent with V1-V3 | `test-api-endpoints.R`, `test-api-metadata.R` | 2 | yes |
| V6 | Row-only and row+superRow requests produce correct `layout`/`cells` shape | `test-table-response.R`, `test-description-builder.R` | 2 | yes |
| V7 | Every new/changed layout-slot error string is a complete, jargon-free sentence naming the parameter and the fix | manual read-through + test assertion on message text | 2 | yes |
| V8 | Doc strings updated, no remaining reference to the old prefix rule | grep review | 2 | yes |
| V9 | Targeted test files pass via `execution_subagent` + `Run-Tests.ps1`; full-suite gate run at phase boundary | test run output | final | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|----|
| C1 | No change to `table_maker()` or files under `R/` beyond what Step 1 proves necessary | diff review | final |
| C2 | Request argument order must never affect validation outcome | test case | 2 |
| C3 | Existing non-layout validation behavior stays identical in trigger condition | existing tests | 2 |
| C4 | Never run `Invoke-Pester` directly; use `execution_subagent` + `Run-Tests.ps1` | process check | 2 |
| C5 | Every layout-slot error message must be directly displayable in the UI with no rewriting (complete sentence, names the parameter and fix, no internal jargon) | manual + test review | 2 |

### Boundaries

- Allowed: `inst/plumber/helpers.R`, `inst/plumber/plumber.R` doc
  strings/schema descriptions, `R/table_response.R` and
  `R/description_builder.R` only if Step 1 finds a real gap, associated test
  files, `dev/` docs mentioning the old rule.
- Out of scope: response shape changes, `table_maker()` computation changes,
  new layout slots, PIP platform UI code.

### Iteration Policy

1. Run Phase 1 (research) fully before any validation code is edited.
2. Implement the two parent-dependency checks, preserving existing per-slot
   checks and their isolation from each other.
3. Write/update user-facing error strings and docs in the same phase as the
   validation change so they cannot drift apart.
4. Update tests alongside the validation and doc changes, not as an
   afterthought.

### Blocked-Stop Conditions

- Step 1 finds that `build_table_layout()`, the flattener, or the
  description/cell-explanation renderer assumes a contiguous
  `columns`-before-`rows` fill order and needs a non-trivial rewrite: stop
  and report before Phase 2.
- Full-suite gate fails and cannot be resolved within the 2-attempt fix
  budget per step.
</content>
