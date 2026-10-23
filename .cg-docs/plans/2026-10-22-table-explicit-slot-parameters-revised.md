---
date: 2026-10-22
title: "/table explicit slot parameters replacing ordered by (revised)"
status: completed
completed-date: 2026-10-23
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-10-22-table-endpoint-explicit-slot-parameters.md"
supersedes: ".cg-docs/plans/2026-10-22-table-explicit-slot-parameters.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-10-22-table-explicit-slot-parameters-revised.md"
completed-phases: [1, 2]
artifact-schema-version: 1
phases: 2
tags: [api, plumber, table-endpoint, request-contract, layout-slots, validation, openapi, breaking-change]
---

# Plan: `/table` Explicit Slot Parameters Replacing Ordered `by` (Revised)

## Objective

Replace the repeatable `by` parameter on `/table` with four named slot parameters — `column`, `row`, `superColumn`, `superRow` — each accepting exactly one variable name. Assemble them into an ordered `by` vector inside `validate_table_input()`. Migrate the `/description` fallback to the same parameters in the same change.

Equivalent requests must return the same `data` and `meta` as fixtures captured before `by` is removed. This is semantic equality of those fields, not raw byte identity. `table_maker()` and every file under `R/` stay unchanged.

## Context

This plan revises `.cg-docs/plans/2026-10-22-table-explicit-slot-parameters.md` after `/cg-plan-review`. That file is historical. Implement this file only.

Locked decisions, not reopened: thin API layer; no `by` shim on `/table`; repeated values, duplicate variables, and gaps are HTTP 400; fill order is `column`, `row`, `superColumn`, `superRow`; all four optional; the 4-dimension cap in `R/measures.R` stays as a direct-caller guard; the `cells[]` + `layout` response reshape is a later plan.

Review fixes baked in:

- P1.1: `validate_table_input()` has a second caller. `inst/plumber/plumber.R:417-436` passes `by = body$by` and then `table_maker(by = body$by)`. `validate_description_input()` lists `"by"` at `helpers.R:553`. The fallback roxygen example is at `plumber.R:333`. `tests/testthat/test-phase2-analysis-var-and-poverty-line.R:75-80` calls `validate_table_input(..., by = "pov_status")`.
- P1.2: capture fixtures before deleting `by`. Compare parsed `data` and `meta`, not undocumented "bytes".
- P1.3: an arity failure must not be rewritten as "slot was empty".
- P2.1: one assembly expression, `unname()`, empty result is `NULL` not `character(0)`.
- P2.2: `:character` is documentation, not a runtime guard. JSON arrays are specified and tested.
- P2.3: `$slots` is a fixed four-key list with `NULL` for unfilled slots. The filtered list is not a contract.
- P2.4: OpenAPI assertions cover `in`, scalar schema, and exact camelCase. A wrong-case query must not disaggregate.
- P2.5: the return-shape pin at `test-api-helpers.R:353` expects five names today and must be updated. Do not trust the old four-line call-site inventory.
- P3.1: trim before the filled test and assemble the trimmed scalar.
- P3.2: delete the contract claim that `/table` rejects a fifth dimension server-side.

`$slots` is produced for the later layout plan. This plan does not build `layout`.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `/table` accepts `column`, `row`, `superColumn`, `superRow`, each one variable, all optional | Brainstorm |
| R2 | `by` is removed from `/table`. No alias and no shim | Brainstorm |
| R3 | More than one value in a slot is HTTP 400, for a repeated query param and for a JSON array of length greater than 1. Never 500 | Brainstorm + P2.2 |
| R4 | The same variable in two slots is HTTP 400 | Brainstorm |
| R5 | A gap is HTTP 400. Fill order is `column`, `row`, `superColumn`, `superRow` | Brainstorm |
| R6 | An arity failure is not reported as an empty slot | P1.3 |
| R7 | `$slots` is always the four keys above. Unfilled slots are `NULL`. `$by` is `unname(unlist(Filter(Negate(is.null), slots), use.names = FALSE))`, or `NULL` when that is empty | P2.1 + P2.3 |
| R8 | Omitting all four slots returns the national table with HTTP 200, not 500 | Brainstorm |
| R9 | Dimension membership runs on the assembled, trimmed vector. `pov_status` keeps its existing carve-out | Brainstorm |
| R10 | `poverty_line` is still required when `pov_status` is a filled slot or `analysis_var` | Brainstorm |
| R11 | No file under `R/` changes, including `table_maker()` and `R/measures.R` | Brainstorm |
| R12 | OpenAPI lists the four slot params, not `by`, each `in == "query"` and a scalar string, on GET and POST | P2.4 |
| R13 | Query keys are case-sensitive. `supercolumn` does not bind to `superColumn` | P2.4 |
| R14 | A JSON string and a length-1 JSON array are accepted. Length greater than 1 is the R3 400 | P2.2 |
| R15 | `" gender "` is `gender`. `"  "`, `""`, `NA`, and `NULL` are unfilled | P3.1 |
| R16 | `data` and `meta` match fixtures captured before `by` is removed | P1.2 |
| R17 | The `/description` fallback, its validator param list, and its roxygen example use the four slot params and `fcheck$by` | P1.1 |
| R18 | `docs/api-contract.md` documents the four params, the gap rationale, and no longer says `/table` rejects more than four dimensions | P3.2 |

## Implementation Steps

## Phase 1: Fixtures, inventory, and validator

### 1. Capture baselines and inventory every caller

- **Requirements**: R16, R17
- **Files**: `tests/testthat/test-api-endpoints.R`, new files under `tests/testthat/fixtures/table-slot-baseline/`, `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `tests/testthat/test-phase2-analysis-var-and-poverty-line.R`, `tests/testthat/test-api-description.R`
- **Details**:
  - Do this before any signature change. The current `by` API must still exist.
  - Capture parsed `/table` responses for a fixed fixture set: no `by` (national); `by = "gender"`; two ordered `by` values that map to column then row; `pov_status` in `by` with a poverty line; the NR-masking request at `test-api-endpoints.R:1274`.
  - Store the parsed `data` and `meta` only. Do not store raw bytes. Record the exact request each fixture came from, including `by` order.
  - Grep for `validate_table_input(`, `body$by`, and `by =` inside those calls. The known set is `plumber.R:149`, `plumber.R:417`, `test-api-helpers.R` via `.vti()`, and `test-phase2-analysis-var-and-poverty-line.R:55`, `:65`, `:76`. Confirm the grep matches this set before editing. A new caller is a blocked stop.
  - Note the shape pin at `test-api-helpers.R:353`. It currently expects `valid`, `errors`, `poverty_line`, `ppp`, `pop_share_threshold`.
- **Test Scenarios**:
  - Happy: each baseline request returns 200 and the fixture file is written.
  - Edge: two-`by` fixture records order, not a sorted set.
  - Error: a failed baseline request is not saved as a fixture.
- **Tests**: `testthat::test_file("tests/testthat/test-api-endpoints.R")` against the unchanged API, plus the fixture files on disk
- **Acceptance criteria**: five fixture files exist with `data` and `meta`; the caller grep has no surprise hits.

### 2. Rewrite slot validation and assembly

- **Requirements**: R1, R3, R4, R5, R6, R7, R9, R10, R15
- **Files**: `inst/plumber/helpers.R`
- **Details**:
  - Replace `by = NULL` on `validate_table_input()` with `column = NULL, row = NULL, superColumn = NULL, superRow = NULL`.
  - Slot order is the literal vector `c("column", "row", "superColumn", "superRow")`. Never sort it.
  - Per slot, classify before any filtering. Length greater than 1 is an arity failure: append `` "`<name>` accepts a single variable; <n> values were supplied." `` and mark the slot `invalid`. Do not drop it yet.
  - Otherwise trim. `NULL`, length 0, `NA`, `""`, and whitespace-only are `unfilled`. Any other scalar is `filled` with the trimmed value. `" gender "` becomes `"gender"`.
  - Gap check uses only `filled` and `unfilled` slots. `invalid` slots are excluded, so a repeated slot is never described as empty. Filled names must be a prefix of the order vector. If not, name the empty earlier slot and the filled later slot.
  - Duplicate check uses filled scalars only. Name the variable and both slots.
  - Build `$slots` as a list of the four names. Filled slots hold the trimmed scalar. Unfilled and invalid slots hold `NULL`.
  - Set `by` with `unname(unlist(Filter(Negate(is.null), slots), use.names = FALSE))`. If the result has length 0, set `by` to `NULL`, not `character(0)`.
  - Run the existing dimension check on that `by`, including `setdiff(by, "pov_status")`.
  - Point the `pov_status` / `poverty_line` check at the assembled `by`, not a removed argument.
  - Keep appending errors. Do not return on the first failure.
  - Add `by` and `slots` to the return list at `helpers.R:507`.
- **Test Scenarios**:
  - Happy: four filled; one filled; none filled; two contiguous filled.
  - Edge: `NULL`, `NA`, `""`, `"  "`, and `" gender "`.
  - Error: length greater than 1; duplicate; gap; unknown dimension; `pov_status` without `poverty_line`; arity plus a later filled slot.
- **Tests**: `tests/testthat/test-api-helpers.R`
- **Acceptance criteria**: `$slots` always has the four names; `$by` matches the specified expression; an arity-plus-later-slot error does not say the repeated slot was empty.

### 3. Unit-test the validator and update pinned callers

- **Requirements**: R3, R4, R5, R6, R7, R8, R9, R10, R15, R17
- **Files**: `tests/testthat/test-api-helpers.R`, `tests/testthat/test-phase2-analysis-var-and-poverty-line.R`
- **Details**:
  - Rewrite `test-api-helpers.R:199` and `:209` from `by =` to slot arguments, keeping the unknown-dimension and valid-dimension intent.
  - Update the shape test at `:353` to expect `valid`, `errors`, `poverty_line`, `ppp`, `pop_share_threshold`, `by`, `slots`, in that order.
  - Assert `expect_identical(names(result$slots), c("column", "row", "superColumn", "superRow"))` on every case.
  - Assert `$by` with the same expression as Step 2, and `expect_null(result$by)` when all slots are unfilled. Do not use `expect_equal(x, character(0L))`.
  - Add rejection tests for repeat, duplicate, and gap. The gap test must contrast `column` + `superColumn` (reject) with `column` + `row` (accept).
  - Add the combined case `column = c("gender", "area")`, `superColumn = "educat4"`. Assert the arity message, and assert the errors do not claim `column` was empty.
  - Add trim tests: `" gender "` is accepted as `gender`; `"  "` is unfilled.
  - Rewrite `test-phase2-analysis-var-and-poverty-line.R:75-80` to pass `column = "pov_status"` instead of `by = "pov_status"`.
  - Confirm red for the three new rejection tests before Step 2 is considered done.
- **Test Scenarios**:
  - Happy: contiguous slots, trimmed value, empty request.
  - Edge: whitespace-only versus padded value.
  - Error: repeat, duplicate, gap, combined arity and later slot, missing poverty line.
- **Tests**: `testthat::test_file("tests/testthat/test-api-helpers.R")`; `testthat::test_file("tests/testthat/test-phase2-analysis-var-and-poverty-line.R")`
- **Acceptance criteria**: both files pass; no remaining `by =` argument is passed to `validate_table_input()` outside the `/description` fallback, which Step 4 removes.

## Phase 2: Endpoint, description fallback, contract tests, and docs

### 4. Update `/table` and migrate the `/description` fallback

- **Requirements**: R1, R2, R8, R14, R17
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**:
  - Replace `@param by:[character]` at `plumber.R:131` with four `#* @param <name>:character` lines. Use `:character`, not `:[character]`. State the fill order in the prose. Document that the annotation does not reject arrays; the handler does.
  - Replace `by = NULL` in the `/table` signature with the four slot arguments, each defaulting to `NULL`.
  - Pass those four arguments into `validate_table_input()`. Set `by <- check$by` next to the existing coerced values. Leave `table_maker(by = by, ...)` as a passthrough of the assembled vector.
  - Do not read `check$slots` in the handler.
  - In the `/description` fallback at `plumber.R:417-436`, stop reading `body$by`. Pass `body$column`, `body$row`, `body$superColumn`, and `body$superRow` into `validate_table_input()`, and pass `by = fcheck$by` into `table_maker()`. Using `body$by` after validation is a bug even if the names match.
  - Replace `"by"` in `validate_description_input()` at `helpers.R:553` with the four slot names, so a slot-only body still counts as the fallback path.
  - Update the fallback roxygen example at `plumber.R:333` to show `"column"` instead of `"by"`.
  - Grep `plumber.R` for `body$by` and for a `/table` `by` formal. Both must be gone.
- **Test Scenarios**:
  - Happy: slot query returns 200; description fallback with `column` still disaggregates.
  - Edge: all four absent on `/table` returns 200, not 500.
  - Error: a description fallback body that still sends only `by` is not treated as a table-parameter body.
- **Tests**: `tests/testthat/test-api-endpoints.R`; `tests/testthat/test-api-description.R`
- **Acceptance criteria**: `plumber::plumb()` succeeds; `body$by` is gone from `plumber.R`; absent slot params do not 500.

### 5. Assert OpenAPI, HTTP rejections, casing, and fixture equality

- **Requirements**: R2, R3, R4, R5, R8, R12, R13, R14, R16, R17
- **Files**: `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-description.R`
- **Details**:
  - Rewrite `test-api-endpoints.R:670` and `:1278` from `by` to the matching slot. `:1278` is the NR-masking case and becomes `column = "gender"`.
  - Compare each Step 1 fixture's `data` and `meta` to the slot-form response for the equivalent request. Parse JSON first. Do not compare raw body bytes. This is R16.
  - Add HTTP 400 tests for a repeated query param, a duplicate, and a gap. Build the repeat with `make_api_req(query = list(column = c("gender", "area")))`.
  - Add POST tests: `{"column": "gender"}` returns 200; `{"column": ["gender"]}` returns 200; `{"column": ["gender", "area"]}` returns 400 with the R3 envelope, never 500. If plumber rejects the array before the handler, stop. That is a blocked condition, not a passing test.
  - OpenAPI: for GET and POST `/table`, each of the four names is present, `"by"` is absent, `in` is `"query"`, and the schema is a string rather than an array. Do not treat a name-only check as done.
  - Add `?supercolumn=gender` with no other slots. It must return the national table, not a gender breakdown. That proves the formal did not bind.
  - Add a `/description` fallback test that sends `column` and still groups, and confirm no description test still requires `by` for disaggregation.
- **Test Scenarios**:
  - Happy: fixture equality; length-1 JSON array; description fallback grouping.
  - Edge: wrong-case query key; all slots omitted.
  - Error: repeated query, JSON array length 2, duplicate, gap.
- **Tests**: `testthat::test_file("tests/testthat/test-api-endpoints.R")`; `testthat::test_file("tests/testthat/test-api-description.R")`
- **Acceptance criteria**: fixture `data` and `meta` match; every rejection is 400; OpenAPI assertions cover name, `in`, and scalar schema for both methods.

### 6. Verify untouched surfaces

- **Requirements**: R11
- **Files**: none modified
- **Details**:
  - Run `devtools::test()`.
  - Confirm `git diff --stat R/` is empty and `DESCRIPTION` is unchanged.
  - Confirm `tests/testthat/test-api-table-download.R` passes with no edits.
  - Confirm `R/measures.R` still has the `length(by) > 4L` guard and that this plan did not edit it.
  - A non-empty `R/` diff is a stop, not a deviation.
- **Test Scenarios**:
  - Happy: full suite green and both diffs empty.
  - Edge: download tests green without modification.
  - Error: any `R/` diff or any `compute_*` failure stops the phase.
- **Tests**: `devtools::test()`; `git diff --stat R/ DESCRIPTION`
- **Acceptance criteria**: suite green; `R/` and `DESCRIPTION` unchanged; download file unchanged.

### 7. Update the API contract

- **Requirements**: R5, R13, R18
- **Files**: `docs/api-contract.md`
- **Details**:
  - Replace the `by` bullet with four slot bullets. Each is optional and holds one variable. State that query names are case-sensitive camelCase.
  - Add a layout-slot note: fill order; repeated slot is 400; duplicate variable is 400; a gap is 400 even though `column=gender&superRow=area` groups the same observations as `column=gender&row=area`. Say the rejection is a layout rule, not a computation failure.
  - Delete the sentence that more than four `by` entries are rejected server-side. Say the four slots are the API cap, and that `table_maker()` still rejects `length(by) > 4` for direct callers.
  - Change the POST example from `"by": ["gender"]` to `"column": "gender"`.
  - Change the `poverty_line` bullet so it no longer says `by` includes `pov_status`.
  - Do not document a `cells[]` or `layout` response. This plan does not change the response shape.
- **Test Scenarios**:
  - Happy: a reader can build a valid request from the section alone.
  - Edge: the gap example is present and explained.
  - Error: no leftover `by` bullet remains in the `/table` section.
- **Tests**: manual diff review against the implemented handler
- **Acceptance criteria**: the `/table` section has no stale `by` parameter, no unreachable fifth-dimension claim, and an explicit gap rationale.

## Testing Strategy

- Capture fixtures first, against the current `by` API, and compare parsed `data` and `meta` after the change.
- Unit-test classification, trim, gap, duplicate, and the arity-plus-later-slot case in `test-api-helpers.R` before touching the handler.
- HTTP tests must cover query repeats and POST arrays separately. A query-only R3 test is not enough.
- OpenAPI tests must assert `in` and scalar schema, not only names. This bug class has already shipped twice in this repo.
- Confirm red for the new rejection tests before writing the checks.
- Use `length(x) == 0L` and `expect_null()` for empty results. Do not expect `character(0)`.
- Guard router tests with `skip_if_not_installed("plumber")` and a null-router skip.

## Documentation Checklist

- [ ] Four slot params replace the `by` bullet in `docs/api-contract.md`
- [ ] Gap rule is explained with the contrasting `superRow` and `row` example
- [ ] Case-sensitive camelCase is stated
- [ ] The server-side "more than four dimensions" claim is removed
- [ ] POST example uses `"column"`
- [ ] `poverty_line` bullet no longer mentions `by`
- [ ] `/description` fallback roxygen no longer shows `"by"`
- [ ] Response-shape section is left unchanged

## Risks & Mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| 1 | A third contract reversal before UI sign-off | Medium | High | This change is behaviour-preserving and ships without the response reshape. Stop before Phase 2 if the UI rejects the four names. |
| 2 | Missing `NULL` defaults turn absence into 500 | Medium | High | R8 and Step 5 require 200 for an empty slot set and 400 for rejections. |
| 3 | Plumber rejects a JSON array before the handler | Medium | High | Step 5 tests it. A pre-handler 500 is a blocked stop, not a swallowed failure. |
| 4 | Wrong-case query silently returns a national table | Medium | Medium | Step 5 asserts `supercolumn` does not disaggregate. The contract states camelCase. |
| 5 | Description fallback keeps `body$by` and drops grouping | Medium | High | R17 and C7. Step 4 greps for `body$by`. Step 5 tests fallback grouping. |
| 6 | Scope creep into `layout` or `R/` | Medium | High | `$slots` is returned and not consumed. Any `R/` diff stops the work. |
| 7 | Fixture comparison is treated as a shape check | Medium | Medium | R16 names `data` and `meta`, captured before the change, for five listed requests including NR masking. |

## Out of Scope

- The `cells[]` + `layout` response reshape.
- Any edit under `R/`, including the 4-dimension cap.
- `/table/download`.
- Suppression behaviour and `include_metadata`.
- The dead registry `slot` lookup in `R/table_maker.R`.
- A `by` compatibility shim on `/table`.
- UI enforcement of fill order.
- Changing `/description` beyond the fallback's parameter names and the `table_maker(by = fcheck$by)` handoff.

## Completion Contract

### Outcome

`/table` accepts four named slot parameters in place of `by`, rejecting repeated, duplicate, and gapped assignments with HTTP 400, and returning `data` and `meta` that match fixtures captured before the change. `table_maker()` and everything under `R/` are unchanged. The `/description` fallback uses the same slot parameters and still disaggregates.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | `data` and `meta` match the five pre-change fixtures | `testthat::test_file("tests/testthat/test-api-endpoints.R")` | 2 | yes |
| V2 | OpenAPI GET and POST list four slot params, not `by`, each `in == "query"` and a scalar string | `router$getApiSpec()` assertion | 2 | yes |
| V3 | `?supercolumn=gender` does not disaggregate | `test-api-endpoints.R` | 2 | yes |
| V4 | Repeated query param and POST JSON array of length greater than 1 return 400, not 500 | `test-api-endpoints.R` | 2 | yes |
| V5 | POST JSON string and length-1 array are accepted | `test-api-endpoints.R` | 2 | yes |
| V6 | Duplicate variable across slots returns 400 | `test-api-helpers.R` and `test-api-endpoints.R` | 2 | yes |
| V7 | Gap returns 400, and an arity failure is not described as an empty slot | `test-api-helpers.R` | 1 | yes |
| V8 | All four omitted returns 200, not 500 | `test-api-endpoints.R` | 2 | yes |
| V9 | `$slots` has four keys; `$by` matches the specified expression; empty `$by` is `NULL` | `test-api-helpers.R` | 1 | yes |
| V10 | The return-shape test expects the new field set and passes | `test-api-helpers.R` | 1 | yes |
| V11 | `/description` fallback accepts slot params and still disaggregates | `test-api-description.R` | 2 | yes |
| V12 | Full suite green; `git diff --stat R/` empty; download tests unmodified | `devtools::test()` | 2 | yes |
| V13 | Contract doc has the gap rationale and no unreachable fifth-dimension claim | diff review | 2 | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | No file under `R/` changes | `git diff --stat R/` is empty | all |
| C2 | The four slot formals default to `NULL` | signature inspection; absence is 400 or 200, never 500 | 2 |
| C3 | No new dependencies | `DESCRIPTION` unchanged | all |
| C4 | `pov_status` cross-check uses assembled `by` | rewritten phase-2 test | 1 |
| C5 | `/table` has no `by` formal and no alias | grep handler and roxygen | 2 |
| C6 | `$by` and `$slots` cannot disagree | V9 expression | 1 |
| C7 | Description fallback does not read `body$by` | grep `plumber.R` | 2 |

### Boundaries

- Allowed: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `docs/api-contract.md`, `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-description.R`, `tests/testthat/test-phase2-analysis-var-and-poverty-line.R`, and new fixture files under `tests/testthat/fixtures/table-slot-baseline/`.
- Out of scope: anything under `R/`; the response reshape; `/table/download`; suppression; `include_metadata`; the dead registry slot lookup; a `by` shim; UI fill-order enforcement.

### Iteration Policy

1. Capture fixtures and grep callers before changing the signature.
2. A caller the inventory missed is a stop, not a silent edit.
3. Prove the validator in unit tests before changing the handler.
4. Confirm red for repeat, duplicate, and gap before writing those checks.
5. Stop on any `R/` diff or any `table_maker()` / `compute_*()` failure.
6. Write `docs/api-contract.md` from the implemented behaviour.
7. Do not add a `by` shim. Do not build a response `layout` block.

### Blocked-Stop Conditions

- A required change cannot be made without editing `R/`.
- Plumber returns 500 for an absent slot param even with a `NULL` default.
- Plumber rejects a JSON array before the handler, so R3 cannot return 400.
- Removing `by` breaks a caller the Step 1 grep did not find.
- The UI team rejects the four parameter names before Phase 2.
- A baseline fixture is not stable across two runs, so R16 cannot be asserted.
