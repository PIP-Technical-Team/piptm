---
date: 2026-10-22
title: "/table explicit slot parameters replacing ordered by"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-10-22-table-endpoint-explicit-slot-parameters.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
tags: [api, plumber, table-endpoint, request-contract, layout-slots, validation, openapi, breaking-change]
---

# Plan: `/table` Explicit Slot Parameters Replacing Ordered `by`

## Objective

Replace the single repeatable `by` parameter on `/table` with four explicit,
named slot parameters - `column`, `row`, `superColumn`, `superRow` - each
accepting exactly one variable name. Assembly back into an ordered `by` vector
happens in `validate_table_input()` at the API boundary.

This change is **behaviour-preserving end to end**: the computation, the
response payload, and the response bytes are identical for equivalent requests.
Only the way a request is *expressed* changes.

## Context

### What this plan is, and is not

There are three distinct bodies of work in this area. This plan covers **only
the first**:

| # | Work | In this plan? |
|---|------|---------------|
| 1 | Request contract: four named slot params replace `by` | **Yes - this entire plan** |
| 2 | Response reshape: `cells[]` + `layout` (Approach C) | No - roadmap feature `table-response-format-layout-slot-assignment` |
| 3 | Changes to `table_maker()` or anything under `R/` | No - **zero lines change**, by design |

**On #3**: the brainstorm chose the thin-API-layer boundary (Option (a)).
`table_maker()` keeps its `by =` argument unchanged. Constraint C1 makes this
enforceable: `git diff --stat R/` must be empty when the work is done.

**On #2**: the Approach C response format is decided but **not built**.
`/table` currently returns flat rows via `piptm::format_table_output()`
(`inst/plumber/plumber.R:198`). Folding the reshape into this plan would block
it behind UI sign-off of the response mocks - still outstanding, and the single
largest revision risk identified in the brainstorm. Keeping the request change
separate makes it independently shippable and independently verifiable.

**The seam between #1 and #2**: `validate_table_input()` will return a `$slots`
named list alongside `$by`. Nothing consumes `$slots` in this plan - it is
built and unit-tested now so the response-format work can pick it up with zero
rework.

### Current implementation state (audited)

| Location | Current state |
|---|---|
| `inst/plumber/plumber.R:131` | `@param by:[character] Disaggregation dimensions (optional, repeatable).` |
| `inst/plumber/plumber.R:145-147` | Handler signature includes `by = NULL` |
| `inst/plumber/plumber.R:149-157` | `validate_table_input(...)` call passes `by = by` |
| `inst/plumber/plumber.R:159-161` | Handler destructures `check$poverty_line`, `check$ppp`, `check$pop_share_threshold` |
| `inst/plumber/plumber.R:180` | `table_maker(by = by, ...)` passthrough |
| `inst/plumber/helpers.R:268-270` | `validate_table_input()` signature, includes `by = NULL` |
| `inst/plumber/helpers.R:365-382` | `by` dimension-membership check against `pip_valid_dimensions()` |
| `inst/plumber/helpers.R:385-391` | Cross-validation: `poverty_line` required when `pov_status` in `by` |
| `inst/plumber/helpers.R:507-513` | Return list: `valid`, `errors`, `poverty_line`, `ppp`, `pop_share_threshold` |
| `R/measures.R:180` | `length(by) > 4L` cap via `cli_abort()` - **left untouched** as a defensive guard |
| `docs/api-contract.md` (~line 161) | `by` bullet in the `/table` parameter list |
| `docs/api-contract.md` (~line 190) | `POST /table` JSON example using `"by": ["gender"]` |

### Test call sites using `by` (full inventory)

Only four relevant sites exist - the blast radius is small:

- `tests/testthat/test-api-helpers.R:203` - `by = c("gender", "not_a_dim")`
- `tests/testthat/test-api-helpers.R:213` - `by = c("gender", "area")`
- `tests/testthat/test-api-endpoints.R:670` - `by = "not_a_valid_dimension"`
- `tests/testthat/test-api-endpoints.R:1278` - `by = "gender"`

(`test-api-endpoints.R:129` is `seq(..., by = 5L)` - unrelated.)

### Prior learnings applied

- **Handler-validated plumber params must have `NULL` defaults.** Plumber
  resolves parameters *before* invoking the handler; a param without a default
  that is absent from the request raises a 500 that bypasses
  `validate_table_input()` entirely. All four slot params therefore need
  `= NULL`. - source: `.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md`
- **`@param` annotations drive the OpenAPI spec, and errors there are
  test-invisible.** This bug class has recurred twice in this repository
  (`/description`, then `/table/download`). Since Swagger-visible explicitness
  is a stated goal of this change, the generated spec must be asserted via
  `router$getApiSpec()`, not assumed. - sources:
  `.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md`,
  `.cg-docs/solutions/bugs/2026-10-16-table-download-serializer-and-swagger-body.md`
- **Test request builder already supports repeated query params.**
  `make_api_req()` builds `?k=v1&k=v2` from `list(k = c("v1","v2"))`, which is
  exactly what R5 (repeated-param rejection) needs. - source: same testing-patterns solution
- **jsonlite assertion traps.** Use `length(x) == 0L`, never
  `expect_equal(x, character(0L))`; JSON `null` drops the key entirely. - source: same

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `/table` accepts `column`, `row`, `superColumn`, `superRow`, each a single variable name, all optional | Brainstorm: Inputs |
| R2 | `by` is removed entirely from `/table` - no alias, no deprecation shim, no dual-path input | Brainstorm: Decision (Approach C rejected) |
| R3 | A slot receiving more than one value returns HTTP 400 | Brainstorm: Edge case 3 |
| R4 | The same variable in two different slots returns HTTP 400 | Brainstorm: Edge case 1 |
| R5 | A gapped assignment (a slot filled while an earlier slot is empty) returns HTTP 400 | Brainstorm: Edge case 2 |
| R6 | Slot fill-order is `column` -> `row` -> `superColumn` -> `superRow` | Brainstorm: Edge case 2 |
| R7 | `validate_table_input()` returns `$by` (ordered character vector) and `$slots` (named list), derived from one structure | Brainstorm: Internal boundary |
| R8 | Omitting all four slots yields the national-level single-cell table, as `by = NULL` does today | Brainstorm: Inputs |
| R9 | Dimension-membership validation operates on the assembled vector | Brainstorm: Next Step 3 |
| R10 | `pov_status` / `poverty_line` cross-validation continues to work against the assembled vector | Brainstorm: Edge case 5 |
| R11 | `table_maker()`'s signature and all files under `R/` are unchanged | Brainstorm: Internal boundary, Option (a) |
| R12 | The 4-dimension cap at `R/measures.R:180` is left in place as a defensive guard | Brainstorm: Edge case 4 |
| R13 | The generated OpenAPI spec exposes the four slot params and no longer exposes `by` | Learning: swagger param bugs |
| R14 | `docs/api-contract.md` documents the four params and explicitly explains why a gapped request is rejected despite being computationally valid | Brainstorm: Documentation risk |
| R15 | All four slot params are declared with `= NULL` defaults | Learning: plumber NULL-default rule |
| R16 | `/table` responses are identical to pre-change behaviour for equivalent requests | Plan objective |

## Implementation Steps

## Phase 1: Validation and assembly

### 1. Add slot assembly and validation to `validate_table_input()`

- **Requirements**: R1, R3, R4, R5, R6, R7, R9, R10
- **Files**: `inst/plumber/helpers.R`
- **Details**:
  - Change the signature at `helpers.R:268-270`: remove `by = NULL`, add
    `column = NULL, row = NULL, superColumn = NULL, superRow = NULL`.
  - Add a slot-validation block, replacing the `by` block at `helpers.R:365-382`.
    Order of checks matters - run per-slot arity first so later checks operate
    on scalars. Apply the six checks in this exact order:
  - Check 1, **Arity (R3)** - for each of the four slots, if `length(slot) > 1L`, append `` "`<name>` accepts a single variable; <n> values were supplied." `` Plumber delivers repeated query params as a character vector, which is why this is a length check rather than a type check.
  - Check 2, **Normalize** - treat `NULL`, `NA`, and empty/whitespace-only strings as unfilled, then build the ordered named list with `slots <- list(column = column, row = row, superColumn = superColumn, superRow = superRow)` followed by `slots <- Filter(function(x) !is.null(x) && length(x) == 1L && !is.na(x) && nzchar(trimws(x)), slots)`. The literal declaration order of this list **is** the slot fill-order (R6) and the `by` order - do not sort it.
  - Check 3, **Gap (R5)** - the filled slot names must be a prefix of `c("column", "row", "superColumn", "superRow")`. If not, append an error naming the empty earlier slot and the filled later slot, and state the required fill order.
  - Check 4, **Duplicate (R4)** - if `anyDuplicated(unlist(slots)) > 0L`, append an error naming the repeated variable and the slots holding it.
  - Check 5, **Assemble** - `by <- unlist(slots, use.names = FALSE)`; when `slots` is empty this yields `NULL`, satisfying R8 without a special case.
  - Check 6, **Membership (R9)** - run the existing dimension check (relocated from `helpers.R:366-382`) against the assembled `by`, keeping the `setdiff(by, "pov_status")` carve-out and the existing message format.
  - Leave the `pov_status` / `poverty_line` cross-validation at
    `helpers.R:385-391` operating on the assembled `by` (R10). Verify the
    variable it reads is the assembled vector, not a stale argument.
  - Extend the return list at `helpers.R:507-513` with `by = by` and
    `slots = slots` (R7). Both derive from the single `slots` structure, so
    they cannot drift.
  - **Error accumulation**: follow the established pattern - append to
    `errors` and continue, so a caller sees every problem at once rather than
    one per round-trip.
- **Test Scenarios**:
  - Happy: all four filled; one filled; none filled; two filled.
  - Edge: `NULL` vs `NA` vs `""` vs `"  "` all treated as unfilled; slot values
    with surrounding whitespace.
  - Error: two values in one slot; duplicate variable across slots; gap; unknown
    dimension; `pov_status` in a slot without `poverty_line`; several of these
    at once (all errors reported together).
- **Tests**: `tests/testthat/test-api-helpers.R`
- **Acceptance criteria**: `validate_table_input()` returns `$by` and `$slots`
  that are consistent for every valid input; each of R3/R4/R5 produces a
  non-empty `$errors` and `valid = FALSE`; no call reaches `table_maker()`.

### 2. Unit-test the validator in isolation

- **Requirements**: R3, R4, R5, R6, R7, R8, R9, R10
- **Files**: `tests/testthat/test-api-helpers.R`
- **Details**:
  - Rewrite the two existing `by =` call sites (`:203`, `:213`) to the new
    slot-param form, preserving their original intent (unknown dimension;
    valid two-dimension request).
  - Add a consistency assertion used across cases:
    `expect_identical(unname(unlist(result$slots)), result$by)` and
    `expect_identical(names(result$slots), <expected ordered slot names>)`.
  - Add explicit rejection tests for R3, R4, and R5, asserting `valid` is
    `FALSE` and that the message names the offending slot(s).
  - Add an all-empty test asserting `is.null(result$by)` and
    `length(result$slots) == 0L` with `valid = TRUE` (R8).
  - Add a fill-order test: `column` + `superColumn` (gap) rejected, while
    `column` + `row` (contiguous) accepted - the two have identical
    computational meaning, which is precisely the documented asymmetry.
  - Use `length(x) == 0L` rather than `expect_equal(x, character(0L))` per the
    jsonlite learning.
- **Test Scenarios**: as enumerated in Step 1.
- **Tests**: `testthat::test_file("tests/testthat/test-api-helpers.R")`
- **Acceptance criteria**: file passes; every one of R3-R10 has at least one
  dedicated assertion; red-phase confirmed for the three rejection rules before
  Step 1 is finalized.

## Phase 2: Endpoint, documentation, and integration tests

### 3. Update the `/table` handler signature and roxygen

- **Requirements**: R1, R2, R13, R15
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - Replace the `@param by` line at `plumber.R:131` with four annotations.
    Use `:character` (scalar), **not** `:[character]` - the bracketed form
    declares a repeatable array and would advertise exactly the behaviour R3
    rejects:
    ```
    #* @param column:character Variable placed in the Columns slot (optional).
    #* @param row:character Variable placed in the Rows slot (optional).
    #* @param superColumn:character Variable placed in the Super Columns slot (optional).
    #* @param superRow:character Variable placed in the Super Rows slot (optional).
    ```
    Document the fill-order rule in the roxygen prose as well, since Swagger
    renders it.
  - In the handler signature (`plumber.R:145-147`), replace `by = NULL` with
    `column = NULL, row = NULL, superColumn = NULL, superRow = NULL`. The
    `NULL` defaults are mandatory (R15) - without them plumber returns 500
    before the handler runs.
  - Pass the four params to `validate_table_input()` (`plumber.R:149-157`).
  - Destructure `by <- check$by` alongside the existing coerced values at
    `plumber.R:159-161`.
  - Leave the `table_maker(by = by, ...)` passthrough at `plumber.R:180`
    **unchanged** - it now receives the assembled vector.
  - Do **not** reference `check$slots` in this plan; it is produced for the
    later response-format work only.
  - Verify no other `/table` reference to `by` survives (R2).
- **Test Scenarios**:
  - Happy: equivalent slot request returns 200 with the same payload as the old
    `by` form.
  - Edge: all four params absent -> 200 national-level table (not 500).
  - Error: each rejection rule returns 400 with a JSON error envelope.
- **Tests**: `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: `plumber::plumb()` loads the router without error;
  absent params produce 400s from the validator, never a 500.

### 4. Assert the OpenAPI contract

- **Requirements**: R2, R13
- **Files**: `tests/testthat/test-api-endpoints.R`
- **Details**:
  - Follow the established spec-inspection pattern: obtain
    `router$getApiSpec()`, navigate to `spec$paths[["/table"]][["get"]]`, and
    extract parameter names via
    `vapply(params, function(p) p$name %||% "", character(1))`.
  - Assert all four slot names are present (R13) and that `"by"` is absent (R2).
  - Repeat for the `post` operation on the same path.
  - This exists because the two prior occurrences of this bug class were
    invisible to runtime tests - the endpoint worked while Swagger advertised
    the wrong contract. Since the entire justification for this change is
    request explicitness, an unasserted spec would undermine its purpose.
- **Test Scenarios**: happy (four present, `by` absent); guard with
  `skip_if_not_installed("plumber")` and a null-router skip.
- **Tests**: `testthat::test_file("tests/testthat/test-api-endpoints.R")`
- **Acceptance criteria**: spec assertions pass for both GET and POST; a
  deliberate reintroduction of `@param by` fails the test.

### 5. Update endpoint integration tests

- **Requirements**: R1, R3, R4, R5, R8, R16
- **Files**: `tests/testthat/test-api-endpoints.R`
- **Details**:
  - Rewrite the two existing `by =` call sites (`:670`, `:1278`) to the slot
    form, preserving intent.
  - Add a **behaviour-preservation** test for R16: issue a slot-form request and
    assert the response body matches the documented pre-change shape for the
    same computation (same rows, same measure values, same `meta`). This is the
    central claim of the plan and must be asserted, not assumed.
  - Add 400 tests for R3, R4, and R5 through the HTTP layer, confirming the
    rejections surface as proper JSON error envelopes rather than 500s.
  - For R3, build the request with `make_api_req(query = list(column = c("gender","area")))`,
    which serializes to `?column=gender&column=area`.
  - Add an all-slots-absent test asserting 200 and a single national-level cell
    per measure (R8).
- **Test Scenarios**: happy (equivalence, national-level); error (R3, R4, R5);
  edge (whitespace-only slot value treated as unfilled).
- **Tests**: `testthat::test_file("tests/testthat/test-api-endpoints.R")`
- **Acceptance criteria**: all new and rewritten tests pass; no test still
  passes `by =` to `/table`.

### 6. Verify the untouched surfaces

- **Requirements**: R11, R12
- **Files**: none modified - verification only
- **Details**:
  - Run the full suite: `devtools::test()`.
  - Assert `git diff --stat R/` is empty (C1). A non-empty diff means the
    thin-API-layer boundary was violated and is a stop condition, not a
    deviation to accept.
  - Confirm `tests/testthat/test-api-table-download.R` passes **unmodified**
    (V10) - `/table/download` takes no slot params and must be unaffected.
  - Confirm `R/measures.R:180` is unchanged (R12). It is now unreachable from
    the API but remains valid for direct `table_maker()` callers.
  - Confirm `DESCRIPTION` is unchanged (C3).
- **Test Scenarios**: full-suite green; diff checks empty.
- **Tests**: `devtools::test()`; `git diff --stat R/ DESCRIPTION`
- **Acceptance criteria**: full suite green; `R/` and `DESCRIPTION` diffs empty;
  download tests green without modification.

### 7. Update `docs/api-contract.md`

- **Requirements**: R14
- **Files**: `docs/api-contract.md`
- **Details**:
  - Replace the `by` bullet (~line 161) with four bullets, one per slot
    parameter, each noting single-value and optional.
  - Add an explicit **Layout slot rules** note covering:
    - the fill-order `column` -> `row` -> `superColumn` -> `superRow`;
    - that repeating a slot parameter is a 400;
    - that the same variable in two slots is a 400;
    - **why a gapped request is rejected even though it is computationally
      valid.** State plainly that `?column=gender&superRow=area` returns 400
      while `?column=gender&row=area` succeeds, that the two describe the same
      grouping, and that the rejection is a deliberate layout constraint
      preventing degenerate renderings - not a validation defect. Without this,
      the asymmetry reads as a bug to anyone reaching the API without UI
      context.
  - Update the `POST /table` JSON example (~line 190) from `"by": ["gender"]`
    to `"column": "gender"`.
  - Update the `poverty_line` bullet, which currently reads "or `by` includes
    `pov_status`", to refer to the slot parameters.
  - Leave the `/table` response-shape documentation unchanged - the response is
    not modified by this plan.
- **Test Scenarios**: not applicable - documentation.
- **Tests**: manual diff review against implemented behaviour.
- **Acceptance criteria**: no stale `by` reference remains in the `/table`
  section; the gap-rule rationale is present and unambiguous.

## Testing Strategy

- **Unit (`test-api-helpers.R`)**: `validate_table_input()` in isolation -
  arity, normalization, gap, duplicate, membership, cross-validation, and
  `$by`/`$slots` consistency. Fastest feedback; most rules land here.
- **Integration (`test-api-endpoints.R`)**: HTTP-layer behaviour via
  `pr$call()` with `make_api_req()` - status codes, error envelopes, and the
  behaviour-preservation equivalence check.
- **Contract (`test-api-endpoints.R`)**: OpenAPI spec inspection via
  `router$getApiSpec()`, asserting the four params exist and `by` does not.
- **Regression**: the full suite must pass with zero changes under `R/`.
- **Red-phase discipline**: for the three rejection rules (R3, R4, R5), confirm
  the test fails before the validation is written. These are the plan's only
  genuinely new behaviours.
- **Assertion conventions**: `length(x) == 0L` for empty vectors;
  `is.null(x) || length(x) == 0L` for absent data; guard every router test with
  `skip_if_not_installed("plumber")` and a null-router skip.

## Documentation Checklist

- [ ] `docs/api-contract.md`: four slot params replace the `by` bullet
- [ ] `docs/api-contract.md`: Layout slot rules note, including the gap-rule rationale
- [ ] `docs/api-contract.md`: `POST /table` example uses slot params
- [ ] `docs/api-contract.md`: `poverty_line` bullet no longer references `by`
- [ ] `plumber.R` roxygen: four `@param` annotations with scalar types
- [ ] `plumber.R` roxygen: fill-order rule stated in prose (Swagger-visible)
- [ ] No stale `by` reference remains in the `/table` surface

## Risks & Mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|------|-----------|--------|------------|
| 1 | **A third contract revision.** This is the second reversal of the same decision in three days, and UI sign-off on the response mocks is still outstanding. | Medium | High | The request change is behaviour-preserving and independently shippable, so it is not invalidated by response-format feedback. Obtain UI sign-off on request *and* response shape together before starting the response-format work. |
| 2 | **Missing `NULL` defaults produce 500s instead of 400s**, bypassing validation entirely. | Medium | High | R15 and C2 make the defaults explicit; Step 5 asserts the all-absent case returns 200 and each rejection returns 400, which would catch a 500. Documented prior incident. |
| 3 | **OpenAPI spec advertises the wrong contract.** This bug class has already occurred twice here and is invisible to runtime tests. | Medium | Medium | Step 4 asserts the spec directly. Using `:character` rather than `:[character]` avoids advertising repeatability that R3 rejects. |
| 4 | **Scope creep into the response format.** `$slots` exists and the pull toward building `layout` will be strong. | Medium | Medium | `$slots` has no consumer in this plan by instruction; boundaries list the response reshape as out of scope; it is a separately tracked roadmap feature with its own blockers. |
| 5 | **Silent violation of the `R/` boundary** - a change that "just needs one line" in `table_maker()`. | Low | High | C1 is mechanically checkable (`git diff --stat R/`) and Step 6 enforces it. A non-empty diff is a blocked-stop condition. |
| 6 | **The gap rule reads as a bug** to API consumers outside the UI context, since two computationally identical requests differ in outcome. | Medium | Low | R14 requires the rationale be documented explicitly, with the concrete contrasting example. |
| 7 | **Slot normalization is under-specified** - empty-string and whitespace query params are common from form encoders and could be read as a filled slot, triggering spurious gap or membership errors. | Medium | Medium | Step 1 normalizes `NULL`/`NA`/`""`/whitespace to unfilled before any other check; Step 2 tests each form. |

## Out of Scope

- The Approach C response reshape (`cells[]` + `layout`) - roadmap feature
  `table-response-format-layout-slot-assignment`, blocked on UI sign-off.
- Any change under `R/`, including `table_maker()`'s signature and the
  4-dimension cap at `R/measures.R:180`.
- `/table/download` - it accepts no slot parameters. Its flat-rows gap is
  roadmap feature `table-download-has-no-flat-rows-under-cells-response-format`.
- Suppression semantics / `pop_share_threshold` behaviour.
- `include_metadata` - still treated as always `false`; the metadata branch is
  not exercised or modified.
- The dead registry-driven `slot` lookup at `R/table_maker.R:293-327`.
- Any `by` compatibility shim, alias, or deprecation window.
- UI-side enforcement of fill-order in the four boxes.

## Completion Contract

### Outcome

`/table` accepts four named slot parameters (`column`, `row`, `superColumn`,
`superRow`) in place of the repeatable `by` parameter, rejecting repeated,
duplicate, and gapped slot assignments with HTTP 400. `table_maker()` and the
computation engine are unchanged, and `/table` responses for equivalent
requests are identical to pre-change behaviour.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | Four slot params accepted; equivalent request returns a response identical to the old `by` form | `testthat::test_file("tests/testthat/test-api-endpoints.R")` | 2 | yes |
| V2 | `by` is no longer an accepted parameter on `/table` | `router$getApiSpec()` assertion, GET and POST | 2 | yes |
| V3 | All four slots appear in the generated OpenAPI spec as named params | `router$getApiSpec()` assertion, GET and POST | 2 | yes |
| V4 | Repeated param per slot -> 400 | `test-api-helpers.R` + `test-api-endpoints.R` | 2 | yes |
| V5 | Duplicate variable across slots -> 400 | `test-api-helpers.R` + `test-api-endpoints.R` | 2 | yes |
| V6 | Gapped assignment -> 400 | `test-api-helpers.R` + `test-api-endpoints.R` | 2 | yes |
| V7 | All four omitted -> national-level table (200, not 500) | `test-api-endpoints.R` | 2 | yes |
| V8 | `validate_table_input()` returns consistent `$by` and `$slots` | `testthat::test_file("tests/testthat/test-api-helpers.R")` | 1 | yes |
| V9 | `table_maker()` / `compute_*()` suites green and `R/` unmodified | `devtools::test()`; `git diff --stat R/` empty | 2 | yes |
| V10 | `/table/download` suite green unmodified | `testthat::test_file("tests/testthat/test-api-table-download.R")` | 2 | yes |
| V11 | `docs/api-contract.md` documents the four params and explains the gap rule | file diff review | 2 | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | No change to `table_maker()`'s signature or any file under `R/` | `git diff --stat R/` is empty | all |
| C2 | All four slot params declared with `= NULL` defaults | handler signature inspection; absence must yield 400, never 500 | 2 |
| C3 | No new package dependencies | `DESCRIPTION` unchanged | all |
| C4 | `pov_status` / `poverty_line` cross-validation works against assembled `by` | existing test green | 1 |
| C5 | `by` fully removed, not aliased - no dual-path input | grep the `/table` handler and roxygen for `by` | 2 |
| C6 | `$by` and `$slots` derive from one structure and cannot disagree | `expect_identical(unname(unlist(slots)), by)` | 1 |

### Boundaries

- **Allowed**: `inst/plumber/plumber.R` (`/table` handler and its roxygen),
  `inst/plumber/helpers.R` (`validate_table_input()`), `docs/api-contract.md`,
  `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`.
- **Out of scope**: anything under `R/`; the Approach C response format;
  `/table/download`; suppression semantics; `include_metadata`; the dead
  registry `slot` lookup; the `/table/download` flat-rows gap; UI-side
  fill-order enforcement.

### Iteration Policy

1. Implement validation and assembly in `helpers.R` first; prove `$by`/`$slots`
   correctness with unit tests before touching the handler.
2. Confirm the red phase for R3, R4, and R5 before writing their validation.
3. Change the handler signature and roxygen only after Phase 1 is green.
4. On any failure in the `table_maker()` / `compute_*()` suites, stop - that
   signals C1 was violated.
5. Update `docs/api-contract.md` last, from the implemented behaviour rather
   than from this plan's prose.
6. Do not add a `by` compatibility shim under any circumstance - Approach C of
   the brainstorm was explicitly rejected.
7. Do not build or consume a response `layout` block; `$slots` is produced for
   a later plan and has no consumer here.

### Blocked-Stop Conditions

- A required change cannot be made without editing a file under `R/` - stop and
  report; the (a)-vs-(b) boundary decision needs revisiting.
- Plumber cannot express four optional scalar query parameters without
  returning 500 on absence, even with `NULL` defaults.
- Removing `by` breaks a consumer not identified in the brainstorm.
- Producing `$slots` turns out to require response-format work - stop; that is
  the deferred plan.
- The UI team rejects the four-parameter request shape during sign-off - stop
  before Phase 2 rather than implementing a contract that is about to change.
