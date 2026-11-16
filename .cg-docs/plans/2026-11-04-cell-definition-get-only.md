---
date: 2026-11-04
title: "Replace POST /cell-definition with GET /cell-definition"
status: completed
completed-date: 2026-11-04
completed-phases: [1, 2]
scope: "Standard"
brainstorm: null
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-11-04-cell-definition-get-only.md"
artifact-schema-version: 1
tags: [cell-definition, api, endpoint, table-maker, get, breaking-change]
---

# Plan: Replace POST /cell-definition with GET /cell-definition

## Objective

Replace the existing `POST /cell-definition` route with a `GET /cell-definition`
route that accepts the same Page 2 configuration fields as query parameters,
matching `GET /table`'s existing convention (repeatable `measures`, a single
JSON-encoded `filter_base` string, plain scalar slots/`ppp`/etc.). The POST
route is removed entirely; `GET /cell-definition` becomes the sole route for
this functionality.

## Context

`POST /cell-definition` (added in
`.cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md`, now
`status: completed`) returns a three-section natural-language explanation of
what each cell in a configured Page 2 table represents. It takes a JSON body
with `analysis_var`, `measures`, `poverty_line`, `column`, `row`,
`superColumn`, `superRow`, `filter_base`, `ppp`, `pop_share_threshold`,
`welfare_type`, and `release`.

`GET|POST /table` already implements the same parameter shape (plus
`pip_id`) as a `GET` route: Plumber typed query args
(`measures:[character]`, `filter_base:character` containing a JSON-encoded
string later parsed with `jsonlite::fromJSON()`) are bound directly to
`validate_table_input()`, which is a thin wrapper around the shared
`.validate_table_shape()` helper. There is no body-parsing step for `/table`
at all — Plumber does the binding.

`/cell-definition`'s underlying logic is already transport-agnostic:
- `build_cell_explanation()` (`R/cell_explanation.R`) is a pure function
  taking plain R scalars/vectors.
- `.validate_table_shape()` (`inst/plumber/helpers.R`) takes plain
  scalars/vectors, not a parsed JSON body.
- `validate_cell_definition_input(body)` is the only piece coupled to the
  POST body shape: it reads `body$analysis_var`, `body$measures`, etc. from
  a parsed JSON list, then calls `.validate_table_shape()` and adds
  `filter_base`/`welfare_type`/target-share-specific checks.

This means the swap to `GET` is a transport change, not a logic change: a
new query-parameter entry point needs to feed the same values into
`.validate_table_shape()` plus the same `filter_base`/`welfare_type`/target-
share checks currently in `validate_cell_definition_input()`, and the
builder and response-shape code do not change at all.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Add a query-parameter validator that accepts the same scalar/vector inputs `GET /table` accepts (plain `analysis_var`, repeatable `measures`, `column`/`row`/`superColumn`/`superRow`, `ppp`, `pop_share_threshold`, `poverty_line`) plus `filter_base` (JSON-encoded string), `welfare_type`, and `release`, producing an identical validation outcome (errors, coerced values) to today's `validate_cell_definition_input()` for equivalent inputs. Must reuse `.validate_table_shape()` — no duplicated shape-validation logic. | User request; Context |
| R2 | Add `GET /cell-definition` in `inst/plumber/plumber.R`, following `GET /table`'s exact pattern: Plumber-typed function arguments bound directly from the query string, `filter_base` parsed via `jsonlite::fromJSON()` after validation succeeds, then delegating to `piptm::build_cell_explanation()` with the same response assembly (`data`, `warnings`, `meta.release`) as the current POST handler. | User request |
| R3 | Remove the `POST /cell-definition` route and its manual body-parsing block (`req$postBody` / `req$rook.input` fallback, JSON parsing, array/empty-body checks) from `inst/plumber/plumber.R`. | User request |
| R4 | Preserve the response envelope and JSON schema exactly: `{status, data: {schema_version, who_is_in_table, what_a_cell_is, what_is_shown, notes}, warnings, errors, meta: {release}}`, with the same 400/422 HTTP semantics (400 for validation failures, 422 for unknown release or domain errors). | User request; Constraints |
| R5 | Update `tests/testthat/test-api-cell-definition.R` to exercise `GET /cell-definition` via `pr$call()` with query-string requests instead of JSON-body requests, covering the same cases the current POST tests cover: welfare/poverty/share happy paths with `filter_base`, fallback-warning path with no registry loaded, and all 400/422 error paths (empty/missing required fields, slot gap, duplicate slot variable, `analysis_var` reused in a slot, missing `poverty_line` for poverty measures, bad target-share/`analysis_var` combination, vector `pop_share_threshold`, unknown `release`). | User request |
| R6 | Update `docs/api-contract.md` and `SWAGGER_GUIDE.md` to document `GET /cell-definition` as a query-parameter endpoint (removing the POST body documentation, including the "UI Step Mapping" table and endpoint quick-reference table entries), consistent with how `/table` is documented. | User request |
| R7 | Sweep the repo for other current-state references to "POST /cell-definition" (README, any other docs) and update them to GET-only. Do not rewrite historical `.cg-docs/plans/` or `.cg-docs/work-reports/` entries — those remain historical record of past work. | User request |

## Implementation Steps

## Phase 1: Query-Parameter Validator

### 1. Add `validate_cell_definition_query()` in `inst/plumber/helpers.R`
- **Requirements**: R1
- **Files**: `inst/plumber/helpers.R`
- **Details**:
  - Add a new function `validate_cell_definition_query(analysis_var = NULL, measures = NULL, poverty_line = NULL, column = NULL, row = NULL, superColumn = NULL, superRow = NULL, filter_base = NULL, ppp = 2021L, pop_share_threshold = 0.001, welfare_type = NULL, release = NULL)` taking plain scalar/vector arguments (the shape Plumber will bind from query parameters), not a `body` list.
  - Internally, call `.validate_table_shape()` exactly as `validate_cell_definition_input()` does today, then apply the same additional checks currently in `validate_cell_definition_input()`: poverty-measure-requires-`poverty_line`, target-share-requires-binary-`analysis_var`, `filter_base` parsing/validation (JSON-encoded string only, since query-string values cannot be a nested object — validate it is parseable JSON naming known dimensions with non-empty code vectors), `welfare_type` (`"INC"`/`"CON"`, case-insensitive), and `release` (single non-empty string).
  - Reuse shared logic by extracting the body-independent parts of `validate_cell_definition_input()`'s post-`.validate_table_shape()` checks (poverty-line-for-poverty-measures, target-share-requires-binary-target, `filter_base` parsing, `welfare_type` normalization, `release` normalization) into small internal helpers callable from both the old and new validators if `validate_cell_definition_input()` is kept during this step, or directly replace `validate_cell_definition_input()`'s body entirely if Step 3 removes it in the same step (decide based on whether any other caller depends on the JSON-body entry point — grep for `validate_cell_definition_input(` call sites first).
  - Return the same named list shape as `validate_cell_definition_input()`: `valid`, `errors`, `analysis_var`, `measures`, `poverty_line`, `ppp`, `pop_share_threshold`, `by`, `slots`, `filter_base` (parsed), `welfare_type`, `release`.
- **Test Scenarios**:
  - Happy path: valid `analysis_var`/`measures`/slots/`filter_base` (JSON string) produces `valid = TRUE` with correctly parsed `filter_base` and coerced values.
  - Edge case: `filter_base` as a JSON-encoded string with multiple variables and multiple codes parses correctly.
  - Error path: malformed `filter_base` JSON string, unknown `filter_base` dimension, invalid `welfare_type`, target share measure with non-binary `analysis_var`, missing `poverty_line` for poverty measures, slot gap, duplicate slot variable — each produces the same error message text as today's `validate_cell_definition_input()` for the equivalent input.
- **Tests**: new test block in `tests/testthat/test-api-helpers.R`
- **Acceptance criteria**: new tests pass; for every error-triggering fixture currently used against `validate_cell_definition_input()`, the equivalent query-arg call to `validate_cell_definition_query()` produces an identical error message string.

## Phase 2: Route Swap, Tests, and Documentation

### 2. Add `GET /cell-definition` and remove `POST /cell-definition` in `inst/plumber/plumber.R`
- **Requirements**: R2, R3, R4
- **Files**: `inst/plumber/plumber.R`
- **Details**:
  - Add `#* @get /cell-definition` with Plumber-typed arguments mirroring `GET /table`'s doc-comment style: `analysis_var:character`, `measures:[character]`, `poverty_line:numeric`, `column:character`, `row:character`, `superColumn:character`, `superRow:character`, `filter_base:character` (JSON-encoded string, documented with the same example as `/table`'s), `ppp:integer` (default `2021`), `pop_share_threshold:numeric` (default `0.001`), `welfare_type:character`, `release:character`.
  - Call `validate_cell_definition_query(...)` with these bound arguments. On `!check$valid`, return `api_error(check$errors, 400L, res)`.
  - Inside `capture_with_warnings()`, resolve `release` the same way the POST handler does today (explicit release via `resolve_release()`, else `piptm::piptm_current_release()` with `tryCatch` fallback to `NULL`), then call `piptm::build_cell_explanation()` with the validated/coerced values — identical to the current POST handler's call.
  - Assemble the response identically to the current POST handler: `data = exp_res[c("schema_version", "who_is_in_table", "what_a_cell_is", "what_is_shown", "notes")]`, merged `warnings`, `meta = list(release = ...)` when resolved.
  - Remove the `#* @post /cell-definition` route entirely, including its manual body-parsing block (`req$postBody` / `req$rook.input$read_lines()` fallback, `jsonlite::fromJSON()` parsing, empty/array-body checks) and the `@parser json` annotation tied to it.
  - Update the file's top-of-file endpoint list comment (`# Endpoints ... POST /cell-definition`) to read `GET /cell-definition`.
- **Test Scenarios**: covered by Step 3's route tests.
- **Tests**: `tests/testthat/test-api-cell-definition.R` (Step 3)
- **Acceptance criteria**: `grep -c "@post /cell-definition" inst/plumber/plumber.R` returns 0; `grep -c "@get /cell-definition" inst/plumber/plumber.R` returns 1.

### 3. Update `tests/testthat/test-api-cell-definition.R` for the GET route
- **Requirements**: R5
- **Files**: `tests/testthat/test-api-cell-definition.R`
- **Details**:
  - Replace `make_cell_def_req()` (which builds a POST JSON-body mock request) with a query-string request builder: construct `req$QUERY_STRING` from a named list/vector of query parameters (repeating `measures=` for each measure, URL-encoding `filter_base`'s JSON string), set `req$REQUEST_METHOD <- "GET"`, and drop the body/`rook.input` plumbing entirely.
  - Port every existing test case from POST body construction to query-string construction, preserving exact coverage: welfare/poverty/share happy paths (including nested-object filter_base converted to its JSON-string form, and the already-JSON-string filter_base case), the no-registry fallback-warning case, and every 400/422 error case (empty query, vector `pop_share_threshold`, target share with continuous `analysis_var`, missing `analysis_var`/`measures`, slot gap, duplicate slot variable, `analysis_var` reused in a slot, missing `poverty_line` for poverty measures, target share with `analysis_var = "welfare"`, unknown `release`).
  - Keep `.get_cell_def_router()` and `.with_api_fixture_registry()` unchanged (transport-independent).
- **Test Scenarios**: all scenarios listed above, adapted to query-string form.
- **Tests**: `tests/testthat/test-api-cell-definition.R` itself
- **Acceptance criteria**: all tests pass against the new `GET /cell-definition` route with identical assertions (status codes, response fields) to the former POST-route tests.

### 4. Update API documentation and sweep remaining current-state references
- **Requirements**: R6, R7
- **Files**: `docs/api-contract.md`, `SWAGGER_GUIDE.md`, any other current (non-historical) docs referencing the POST route
- **Details**:
  - In `docs/api-contract.md`: change the `/cell-definition` section header to `### /cell-definition — GET (Step 2)`, rewrite the request description as query parameters (matching `/table`'s GET documentation style: parameter table with type/required/notes, `filter_base` documented as a JSON-encoded string example), update the "UI Step Mapping" table row and the endpoint quick-reference table to say `GET` instead of `POST`.
  - In `SWAGGER_GUIDE.md`: replace the `### POST /cell-definition` section with `### GET /cell-definition — Page 2 cell explanation (Step 2)`, with a query-string example (`?analysis_var=welfare&measures=mean&measures=median&measures=gini&column=gender&row=educat4&superColumn=area&filter_base=%7B%22age_group%22%3A%5B%223%22%5D%7D&ppp=2021&welfare_type=INC` or an unencoded display form matching `/table`'s example style), and update the "Key points" section's `filter_base` encoding-differences note (point 3) to remove `/cell-definition`'s now-obsolete note about accepting both a nested object and a string.
  - Grep the repo for other current-state (non-`.cg-docs/plans/`, non-`.cg-docs/work-reports/`) references to `POST /cell-definition` or `POST.*cell-definition` (e.g. README.md, any other docs/*.md) and update them to GET. Explicitly do not modify files under `.cg-docs/plans/` or `.cg-docs/work-reports/` — those are historical record.
- **Test Scenarios**: N/A (documentation).
- **Tests**: none (manual review); verified via the grep checks in Verification Surface V5.
- **Acceptance criteria**: `docs/api-contract.md` and `SWAGGER_GUIDE.md` contain no remaining `POST /cell-definition` references; both documents accurately describe the GET query-parameter contract.

## Testing Strategy

- **Validator unit tests** (`tests/testthat/test-api-helpers.R`): new `validate_cell_definition_query()` tests proving parity with the removed body-based validator's error messages and coerced values.
- **Route tests** (`tests/testthat/test-api-cell-definition.R`): full rewrite from POST-body to GET-query-string request construction, same coverage breadth as before.
- **Regression suite**: existing `/table` and `/description` API test files re-run unchanged to confirm no collateral impact from `inst/plumber/helpers.R` or `inst/plumber/plumber.R` edits.

## Documentation Checklist

- [ ] `docs/api-contract.md` — `/cell-definition` section rewritten as GET query parameters; UI Step Mapping and quick-reference tables updated
- [ ] `SWAGGER_GUIDE.md` — `/cell-definition` section rewritten as GET; "Key points" `filter_base` note updated
- [ ] Repo-wide grep sweep for stray current-state `POST /cell-definition` references (excluding `.cg-docs/plans/` and `.cg-docs/work-reports/`)
- [ ] `inst/plumber/plumber.R` top-of-file endpoint list comment updated

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Query-arg validator silently diverges from the removed body validator's rules (e.g. different error wording or missed check) | UI code or API consumers see different validation behavior than before | Step 1's acceptance criteria require identical error strings for the same logical inputs across the old and new validators, verified before removing the old one |
| `filter_base` URL-encoding in tests masks a real client-side encoding bug | Tests pass but real browser/fetch clients send malformed query strings | Route tests build the query string the same way a browser's `URLSearchParams` would (JSON-string value, then standard URL encoding), not a hand-crafted shortcut |
| Removing the POST route breaks an existing UI client mid-migration | API consumers calling POST get 404s with no warning | Out of scope per explicit user instruction (GET-only end state); flag in the handoff summary that this is a breaking change requiring coordinated UI update |
| Repo-wide reference sweep accidentally touches historical plan/work-report content | Loses historical accuracy of past work records | Explicit file-permission boundary: only `docs/`, `SWAGGER_GUIDE.md`, `README.md`, and other current-state docs are in scope; `.cg-docs/plans/` and `.cg-docs/work-reports/` are explicitly excluded |

## Out of Scope

- Changing `/table`'s or `/description`'s behavior, route, or documentation.
- Changing `build_cell_explanation()`'s logic, prose, or response schema.
- Rewriting historical `.cg-docs/plans/` or `.cg-docs/work-reports/` entries.
- Adding new response fields, a `format=text`/`format=markdown` rendering mode, or any other feature beyond the GET/POST transport swap.
- Coordinating or updating any external/PIP-platform UI client code that currently calls `POST /cell-definition`.

## Completion Contract

### Outcome
`GET /cell-definition` exists as the sole route for Page 2 cell explanations, accepting query parameters in the same style as `GET /table` (repeatable `measures`, JSON-string `filter_base`, plain scalar slots/ppp/etc.). `POST /cell-definition` is removed. All existing explanation semantics, response envelope shape, and validation rules are unchanged; only the transport changes.

### Verification Surface
| ID | Phase | Evidence Required | Command/Artifact | Required |
|----|-------|-------------------|------------------|----------|
| V1 | 1 | New query-arg validator reuses `.validate_table_shape()` with identical validation rules/messages to the removed `validate_cell_definition_input()`; unit tests pass | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-helpers.R')"` | yes |
| V2 | 2 | `GET /cell-definition` route tests pass (200/400/422), covering all cases the former POST tests covered (filter_base happy path, validation errors, unknown release, fallback-warning path) | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-cell-definition.R')"` | yes |
| V3 | 2 | `POST /cell-definition` route no longer exists in `inst/plumber/plumber.R` | `Select-String -Path inst/plumber/plumber.R -Pattern '@post /cell-definition'` returns no match | yes |
| V4 | final | Existing `/table` and `/description` tests continue to pass without regression | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-description.R')"` plus the `/table` API test file(s) | yes |
| V5 | final | `docs/api-contract.md` and `SWAGGER_GUIDE.md` document only `GET /cell-definition`; no remaining `POST /cell-definition` references in either file | `Select-String -Path docs/api-contract.md,SWAGGER_GUIDE.md -Pattern 'POST.*cell-definition'` returns no match | yes |

### Constraints
| ID | Phase | Constraint | Check |
|----|-------|------------|-------|
| C1 | 1 | `build_cell_explanation()` and `.validate_table_shape()` logic must not change | `git diff R/cell_explanation.R` shows no logic changes beyond doc comments (if any) |
| C2 | 2 | Response envelope shape (`schema_version`, `who_is_in_table`, `what_a_cell_is`, `what_is_shown`, `notes`, `warnings`, `meta.release`) must be byte-identical to the current POST route's output for equivalent inputs | Route test asserts the full field list on a representative request |
| C3 | 2 | No validation rule changes — same required fields, same error messages, same 400/422 conditions | Route tests reuse the exact error-triggering fixtures from the former POST test file |
| C4 | final | `/table` and `/description` endpoints unaffected | V4 passes |

### Boundaries
- **Allowed**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `tests/testthat/test-api-cell-definition.R`, `tests/testthat/test-api-helpers.R` (additive new-validator tests only), `docs/api-contract.md`, `SWAGGER_GUIDE.md`, and current-state doc references (e.g. `README.md`) to the route.
- **Out of scope**: `/table`, `/description`, `R/cell_explanation.R` logic, response schema changes, historical `.cg-docs/plans/` and `.cg-docs/work-reports/` content, any new feature beyond the transport swap.

### Iteration Policy
1. Implement Phase 1 (query-parameter validator) and verify V1 before starting Phase 2.
2. Implement Phase 2 (route swap, tests, docs) and verify V2–V5.
3. Follow `deviation-policy: ask` if the query-arg validator cannot cleanly reuse `.validate_table_shape()` without duplicating logic, or if achieving byte-identical response-shape parity (C2) would require changing `build_cell_explanation()`.

### Blocked-Stop Conditions
- Required verification cannot be run through `Rscript -e testthat::test_file(...)`.
- Any required evidence item (V1–V5) fails after allowed recovery attempts.
- Byte-identical response-shape parity (C2) cannot be achieved without changing `build_cell_explanation()`.
- A required deviation is discovered under policy `ask` and user approval is unavailable.
- `/cg-work` cannot durably create or update the execution report.
