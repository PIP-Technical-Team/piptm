---
date: 2026-11-04
title: "Remove Obsolete Session Survey API Endpoints and Documentation"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-07-31-session-storage-endpoints.md"
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
tags: [api, plumber, session, cleanup, breaking-change, documentation]
---

# Plan: Remove Obsolete Session Survey API Endpoints and Documentation

## Objective

Remove the obsolete `POST /session/surveys` and `GET /session/<id>/surveys` endpoints from the `piptm` Plumber API, purge all unused session validation and in-memory storage helpers, update unit tests to assert route absence (HTTP 404), and remove all references from package documentation, API contracts, guides, and the Insomnia collection.

## Context

The session storage endpoints were originally drafted in July 2026 (`.cg-docs/brainstorms/2026-07-31-session-storage-endpoints.md`) as an in-memory mechanism to persist Step 1 survey selections (`pip_id[]`) across page reloads. As clarified in later UI architecture decisions (`.cg-docs/brainstorms/2026-07-31-survey-specific-variable-filtering.md`), the UI stores survey selections client-side (`localStorage` / `sessionStorage`), making server-side session persistence redundant and unused.

Currently, the endpoints introduce unnecessary process-local state (`.SESSION_STORE`, `.SESSION_TTL_SECONDS`), dead helper code, and outdated documentation. Getting rid of them keeps the API purely stateless and simplifies maintenance.

## Requirements

| ID | Requirement | Source |
|---|---|---|
| R1 | Remove `POST /session/surveys` and `GET /session/<id>/surveys` routes from `inst/plumber/plumber.R` | User request |
| R2 | Remove session validation, allocation, retrieval helpers, and in-memory environment variables from `inst/plumber/helpers.R` | User request & code hygiene |
| R3 | Update `tests/testthat/test-api-endpoints.R` by replacing obsolete session creation/retrieval tests with route-absence checks (HTTP 404) | Test strategy |
| R4 | Remove session endpoints from `R/api.R`, update roxygen documentation, and sync `man/run_api.Rd` | Docs & package contract |
| R5 | Remove session endpoints, examples, and the `## Session Management Details` section from `docs/api-contract.md`, `README.md`, and `SWAGGER_GUIDE.md` | User request |
| R6 | Remove the `fld_session` folder and all session requests from `insomnia-collection.json` | API tooling hygiene |

## Phase 1: Code and Test Removal

phases: 2

## Phase 1: Code and Test Removal

### 1. Excise Plumber Route Handlers and Session Helpers
- **Requirements**: R1, R2
- **Files**:
  - `inst/plumber/plumber.R`
  - `inst/plumber/helpers.R`
- **Details**:
  - In `inst/plumber/plumber.R`: Remove header comment references to `POST /session/surveys` and `GET /session/:id/surveys`. Remove the handler blocks `# ── POST /session/surveys` (lines ~804-825) and `# ── GET /session/:id/surveys` (lines ~827-846).
  - In `inst/plumber/helpers.R`: Remove `.SESSION_TTL_SECONDS <- 3600L` and `.SESSION_STORE <- new.env(parent = emptyenv())` (lines ~22-23). Remove `validate_session_pip_id()` (lines ~73-108), `create_session()` (lines ~115-137), and `get_session_surveys()` (lines ~144-162). Verify that no other helper or endpoint relies on these symbols.
- **Test Scenarios**:
  - Happy path: Plumber router builds cleanly via `plumber::plumb()` without warning or missing symbol errors.
  - Regression check: All remaining endpoints (`/health`, `/releases`, `/surveys`, `/surveys-ui`, `/countries`, `/regions`, `/dimensions`, `/analysis-variables`, `/statistics`, `/categories`, `/covariates`, `/lookup`, `/table`, `/table/download`, `/description`) continue operating unchanged.
- **Tests**:
  - `tests/testthat/test-api-helpers.R`
  - `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**:
  - `grep -E "session.*surveys|\.SESSION_" inst/plumber/` returns 0 matches.

### 2. Update API Endpoint Test Suite
- **Requirements**: R3
- **Files**:
  - `tests/testthat/test-api-endpoints.R`
- **Details**:
  - Replace the `# ── /session endpoints` block (lines 482-540) containing active test cases for session creation, retrieval, validation, and TTL expiry.
  - Add negative test assertions verifying that `POST /session/surveys` and `GET /session/nonexistent123/surveys` (or `GET /session/123/surveys`) return HTTP 404 (route not found) from `.ep_router`.
- **Test Scenarios**:
  - Happy path: `test-api-endpoints.R` passes completely with 0 failures.
  - Route absence: Calling former session URLs returns HTTP 404.
- **Tests**:
  - `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**:
  - All tests in `test-api-endpoints.R` pass; no obsolete session assertions remain.

## Phase 2: Documentation, Specifications, and Tooling

### 3. Update R Package Documentation and Man Pages
- **Requirements**: R4
- **Files**:
  - `R/api.R`
  - `man/run_api.Rd`
- **Details**:
  - In `R/api.R`: Remove table rows for `/session/surveys` and `/session/<id>/surveys` from the roxygen documentation table of `run_api()`.
  - In `man/run_api.Rd`: Remove corresponding `\verb{/session/surveys}` and `\verb{/session/<id>/surveys}` table entries, or regenerate via `devtools::document()` / `roxygen2`.
- **Test Scenarios**:
  - `man/run_api.Rd` matches roxygen source in `R/api.R`.
  - Package check / documentation lint passes without dangling cross-references.
- **Tests**:
  - `devtools::document()` or manual inspection of Rd file.
- **Acceptance criteria**:
  - `grep -E "/session" R/api.R man/run_api.Rd` returns 0 matches.

### 4. Update Markdown Guides and API Contracts
- **Requirements**: R5
- **Files**:
  - `docs/api-contract.md`
  - `README.md`
  - `SWAGGER_GUIDE.md`
- **Details**:
  - In `docs/api-contract.md`:
    - Remove `/session/<id>/surveys` from 404 status code table (line ~52).
    - Remove `/session/surveys` and `/session/<id>/surveys` from Step 1 summary table (line ~60).
    - Delete endpoint documentation sections `### /session/surveys — POST (Step 1)` and `### /session/<id>/surveys — GET (Step 1)` (lines ~139-155).
    - Remove mention of `/session/surveys` in survey cap note (line ~337).
    - Remove `6. Sessions` constraint (line ~342).
    - Delete entire section `## Session Management Details` (lines ~348-354).
    - Remove `/session/surveys & /session/<id>/surveys` row from Examples & Quick Reference table (line ~379).
  - In `README.md`:
    - Remove `/session/surveys` and `/session/<id>/surveys` from the endpoint summary table (lines ~136-137).
  - In `SWAGGER_GUIDE.md`:
    - Remove mention of `/session/surveys` from `pip_id` validation note (line ~81).
    - Delete subsections `### POST /session/surveys` and `### GET /session/:id/surveys` (lines ~280-296).
    - Remove session endpoints from UI wizard mapping table for Step 1 (line ~315).
- **Test Scenarios**:
  - Markdown files render cleanly without broken links or references to removed endpoints.
- **Tests**:
  - Text search for `/session` across all markdown documentation files.
- **Acceptance criteria**:
  - `grep -E "/session/(surveys|<id>/surveys|:id/surveys)" docs/api-contract.md README.md SWAGGER_GUIDE.md` returns 0 matches.

### 5. Clean Insomnia Collection
- **Requirements**: R6
- **Files**:
  - `insomnia-collection.json`
- **Details**:
  - Remove folder `_id: "fld_session"` and its 6 child request objects:
    - `req_session_create` (`POST /session/surveys - create session`)
    - `req_session_retrieve` (`GET /session/<id>/surveys - retrieve session`)
    - `req_session_create_empty` (`POST /session/surveys - missing pip_id (400)`)
    - `req_session_create_invalid_format` (`POST /session/surveys - invalid pip_id format (400)`)
    - `req_session_retrieve_unknown` (`GET /session/<id>/surveys - unknown session (404)`)
    - `req_session_roundtrip` (`POST /session/surveys - roundtrip test (5 surveys)`)
  - Ensure JSON syntax remains strictly valid after removing the objects.
- **Test Scenarios**:
  - `insomnia-collection.json` parses as valid JSON via `jsonlite::fromJSON()`.
- **Tests**:
  - JSON parse validation.
- **Acceptance criteria**:
  - `grep -E "fld_session|req_session" insomnia-collection.json` returns 0 matches and JSON is valid.

## Testing Strategy

- Unit testing: Run `tests/testthat/test-api-endpoints.R` and `tests/testthat/test-api-helpers.R` via `devtools::test_active_file()` or testrunner.
- Negative routing: Assert that requests to `POST /session/surveys` and `GET /session/123/surveys` return HTTP 404 from the Plumber router.
- Full API regression: Run full API test suite (`test-api-*.R`) to verify no unintended side effects on other routes.
- JSON validity: Verify `insomnia-collection.json` parses with no syntax errors.

## Documentation Checklist

- [x] `R/api.R` endpoint table updated
- [x] `man/run_api.Rd` synced
- [x] `docs/api-contract.md` updated and `## Session Management Details` removed
- [x] `README.md` endpoint table updated
- [x] `SWAGGER_GUIDE.md` sections and mapping table updated
- [x] `insomnia-collection.json` cleaned

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Uncaught reference to `.SESSION_STORE` or helper functions causes runtime error | Low | Grep workspace for all occurrences before and after deletion; verify clean router compilation |
| Frontend or external consumers still sending requests to `/session/surveys` | Low | UI confirmed client-side localStorage architecture; returning standard HTTP 404 is correct REST behavior |
| Accidental syntax error when editing large `insomnia-collection.json` | Low | Parse with `jsonlite::fromJSON()` immediately following edit |

## Out of Scope

- Changes to frontend code or client-side storage
- Changes to `/categories`, `/covariates`, `/table`, or other Plumber endpoints
- Editing historical brainstorming notes or past plans in `.cg-docs/`

## Completion Contract

### Outcome
The `piptm` Plumber API service no longer exposes `POST /session/surveys` or `GET /session/<id>/surveys`; in-memory session storage and validation helpers are cleanly excised from `inst/plumber/helpers.R`; obsolete session tests in `tests/testthat/test-api-endpoints.R` are replaced with 404 route-absence assertions; and all references in `R/api.R`, `man/run_api.Rd`, `docs/api-contract.md`, `README.md`, `SWAGGER_GUIDE.md`, and `insomnia-collection.json` are removed.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|---|---|---|---|
| V1 | `inst/plumber/plumber.R` and `inst/plumber/helpers.R` contain zero references to session endpoints or helpers | `grep -E "session.*surveys|\.SESSION_" inst/plumber/plumber.R inst/plumber/helpers.R` returns 0 matches | yes |
| V2 | API test suite passes with 0 failures and confirms `/session/surveys` and `/session/123/surveys` return HTTP 404 | `tests/testthat/test-api-endpoints.R` executed via test runner | yes |
| V3 | Package documentation is in sync (`R/api.R` and `man/run_api.Rd` contain no session endpoint entries) | `grep -E "/session" R/api.R man/run_api.Rd` returns 0 matches | yes |
| V4 | Markdown guides and schemas (`docs/api-contract.md`, `README.md`, `SWAGGER_GUIDE.md`) contain no session endpoint entries or session management section | `grep -E "/session/(surveys|<id>/surveys|:id/surveys)" docs/api-contract.md README.md SWAGGER_GUIDE.md` returns 0 matches | yes |
| V5 | Insomnia collection has `fld_session` and child session requests removed and is valid JSON | `grep -E "fld_session|req_session" insomnia-collection.json` returns 0 matches | yes |

### Constraints

| ID | Constraint | Check |
|---|---|---|
| C1 | Existing stateless endpoints (`/table`, `/description`, `/categories`, `/covariates`, `/lookup`, `/surveys-ui`, etc.) and their test suites must not be broken or altered | Full test suite passes |
| C2 | No other package dependencies or exported functions in `NAMESPACE` are modified | `git diff --stat NAMESPACE` is empty |
| C3 | No protected assets or roadmap files modified directly | Only allowed paths touched |

### Boundaries

- Allowed:
  - `inst/plumber/plumber.R`
  - `inst/plumber/helpers.R`
  - `tests/testthat/test-api-endpoints.R`
  - `R/api.R`
  - `man/run_api.Rd`
  - `docs/api-contract.md`
  - `README.md`
  - `SWAGGER_GUIDE.md`
  - `insomnia-collection.json`
- Out of scope:
  - Modifying other endpoints or parameter schemas
  - Modifying historical files in `.cg-docs/`

### Iteration Policy

1. Code and helper removals must occur before test suite updates.
2. Test suite must establish that requests to `/session/surveys` and `/session/<id>/surveys` return HTTP 404.
3. Documentation updates must match the exact remaining endpoint surface.

### Blocked-Stop Conditions

- Existing non-session endpoints fail validation or unit tests after removal.
- Unexpected inter-module dependencies on `create_session` or `get_session_surveys` outside Plumber helpers.
