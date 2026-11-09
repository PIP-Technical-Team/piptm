---
date: 2026-11-04
title: "Remove Obsolete Session Survey API Endpoints and Documentation"
status: completed
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-07-31-session-storage-endpoints.md"
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
artifact-schema-version: 1
execution-report: .cg-docs/work-reports/2026-11-04-remove-session-surveys-endpoints-corrected.md
completed-date: 2026-11-05
failing-steps: []
completed-phases: [1, 2]
phases: 2
tags: [api, plumber, session, cleanup, breaking-change, documentation]
---

# Plan: Remove Obsolete Session Survey API Endpoints and Documentation

## Objective

Remove the obsolete `POST /session/surveys` and `GET /session/<id>/surveys` endpoints from the `piptm` Plumber API, purge their unused validation and in-memory storage helpers, verify route absence, and remove endpoint references from package documentation, API contracts, guides, and the Insomnia collection.

## Context

The session endpoints were proposed to persist Step 1 survey selections across page reloads. The later UI design uses client-side `localStorage` / `sessionStorage`, making server-side persistence unnecessary. Removing the routes also removes process-local state and outdated API references.

## Requirements

| ID | Requirement | Source |
|---|---|---|
| R1 | Remove both session survey routes from the Plumber API | User request |
| R2 | Remove session validation, allocation, retrieval helpers, and in-memory store state | User request; dead-code cleanup |
| R3 | Replace session behavior tests with route-absence assertions | Verification requirement |
| R4 | Remove the endpoints from R API documentation and the generated Rd page | Documentation consistency |
| R5 | Remove endpoint and session-management references from API guides | User request |
| R6 | Remove the session folder and related requests from the Insomnia collection | API tooling consistency |

## Phase 1: Code and Test Removal

### 1. Remove Plumber Routes and Session Helpers
- **Requirements**: R1, R2
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**:
  - Remove the header comments and route handlers for `POST /session/surveys` and `GET /session/<id>/surveys` from `inst/plumber/plumber.R`.
  - Remove `.SESSION_TTL_SECONDS`, `.SESSION_STORE`, `validate_session_pip_id()`, `create_session()`, and `get_session_surveys()` from `inst/plumber/helpers.R`.
  - Search the source for references to these symbols before deletion and confirm no other API code depends on them.
- **Test Scenarios**: Router creation succeeds; remaining routes continue to register; removed symbols have no live references.
- **Tests**: `tests/testthat/test-api-endpoints.R`; `tests/testthat/test-api-helpers.R`.
- **Acceptance criteria**: Neither session route nor session helper/store symbol remains in `inst/plumber/`.

### 2. Verify Removed Routes Return Not Found
- **Requirements**: R3
- **Files**: `tests/testthat/test-api-endpoints.R`
- **Details**:
  - Remove the session endpoint behavior test block.
  - Add requests asserting `POST /session/surveys` and `GET /session/123/surveys` return HTTP 404 from the router.
  - Preserve tests for unrelated endpoints.
- **Test Scenarios**: Former POST route is absent; former parameterized GET route is absent; endpoint test file passes.
- **Tests**: Run `tests/testthat/test-api-endpoints.R` and `tests/testthat/test-api-helpers.R` using the project's testthat workflow.
- **Acceptance criteria**: Both route-absence assertions and all tests in the targeted files pass.

## Phase 2: Documentation, Specifications, and Tooling

### 3. Update R Package Documentation and Man Page
- **Requirements**: R4
- **Files**: `R/api.R`, `man/run_api.Rd`
- **Details**:
  - Remove both route entries from the `run_api()` roxygen endpoint table.
  - Synchronize `man/run_api.Rd` with the roxygen source, using the established documentation workflow or a focused manual edit.
- **Test Scenarios**: Roxygen source and Rd endpoint listings agree; neither lists a session route.
- **Tests**: Documentation generation or targeted source/Rd search.
- **Acceptance criteria**: No session route references remain in `R/api.R` or `man/run_api.Rd`.

### 4. Update Markdown API Documentation
- **Requirements**: R5
- **Files**: `docs/api-contract.md`, `README.md`, `SWAGGER_GUIDE.md`
- **Details**:
  - Remove session route entries from API endpoint summaries and the Step 1 UI mapping.
  - Remove both route-specific documentation sections and session-specific error/status references.
  - Remove the `## Session Management Details` section and references suggesting session storage is part of the current API.
  - Retain general uses of the words “session” that refer to user sessions or unrelated runtime behavior.
- **Test Scenarios**: All endpoint catalogs and wizard mappings reflect the remaining API; markdown contains no removed route path.
- **Tests**: Targeted search in these three documents; existing documentation checks if available.
- **Acceptance criteria**: No references to the removed endpoint paths or session management API remain in these files.

### 5. Remove Insomnia Session Requests
- **Requirements**: R6
- **Files**: `insomnia-collection.json`
- **Details**:
  - Remove folder `_id: "fld_session"` and all six child requests whose IDs begin `req_session_`.
  - Preserve all other requests and ensure the collection remains valid JSON.
- **Test Scenarios**: JSON parses; other request objects remain present.
- **Tests**: Parse the collection as JSON and search for `fld_session` and `req_session`.
- **Acceptance criteria**: Session folder and requests are absent and the JSON collection parses successfully.

## Testing Strategy

- Run targeted endpoint and helper tests after Phase 1 changes.
- Run the full test suite at the Phase 1 boundary and again after Phase 2 changes.
- Check the specified source and documentation files for obsolete endpoint and helper references.
- Parse `insomnia-collection.json` as JSON after editing.

## Documentation Checklist

- [ ] `R/api.R` and `man/run_api.Rd` list only current endpoints
- [ ] `docs/api-contract.md`, `README.md`, and `SWAGGER_GUIDE.md` omit the removed routes and session API details
- [ ] `insomnia-collection.json` has no session folder or requests and remains valid JSON

## Risks & Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| A remaining reference to deleted helpers causes router initialization or request failures | Medium | Search source references and create the Plumber router in targeted tests |
| A client still calls the removed routes | Medium | Confirm the UI uses client-side storage; document the intentional 404 behavior in route-absence tests |
| Documentation catalogs drift from the actual route registry | Low | Search all named API catalogs and run targeted endpoint tests |
| Insomnia JSON becomes invalid while removing multiple request objects | Low | Parse the edited collection as JSON immediately |

## Out of Scope

- Frontend code or client-side storage changes
- Changes to any remaining endpoint or its request/response contract
- Edits to historical brainstorms, prior plans, or roadmap files

## Completion Contract

### Outcome

The Plumber API no longer exposes `POST /session/surveys` or `GET /session/<id>/surveys`, and the unused in-memory session implementation is removed. Tests verify both routes are absent, while current API documentation and the Insomnia collection no longer describe them.

### Verification Surface

| ID | Phase | Evidence Required | Command/Artifact | Required |
|---|---:|---|---|---|
| V1 | 1 | Session routes, helpers, and store state are absent from Plumber files | Targeted source search over `inst/plumber/plumber.R` and `inst/plumber/helpers.R` | yes |
| V2 | 1 | Both removed routes return HTTP 404 and targeted API tests pass | Run `tests/testthat/test-api-endpoints.R` and `tests/testthat/test-api-helpers.R` | yes |
| V3 | 2 | R API documentation and Rd page omit session routes | Targeted search of `R/api.R` and `man/run_api.Rd` | yes |
| V4 | 2 | Markdown API catalogs omit session routes and session-management details | Targeted search of `docs/api-contract.md`, `README.md`, and `SWAGGER_GUIDE.md` | yes |
| V5 | 2 | Insomnia session folder/requests are absent and collection is valid JSON | Targeted search and JSON parse of `insomnia-collection.json` | yes |
| V6 | final | Full suite passes without regression to remaining API routes | Run the project-wide testthat suite | yes |

### Constraints

| ID | Phase | Constraint | Check |
|---|---:|---|---|
| C1 | 1 | Do not alter behavior or parameters of remaining API routes | Targeted endpoint tests and full test suite |
| C2 | 2 | Do not modify package dependencies or exported API functions | Inspect diff for `DESCRIPTION` and `NAMESPACE` |
| C3 | final | Do not modify protected artifacts or roadmap files | Inspect changed-file list |

### Boundaries

- Allowed: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `tests/testthat/test-api-endpoints.R`, `R/api.R`, `man/run_api.Rd`, `docs/api-contract.md`, `README.md`, `SWAGGER_GUIDE.md`, `insomnia-collection.json`.
- Out of scope: frontend/client storage, remaining API endpoints, historical `.cg-docs/` artifacts, and direct `roadmap.json` edits.

### Iteration Policy

1. Remove the route handlers and unused helper state, then update route tests to assert absence.
2. Run targeted functional tests; investigate failures without weakening unrelated assertions.
3. Update documentation and Insomnia artifacts, then validate them and run the full suite.
4. Under `deviation-policy: ask`, pause for approval before any scope change.

### Blocked-Stop Conditions

- A required test or evidence check cannot be run or fails after the allowed recovery attempts.
- A remaining API route regresses after session removal.
- An unexpected dependency on removed session helpers is discovered outside the planned Plumber implementation.
- Work would require changes outside the approved boundaries or direct roadmap modification.
