---
date: 2026-11-11
title: "Rename POST /description to POST /metadata"
status: completed
completed-date: 2026-11-11
completed-phases: [1, 2]
scope: "Standard"
brainstorm: null
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-11-11-rename-description-to-metadata.md"
phases: 2
artifact-schema-version: 1
tags: [api, plumber, metadata, breaking-change, rename]
---

# Plan: Rename POST /description to POST /metadata

## Objective

Rename the public Plumber route `POST /description` to `POST /metadata` and
update every live dependent (router maps, OpenAPI path keys, tests, and
current-state docs) so the old path is no longer advertised or callable.
Request body, serializers, status codes, and internal builder/renderer names
stay unchanged.

## Context

`POST /description` in `inst/plumber/plumber.R` renders table metadata as HTML
or Markdown. The handler, `validate_description_input()`, and the
`description_*` builders are already the metadata document; only the public
path name is wrong for the current Step 3 product language.

The path is not only the `@post` annotation. `inst/plumber/plumber.R` also
keys tag maps, summary maps, and post-`getApiSpec()` OpenAPI patches on the
string `"/description"`. Brain finding
`.cg-docs/solutions/bugs/2026-08-31-swagger-req-parameter-exposed.md`: Swagger
path keys are hand-patched after `getApiSpec()`. Changing only `@post` leaves
OpenAPI on `/description`.

There is no existing `/metadata` route. Insomnia has no `/description` URL.
Historical `.cg-docs/` notes and `roadmap.json` titles stay as historical
record.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `POST /metadata` is the only public route for this handler. `POST /description` is removed. No compatibility alias. | User request |
| R2 | Router maps and OpenAPI patches in `inst/plumber/plumber.R` use `"/metadata"` everywhere they currently key `"/description"`. Request/response schemas, serializers, and the `req` exclusion stay the same. | Brain; plumber.R |
| R3 | Tests that call or assert `POST /description` call and assert `POST /metadata`, including OpenAPI path lookup and the Swagger `req` guard. | tests/testthat/test-api-description.R, test-api-endpoints.R |
| R4 | Live docs and roxygen route tables name `/metadata` and no longer advertise `/description` as a callable route. | README.md, R/api.R, docs/api-contract.md, docs/description-endpoint-spec.md, SWAGGER_GUIDE.md, table_maker.R route comments |
| R5 | Do not rename `description_metadata`, `render_description_*`, `description_builder.R`, `validate_description_input()`, or historical `.cg-docs/` / `roadmap.json` records. | Contract C1, C2 |

## Implementation Steps

## Phase 1: Route rename and tests

### 1. Move the Plumber route and OpenAPI keys to `/metadata`
- **Requirements**: R1, R2, R5
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R` (comments only if they name the route)
- **Details**:
  - Change `#* @post /description` to `#* @post /metadata`.
  - Update the header comment, root-doc prose, `tag_by_path`, summary map, include_metadata param text, categories/covariates cross-references, and the post-spec block that reads and writes `spec$paths[["/description"]]`.
  - Keep the handler body, `@serializer text`, `@parser json`, `@param body:object`, and `req` formal unchanged.
  - Do not add a second route on `/description`.
  - Confirm no other `@post /metadata` or `@get /metadata` exists before editing.
- **Test Scenarios**:
  - Happy path: covered by Step 2 after the path string changes.
  - Edge case: OpenAPI still documents the same requestBody and does not expose `req`.
  - Error path: empty body still returns 400 on the new path.
- **Tests**: `tests/testthat/test-api-description.R`, `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: `Select-String -Path inst/plumber/plumber.R -Pattern '@post /description'` returns no match; `@post /metadata` returns one match; `spec$paths` no longer contains `"/description"`.

### 2. Retarget API tests to `/metadata`
- **Requirements**: R3
- **Files**: `tests/testthat/test-api-description.R`, `tests/testthat/test-api-endpoints.R`, and any other test file that requests `path = "/description"` or indexes `spec$paths[["/description"]]`
- **Details**:
  - Replace request paths and OpenAPI lookups with `"/metadata"`.
  - Update test names and comments that assert the public route.
  - Do not change expected status codes, body fields, or Content-Type assertions.
  - Leave tests of `description_metadata` field names and `description_builder.R` alone.
- **Test Scenarios**:
  - Happy path: fast path Markdown and default HTML still return 200.
  - Edge case: OPTIONS preflight succeeds on `/metadata`.
  - Error path: empty body 400, invalid format 400, both metadata and params 400, incomplete metadata 422.
- **Tests**: `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-description.R'); testthat::test_file('tests/testthat/test-api-endpoints.R')"`
- **Acceptance criteria**: those two files pass; a request to `/description` is not asserted as a supported route.

## Phase 2: Live documentation

### 3. Update current-state route documentation
- **Requirements**: R4, R5
- **Files**: `README.md`, `R/api.R`, `R/table_maker.R` (route comments only), `docs/api-contract.md`, `docs/description-endpoint-spec.md`, `SWAGGER_GUIDE.md`, regenerated `man/run_api.Rd` and any other Rd file that documents the public route path
- **Details**:
  - Replace callable-route mentions of `POST /description` and `` `/description` `` with `POST /metadata` / `` `/metadata` ``.
  - In `docs/api-contract.md`, rename the section heading and the endpoint inventory row. Keep the `description_metadata` field name.
  - In `docs/description-endpoint-spec.md`, update the route name in current-state prose. Do not rename the spec file.
  - Regenerate Rd from roxygen for touched exported docs (`devtools::document()`), limited to route-table updates.
  - Do not edit `.cg-docs/**` or `roadmap.json`.
- **Test Scenarios**: N/A (documentation).
- **Tests**: grep checks in Verification Surface V3.
- **Acceptance criteria**: `rg "/description" README.md R/api.R R/table_maker.R inst/plumber docs SWAGGER_GUIDE.md` finds no live route path. Field name `description_metadata` may remain.

## Testing Strategy

- Red-phase: existing `/description` path assertions fail once the route moves and before test paths are updated.
- Route coverage stays in `tests/testthat/test-api-description.R` (status, format, CORS, OpenAPI `req` guard).
- Inventory coverage stays in `tests/testthat/test-api-endpoints.R` (path list and requestBody schema).
- Do not run the full suite unless a fix touches shared helpers beyond comments.

## Documentation Checklist

- [ ] `README.md` endpoint table
- [ ] `R/api.R` and regenerated `man/run_api.Rd`
- [ ] `docs/api-contract.md` section, step map, and inventory
- [ ] `docs/description-endpoint-spec.md` route name
- [ ] `SWAGGER_GUIDE.md` step list, exceptions, and endpoint section
- [ ] `inst/plumber/plumber.R` root doc and cross-references

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| OpenAPI key left on `/description` after `@post` rename | Swagger still advertises the old path | Step 1 explicitly moves every `spec$paths[["/description"]]` read/write; V1 asserts the new key |
| Broad replace renames `description_metadata` or builder files | Breaks `/table` fast path and historical tests | R5 forbids those renames; replace only the route token `/description` |
| External UI still calls `/description` | Clients get 404 | Breaking change is intended; no alias. Call it out in the work summary |
| Doc sweep rewrites `.cg-docs/` history | Loses the record of past work | C2 excludes `.cg-docs/**` and `roadmap.json` |

## Out of Scope

- Renaming R functions, files, or the `description_metadata` payload field.
- Changing HTML/Markdown rendering, validation rules, or status codes.
- Adding a `/description` compatibility alias or redirect.
- Editing historical `.cg-docs/` artifacts or `roadmap.json`.
- Updating the Insomnia collection (it has no `/description` URL).
- Coordinating the PIP platform UI client.

## Completion Contract

### Outcome

`POST /metadata` is the only public route for table metadata rendering. `/description` is gone from the router, OpenAPI spec, and live docs. Request body, serializers, and status codes are unchanged.

### Verification Surface

| ID | Phase | Evidence Required | Command/Artifact | Required |
|----|-------|-------------------|------------------|----------|
| V1 | 1 | Router and OpenAPI expose `POST /metadata` and not `POST /description` | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-description.R'); testthat::test_file('tests/testthat/test-api-endpoints.R')"` | yes |
| V2 | 1 | Fast path, fallback, 400/422, CORS, and Swagger `req` guard still pass on `/metadata` | same command as V1 | yes |
| V3 | 2 | Live docs name `/metadata` and no longer advertise `/description` as a callable route | `rg "/description" README.md R/api.R R/table_maker.R inst/plumber docs SWAGGER_GUIDE.md` | yes |
| V4 | final | Touched API tests pass | same command as V1 | yes |

### Constraints

| ID | Phase | Constraint | Check |
|----|-------|------------|-------|
| C1 | 1 | Do not rename `description_metadata`, `render_description_*`, or `description_builder.R` | diff has no file renames and no payload-field renames |
| C2 | 2 | Do not rewrite historical `.cg-docs/` or `roadmap.json` | those paths absent from the implementation diff |
| C3 | 1 | No compatibility alias on `/description` | router introspection has only `/metadata` |
| C4 | 1 | Swagger still must not expose `req` as a query parameter | existing OpenAPI assertion, retargeted to `/metadata` |

### Boundaries

- Allowed: `inst/plumber/plumber.R`, `inst/plumber/helpers.R` comments, `R/api.R`, `R/table_maker.R` route comments, `tests/testthat/test-api-description.R`, `tests/testthat/test-api-endpoints.R`, `docs/api-contract.md`, `docs/description-endpoint-spec.md`, `README.md`, `SWAGGER_GUIDE.md`, regenerated `man/run_api.Rd` and related Rd route mentions.
- Out of scope: renderer/builder behavior, payload field names, historical plans/reviews/solutions, roadmap feature titles, Insomnia collection.

### Iteration Policy

1. Deviation policy is `ask`: stop and record before any extra rename or alias.
2. Update tests in the same change as the route so red-phase is the old path.
3. Do not weaken assertions except the path string itself.

### Blocked-Stop Conditions

- A second live route named `/metadata` already exists.
- OpenAPI path cannot be moved without changing request/response schemas.
- Required tests cannot be run.
- A required deviation is discovered under policy `ask` and user approval is unavailable.
