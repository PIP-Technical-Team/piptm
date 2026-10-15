---
date: 2026-10-14
title: "API-to-UI mapping developer guide (Part 1)"
status: active
scope: "Lightweight"
brainstorm: null
language: "R"
estimated-effort: "small"
deviation-policy: ask
tags: [api, documentation, ui-mapping, plumber]
execution-report: ".cg-docs/work-reports/2026-10-14-api-ui-mapping-guide.md"
---

# Plan: API-to-UI mapping developer guide (Part 1)

## Objective

Produce a verified Part 1 developer guide that maps every public Table Maker Plumber endpoint to the UI page, step, or element it powers, using only `inst/plumber/plumber.R`, `docs/ui-description.md`, and related verified API sources. Stop and ask on any unverifiable mapping.

## Context

- Charter current focus includes the API service layer connecting the PIP platform to `table_maker()`.
- UI source of truth: `docs/ui-description.md` (three-step wizard).
- Endpoint source of truth: `inst/plumber/plumber.R`.
- Existing related docs (`docs/api-contract.md`, `SWAGGER_GUIDE.md`) may be used as secondary sources but must not invent UI mappings.
- Part 2 (worked examples with sample parameter values) is out of scope until Part 1 is confirmed.

## Implementation Steps

### 1. Inventory endpoints from the Plumber source

- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**: List every `@get`/`@post` route with method, path, required/optional parameters, sequencing dependencies, and documented purpose. Do not infer undocumented behavior.
- **Acceptance criteria**: Complete endpoint inventory covering all routes in `plumber.R`.

### 2. Cross-walk endpoints to UI description

- **Files**: `docs/ui-description.md`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`
- **Details**: For each endpoint, map what it does, which UI step/page/element it powers, and how it should be called (parameters, order, dependencies). If a mapping cannot be verified from these sources, halt and ask before writing that section.
- **Acceptance criteria**: Every endpoint has a verified mapping or an explicit open question to the user.

### 3. Write Part 1 guide

- **Files**: `docs/api-ui-mapping.md`
- **Details**: Structured cheat sheet for Table Maker developers and UI developers. One section per endpoint. No sample-parameter use cases (Part 2).
- **Acceptance criteria**: Guide exists, covers all inventoried endpoints, and contains only verified statements.

### 4. Flag gaps and wait for Part 1 confirmation

- **Details**: List any remaining ambiguities. Do not start Part 2 until the user confirms Part 1 is accurate.
- **Acceptance criteria**: User-facing confirmation request; Part 2 not written.

## Completion Contract

### Outcome

`docs/api-ui-mapping.md` exists as Part 1 of the API developer guide: every public endpoint is mapped to a verified UI step/element with call sequencing, and unverifiable points are asked rather than invented.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | All `@get`/`@post` routes in `inst/plumber/plumber.R` appear in the guide | `docs/api-ui-mapping.md` vs `inst/plumber/plumber.R` | yes |
| V2 | Each endpoint section states what it does, which UI element it powers, and how to call it | `docs/api-ui-mapping.md` | yes |
| V3 | No mapping is documented that cannot be traced to `plumber.R` or `docs/ui-description.md` (or another cited verified source) | Manual source check | yes |
