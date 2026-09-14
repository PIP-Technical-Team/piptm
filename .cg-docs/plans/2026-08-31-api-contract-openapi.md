---
date: 2026-08-31
title: "API Contract — Markdown with OpenAPI-Style Schemas"
status: completed
completed-date: 2026-08-31
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-08-31-api-contract-openapi.md"
language: "R"
estimated-effort: "small"
tags: [api, openapi, documentation, handoff, phase-4, markdown]
---

# Plan: API Contract — Markdown with OpenAPI-Style Schemas

## Objective

Create a single Markdown file (`docs/api-contract.md`) documenting all 15 Table Maker API endpoints with OpenAPI-style schemas, inline UI mapping, structured response envelopes, error handling, and input validation rules. This serves as the internal handoff document for the UI/IT team to build against the existing API.

## Context

- The Table Maker API (`piptm`) has 15 Plumber endpoints implemented in `inst/plumber/plumber.R` (16 route decorators total, since `/table` supports both GET and POST).
- The existing Insomnia collection (`insomnia-collection.json`) provides request templates but lacks formal documentation.
- The UI/IT team needs a clear, readable contract to understand all endpoints, their inputs/outputs, and how each maps to the 3-step wizard UI.
- Approach chosen: Markdown with OpenAPI-style schemas (single file, no external tooling required).

## Requirements

| ID  | Requirement                                              | Source        |
|-----|----------------------------------------------------------|---------------|
| R1  | Document all 15 existing endpoints                       | brainstorm    |
| R2  | Markdown with OpenAPI-style schemas (single file)        | brainstorm    |
| R3  | Internal handoff — reference doc for UI/IT team           | brainstorm    |
| R4  | Single UI/IT team as primary consumer                     | brainstorm    |
| R5  | Error handling + validation rules documented              | brainstorm    |
| R6  | Implementation details, deployment, and testing out of scope | brainstorm |
| R7  | Structured response envelope: `status`, `data`, `warnings`, `errors`, `meta` | existing API |
| R8  | Max 15 surveys per request (enforced server-side)        | existing API   |
| R9  | PPP parameter validation (type guard, digit guard, positive guard) | existing API |
| R10 | Filter base JSON object support for sample-base filtering | existing API   |
| R11 | Pop share threshold parameter for cell suppression        | existing API   |
| R12 | Session management (in-memory, 1-hour TTL)               | existing API   |
| R13 | CORS filter for browser access                           | existing API   |
| R14 | Release-aware endpoints (default to current release)     | existing API   |

## Implementation Steps

### Step 1: Create `docs/api-contract.md` — Document Structure

Create the Markdown file with the following sections:

1. **Header** — Title, version, date, purpose
2. **Overview** — What this contract covers, who it's for, how to read it
3. **Response Envelope** — The structured response format (`status`, `data`, `warnings`, `errors`, `meta`)
4. **Error Codes** — HTTP status codes (400, 422, 500) and their meanings
5. **UI Step Mapping** — How endpoints map to the 3-step wizard (Step 1: Survey selection, Step 2: Measure & cut configuration, Step 3: Results)
6. **Endpoints** — Each endpoint with:
   - Method and path
   - UI step mapping
   - Description
   - Request parameters (query, body, path)
   - Response schema (OpenAPI-style in fenced code blocks)
   - Validation rules
   - Example request/response
7. **Shared Schemas** — Reusable schema definitions (ResponseEnvelope, ErrorEnvelope, etc.)
8. **Validation Rules** — Input validation rules (max 15 surveys, PPP validation, filter base JSON, pop share threshold)
9. **Session Management** — In-memory session with 1-hour TTL
10. **CORS** — CORS filter for browser access

### Step 2: Document Each Endpoint

For each of the 15 endpoints, document:
- Method(s) and path
- UI step mapping (Step 1, Step 2, Step 3, or utility)
- Description
- Request parameters with types and validation rules
- Response schema (OpenAPI-style YAML in fenced code blocks)
- Error responses (400, 422, 500)
- Example request and response

**All 15 endpoints** (note: `/table` supports both GET and POST, counted as one endpoint with two methods):
- `/health` (GET) — Server health check
- `/releases` (GET) — List loaded releases + current
- `/surveys` (GET) — Full manifest rows for a release
- `/surveys-ui` (GET) — UI-ready survey catalogue
- `/countries` (GET) — Country list from manifest
- `/regions` (GET) — Region list with member countries
- `/categories` (GET) — Filter categories for sample-base UI
- `/covariates` (GET) — Layout covariates for table slicing
- `/dimensions` (GET) — Valid disaggregation dimensions
- `/analysis-variables` (GET) — Analysis variable catalogue
- `/statistics` (GET) — Measure-group catalogue
- `/lookup` (GET) — Resolve triplets to `pip_id`
- `/table` (GET, POST) — Compute table output *(one endpoint, two methods)*
- `/description` (POST) — Render natural-language description
- `/session/surveys` (POST) — Create session with survey selection
- `/session/<id>/surveys` (GET) — Retrieve survey selection for session

### Step 3: Add UI Step Mapping

For each endpoint, add a UI step mapping section that connects the endpoint to the 3-step wizard flow:

- **Step 1**: Survey selection (`/surveys-ui`, `/countries`, `/regions`, `/categories`, `/lookup`, `/session/surveys`, `/session/<id>/surveys`)
- **Step 2**: Measure & cut configuration (`/analysis-variables`, `/statistics`, `/covariates`, `/dimensions`)
- **Step 3**: Results (`/table`, `/description`)

### Step 4: Validate Against Implementation

- Cross-reference the contract with the existing implementation in `inst/plumber/plumber.R` and `inst/plumber/helpers.R`.
- Ensure all endpoints, parameters, and response schemas match the actual implementation.
- Validate that validation rules (max 15 surveys, PPP validation, filter base JSON, pop share threshold) are correctly documented.
- **Additional validation points from plan review**:
  - Session edge cases: 1-hour TTL expiry, invalid session IDs, 12-character alphanumeric IDs with 20-try collision handling, process-local scope (not shared across API instances)
  - `filter_base` JSON schema with example structure showing key-value pairs (e.g., `{"gender": [0], "age_group": [1,2]}`)
  - `/description` dual-mode structure: fast path (description_metadata only) vs fallback path (table parameters only), with mutual exclusion rule (supplying both returns HTTP 400)
  - `/categories` and `/covariates` intersection logic when `pip_id` is provided (returns only dimensions present in ALL selected surveys)
  - Optional parameters documented with their defaults (`ppp=2021`, `pop_share_threshold=0.01`, `include_metadata=false`, `release=NULL`)
  - `/lookup` parallel array structure: `country_code`, `year`, and `welfare_type` are zip/triplet structure with equal length requirement

### Step 5: Update README.md

- Add a reference to the new API contract in the README.
- Provide instructions for viewing the contract (e.g., using a Markdown renderer or IDE).

## Validation Criteria

- All 15 endpoints are documented with method, path, UI step mapping, request parameters, response schemas, and validation rules.
- The response envelope schema is consistent across all endpoints.
- Error codes (400, 422, 500) are correctly documented for each endpoint.
- Input validation rules are documented (max 15 surveys, PPP validation, filter base JSON, pop share threshold).
- The contract is readable and maintainable by the UI/IT team.
- The contract is placed in `docs/api-contract.md`.
- The README.md is updated to reference the new contract.
