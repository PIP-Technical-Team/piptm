---
date: 2026-08-31
title: "Table Maker API Contract — Markdown with OpenAPI-Style Schemas"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Markdown Contract with OpenAPI-Style Schemas"
tags: [api, openapi, documentation, handoff, phase-4, markdown]
---

# Table Maker API Contract — Markdown with OpenAPI-Style Schemas

## Context

The Table Maker API (`piptm`) has 14+ Plumber endpoints implemented in `inst/plumber/plumber.R`. A separate UI/IT team will consume this API to build the PIP platform frontend. The team needs clear documentation to understand all available endpoints, their inputs and outputs, and how each endpoint maps to the UI steps and components.

The existing Insomnia collection (`insomnia-collection.json`) provides request templates, but lacks a formal, machine-readable specification. The API contract needs to be self-contained, maintainable, and structured as a Markdown document with OpenAPI-style schemas embedded, providing the UI/IT team a readable, maintainable document that includes UI mapping and validation rules, without the overhead of maintaining a separate OpenAPI file.

## Requirements

| ID  | Requirement                                              | Source        |
|-----|----------------------------------------------------------|---------------|
| R1  | Document all 14+ existing endpoints                      | user Q3       |
| R2  | Markdown with OpenAPI-style schemas (single file)        | user Q4       |
| R3  | Internal handoff — reference doc for UI/IT team           | user Q1       |
| R4  | Single UI/IT team as primary consumer                     | user Q2       |
| R5  | Error handling + validation rules documented              | user Q5       |
| R6  | Implementation details, deployment, and testing out of scope | user Q6   |
| R7  | Structured response envelope: `status`, `data`, `warnings`, `errors`, `meta` | existing API |
| R8  | Max 15 surveys per request (enforced server-side)        | existing API   |
| R9  | PPP parameter validation (type guard, digit guard, positive guard) | existing API |
| R10 | Filter base JSON object support for sample-base filtering | existing API   |
| R11 | Pop share threshold parameter for cell suppression        | existing API   |
| R12 | Session management (in-memory, 1-hour TTL)               | existing API   |
| R13 | CORS filter for browser access                           | existing API   |
| R14 | Release-aware endpoints (default to current release)     | existing API   |

## Endpoints to Document

| Endpoint | Method(s) | UI Step | Purpose |
|---|---|---|---|
| `/health` | GET | — | Server health check |
| `/releases` | GET | — | List loaded releases + current |
| `/surveys` | GET | Step 1 | Full manifest rows for a release |
| `/surveys-ui` | GET | Step 1 | UI-ready survey catalogue |
| `/countries` | GET | Step 1 | Country list from manifest |
| `/regions` | GET | Step 1 | Region list with member countries |
| `/categories` | GET | Step 1/2 | Filter categories for sample-base UI |
| `/covariates` | GET | Step 2/3 | Layout covariates for table slicing |
| `/dimensions` | GET | Step 2 | Valid disaggregation dimensions |
| `/analysis-variables` | GET | Step 2 | Analysis variable catalogue |
| `/statistics` | GET | Step 2 | Measure-group catalogue |
| `/lookup` | GET | Step 1 | Resolve triplets to `pip_id` |
| `/table` | GET, POST | Step 3 | Compute table output |
| `/description` | POST | Step 3 | Render natural-language description |
| `/session/surveys` | POST | Step 1 | Create session with survey selection |
| `/session/<id>/surveys` | GET | Step 1 | Retrieve survey selection for session |

## Approaches Considered

### Approach 1: Single OpenAPI 3.0 YAML Specification

Create one `openapi.yaml` file with all 14+ endpoints, schemas, and UI mapping annotations in the OpenAPI 3.0 format.

**Pros**:
- Industry-standard format that any UI/IT team can consume
- Swagger UI or Redoc can render it as interactive documentation
- Self-contained single file, easy to version control
- Supports all existing endpoints with clear request/response schemas

**Cons**:
- OpenAPI YAML is verbose; requires careful schema design for complex nested objects (e.g., `filter_base`, `description_metadata`)
- No built-in UI-step mapping; must be documented in `x-` extensions or separate markdown
- Harder to maintain if endpoints evolve frequently

**Effort**: Medium

### Approach 2: OpenAPI + Separate UI Mapping Document

Create `openapi.yaml` for the API spec plus a separate Markdown file (`docs/api-contract.md`) that maps each endpoint to the UI steps and components.

**Pros**:
- Clean separation of concerns: API spec + UI mapping
- The OpenAPI file stays focused on technical details; the Markdown file adds business context
- Easier to update the UI mapping without touching the OpenAPI spec

**Cons**:
- Two files to maintain; risk of drift between them
- Requires manual synchronization when endpoints change

**Effort**: Medium

### Approach 3: Markdown Contract with OpenAPI Embedded

Create a single Markdown file (`docs/api-contract.md`) with OpenAPI-style schemas embedded in fenced code blocks, plus inline UI mapping sections.

**Pros**:
- Single file, easy to read and maintain
- Can include rich narrative and UI mapping alongside technical specs
- No external tooling required to render

**Cons**:
- Not machine-readable; cannot be imported into Swagger UI or Redoc
- Less standard; UI/IT team may need to adapt
- Harder to validate against actual implementation

**Effort**: Small

## Decision

**Approach 3: Markdown Contract with OpenAPI-Style Schemas** was chosen. The contract will be a single Markdown file (`docs/api-contract.md`) with:
- OpenAPI-style schemas embedded in fenced code blocks for all 14+ endpoints
- Inline UI mapping sections connecting endpoints to the 3-step wizard flow
- Structured response envelope (`status`, `data`, `warnings`, `errors`, `meta`)
- Error handling (400, 422, 500 status codes)
- Input validation rules (max 15 surveys, PPP validation, filter base JSON, pop share threshold)
- Rich narrative and UI mapping alongside technical specs

The file will be placed in `docs/api-contract.md`. If needed later, an OpenAPI YAML spec can be generated from this Markdown document.

## Next Steps

1. **Create the Markdown API contract** in `docs/api-contract.md` with:
   - OpenAPI-style schemas embedded in fenced code blocks for all 14+ endpoints
   - Inline UI mapping sections connecting endpoints to the 3-step wizard flow
   - Structured response envelope schema
   - Error handling (400, 422, 500 status codes)
   - Input validation rules
   - Session management (in-memory, 1-hour TTL)
   - CORS filter documentation
   - Release-aware endpoints

2. **Update the README.md** to reference the new API contract and provide instructions for viewing it.

3. **Consider generating an OpenAPI YAML spec** from the Markdown document if interactive documentation is needed later.

4. **Validate the API contract** against the existing implementation to ensure accuracy.