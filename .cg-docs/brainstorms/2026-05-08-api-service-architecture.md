---
date: 2026-05-08
title: "API Service Architecture for Table Maker"
status: decided
scope: "Standard"
chosen-approach: "Approach A — Monolithic Plumber Router"
tags: [api, plumber, endpoints, phase-4]
---

# API Service Architecture for Table Maker

## Context

The computation engine (`table_maker()`) is complete and optimised. The next
step is a Plumber-based API service layer that connects the PIP platform UI
to the engine. The existing brainstorm
(`2026-04-27-table-maker-input-parameters.md`) defined the `/table` and
`/lookup` endpoint signatures and URL patterns. This brainstorm expands that
to the full API surface.

## Requirements

1. **8 endpoints** covering computation, discovery, and health
2. **Public API** — no authentication for v1, but input validation enforced
3. **Single R process** — no multi-worker or load balancing in v1
4. **≤3 second response time** for up to 15 surveys (UI hard cap)
5. **Partial success** — invalid pip_ids are skipped with warnings echoed in response metadata
6. **Max 15 surveys per request** — enforced server-side
7. **API serves manifest** — UI gets survey/release data from the API, not from files
8. **`release` is an API parameter** — passed to all relevant endpoints
9. **Structured response envelope** — every response has `status`, `data`, `warnings`, `errors`
10. **Full manifest rows** returned from `/surveys` (no field filtering)

## Out of Scope (v1)

- Authentication / API keys
- Rate limiting
- Multi-worker / load balancing
- Caching layer (pre-computed results)
- Websocket / streaming responses
- API versioning (`/v1/...`)
- CSV export (deferred — UI can convert JSON client-side)

## Approaches Considered

### Approach A: Monolithic Plumber Router (Chosen)

Single `plumber.R` file with all endpoints defined inline. Thin handlers
that validate inputs and delegate to existing `{piptm}` functions. A
separate `helpers.R` for reusable input coercion and response builders.

```
inst/plumber/
  plumber.R      # Filters + all 8 endpoints (~150–200 lines)
  helpers.R      # Input validation, response envelope, error formatting
```

**Pros**:
- Simple — one file to read, all handlers 5–15 lines
- All existing functions (`table_maker`, `pip_lookup`, `piptm_manifest`,
  `pip_measures`) already tested; handlers just wrap them
- Easy to test with plumber's test client or `httr`
- Refactor to sub-routers is trivial if endpoint count grows

**Cons**:
- Single file gets crowded beyond ~15 endpoints (not a concern at 8)

**Effort**: Small-to-medium (2–3 days)

### Approach B: Modular Router with Mounted Sub-Routers

Split into `routes/table.R`, `routes/discovery.R`, etc. mounted onto a
parent router.

**Pros**: Clean separation, scales well.
**Cons**: More boilerplate, plumber `mount()` quirks with filter
inheritance, overkill for 8 endpoints.

**Effort**: Medium (3–4 days)

### Approach C: Package-Exported API Factory (`piptm_api()`)

Programmatically build the router via R6/plumber API.

**Pros**: Fully testable, customizable.
**Cons**: Less readable, harder for contributors unfamiliar with plumber's
programmatic interface.

**Effort**: Medium (3–4 days)

## Decision

**Approach A** — Monolithic Plumber Router. With 8 thin endpoints all
delegating to tested functions, a single router file is the right size.
If the count doubles later, refactoring to Approach B is a 1-hour task.

## Endpoint Inventory

| Endpoint | Method | Purpose | Handler delegates to |
|---|---|---|---|
| `/table` | GET, POST | Compute measures (max 15 surveys) | `table_maker()` |
| `/lookup` | GET | Resolve triplets → pip_ids | `pip_lookup()` |
| `/surveys` | GET | List surveys for a release (full manifest rows) | `piptm_manifest(release)` |
| `/releases` | GET | List available release IDs + current | `names(piptm_manifests())` + `piptm_current_release()` |
| `/measures` | GET | List valid measure names and families | `pip_measures()` |
| `/dimensions` | GET | List valid disaggregation dimensions | `.VALID_DIMENSIONS` |
| `/health` | GET | Server status | `list(status = "ok")` |

## Response Envelope

Every response follows this structure:

```json
{
  "status": "success",
  "data": [...],
  "warnings": ["pip_id 'X' not found in manifest — skipped"],
  "errors": [],
  "meta": {
    "release": "20260401",
    "n_surveys": 15,
    "elapsed_ms": 1230
  }
}
```

Error responses (400/422):

```json
{
  "status": "error",
  "data": null,
  "warnings": [],
  "errors": ["Poverty measures require `poverty_lines`."],
  "meta": {}
}
```

## Error Handling Strategy

- A global plumber filter wraps all handlers in `tryCatch()`
- `cli_abort()` errors (from `table_maker()` validators) → HTTP 422 + structured error
- Missing/malformed parameters → HTTP 400
- Unexpected errors → HTTP 500 + generic message (no stack trace in response)
- Warnings from `table_maker()` (partial matches, missing dimensions) are captured
  via `withCallingHandlers()` and included in `response$warnings`

## Input Validation (API-level, before `table_maker()`)

| Parameter | Validation |
|---|---|
| `pip_id` | Required for `/table`. Character vector, max length 15. |
| `measures` | Required for `/table`. Must be subset of `names(pip_measures())`. |
| `poverty_lines` | Coerce to numeric. Must be positive finite values. |
| `by` | Optional. Must be subset of `.VALID_DIMENSIONS`. |
| `release` | Optional. Must match a loaded release ID. |
| `country_code` | Required for `/lookup`. Character vector. |
| `year` | Required for `/lookup`. Coerce to integer. |
| `welfare_type` | Required for `/lookup`. Must be "INC" or "CON". |

## Data Flow

```
PIP UI (browser)
  │
  ├─ On load: GET /releases → pick current release
  ├─ On load: GET /surveys?release=X → populate survey picker
  ├─ On load: GET /measures → populate measure picker
  ├─ On load: GET /dimensions → populate dimension picker
  │
  └─ On submit: GET /table?pip_id=A&pip_id=B&measures=headcount&...
       │
       ▼
  Plumber (single R process)
       │
       ├─ Filter: CORS, error handler, request logging
       ├─ Handler: validate inputs → table_maker(...) → envelope response
       │
       ▼
  JSON response → UI renders table
```

## File Structure

```
inst/
  plumber/
    plumber.R          # Global filters + all endpoint handlers
    helpers.R          # validate_api_input(), api_response(), api_error()
tests/
  testthat/
    test-api-endpoints.R   # Integration tests via plumber test client
```

## Next Steps

1. Implement `helpers.R` — response envelope builder, input validators
2. Implement `plumber.R` — filters + 8 endpoints
3. Write integration tests using plumber's `pr_run()` test mode
4. Document endpoint contracts (OpenAPI/Swagger auto-generated by plumber)
5. Manual testing against live manifest + Arrow data
