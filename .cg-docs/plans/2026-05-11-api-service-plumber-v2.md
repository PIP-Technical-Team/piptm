---
date: 2026-05-11
title: "API Service Layer — Plumber Endpoints (v2)"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-05-08-api-service-architecture.md"
supersedes: ".cg-docs/plans/2026-05-08-api-service-plumber.md"
language: "R"
estimated-effort: "medium"
tags: [api, plumber, endpoints, phase-4]
---

# Plan: API Service Layer — Plumber Endpoints (v2)

## Objective

Implement a Plumber-based API service that exposes `table_maker()` and
supporting discovery functions to the PIP platform UI and public consumers.
Eight thin endpoints, a structured response envelope, and a global error
handler — all delegating to existing tested {piptm} functions.

## Context

- `table_maker()`, `pip_lookup()`, `piptm_manifest()`, `piptm_manifests()`,
  `piptm_current_release()`, `pip_measures()`, and `.VALID_DIMENSIONS` are
  all implemented and tested.
- The brainstorm decided on Approach A (monolithic router): one `plumber.R`
  with a `helpers.R` for shared utilities.
- No authentication, rate limiting, or multi-worker support in v1.
- Max 15 surveys per request (UI hard cap, enforced server-side).
- Response time target: ≤3 seconds for 15 surveys.

### Revision Notes (v2)

This plan supersedes `2026-05-08-api-service-plumber.md`. Changes address
7 findings from the `/cg-plan-review` session (2 P1, 5 P2, 4 P3):

| Finding | Change |
|---------|--------|
| P1.1 | `resolve_release()` now aborts on invalid input instead of returning a dual-type (string or list). Handlers no longer need `if (is.list(release))` guards. |
| P1.2 | `coerce_poverty_lines()` folded into `validate_table_input()`. All input validation in one function. |
| P2.1 | `/surveys` handler wraps response in `capture_with_warnings()` and verifies list-column serialization. Risk table updated. |
| P2.2 | `.plumber_dir` replaced with `system.file("plumber", "helpers.R", package = "piptm")`. |
| P2.3 | `plumber` added to `Suggests:` in Step 1, not Step 5. |
| P2.4 | `pr_set_error()` simplified to always return 500. Handler-level `capture_with_warnings()` owns all domain errors (422). |
| P2.5 | `/surveys` handler wrapped in `capture_with_warnings()`. |
| P3.1–4 | `uniqueN()`, `data.table()` in `/measures`, uniform `@serializer`, `@param` annotations added. |

## Requirements

| ID  | Requirement                                              | Source        |
|-----|----------------------------------------------------------|---------------|
| R1  | `/table` GET+POST: compute measures, max 15 pip_ids     | brainstorm    |
| R2  | `/lookup` GET: resolve triplets to pip_ids               | brainstorm    |
| R3  | `/surveys` GET: full manifest rows for a release         | brainstorm    |
| R4  | `/releases` GET: list release IDs + current              | brainstorm    |
| R5  | `/measures` GET: list measure names and families         | brainstorm    |
| R6  | `/dimensions` GET: list valid dimension names            | brainstorm    |
| R7  | `/health` GET: server status                             | brainstorm    |
| R8  | Structured response envelope (status/data/warnings/errors/meta) | brainstorm |
| R9  | Global error handler: unexpected errors → 500 only       | review P2.4   |
| R10 | Warning capture: partial matches echoed in response      | brainstorm    |
| R11 | Input validation before delegation (max 15, valid measures, etc.) | brainstorm |
| R12 | CORS filter for browser access                           | implied       |
| R13 | `release` defaults to current when omitted; API-internal param, not user-facing | brainstorm |
| R14 | `plumber` declared in DESCRIPTION `Suggests:` before any API code | review P2.3 |

## Implementation Steps

### Step 1: Dependency & Response Helpers

- **Requirements**: R8, R9, R10, R11, R13, R14
- **Files**: `DESCRIPTION` (modify), `inst/plumber/helpers.R` (new)
- **Details**:

  **1a. Add `plumber` to DESCRIPTION `Suggests:`** (R14)

  **1b. `api_response(data, warnings = character(), meta = list())`** (R8)

  Builds the success envelope:
  `list(status = "success", data = data, warnings = warnings, errors = character(), meta = meta)`.

  **1c. `api_error(errors, status_code, res)`** (R8)

  Builds error envelope, sets `res$status`. Returns:
  `list(status = "error", data = NULL, warnings = character(), errors = errors, meta = list())`.

  **1d. `resolve_release(release)`** (R13, addresses P1.1)

  - If `release` is NULL: returns `piptm_current_release()`.
  - Otherwise: validates against `names(piptm_manifests())`.
  - If invalid: **`cli_abort()`** (not a list return).
  - Always returns a string. Errors are caught by `capture_with_warnings()`
    in the calling handler.

  **1e. `validate_table_input(pip_id, measures, poverty_lines, by)`** (R11, addresses P1.2)

  Checks:
  - `pip_id` is character, length 1–15
  - `measures` is character, all in `names(pip_measures())`
  - `poverty_lines`: if not NULL, coerce via `as.numeric()` — if coercion
    introduces NA, add to errors. If valid, must be positive and finite.
    (Poverty line coercion is now inside this function, not separate.)
  - `by` (if not NULL) is subset of `.VALID_DIMENSIONS`

  Returns `list(valid = TRUE/FALSE, errors = character(), poverty_lines = <coerced or NULL>)`.
  The coerced `poverty_lines` value is returned so the handler can use it
  without re-coercing.

  Note: `release` validation is handled by `resolve_release()`, not here.

  **1f. `validate_lookup_input(country_code, year, welfare_type)`** (R11)

  Checks:
  - All three non-NULL, same length
  - `welfare_type` all in `c("INC", "CON")`
  - `year` coercible to integer

  Returns `list(valid = TRUE/FALSE, errors = character())`.

  **1g. `capture_with_warnings(expr)`** (R10)

  Wraps `expr` in `withCallingHandlers()` (collect warnings) inside
  `tryCatch()` (catch errors). Returns:
  `list(result = ..., warnings = character(), error = NULL|character)`.

  This is the single error-catching boundary for all domain logic.
  `resolve_release()` aborts are caught here too.

- **Test Scenarios**:
  - ✅ `api_response()` produces correct envelope shape
  - ✅ `validate_table_input()` passes with valid inputs, returns coerced `poverty_lines`
  - 🛑 `validate_table_input()` rejects >15 pip_ids
  - 🛑 `validate_table_input()` rejects unknown measure names
  - ❌ `validate_table_input(poverty_lines = "abc")` returns `valid = FALSE`
  - ✅ `resolve_release(NULL)` returns `piptm_current_release()`
  - ✅ `resolve_release("20260401_TEST")` returns the value unchanged
  - ❌ `resolve_release("bogus")` aborts with `cli_abort()`
  - ✅ `capture_with_warnings()` collects cli warnings
  - ✅ `capture_with_warnings()` catches `resolve_release("bogus")` abort
- **Tests**: `tests/testthat/test-api-helpers.R`
- **Acceptance criteria**: All helper functions return predictable structures;
  100% unit test coverage on validation logic. `plumber` in DESCRIPTION.

### Step 2: Global Filters (`inst/plumber/plumber.R` — filter section)

- **Requirements**: R9, R12
- **Files**: `inst/plumber/plumber.R` (new — top section)
- **Details**:

  **2a. Source helpers** (addresses P2.2)

  ```r
  source(system.file("plumber", "helpers.R", package = "piptm"))
  ```

  **2b. CORS filter** (R12)

  Set `Access-Control-Allow-Origin: *`,
  `Access-Control-Allow-Methods: GET, POST, OPTIONS`,
  `Access-Control-Allow-Headers: Content-Type`. Handle OPTIONS preflight
  with 200 and empty body.

  **2c. Global error handler** (R9, addresses P2.4)

  Register via `pr_set_error(pr, function(req, res, err) { ... })`.
  **Always returns 500** with "Internal server error" (no stack trace).

  Rationale: all domain errors (422) and input errors (400) are caught at
  the handler level by `capture_with_warnings()` and `validate_*()`.
  Anything reaching the global handler is unexpected → 500.

  **2d. Request logger filter** (lightweight)

  `message()` with timestamp, method, path, elapsed time.

- **Test Scenarios**:
  - ✅ CORS headers present on all responses
  - ✅ OPTIONS returns 200 with no body
  - ❌ Unhandled error returns 500 with generic message (no stack trace)
- **Tests**: Covered in integration tests (Step 4)
- **Acceptance criteria**: Every response includes CORS headers; unhandled
  errors always produce 500; request log lines emitted to stdout.

### Step 3: Endpoint Handlers (`inst/plumber/plumber.R` — endpoints)

- **Requirements**: R1–R7, R13
- **Files**: `inst/plumber/plumber.R` (endpoint section)
- **Details**:

  All endpoints use `@serializer json list(na = "null")` uniformly (P3.3)
  and include `@param` annotations for OpenAPI spec generation (P3.4).

  **3a. `GET|POST /table`** (R1, R13)

  > **Vector parameters**: Plumber delivers repeated query params
  > (`?pip_id=A&pip_id=B`) as character vectors. For POST with JSON body,
  > arrays map directly. No comma-splitting needed.

  ```r
  #* @get /table
  #* @post /table
  #* @param pip_id:character Survey identifiers (max 15, repeatable)
  #* @param measures:character Measure names (repeatable)
  #* @param poverty_lines:numeric Poverty line values (optional, repeatable)
  #* @param by:character Disaggregation dimensions (optional, repeatable)
  #* @param release:character Release ID (optional, defaults to current)
  #* @serializer json list(na = "null")
  function(pip_id, measures, poverty_lines = NULL, by = NULL, release = NULL, res) {
    check <- validate_table_input(pip_id, measures, poverty_lines, by)
    if (!check$valid) return(api_error(check$errors, 400L, res))
    poverty_lines <- check$poverty_lines

    out <- capture_with_warnings(
      table_maker(pip_id = pip_id, measures = measures,
                  poverty_lines = poverty_lines, by = by,
                  release = resolve_release(release))
    )
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))

    release_used <- resolve_release(release)
    api_response(out$result, warnings = out$warnings,
                 meta = list(release = release_used,
                             n_surveys = data.table::uniqueN(out$result, by = "pip_id")))
  }
  ```

  Wait — `resolve_release()` is called inside `capture_with_warnings()` AND
  again for the meta. We should resolve once before the capture block and
  reuse. Since `resolve_release()` now aborts on invalid input, we wrap it:

  ```r
  function(pip_id, measures, poverty_lines = NULL, by = NULL, release = NULL, res) {
    check <- validate_table_input(pip_id, measures, poverty_lines, by)
    if (!check$valid) return(api_error(check$errors, 400L, res))
    poverty_lines <- check$poverty_lines

    out <- capture_with_warnings({
      rel <- resolve_release(release)
      table_maker(pip_id = pip_id, measures = measures,
                  poverty_lines = poverty_lines, by = by, release = rel)
    })
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))

    api_response(out$result, warnings = out$warnings,
                 meta = list(release = release %||% piptm_current_release(),
                             n_surveys = data.table::uniqueN(out$result, by = "pip_id")))
  }
  ```

  Note: `meta$release` uses `release %||% piptm_current_release()` outside
  the capture block because if we got here, `resolve_release()` succeeded
  inside the block, so this is safe. Alternatively, attach `rel` to `out`
  inside the block — either works. The simpler `%||%` form is fine since
  `piptm_current_release()` is a cheap lookup.

  **3b. `GET /lookup`** (R2, R13)

  ```r
  #* @get /lookup
  #* @param country_code:character Country codes (repeatable)
  #* @param year:integer Survey years (repeatable)
  #* @param welfare_type:character Welfare type: INC or CON (repeatable)
  #* @param release:character Release ID (optional, defaults to current)
  #* @serializer json list(na = "null")
  function(country_code, year, welfare_type, release = NULL, res) {
    year <- as.integer(year)
    check <- validate_lookup_input(country_code, year, welfare_type)
    if (!check$valid) return(api_error(check$errors, 400L, res))

    out <- capture_with_warnings({
      rel <- resolve_release(release)
      pip_lookup(country_code, year, welfare_type, rel)
    })
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))

    api_response(out$result, warnings = out$warnings,
                 meta = list(release = release %||% piptm_current_release()))
  }
  ```

  **3c. `GET /surveys`** (R3, R13, addresses P2.1 + P2.5)

  ```r
  #* @get /surveys
  #* @param release:character Release ID (optional, defaults to current)
  #* @serializer json list(na = "null")
  function(release = NULL, res) {
    out <- capture_with_warnings({
      rel <- resolve_release(release)
      piptm_manifest(rel)
    })
    if (!is.null(out$error)) return(api_error(out$error, 422L, res))

    api_response(out$result, warnings = out$warnings,
                 meta = list(release = release %||% piptm_current_release()))
  }
  ```

  Note on list-column serialization (P2.1): `piptm_manifest()` returns a
  `dimensions` list-column. `jsonlite::toJSON()` serializes each row's
  `dimensions` as a JSON array, which is the correct shape for the UI.
  **Verified — no conversion needed.** Risk table updated below.

  **3d. `GET /releases`** (R4)

  ```r
  #* @get /releases
  #* @serializer json list(na = "null")
  function() {
    api_response(list(
      releases = names(piptm_manifests()),
      current  = piptm_current_release()
    ))
  }
  ```

  **3e. `GET /measures`** (R5, addresses P3.2)

  ```r
  #* @get /measures
  #* @serializer json list(na = "null")
  function() {
    m <- pip_measures()
    api_response(data.table::data.table(
      measure = names(m), family = unname(m)
    ))
  }
  ```

  **3f. `GET /dimensions`** (R6)

  ```r
  #* @get /dimensions
  #* @serializer json list(na = "null")
  function() {
    api_response(.VALID_DIMENSIONS)
  }
  ```

  **3g. `GET /health`** (R7)

  ```r
  #* @get /health
  #* @serializer json list(na = "null")
  function() {
    api_response(list(status = "ok", release = piptm_current_release()))
  }
  ```

- **Test Scenarios**:
  - ✅ `/table` with valid inputs returns success envelope + data.table rows
  - ✅ `/table` with no `release` → `meta.release` equals current
  - ✅ `/lookup` resolves known triplets, echoes release in meta
  - ✅ `/surveys` returns full manifest rows; `dimensions` serializes as JSON arrays
  - ✅ `/releases` lists all loaded release IDs + current
  - ✅ `/measures` returns 19 measures with families (as data.table)
  - ✅ `/dimensions` returns valid dimension names
  - ✅ `/health` returns status ok + current release
  - 🛑 `/table` with >15 pip_ids returns 400
  - 🛑 `/table` with unknown measure returns 400
  - 🛑 `/table` with `poverty_lines = "abc"` returns 400
  - ❌ `/table` with poverty measures but no poverty_lines returns 422
  - ❌ `/table` with `release = "bogus"` returns 422
  - ❌ `/lookup` with mismatched vector lengths returns 400
- **Tests**: `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: All 8 endpoints return correct envelope shape;
  HTTP status codes match the error type; all responses include `meta.release`.

### Step 4: Integration Tests

- **Requirements**: R1–R14
- **Files**: `tests/testthat/test-api-endpoints.R` (new)
- **Details**:
  1. Use plumber's programmatic testing:
     ```r
     pr <- plumber::plumber$new(
       system.file("plumber", "plumber.R", package = "piptm")
     )
     ```
     Then `pr$call(make_req("GET", "/health"))` etc.
  2. Or use `httr2` against a background plumber process for E2E tests.
  3. Test matrix:
     - Each endpoint: happy path, missing required params, invalid values
     - `/table`: verify response shape matches `table_maker()` output
     - Release defaulting: omit `release` → `meta.release` = current
     - Release validation: `release = "bogus"` → 422
     - Error handler: unexpected errors → 500 (never 422 from global handler)
     - CORS: verify headers on success and error responses
     - Warning capture: request with partial-match surveys, verify warnings
     - Serialization: `/surveys` dimensions column → JSON arrays
  4. Use `withr::local_envvar()` to point at test manifests if needed, or
     rely on the live manifest already loaded in the test session.
- **Test Scenarios**:
  - ✅ Full round-trip: `/table` with 3 real pip_ids → check result shape
  - ✅ `/table` with no `release` param → `meta.release` equals `piptm_current_release()`
  - ✅ `/surveys` → `dimensions` field is a JSON array per row
  - 🛑 `/table` with 1 valid + 1 invalid pip_id → partial success + warning
  - ❌ `/table` with `release = "bogus"` → HTTP 422 with error message
  - ❌ Unexpected error → HTTP 500, no stack trace
- **Tests**: `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: ≥32 integration test cases covering all endpoints,
  error paths, release default/invalid behaviour, and serialization. All green.

### Step 5: Launch Script and Documentation

- **Requirements**: (operational)
- **Files**: `inst/plumber/run.R` (new), `man/` updates
- **Details**:
  1. `inst/plumber/run.R` — convenience launcher:
     ```r
     library(piptm)
     pr <- plumber::plumber$new(
       system.file("plumber", "plumber.R", package = "piptm")
     )
     pr$run(host = "0.0.0.0", port = 8080)
     ```
  2. Exported convenience function (optional):
     ```r
     #' @export
     run_api <- function(port = 8080, host = "0.0.0.0") { ... }
     ```
  3. roxygen2 docs for `run_api()`.
  4. Brief section in README.md about starting the API.
- **Test Scenarios**:
  - ✅ `run.R` sources cleanly without errors
- **Tests**: Manual verification
- **Acceptance criteria**: A user can start the API with one command
  (`piptm::run_api()` or `Rscript inst/plumber/run.R`).

## Error Handling Architecture

Two layers, clearly separated (addresses P2.4):

```
Request arrives
  │
  ├─ validate_*() → bad input? → api_error(400) → return
  │
  ├─ capture_with_warnings({
  │     resolve_release()  ← aborts if invalid release
  │     table_maker()      ← aborts on domain errors
  │   })
  │   ├─ error caught   → api_error(422) → return
  │   └─ success        → api_response() → return
  │
  └─ (anything uncaught falls to pr_set_error)
      └─ ALWAYS 500, generic message, no stack trace
```

- **Handler level**: `validate_*()` → 400. `capture_with_warnings()` → 422.
- **Global level**: `pr_set_error()` → 500 only. Safety net for bugs.

## Testing Strategy

- **Unit tests** (Step 1): All helper functions tested in isolation with
  synthetic inputs. No network, no Arrow data needed.
- **Integration tests** (Step 4): Hit plumber router programmatically against
  live manifest data. Test the full request → response cycle.
- **Manual smoke test**: Start server, hit endpoints with curl/browser, verify
  JSON shapes and timing.

## Documentation Checklist

- [ ] roxygen2 for `run_api()` (exported convenience)
- [ ] README section: "Running the API"
- [ ] Inline comments in `plumber.R` for filter logic
- [ ] `@param` annotations on all endpoints for OpenAPI spec generation
- [ ] OpenAPI spec auto-generated by plumber

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| `piptm_manifest()` `dimensions` list-column serialization | `/surveys` response shape | Verified: `jsonlite` serializes list-columns as JSON arrays per row — correct shape. No conversion needed. |
| `table_maker()` warnings not captured (swallowed by plumber internals) | Silent partial failures | `capture_with_warnings()` wraps at handler level, before plumber touches the result |
| Single-threaded R blocks all requests during long computation | UI unresponsive for other users | Acceptable at v1 (≤3s per request); document as known limitation |
| `resolve_release()` abort not caught | Unstructured 500 instead of 422 | Always called inside `capture_with_warnings()` block |

## Out of Scope

- Authentication / API keys
- Rate limiting
- Multi-worker / load balancing
- Caching (pre-computed results)
- CSV export endpoint (deferred)
- API versioning (`/v1/...`)
- WebSocket / streaming
