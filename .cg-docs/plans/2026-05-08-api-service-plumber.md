---
date: 2026-05-08
title: "API Service Layer — Plumber Endpoints"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-05-08-api-service-architecture.md"
language: "R"
estimated-effort: "medium"
tags: [api, plumber, endpoints, phase-4]
---

# Plan: API Service Layer — Plumber Endpoints

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
| R9  | Global error filter: cli_abort → 422, bad params → 400, unexpected → 500 | brainstorm |
| R10 | Warning capture: partial matches echoed in response      | brainstorm    |
| R11 | Input validation before delegation (max 15, valid measures, etc.) | brainstorm |
| R12 | CORS filter for browser access                           | implied       |

## Implementation Steps

### Step 1: Response Helpers (`inst/plumber/helpers.R`)

- **Requirements**: R8, R9, R10, R11
- **Files**: `inst/plumber/helpers.R` (new)
- **Details**:
  1. `api_response(data, warnings = character(), meta = list())` — builds the
     success envelope: `list(status = "success", data = data, warnings = warnings, errors = character(), meta = meta)`.
  2. `api_error(errors, status_code, res)` — builds error envelope and sets
     `res$status` to the HTTP code. Returns `list(status = "error", data = NULL, warnings = character(), errors = errors, meta = list())`.
  3. `validate_table_input(pip_id, measures, poverty_lines, by, release)` —
     checks:
     - `pip_id` is character, length 1–15
     - `measures` is character, all in `names(pip_measures())`
     - `poverty_lines` (if not NULL) is numeric, positive, finite
     - `by` (if not NULL) is subset of `.VALID_DIMENSIONS`
     - `release` (if not NULL) is in `names(piptm_manifests())`
     Returns a list: `list(valid = TRUE/FALSE, errors = character())`.
  4. `validate_lookup_input(country_code, year, welfare_type)` — checks:
     - All three non-NULL, same length
     - `welfare_type` all in `c("INC", "CON")`
     - `year` coercible to integer
     Returns same structure.
  5. `coerce_poverty_lines(x)` — `as.numeric(x)`, errors on NA introduction.
  6. `capture_with_warnings(expr)` — wraps `expr` in `withCallingHandlers()`
     to collect `cli_warn` messages into a character vector while still
     executing. Returns `list(result = ..., warnings = ...)`.
- **Test Scenarios**:
  - ✅ `api_response()` produces correct envelope shape
  - ✅ `validate_table_input()` passes with valid inputs
  - 🛑 `validate_table_input()` rejects >15 pip_ids
  - 🛑 `validate_table_input()` rejects unknown measure names
  - ❌ `coerce_poverty_lines("abc")` returns appropriate error
  - ✅ `capture_with_warnings()` collects cli warnings
- **Tests**: `tests/testthat/test-api-helpers.R`
- **Acceptance criteria**: All helper functions return predictable structures;
  100% unit test coverage on validation logic.

### Step 2: Global Filters (`inst/plumber/plumber.R` — filter section)

- **Requirements**: R9, R12
- **Files**: `inst/plumber/plumber.R` (new — top section)
- **Details**:
  1. Source `helpers.R` at the top: `source(file.path(.plumber_dir, "helpers.R"))`.
  2. **CORS filter**: Set `Access-Control-Allow-Origin: *`,
     `Access-Control-Allow-Methods: GET, POST, OPTIONS`,
     `Access-Control-Allow-Headers: Content-Type`. Handle OPTIONS preflight
     with 200 and empty body.
  3. **Error handler filter**: Wrap `forward()` in `tryCatch()`:
     - Catch `rlang_error` (cli_abort): extract message, return 422 via `api_error()`.
     - Catch generic `error`: return 500 with "Internal server error" (no stack trace).
  4. **Request logger filter** (lightweight): `message()` with timestamp,
     method, path, elapsed time after forward completes.
- **Test Scenarios**:
  - ✅ CORS headers present on all responses
  - ✅ OPTIONS returns 200 with no body
  - ❌ Invalid endpoint returns 404
  - ❌ table_maker() abort caught and returned as 422
- **Tests**: Covered in integration tests (Step 4)
- **Acceptance criteria**: Every response includes CORS headers; errors never
  leak R stack traces; request log lines emitted to stdout.

### Step 3: Endpoint Handlers (`inst/plumber/plumber.R` — endpoints)

- **Requirements**: R1–R7
- **Files**: `inst/plumber/plumber.R` (endpoint section)
- **Details**:

  **3a. `GET|POST /table`**
  ```r
  #* @get /table
  #* @post /table
  #* @serializer json list(na = "null")
  function(pip_id, measures, poverty_lines = NULL, by = NULL, release = NULL, res) {
    poverty_lines <- coerce_poverty_lines(poverty_lines)
    check <- validate_table_input(pip_id, measures, poverty_lines, by, release)
    if (!check$valid) return(api_error(check$errors, 400L, res))

    out <- capture_with_warnings(
      table_maker(pip_id = pip_id, measures = measures,
                  poverty_lines = poverty_lines, by = by, release = release)
    )
    api_response(out$result, warnings = out$warnings,
                 meta = list(release = release %||% piptm_current_release(),
                             n_surveys = length(unique(out$result$pip_id))))
  }
  ```

  **3b. `GET /lookup`**
  ```r
  #* @get /lookup
  #* @serializer json
  function(country_code, year, welfare_type, release = NULL, res) {
    year <- as.integer(year)
    check <- validate_lookup_input(country_code, year, welfare_type)
    if (!check$valid) return(api_error(check$errors, 400L, res))

    out <- capture_with_warnings(
      pip_lookup(country_code, year, welfare_type, release)
    )
    api_response(out$result, warnings = out$warnings)
  }
  ```

  **3c. `GET /surveys`**
  ```r
  #* @get /surveys
  #* @serializer json list(na = "null")
  function(release = NULL, res) {
    mf <- piptm_manifest(release)
    # Convert list-column 'dimensions' to JSON-friendly format
    mf_out <- data.table::copy(mf)
    mf_out[, dimensions := lapply(dimensions, identity)]
    api_response(mf_out)
  }
  ```

  **3d. `GET /releases`**
  ```r
  #* @get /releases
  #* @serializer json
  function() {
    api_response(list(
      releases = names(piptm_manifests()),
      current  = piptm_current_release()
    ))
  }
  ```

  **3e. `GET /measures`**
  ```r
  #* @get /measures
  #* @serializer json
  function() {
    m <- pip_measures()
    api_response(data.frame(measure = names(m), family = unname(m),
                            stringsAsFactors = FALSE))
  }
  ```

  **3f. `GET /dimensions`**
  ```r
  #* @get /dimensions
  #* @serializer json
  function() {
    api_response(.VALID_DIMENSIONS)
  }
  ```

  **3g. `GET /health`**
  ```r
  #* @get /health
  #* @serializer json
  function() {
    list(status = "ok", release = piptm_current_release())
  }
  ```

- **Test Scenarios**:
  - ✅ `/table` with valid inputs returns success envelope + data.table rows
  - ✅ `/lookup` resolves known triplets
  - ✅ `/surveys` returns full manifest rows with dimensions as arrays
  - ✅ `/releases` lists all loaded release IDs
  - ✅ `/measures` returns 19 measures with families
  - ✅ `/dimensions` returns 6 dimension names
  - ✅ `/health` returns status ok
  - 🛑 `/table` with >15 pip_ids returns 400
  - 🛑 `/table` with unknown measure returns 400
  - ❌ `/table` with poverty measures but no poverty_lines returns 422
  - ❌ `/lookup` with mismatched vector lengths returns 400
- **Tests**: `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: All 8 endpoints return correct envelope shape;
  HTTP status codes match the error type.

### Step 4: Integration Tests

- **Requirements**: R1–R12
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
     - Error filter: confirm 422 for validation errors, 500 for unexpected
     - CORS: verify headers on success and error responses
     - Warning capture: request with partial-match surveys, verify warnings in envelope
  4. Use `withr::local_envvar()` to point at test manifests if needed, or
     rely on the live manifest already loaded in the test session.
- **Test Scenarios**:
  - ✅ Full round-trip: `/table` with 3 real pip_ids → check result shape
  - 🛑 `/table` with 1 valid + 1 invalid pip_id → partial success + warning
  - ❌ Server error simulation (if feasible with mock)
- **Tests**: `tests/testthat/test-api-endpoints.R`
- **Acceptance criteria**: ≥30 integration test cases covering all endpoints
  and error paths. All green.

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
  2. Add `plumber` to `Suggests:` in DESCRIPTION (it's not needed for
     the computation engine, only for serving).
  3. Exported convenience function (optional):
     ```r
     #' @export
     run_api <- function(port = 8080, host = "0.0.0.0") { ... }
     ```
  4. roxygen2 docs for `run_api()`.
  5. Brief section in README.md about starting the API.
- **Test Scenarios**:
  - ✅ `run.R` sources cleanly without errors
- **Tests**: Manual verification
- **Acceptance criteria**: A user can start the API with one command
  (`piptm::run_api()` or `Rscript inst/plumber/run.R`).

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
- [ ] OpenAPI spec auto-generated by plumber (comes free with annotations)

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Plumber serializes data.table list-columns (dimensions) incorrectly | `/surveys` response malformed | Explicit conversion to list-of-vectors before response |
| `table_maker()` warnings not captured (swallowed by plumber internals) | Silent partial failures | `capture_with_warnings()` wraps at handler level, before plumber touches the result |
| Single-threaded R blocks all requests during long computation | UI unresponsive for other users | Acceptable at v1 (≤3s per request); document as known limitation |

## Out of Scope

- Authentication / API keys
- Rate limiting
- Multi-worker / load balancing
- Caching (pre-computed results)
- CSV export endpoint (deferred)
- API versioning (`/v1/...`)
- WebSocket / streaming
