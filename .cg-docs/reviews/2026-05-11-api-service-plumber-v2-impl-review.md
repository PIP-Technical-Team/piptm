---
plan: 2026-05-11-api-service-plumber-v2.md
reviewed-on: 2026-05-13
branch: api-endpoints
commits: 0024120..aa96146 (committed) + 9 uncommitted Step-5 files
depth: thorough
agents:
  - "@cg-code-quality"
  - "@cg-testing"
  - "@cg-documentation"
  - "@cg-version-control"
  - "@cg-reproducibility"
  - "@cg-performance"
  - "@cg-architecture"
  - "@cg-data-quality"
  - "@cg-learnings-researcher"
  - "@cg-adversarial"
scope:
  - inst/plumber/helpers.R
  - inst/plumber/plumber.R
  - inst/plumber/run.R
  - R/api.R
  - tests/testthat/test-api-endpoints.R
  - tests/testthat/test-api-helpers.R
  - DESCRIPTION
  - NAMESPACE
  - README.md
findings:
  P1:
    - id: P1.1
      status: fixed
    - id: P1.2
      status: fixed
    - id: P1.3
      status: fixed
  P2:
    - id: P2.1
      status: open
    - id: P2.2
      status: fixed
    - id: P2.3
      status: fixed
    - id: P2.4
      status: fixed
    - id: P2.5
      status: fixed
  P3:
    - id: P3.1
      status: fixed
    - id: P3.2
      status: fixed
    - id: P3.3
      status: fixed
---

# Implementation Review — API Service Layer (Plumber v2)

Reviewed against the completed implementation of
`.cg-docs/plans/2026-05-11-api-service-plumber-v2.md` (all 5 steps).
This is an **implementation review**; the prior file
`2026-05-11-api-service-plumber-v2-review.md` was a plan review and is
separate.

## Overall Assessment

The implementation is solid. The response-envelope contract is consistent, the
validation layer is defence-in-depth, the `capture_with_warnings()` sentinel
pattern is elegant, and the 85 integration tests drive the router
programmatically without needing a socket. No P0 findings. Three P1 items
require attention before the branch is merged; one of them (`rprojroot` in
`DESCRIPTION`) will cause `R CMD check` to fail on a clean machine.

---

## P1 — Significant (fix before merge)

### P1.1 · `rprojroot` used in tests but absent from `DESCRIPTION Suggests:`

**Files:** `tests/testthat/test-api-endpoints.R:70`,
`tests/testthat/test-api-helpers.R:10`  
**Agent:** @cg-reproducibility / @cg-version-control

Both test files call `rprojroot::find_package_root_file()` as a fallback when
`system.file()` returns an empty string.  `rprojroot` is not listed in
`DESCRIPTION`.  `R CMD check` will report a NOTE or ERROR for
`rprojroot` appearing in tests without a `Suggests:` entry, and the tests will
fail on a clean CI machine that does not have `rprojroot` pre-installed.

**Fix:** Add `rprojroot` to `Suggests:` in `DESCRIPTION`.

```dcf
Suggests:
    rprojroot,
    testthat (>= 3.0.0),
    withr,
    ...
```

---

### P1.2 · `/dimensions` endpoint uses triple-colon `piptm:::.VALID_DIMENSIONS`

**File:** `inst/plumber/plumber.R` — `GET /dimensions` handler  
**Agent:** @cg-architecture / @cg-code-quality

```r
# current — fragile internal access
function() {
  api_response(piptm:::.VALID_DIMENSIONS)
}
```

`:::` accesses non-exported internal objects.  The exported function
`piptm::pip_valid_dimensions()` already exists and is the correct public
interface.  Using `:::` means a rename or restructuring of `.VALID_DIMENSIONS`
silently breaks this endpoint at runtime.

**Fix:**

```r
function() {
  api_response(piptm::pip_valid_dimensions())
}
```

---

### P1.3 · Step 5 files are uncommitted

**Files:** `R/api.R`, `inst/plumber/run.R`, `man/run_api.Rd`, `README.md`,
`roadmap.json`, `.cg-docs/plans/2026-05-11-api-service-plumber-v2.md` (final
completed status), `.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md`  
**Agent:** @cg-version-control

`git status` shows 9 uncommitted items including all Step 5 deliverables.
The branch cannot be reviewed or merged until these are committed.

**Fix:** Commit Step 5 artifacts as a single logical commit, e.g.:

```
feat(api): add run_api() launcher, CLI script, and README docs (Step 5)
```

---

## P2 — Moderate (fix before next release)

### P2.1 · `DESCRIPTION` has placeholder author

**File:** `DESCRIPTION`  
**Agent:** @cg-documentation

```dcf
Authors@R: 
    person("First", "Last", email = "your@email.org", role = c("aut", "cre"))
```

This placeholder will propagate into `DESCRIPTION` bundled with the installed
package and any package tarballs.  Must be replaced with actual author
details before any release or deployment.

---

### P2.2 · `inst/plumber/run.R` comment claims `--port` CLI arg support

**File:** `inst/plumber/run.R:4`  
**Agent:** @cg-documentation

```r
# Rscript inst/plumber/run.R --port 8888
```

The launcher reads only env vars (`PIPTM_API_PORT` / `PIPTM_API_HOST`);
`commandArgs()` is never called.  A user following the comment will find it
silently ignored.

**Fix (option A):** Remove the misleading comment line:
```r
# Usage:
#   Rscript inst/plumber/run.R
```

**Fix (option B):** Implement minimal arg parsing:
```r
args  <- commandArgs(trailingOnly = TRUE)
port  <- as.integer(args[grep("^--port=", args)])
if (!length(port)) port <- as.integer(Sys.getenv("PIPTM_API_PORT", "8080"))
```

---

### P2.3 · `capture_with_warnings()` docstring incorrectly describes `substitute()`

**File:** `inst/plumber/helpers.R:330` (approx.)  
**Agent:** @cg-documentation

The `@param` doc reads:

> "An expression to evaluate (passed with `substitute()`; do not wrap in
> `quote()` at the call site)."

The function signature is `capture_with_warnings(expr)` and `expr` is a plain
eager argument — no `substitute()` or `quote()` is involved.  The instruction
"do not wrap in `quote()`" is actively misleading.

**Fix:** Replace with:

```r
#' @param expr An expression to evaluate. Passed and evaluated normally;
#'   do not wrap in `quote()`.
```

---

### P2.4 · Logger filter does not log 500 errors caught by the global errorHandler

**File:** `inst/plumber/plumber.R` — `@filter logger`  
**Agent:** @cg-architecture

The logger is structured as:

```r
start <- proc.time()[["elapsed"]]
plumber::forward()               # <- execution chain continues here
elapsed <- round(...)
message(...)
```

When the global `setErrorHandler` intercepts an uncaught error, plumber may
short-circuit the filter chain's return path.  The `elapsed` line and
`message()` call may not execute, leaving 500 errors unlogged.

This is a known plumber filter limitation.  Documenting it explicitly (or
switching to a `@hook postroute` / `@hook preroute` pair in plumber 2.x when
available) would close the gap.

**Fix (minimal):** Add a comment acknowledging the limitation:

```r
#* @filter logger
function(req) {
  start <- proc.time()[["elapsed"]]
  plumber::forward()
  # Note: if the global errorHandler fires for an uncaught exception, the
  # lines below may not execute for that request.  500-level errors are
  # therefore not guaranteed to appear in the log.
  elapsed <- round(proc.time()[["elapsed"]] - start, 3L)
  message(...)
}
```

---

### P2.5 · `unlist(body$data)` membership check is fragile in Block 7 tests

**File:** `tests/testthat/test-api-endpoints.R` — blocks 7 and 9  
**Agent:** @cg-testing

```r
expect_true("COL_2010_ECH_INC_ALL" %in% unlist(body$data))
```

`unlist()` flattens all columns; if the response schema adds a new column
whose values happen to contain `"COL_2010_ECH_INC_ALL"`, the test still passes
(false positive).  If the `pip_id` column is renamed, it fails (false
negative) without a helpful message.

**Fix:**

```r
expect_true("COL_2010_ECH_INC_ALL" %in% body$data$pip_id)
```

Applies to:
- `test-api-endpoints.R:394` (block 7, lookup round-trip)
- `test-api-endpoints.R:430` (block 7, unmatched triplet)
- `test-api-endpoints.R:467` (block 9, warning capture)

---

## P3 — Minor (nice to have)

### P3.1 · `run_api()` doc implies user must wire env-var override themselves

**File:** `R/api.R`  
**Agent:** @cg-documentation

The `@param port` doc shows:
> Override at deployment time via the `PIPTM_API_PORT` environment variable:
> `port = as.integer(Sys.getenv("PIPTM_API_PORT", unset = "8080"))`.

This implies users should pass `port = as.integer(...)` themselves.  In
practice, `inst/plumber/run.R` already does this.  A cross-reference would
clarify:

```r
#' @param port Integer. TCP port to listen on. Defaults to `8080`.
#'   The CLI launcher (`inst/plumber/run.R`) reads the `PIPTM_API_PORT`
#'   environment variable automatically.
```

---

### P3.2 · 15-survey limit is a bare magic number in validation and tests

**Files:** `inst/plumber/helpers.R`, `tests/testthat/test-api-endpoints.R`  
**Agent:** @cg-code-quality

`15L` appears directly in `validate_table_input()` and in test comments/error
messages.  Extracting it as a named constant (`MAX_SURVEYS_PER_REQUEST`) in
`helpers.R` would make it trivial to adjust and self-document:

```r
.MAX_SURVEYS_PER_REQUEST <- 15L
```

---

### P3.3 · Plumber version check executes after helpers are sourced

**File:** `inst/plumber/plumber.R:33–38`  
**Agent:** @cg-architecture

The plumber version check (`packageVersion("plumber") < "1.1.0"`) runs after
`source(.helpers_path)`.  If helpers load cleanly but the version check then
throws, the error message is less actionable than if the version was checked
first.  Reordering to fail-fast is a one-line change.

---

## Learnings Captured

The `cw_error` S3 sentinel for distinguishing error returns from a
`tryCatch`/`withCallingHandlers` wrapper (rather than a list-key sentinel) is
documented in:
`.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md`

The NULL-default pattern for plumber handler params (required to prevent
plumber from 500-ing before the handler body runs) should be added as a note
to that same document.
