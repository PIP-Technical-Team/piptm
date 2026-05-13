---
plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md
date: 2026-05-13
depth: thorough
findings:
  P0.1: fixed
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: fixed
  P1.6: fixed
  P1.7: fixed
  P1.8: fixed
  P1.9: fixed
  P1.10: fixed
  P2.1: open
  P2.2: open
  P2.3: open
  P2.4: open
  P2.5: open
  P2.6: open
  P2.7: open
  P2.8: open
  P2.9: open
  P2.10: open
  P3.1: open
  P3.2: open
  P3.3: open
  P3.4: open
  P3.5: open
---

## Review Report

**Review depth**: thorough
**Files reviewed**: 4 (`inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `tests/testthat/test-api-helpers.R`, `DESCRIPTION`)
**Findings**: 34 (P0: 1, P1: 10, P2: 10, P3: 5)

---

### P0 — BLOCKING (immediate remediation required)

- **[P0.1]** [cg-adversarial] `plumber.R:44` — OPTIONS preflight returns `200 {}` instead of `204 ""`
  **Why**: The OPTIONS handler returns `list()` serialised as `{}` with status 200. RFC 7231 permits this, but many browser `fetch()` CORS implementations (Chrome ≥ 124 with `Sec-Fetch-Mode: cors`) treat a non-empty body or status 200 on preflight as a violation and reject the request. The API will appear to work from curl/Postman but fail silently from web clients — i.e. the PIP platform UI.
  **Fix**:
  ```r
  if (identical(req$REQUEST_METHOD, "OPTIONS")) {
    res$status <- 204L
    return("")
  }
  ```

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-adversarial + cg-code-quality] `helpers.R:283` — `capture_with_warnings()` sentinel `.__error__` causes silent data loss if `table_maker()` legitimately returns `list(.__error__ = ...)`.
  **Why**: The sentinel detection `is.list(result) && !is.null(result$.__error__)` misidentifies any valid result containing that key as a caught error, returns HTTP 422, and discards the valid data with no diagnostic.
  **Fix**: Replace list-key sentinel with a typed S3 wrapper:
  ```r
  # on error:
  structure(list(message = conditionMessage(e)), class = "cw_error")
  # detection:
  if (inherits(result, "cw_error")) { ... }
  ```

- **[P1.2]** [cg-adversarial + cg-reproducibility] `plumber.R:100,148,165` — `meta.release` can disagree with the release actually used for computation (race condition).
  **Why**: `rel` is resolved inside `capture_with_warnings()` at time T₁. `meta.release` is populated via `rlang::%||%(release, piptm_current_release())` at time T₂. If the current release changes between T₁ and T₂, `data` reflects release A but `meta.release` reports release B — corrupting any downstream cache keyed on release.
  **Fix**: Return `rel` from inside the capture block:
  ```r
  out <- capture_with_warnings({
    rel <- resolve_release(release)
    list(rel = rel, data = piptm::table_maker(..., release = rel))
  })
  # use out$result$rel in meta
  ```
  Apply the same pattern to `/lookup` and `/surveys`.

- **[P1.3]** [cg-adversarial] `plumber.R:112` — `data.table::uniqueN(out$result, by = "pip_id")` is outside all error boundaries; an unexpected result shape (no `pip_id` column, non-data.table) causes an opaque 500.
  **Why**: The call is in the `api_response()` meta argument, after the `capture_with_warnings` block, so any error here escapes to the global handler.
  **Fix**:
  ```r
  n_surveys = tryCatch(
    data.table::uniqueN(out$result, by = "pip_id"),
    error = function(e) NA_integer_
  )
  ```

- **[P1.4]** [cg-code-quality] `helpers.R:155` — `validate_table_input()` silently accepts explicit `NA` in `poverty_lines` with a misleading "not positive finite" error.
  **Why**: The guard `anyNA(coerced_pl) && !anyNA(poverty_lines)` short-circuits when the input already contains `NA`, letting it fall through to the finite/positive check with the wrong message. NA should be rejected with a dedicated message.
  **Fix**: Check `anyNA(coerced_pl)` unconditionally after coercion; emit a dedicated "contains NA or non-numeric" message before the positivity check.

- **[P1.5]** [cg-code-quality + cg-data-quality] `helpers.R:197` + `plumber.R:120` — `year` is pre-coerced to `NA_integer_` in the `/lookup` handler before `validate_lookup_input()` sees it, bypassing the NA detection guard in the validator. `NA_integer_` propagates silently to `pip_lookup()`.
  **Why**: The validator checks `anyNA(coerced_year) && !anyNA(year)`, but after the handler's pre-coercion both are `NA_integer_`, so the condition is always `FALSE`. A bad string like `"twenty-twelve"` passes validation and reaches domain code.
  **Fix**: Either remove the pre-coercion from the handler and handle it inside the validator (returning coerced year like `validate_table_input()` returns `poverty_lines`), or change the validator to reject any `anyNA(year)` unconditionally.

- **[P1.6]** [cg-code-quality] `helpers.R:196` + `plumber.R:170` — `piptm:::.VALID_DIMENSIONS` is accessed via `:::` from a sourced file.
  **Why**: `:::` is explicitly prohibited in production code, raises `R CMD check` warnings, and is fragile — if the internal symbol is renamed, it fails at runtime with no build-time detection. The `/dimensions` endpoint exposes this as a public API value while treating it as internal.
  **Fix**: Export a thin accessor `piptm_valid_dimensions()` returning `.VALID_DIMENSIONS` and use `piptm::piptm_valid_dimensions()` in both files. This mirrors `/measures` using `piptm::pip_measures()`.

- **[P1.7]** [cg-reproducibility] `plumber.R:24` — `system.file()` returns `""` silently if `piptm` is not loaded; `source("")` produces a cryptic error with no hint about the real cause.
  **Why**: A developer starting the API without `devtools::load_all()` or `library(piptm)` first gets `cannot open file '': No such file or directory` with no mention of the package.
  **Fix**:
  ```r
  helpers_path <- system.file("plumber", "helpers.R", package = "piptm")
  if (!nzchar(helpers_path)) {
    stop("Could not locate helpers.R — ensure piptm is installed or loaded via devtools::load_all().")
  }
  source(helpers_path)
  ```

- **[P1.8]** [cg-reproducibility] `plumber.R:47–55` — `@plumber` decorator and `pr$setErrorHandler()` require plumber ≥ 1.1.0. With no version pin and no `renv.lock`, a deployment with plumber 0.x silently ignores the `@plumber` block, leaving the API without the global error handler.
  **Fix**: Add `plumber (>= 1.1.0)` to `DESCRIPTION Suggests:` and add a startup version assertion:
  ```r
  if (utils::packageVersion("plumber") < "1.1.0")
    stop("plumber >= 1.1.0 required. Found: ", utils::packageVersion("plumber"))
  ```

- **[P1.9]** [cg-testing] `test-api-helpers.R` — `validate_table_input()` boundary at exactly 15 `pip_id`s is not tested (the check is `> 15L`, so 15 should pass; a regression to `>= 15L` would go undetected).
  **Fix**: Add `expect_true(validate_table_input(pip_id = paste0("S_", 1:15, "_ABC_INC_ALL"), measures = "mean")$valid)`.

- **[P1.10]** [cg-data-quality] `plumber.R:120` — no allowlist regex on `pip_id` values; `pip_id` is used to construct Arrow partition paths, making path-traversal a realistic risk (special characters like `/`, `..`, null bytes).
  **Fix**: Validate each `pip_id` element against `^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$` in `validate_table_input()` and return 400 on violation.

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-adversarial] `helpers.R:88` — `api_error()` HTTP status mutation is a no-op on plain lists (no-op in unit tests; correct only on plumber R6 `res`).
  **Why**: `res$status <- status_code` on a list creates a local copy; the caller's `res` is unchanged. Tests using `fake_res <- list(...)` do not catch this. In production, plumber passes an R6 object so it works — but the test gap hides future regressions.
  **Fix**: Add guard `if (is.environment(res)) res$status <- status_code`. Consolidate test coverage on a `new.env()` mock for all mutation assertions.

- **[P2.2]** [cg-adversarial] `plumber.R:73` — logger `elapsed` line is never written if `plumber::forward()` throws (no `on.exit()` guard).
  **Why**: Failed requests — the ones most worth logging — leave no trace.
  **Fix**:
  ```r
  function(req) {
    start <- proc.time()[["elapsed"]]
    on.exit({
      elapsed <- round(proc.time()[["elapsed"]] - start, 3L)
      message(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3"), " ", req$REQUEST_METHOD, " ", req$PATH_INFO, " [", elapsed, "s]")
    }, add = TRUE)
    plumber::forward()
  }
  ```

- **[P2.3]** [cg-architecture] `plumber.R:47–55` — global error handler overrides 4xx codes that plumber may have already set (e.g. 404 Not Found), replacing them all with 500.
  **Why**: A request to an unknown route (`GET /nonexistent`) would return 500 instead of 404.
  **Fix**: Preserve 4xx codes plumber already set:
  ```r
  pr$setErrorHandler(function(req, res, err) {
    if (!is.null(res$status) && res$status >= 400L && res$status < 500L) {
      return(list(status = "error", data = NULL, warnings = character(),
                  errors = conditionMessage(err), meta = list()))
    }
    res$status <- 500L
    list(status = "error", data = NULL, warnings = character(),
         errors = "Internal server error.", meta = list())
  })
  ```

- **[P2.4]** [cg-code-quality] `helpers.R:~220` — `@param expr` docstring incorrectly states `(passed with substitute(); do not wrap in quote())`. The function uses standard lazy evaluation, not `substitute()`.
  **Fix**: Replace with: *"An unevaluated expression; lazily evaluated inside `withCallingHandlers`/`tryCatch`."*

- **[P2.5]** [cg-code-quality] `plumber.R:116,152,167` — `rlang::\`%||\`` used three times; verbose and couples the sourced file to `rlang`.
  **Fix**: Define once in `helpers.R`:
  ```r
  `%||%` <- function(x, y) if (is.null(x)) y else x
  ```

- **[P2.6]** [cg-code-quality] `plumber.R:73` — logger filter has signature `function(req)` while CORS filter has `function(req, res)`. Inconsistent; adding response tagging later requires a signature change.
  **Fix**: Change to `function(req, res)`.

- **[P2.7]** [cg-testing] `test-api-helpers.R:8` — `source(helpers_path)` at test-file top level pollutes `globalenv()` for the whole test session; incompatible with future parallel test execution.
  **Fix**: Add a comment acknowledging the global-env side effect and disable parallelism for this file, or wrap in `local({ source(..., local = TRUE); ... })`.

- **[P2.8]** [cg-data-quality] `helpers.R:197` — `validate_lookup_input()` does not check `year` for a plausible range; `year = 0`, `year = -500`, `year = 9999` all pass.
  **Fix**: Add a range check: `year < 1900L | year > 2100L` → 400 with clear message.

- **[P2.9]** [cg-reproducibility] `renv.lock` does not exist. Project essential rules require lockfiles to be version-controlled.
  **Fix**: Run `renv::init(); renv::snapshot()` and commit `renv.lock`. Add `chore(deps): initialise renv` commit.

- **[P2.10]** [cg-performance] `validate_table_input()` calls `pip_measures()` → `unlist(.MEASURE_REGISTRY)` on every request, allocating a new named character vector each time.
  **Fix**: Pre-compute `.VALID_MEASURES <- unlist(.MEASURE_REGISTRY, use.names = TRUE)` in `R/measures.R`; make `pip_measures()` return the constant directly.

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-documentation] `plumber.R` — the 400/422/500 status convention is not documented in one place. Readers must infer the contract from scattered code.
  **Fix**: Add a comment block at the start of the endpoints section:
  ```r
  # Error status convention:
  #   400 — invalid input (validate_* catches before domain logic)
  #   422 — well-formed request, domain/data error (capture_with_warnings catches)
  #   500 — unhandled internal error (global handler, see 2c)
  ```

- **[P3.2]** [cg-testing] `test-api-helpers.R` — `capture_with_warnings()` with warnings emitted *before* an error is not tested (both `warnings` and `error` should be non-empty simultaneously).
  **Fix**: Add: `out <- capture_with_warnings({ warning("w1"); stop("err1") }); expect_true(!is.null(out$error)); expect_true("w1" %in% out$warnings)`.

- **[P3.3]** [cg-testing] `test-api-helpers.R` — `validate_table_input()` with `poverty_lines = 0` (zero, boundary of `> 0` check) is not tested.
  **Fix**: Add test asserting `valid == FALSE` for `poverty_lines = 0`.

- **[P3.4]** [cg-reproducibility] `plumber.R:64` — `format(Sys.time(), "%Y-%m-%dT%H:%M:%S")` truncates to whole seconds; concurrent requests within the same second get identical timestamps, making log ordering ambiguous.
  **Fix**: Use `"%Y-%m-%dT%H:%M:%OS3"` for millisecond precision.

- **[P3.5]** [cg-architecture] `plumber.R:75` — the dual `@get`/`@post` decorator pattern is not commented; a future maintainer may "fix" it to a single decorator.
  **Fix**: Add comment: `# Both GET and POST registered — plumber v1 supports multiple @get/@post decorators on one handler`.

---

### ✅ Passed

- **cg-version-control**: No secrets, credentials, or sensitive data. Conventional commit messages are well-formed. `inst/plumber/` correctly absent from `.Rbuildignore`. `plumber` in `Suggests:` is correct.
- **cg-architecture**: `inst/plumber/` sourced-file approach is idiomatic and correct. `plumber` in `Suggests:` (not `Imports:`) is correct. `@get` + `@post` dual-decorator syntax is valid plumber v1.
- **cg-performance**: No issues with CORS filter, logger syscalls, `proc.time()`, `uniqueN`, or `piptm_current_release()` call cost. Only `pip_measures()` allocation and `<<- c(...)` accumulation pattern flagged (P2.10 and advisory).
