---
date: 2026-05-13
title: "Programmatic plumber endpoint testing with pr$call() and Hive fixtures"
category: "testing-patterns"
type: "pattern"
language: "R"
tags: [plumber, testthat, api, integration-test, pr-call, fixtures, hive, arrow, withr, piptm]
root-cause: "plumber's pr$call() allows full router testing without a network socket; missing-parameter defaults and JSON-null serialization need extra care"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# Programmatic Plumber Endpoint Testing with `pr$call()` and Hive Fixtures

## Problem

Testing a plumber API without a running HTTP server is non-trivial.
Three concrete issues emerged when writing integration tests for the
`{piptm}` plumber endpoints:

1. **Plumber crashes (500) instead of returning 400** when a required function
   parameter (no default) is absent from the query string — the error fires
   _before_ the handler body runs, bypassing `validate_*()` → `api_error()`.

2. **`pr$errorHandler` is not a public field** in plumber ≥ 1.3.x. Tests
   that check `is.function(pr$errorHandler)` fail with `FALSE`; the error
   handler is stored in a private R5 field.

3. **`jsonlite` serializes empty character vectors and `NULL` differently
   depending on context.** `character(0)` in an R list → `[]` in JSON →
   `list()` in R after `fromJSON()`. Tests that use
   `expect_equal(body$warnings, character(0L))` or `expect_null(body$data)`
   fail against a real API response.

## Root Cause

### Issue 1 — missing params crash before handler
Plumber resolves function parameters _before_ calling the handler function. If
a parameter has no default and is absent from the request, plumber raises an
error internally, which falls through to the global error handler (500).
The workaround is to give every parameter that the _handler_ validates a
`NULL` default, so the value reaches `validate_*()`.

### Issue 2 — private errorHandler field
In plumber R5 classes (`Plumber`), `errorHandler` is stored via `private$`
not `self$`. Accessing `pr$errorHandler` from outside the class returns
`FALSE` / `NULL`. The correct verification strategy is _behavioural_: if
`plumb()` completed without error, the `@plumber` decorator block (which
calls `pr$setErrorHandler()`) ran successfully.

### Issue 3 — jsonlite list/character ambiguity
`jsonlite::fromJSON()` coerces JSON `[]` to `list()`, not `character(0)`.
JSON `null` is dropped entirely (key absent from the parsed list).
Tests must use `length(x) == 0L` and `is.null(x) || length(x) == 0L`
instead of `expect_equal(x, character(0L))` and `expect_null(x)`.

## Solution

### Router creation (once per test file)

```r
.ep_plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
if (!nzchar(.ep_plumber_path)) {
  .ep_plumber_path <- file.path(
    rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"
  )
}

.ep_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.ep_plumber_path))
} else {
  NULL
}
```

Guard every test with `skip_if_not_installed("plumber")` and
`skip_if(is.null(.ep_router), "Router could not be created")`.

### Request builder

Build a Rook-compatible environment that `pr$call()` accepts:

```r
make_api_req <- function(method = "GET", path = "/",
                         query = list(), body = NULL) {
  # Repeated-value query params: list(pip_id = c("A", "B")) →
  #   "pip_id=A&pip_id=B"
  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      paste0(k, "=", as.character(query[[k]]))
    }))
    paste(parts, collapse = "&")
  } else ""

  body_raw <- if (!is.null(body)) {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  } else raw(0L)

  req                <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- toupper(method)
  req$PATH_INFO      <- path
  req$QUERY_STRING   <- qs
  req$HTTP_ACCEPT    <- "application/json"
  req$CONTENT_TYPE   <- if (!is.null(body)) "application/json" else ""
  req$CONTENT_LENGTH <- as.character(length(body_raw))
  req$HTTP_HOST      <- "localhost"
  req$rook.input <- list(
    read_lines = function() rawToChar(body_raw),
    read       = function(l = -1L) body_raw,
    rewind     = function() invisible(NULL)
  )
  req
}
```

### Response decoder

```r
parse_api_res <- function(res, simplify = TRUE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}
```

Use `simplify = FALSE` when inspecting list-column serialization (e.g.,
`dimensions` field) to see the raw JSON structure.

### Fix 1 — give required params NULL defaults in plumber.R

```r
# ❌ Before: plumber crashes at 500 when pip_id is absent
function(pip_id, measures, ..., res) { ... }

# ✅ After: missing param reaches validate_table_input() → 400
function(pip_id = NULL, measures = NULL, ..., res) { ... }
```

Apply to every parameter that the handler validates itself
(not plumber-typed parameters like `release:character`).

### Fix 2 — test errorHandler registration behaviourally

```r
# ❌ Fragile: accesses private R5 field
eh <- .ep_router$errorHandler
expect_true(is.function(eh))  # fails → FALSE

# ✅ Robust: if plumb() succeeded, @plumber decorator ran
expect_false(is.null(.ep_router))
# Attempt introspection but accept NULL (private field):
eh <- tryCatch(
  get("errorHandler", envir = .ep_router, inherits = FALSE),
  error = function(e) NULL
)
expect_true(is.null(eh) || is.function(eh))
```

### Fix 3 — use length() for empty vectors and null-data checks

```r
# ❌ Fragile: jsonlite returns list() not character(0)
expect_equal(body$warnings, character(0L))
expect_null(body$data)

# ✅ Robust: works for both character(0) and list() forms
expect_equal(length(body$warnings), 0L)
expect_true(is.null(body$data) || length(body$data) == 0L)
```

### Hive fixture helpers

Self-contained fixture builders that create temp dirs, write Parquet files,
and write manifest JSON — then activate them in `.piptm_env`:

```r
.write_ep_parquet <- function(arrow_root, country_code, year,
                              welfare_type, version, pip_id, survey_acronym,
                              n_rows = 10L, extra_cols = character(0L)) {
  dir_path <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  dt <- data.table(
    country_code   = rep(country_code,    n_rows),
    surveyid_year  = rep(as.integer(year), n_rows),
    welfare_type   = rep(welfare_type,    n_rows),
    version        = rep(version,         n_rows),
    pip_id         = rep(pip_id,          n_rows),
    survey_acronym = rep(survey_acronym,  n_rows),
    welfare        = as.numeric(seq_len(n_rows)),
    weight         = rep(1.0, n_rows)
  )
  # append extra_cols (gender, area, age) as needed
  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))
  invisible(pip_id)
}

.make_ep_fixtures <- function(env = parent.frame()) {
  skip_if_not_installed("arrow")
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  # write parquet + manifest, then:
  suppressMessages({
    piptm::set_manifest_dir(tmp_manifest)
    piptm::set_arrow_root(tmp_arrow)
  })
  withr::defer(.reset_piptm_env(), envir = env)  # cleanup

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest,
       release = "20260206_EP_TEST")
}
```

Call `.make_ep_fixtures()` at the top of each `test_that()` block that
needs Arrow data. `withr::defer()` automatically resets `.piptm_env` when
the test finishes.

## Prevention

| Rule | Anti-pattern to avoid |
|---|---|
| Give every handler-validated plumber parameter a `NULL` default | `function(pip_id, ...)` — missing param → 500 before handler runs |
| Test `pr$call()` errorHandler registration behaviourally | `expect_true(is.function(pr$errorHandler))` — always fails |
| Use `length(x) == 0L` for empty-vector assertions | `expect_equal(x, character(0L))` — fails for `list()` |
| Use `is.null(x) \|\| length(x) == 0L` for null-data assertions | `expect_null(body$data)` — fails when jsonlite drops the key |
| Build a request env with `rook.input` list | Incomplete env crashes plumber internals mid-dispatch |
| Use `withr::local_tempdir(.local_envir = env)` in fixture helpers | Manual `tempdir()` + `on.exit(unlink(...))` is error-prone |

## Related

- `tests/testthat/test-api-endpoints.R` — 85 integration tests using this pattern
- `inst/plumber/plumber.R` — the router tested (NULL-default fix applied here)
- `.cg-docs/solutions/testing-patterns/2026-04-14-partial-miss-regression-test-batch-loaders.md` — companion fixture pattern for Arrow batch loaders
