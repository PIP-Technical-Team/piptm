---
date: 2026-11-04
title: "Query-parameter endpoint migration: testing and validator hardening for GET routes"
category: "testing-patterns"
language: "R"
tags: [plumber, api, endpoint, query-parameter, migration, testing, regression, json-parsing, validator, empty-query]
root-cause: "POST→GET migration requires explicit regression tests for query-string behavior (empty values, missing params) and hardening of nested-JSON flatten logic to prevent silent data corruption"
severity: "P1"
---

# Query-Parameter Endpoint Migration: Testing and Validator Hardening for GET Routes

## Problem

Migrating a plumber endpoint from `POST /cell-definition` (JSON-body request) to `GET /cell-definition` (query-parameter request) surfaces three distinct challenges:

1. **Empty-query disable semantics are contract-significant but not asserted** at router level.
   - Over GET, empty query strings (`filter_base=""`) are read as the string `""`, not `NULL`.
   - This enables disable patterns (empty value = no filter, no suppression).
   - Tests that omit this case allow regressions where a parser fix or validator bypass silently changes behavior.

2. **Validator deserializes nested JSON without guards**, creating silent data corruption.
   - `jsonlite::fromJSON()` + `unlist()` flattens nested objects and arrays without validation.
   - Malformed queries like `{"age_group": {"min": 1, "max": 5}}` silently flatten to `c(1, 5)`, stripping keys and generating corrupted cell definitions.
   - No validation error is raised; the request proceeds with wrong data.

3. **Query-parameter binding bypasses traditional error handling**.
   - Plumber binds typed query params **before** the handler runs; missing required params cause 500 crashes instead of reaching the validator.
   - Workaround: give every handler-validated param a `NULL` default so the value reaches validation logic and returns 400.

## Root Cause

### Issue 1 — Empty-query semantics are transport-dependent
In POST, omitting a JSON body key means `NULL` in R after parsing. Over GET, omitting a query parameter also means no value in `query_string`, so the URL builder must handle `NULL` → omit param logic. But over GET, an **explicitly empty param** (`?filter_base=`) is parsed as the string `""`, allowing the disable pattern.

This contract is fragile: if the validator later changes how it interprets empty strings, or if a test harness accidentally uses `NULL` instead of `""` to disable, the behavior regresses silently. Tests must assert the exact behavior for both `NULL` (missing) and `""` (empty disable).

### Issue 2 — nested JSON flattening is unguarded
`jsonlite::fromJSON()` with `simplifyVector = TRUE` (default) converts arrays to R vectors and objects to named lists. A subsequent `unlist()` without checking `names()` or `dimensions` flattens arbitrarily nested structures:

- `{"age_group": [1, 2, 3]}` → `list(age_group = c(1, 2, 3))` → `unlist()` → `c(1, 2, 3)`  ✓ correct
- `{"age_group": {"min": 1, "max": 5}}` → `list(age_group = list(min = 1, max = 5))` → `unlist()` → `c(1, 5)` ✗ **corrupted**, keys stripped
- `{"age_group": [{"code": "1"}, {"code": "2"}]}` → `list(age_group = list(...))` → `unlist()` → `c("1", "2")` ✗ **corrupted**, object discarded

The validator must check the structure **before** unlisting: reject any `parsed_fb[[fnm]]` that has `names()`, `dim()`, or nested lists.

### Issue 3 — missing params crash at plumber's binding layer
Plumber resolves `function(analysis_var, measures, ...)` parameter names against the incoming request. If `analysis_var` is required (no default) and missing from the query string, plumber raises an error **before the function body runs**. This error bypasses the handler's `validate_*()` call and custom `api_error()` response, returning a generic 500.

The fix: give every handler-validated param a `NULL` default, e.g. `function(analysis_var = NULL, measures = NULL, ...)`. Missing params reach the validator, which returns 400 + error details.

## Solution

### Fix 1: Guard nested-JSON flattening in validators

Before `unlist()`, verify that the value is a flat vector:

```r
validate_cell_definition_query <- function(analysis_var = NULL, measures = NULL, 
                                           poverty_line = NULL, column = NULL, 
                                           row = NULL, superColumn = NULL, 
                                           superRow = NULL, filter_base = NULL, 
                                           ppp = 2021L, pop_share_threshold = 0.001, 
                                           welfare_type = NULL, release = NULL) {
  errors <- character(0L)
  
  # ... standard validations first ...
  
  # Parse and validate filter_base (JSON-encoded string over GET)
  if (!is.null(filter_base) && nzchar(filter_base)) {
    tryCatch({
      parsed_fb <- jsonlite::fromJSON(filter_base, simplifyVector = TRUE, 
                                       simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
    }, error = function(e) {
      errors <<- c(errors, "filter_base must be a valid JSON object")
      parsed_fb <<- NULL
    })
    
    if (!is.null(parsed_fb) && is.list(parsed_fb)) {
      for (fnm in names(parsed_fb)) {
        val_raw <- parsed_fb[[fnm]]
        
        # ✅ GUARD: reject nested structures
        if (is.data.frame(val_raw) || !is.null(dim(val_raw)) ||
            (is.list(val_raw) && !is.null(names(val_raw))) ||
            (is.list(val_raw) && any(vapply(val_raw, function(x) is.list(x) || !is.null(dim(x)), logical(1))))) {
          errors <- c(
            errors,
            paste0("`filter_base$", fnm, "` must be a flat vector of category codes, not a nested object or table.")
          )
          next
        }
        
        # Now safe to unlist
        vals <- unlist(val_raw, use.names = FALSE)
        trimmed_vals <- trimws(as.character(vals))  # precompute once per dimension
        
        # ... validate against known dimensions ...
      }
    }
  }
  
  # Return named list with parsed/coerced values
  list(valid = length(errors) == 0L, errors = errors, 
       analysis_var = analysis_var, measures = measures,
       filter_base = if (!is.null(filter_base)) parsed_fb else NULL,
       welfare_type = welfare_type, release = release)
}
```

**Key points**:
- Check `!is.null(dim(x))` for data frames and arrays
- Check `!is.null(names(x))` for named lists (objects)
- Recursively check for nested lists/dimensions inside each element
- Precompute `trimmed_vals` once per dimension to avoid duplicate string coercion

### Fix 2: Give all handler-validated params NULL defaults in plumber.R

```r
#* @get /cell-definition
#* @param analysis_var The analysis variable
#* @param measures Repeatable measure names
#* @param ... other params ...
function(analysis_var = NULL, measures = NULL, 
         poverty_line = NULL, column = NULL, row = NULL,
         superColumn = NULL, superRow = NULL, 
         filter_base = NULL, ppp = 2021L, pop_share_threshold = 0.001,
         welfare_type = NULL, release = NULL, res) {
  check <- validate_cell_definition_query(
    analysis_var = analysis_var, measures = measures,
    poverty_line = poverty_line, column = column, row = row,
    superColumn = superColumn, superRow = superRow,
    filter_base = filter_base, ppp = ppp, 
    pop_share_threshold = pop_share_threshold,
    welfare_type = welfare_type, release = release
  )
  
  if (!check$valid) {
    return(api_error(check$errors, 400L, res))
  }
  
  # ... build response ...
}
```

### Fix 3: Add explicit regression tests for empty-query semantics

In `tests/testthat/test-api-cell-definition.R`, add router-level tests asserting behavior for empty query parameters:

```r
test_that("GET /cell-definition with empty filter_base disables filter (returns data)", {
  pr <- .get_cell_def_router()
  
  # Empty filter_base="" should disable filtering (not error)
  req <- make_cell_def_get_req(list(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = ""  # ← explicitly empty, not NULL
  ))
  
  res <- pr$call(req)
  body <- parse_api_res(res)
  
  expect_equal(res$status, 200L)
  expect_equal(body$status, "success")
  expect_true(is.data.frame(body$data$who_is_in_table) || is.list(body$data))
  # Assert filter was NOT applied
  expect_false(body$data$who_is_in_table$filtered)
})

test_that("GET /cell-definition with empty pop_share_threshold disables suppression", {
  pr <- .get_cell_def_router()
  
  # Empty pop_share_threshold="" should disable suppression
  req <- make_cell_def_get_req(list(
    analysis_var = "welfare",
    measures = "share",
    pop_share_threshold = ""  # ← empty disable
  ))
  
  res <- pr$call(req)
  body <- parse_api_res(res)
  
  expect_equal(res$status, 200L)
  expect_equal(body$status, "success")
  # Assert suppression note is absent (not triggered)
  expect_equal(length(body$data$notes), 0L)
})

test_that("GET /cell-definition rejects nested filter_base objects (not flat)", {
  pr <- .get_cell_def_router()
  
  # Nested object: {"age_group": {"min": 1, "max": 5}}
  req <- make_cell_def_get_req(list(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = '{"age_group": {"min": 1, "max": 5}}'  # ← nested object
  ))
  req$QUERY_STRING <- gsub(
    'filter_base=[^&]*', 
    'filter_base=%7B%22age_group%22%3A%7B%22min%22%3A1%2C%22max%22%3A5%7D%7D',
    req$QUERY_STRING
  )
  
  res <- pr$call(req)
  body <- parse_api_res(res)
  
  expect_equal(res$status, 400L)
  expect_equal(body$status, "error")
  expect_true(any(grepl("must be a flat vector", body$errors)))
})
```

**Key assertion patterns**:
- Empty query params should return 200 + success payload (not error)
- Check payload structure, not just status code
- Assert suppression/filter behavior through payload fields (`filtered`, `notes`)
- Malformed nested JSON should return 400 + specific error message

### Prevention Rules

| Rule | Anti-pattern to avoid |
|------|----------------------|
| Guard `unlist()` before flattening | Assume `fromJSON()` output is safe; unlist nested objects without checking structure |
| Give every handler-validated param `NULL` default | `function(param)` — missing param → 500 before handler runs |
| Test empty-query disable semantics at router level | `filter_base = NULL` tests only; miss empty-string `= ""` behavior |
| Assert payload structure, not just status | `expect_equal(res$status, 200L)` — miss silent data corruption |
| Precompute trimmed values in loops | `trimws(as.character(vals))` inside the loop — evaluate twice per value |

## Related

- `.cg-docs/solutions/testing-patterns/2026-05-13-plumber-programmatic-testing-with-fixtures.md` — foundational pr$call() testing and fixture patterns
- `.cg-docs/reviews/2026-11-04-cell-definition-get-only-review.md` — standard review report; [P0.1] on nested-JSON flattening, [P2.5] on empty-query regression coverage
- `inst/plumber/helpers.R:validate_cell_definition_query()` — implementation of the guarded flatten logic
- `tests/testthat/test-api-cell-definition.R:198–228` — regression test cases for empty-query semantics
- `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md` — related: defensive JSON parsing patterns
