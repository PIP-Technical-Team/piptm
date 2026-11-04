---
date: 2026-11-04
depth: standard
type: standard
plan: .cg-docs/plans/2026-11-04-cell-definition-get-only.md
findings:
  P0.1: fixed
  P1.1: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: fixed
  P2.8: fixed
  P2.9: fixed
  P2.10: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  P3.5: fixed
  P3.6: fixed
  P3.7: skipped
  P3.8: fixed
---

# Review Report: Replace POST /cell-definition with GET /cell-definition

**Review mode**: standard  
**Files reviewed**: 8 (`inst/plumber/plumber.R`, `inst/plumber/helpers.R`, `R/cell_explanation.R`, `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-cell-definition.R`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`, `roadmap.json`)  
**Findings**: 20 (P0: 1, P1: 1, P2: 10, P3: 8)

---

### P0 — BLOCKING (immediate remediation required)

- **[P0.1]** [cg-data-quality] [safe_auto] `inst/plumber/helpers.R:852` — `filter_base` dimension values allow nested JSON objects and tables to silently flatten into category codes
  **Why**: In `validate_cell_definition_input()`, `vals <- unlist(parsed_fb[[fnm]], use.names = FALSE)` flattens without checking if `parsed_fb[[fnm]]` is an object, dictionary, or data frame. An unsupported range or dictionary query like `{"age_group": {"min": 1, "max": 5}}` or `{"age_group": [{"code": "1"}]}` silently flattens to `c(1, 5)` or `"1"`, stripping keys and generating corrupted cell definitions rather than returning HTTP 400.
  **Fix**: Before calling `unlist()`, verify that `parsed_fb[[fnm]]` is not a list with names, not a data frame, and has no dimensions:
  ```r
  val_raw <- parsed_fb[[fnm]]
  if (is.data.frame(val_raw) || !is.null(dim(val_raw)) ||
      (is.list(val_raw) && !is.null(names(val_raw))) ||
      (is.list(val_raw) && any(vapply(val_raw, function(x) is.list(x) || !is.null(dim(x)), logical(1))))) {
    errors <- c(
      errors,
      paste0("`filter_base$", fnm, "` must be a flat vector of category codes, not a nested object or table.")
    )
    next
  }
  ```

---

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-testing] [safe_auto] `tests/testthat/test-api-cell-definition.R:253` — Missing regression test asserting rejection of `POST /cell-definition`
  **Why**: The goal of this change is replacing `POST` with `GET` as the sole route. Without an explicit test that `POST /cell-definition` returns HTTP 405 Method Not Allowed, an accidental re-addition of the `#* @post` decorator would pass unnoticed.
  **Fix**: Add an integration test using `pr$call()`:
  ```r
  test_that("POST /cell-definition is not routed (returns 405)", {
    pr <- .get_cell_def_router()
    req <- make_cell_def_get_req(list(analysis_var = "welfare", measures = "mean"))
    req$REQUEST_METHOD <- "POST"
    res <- pr$call(req)
    expect_equal(res$status, 405L)
  })
  ```

---

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] [safe_auto] `inst/plumber/helpers.R:733-744` — Inaccurate roxygen header and missing `@return` parameter in `validate_cell_definition_input()`
  **Why**: Roxygen describes the function as validating a parsed JSON body for the endpoint (now GET), omits `release` from `@return` (which line 909 returns), and lacks `#' @keywords internal`.
  **Fix**: Update roxygen block, add `release` to `@return`, and mark `#' @keywords internal`.

- **[P2.2]** [cg-code-quality] [safe_auto] `inst/plumber/helpers.R:697` — Incomplete `@param pop_share_threshold` default value documentation in `validate_cell_definition_query()`
  **Why**: Documents default for `ppp` (`2021L`), but omits default `(default 0.001)` for `pop_share_threshold`.
  **Fix**: Document `(default 0.001)` in the roxygen param tag.

- **[P2.3]** [cg-documentation] [safe_auto] `inst/plumber/plumber.R:895-896` — Plumber roxygen annotation for `poverty_line` omits layout slot dependency
  **Why**: Roxygen states required when `analysis_var = "pov_status"` or poverty measures are requested, but misses when `"pov_status"` appears in layout slots (enforced in code). Plumber derives Swagger descriptions from this.
  **Fix**: Add `, \"pov_status\" appears in any layout slot,` to the `#* @param poverty_line` annotation.

- **[P2.4]** [cg-testing] [safe_auto] `tests/testthat/test-api-cell-definition.R:180-243` — Error tests assert only HTTP 400 status without inspecting error payload
  **Why**: All 9 validation error cases assert `expect_equal(res$...$status, 400L)` without checking `status == "error"` or the specific error message text in `errors`.
  **Fix**: Parse the response and assert specific error substrings for each case.

- **[P2.5]** [cg-testing] [safe_auto] `tests/testthat/test-api-cell-definition.R:176` — Missing endpoint-level edge case tests for query string parameters
  **Why**: Router-level tests omit malformed `filter_base` JSON, invalid `welfare_type`, and empty `filter_base=""` (treated as NULL).
  **Fix**: Add router-level query parameter edge case assertions in `test-api-cell-definition.R`.

- **[P2.6]** [cg-performance] [safe_auto] `inst/plumber/helpers.R:813` — Inefficient JSON parsing with default data frame simplification enabled
  **Why**: `jsonlite::fromJSON(fb_raw, simplifyVector = TRUE)` leaves `simplifyDataFrame = TRUE`, causing unnecessary R data.frame object allocation when malformed/object arrays are parsed.
  **Fix**: Add `simplifyDataFrame = FALSE, simplifyMatrix = FALSE`.

- **[P2.7]** [cg-performance] [safe_auto] `inst/plumber/helpers.R:855` — Duplicate string coercion and trimming in `filter_base` validation loop
  **Why**: Evaluates `trimws(as.character(vals))` twice per dimension.
  **Fix**: Precompute `trimmed_vals <- trimws(as.character(vals))` once per dimension.

- **[P2.8]** [cg-reproducibility] [safe_auto] `tests/testthat/test-api-cell-definition.R:78` — Test fixture helper `.with_api_fixture_registry()` conditionally skips registering `"20260401_TEST"` when host manifests are loaded
  **Why**: `if (length(env$manifests) == 0L)` skips registering the test release if `PIPTM_MANIFEST_DIR` is set on the developer host, causing explicit `release` tests to fail on developer machines.
  **Fix**: Always ensure `"20260401_TEST"` is assigned: `env$manifests[["20260401_TEST"]] <- data.frame(pip_id = "ARG_2014_EPHC_INC_URB")`.

- **[P2.9]** [cg-reproducibility] [safe_auto] `docs/api-contract.md:165` — Contract specifies `null` disables cell suppression on a GET query-string endpoint where `pop_share_threshold=null` triggers HTTP 400
  **Why**: Over GET, `?pop_share_threshold=null` is read as the string `"null"`, returning 400. An empty value (`pop_share_threshold=`) disables it.
  **Fix**: Update `docs/api-contract.md` to specify empty string disables suppression.

- **[P2.10]** [cg-version-control] [safe_auto] `.cg-docs/active-state/current.json:4` — Active-state record schema drift and missing required fields
  **Why**: Dropped `workflow`, `artifactRefs`, and renamed `plan` to `activePlan`.
  **Fix**: Align with `compound-gpid-active-state-v1` schema contract.

---

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] [safe_auto] `tests/testthat/test-api-helpers.R:1092-1095` — Line length exceeds 80 characters in test fixtures
- **[P3.2]** [cg-code-quality] [safe_auto] `tests/testthat/test-api-cell-definition.R:7-16` — Incremental vector growth inside nested loop in `make_cell_def_get_req()`
- **[P3.3]** [cg-documentation] [safe_auto] `SWAGGER_GUIDE.md:310` — Duplicate phrase in parameter table introductory text
- **[P3.4]** [cg-documentation] [safe_auto] `SWAGGER_GUIDE.md:324-337` — Example query parameter block omits assembled percent-encoded URL example
- **[P3.5]** [cg-data-quality] [safe_auto] `inst/plumber/helpers.R:749` — Error message mentions "Request body" for GET endpoint validation
- **[P3.6]** [cg-reproducibility] [safe_auto] `tests/testthat/test-api-helpers.R:1` — Inconsistent CRLF line endings in working tree
- **[P3.7]** [cg-architecture] [advisory] `inst/plumber/helpers.R:704-731` — `validate_cell_definition_query()` delegates via `body_repr` list
- **[P3.8]** [cg-testing] [advisory] `tests/testthat/test-api-cell-definition.R:42` — Router re-instantiated across multiple test blocks

---

### ✅ Passed
- `@cg-version-control`: Clean branch hygiene; no secrets, API keys, or credentials exposed; non-package files covered by `.Rbuildignore` and `.gitignore`.
- `@cg-reproducibility`: Deterministic response assembly, release resolution, and pure builder isolation preserved.
