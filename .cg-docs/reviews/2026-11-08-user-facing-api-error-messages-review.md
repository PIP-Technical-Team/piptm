---
date: 2026-11-08
depth: architecture
type: standard
plan: .cg-docs/plans/2026-11-08-user-facing-api-error-messages.md
findings:
  P1.1: fixed
  P1.2: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: fixed
  P2.8: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: skipped
---

## Review Report

**Review mode**: architecture
**Files reviewed**: 21
**Findings**: 14 (P0: 0, P1: 2, P2: 8, P3: 4)

### P0 — BLOCKING (immediate remediation required)
- None.

### P1 — CRITICAL (must fix before merge)
- **[P1.1]** [cg-testing / cg-code-quality] `tests/testthat/test-api-endpoints.R:963-1018`, `tests/testthat/test-errors.R:66-139` — Planned 0-row `filter_base` path (`R/load_data.R:611`) and several migrated `piptm_abort()` sites lack test assertions `[safe_auto]`
  **Why**: The test block at `test-api-endpoints.R:963` is titled `"GET /table returns exact user-facing 422 messages for missing survey, unavailable PPP, excluded dimensions, and 0-row filter_base"`, but subcase #4 tests `filter_base` dimension exclusion (`R/table_maker.R:727`) rather than a 0-row `filter_base` result (`R/load_data.R:611`). In addition, `test-errors.R` does not yet directly test `user_message` extraction on `load_survey_microdata()`, `.classify_measures()`, `.validate_by()`, `.validate_poverty_lines()`, `piptm_manifest()`, or `table_maker(..., by = "pov_status", poverty_line = NULL)`.
  **Fix**: Add the 5th subcase in `test-api-endpoints.R` (querying a survey that has the filter dimension with an unmatched category code so `load_surveys()` returns 0 rows) and add direct `piptm_user_error_message()` assertions in `test-errors.R` for the remaining migrated functions.

- **[P1.2]** [cg-architecture / cg-documentation / cg-data-quality] `docs/api-contract.md:59,343`, `inst/plumber/plumber.R:176-192,473` — API boundary contract drift across HTTP 400/422 error documentation and `POST /description` fallback `filter_base` parsing `[safe_auto]`
  **Why**: (1) `docs/api-contract.md:343` still states `Invalid JSON returns HTTP 400` for `filter_base`, contradicting lines 69 and 194 (which document HTTP 422); (2) `docs/api-contract.md:59` overstates that HTTP 400 validation errors contain no parameter names (`validate_table_input()` returns backticked parameter names on 400, whereas 422/500 domain errors are plain-English user sentences); (3) `POST /description` fallback path (`inst/plumber/plumber.R:473`) passes `body$filter_base` to `table_maker()` without running the same JSON-string `filter_base` parser used by `GET|POST /table`.
  **Fix**: Extract a shared `parse_filter_base_input(filter_base)` helper in `inst/plumber/helpers.R` used by both `/table` and `POST /description`, update `docs/api-contract.md:343` to HTTP 422, and clarify `docs/api-contract.md:59-73`.

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-code-quality / cg-performance / cg-architecture] `inst/plumber/helpers.R:761-770`, `R/errors.R:97-110` — Duplicated `user_message` validation and ANSI-stripping logic across `capture_with_warnings()` and `piptm_user_error_message()` `[safe_auto]`
  **Why**: `capture_with_warnings()` manually inspects `e[["user_message"]]` before calling `piptm_user_error_message(e)`, duplicating the 4-part validation check and checking `nzchar(trimws(um))` before `cli::ansi_strip()`.
  **Fix**: Have `piptm_user_error_message(e, fallback = NULL)` perform a single strip-and-trim check and return `NULL` when unmapped so `capture_with_warnings()` only logs and falls back to `.PIPTM_GENERIC_USER_ERROR` when `is.null(user_msg)`.

- **[P2.2]** [cg-documentation / cg-version-control / cg-reproducibility / cg-architecture] `R/errors.R:1-3`, `NAMESPACE` — `NAMESPACE` out of sync with `@importFrom` directives in `R/errors.R` `[safe_auto]`
  **Why**: `R/errors.R:1-2` adds `@importFrom cli cli_abort ansi_strip` and `@importFrom rlang caller_env`, but `NAMESPACE` was not regenerated and all calls in `R/errors.R` already use explicit `cli::` and `rlang::` qualification.
  **Fix**: Remove the redundant `@importFrom` block at the top of `R/errors.R` so `NAMESPACE` and roxygen annotations remain in sync.

- **[P2.3]** [cg-code-quality / cg-testing / cg-architecture] `tests/testthat/test-error-coverage-audit.R:40-49,73` — AST audit accepts `user_message = NULL` / `""` and blanket-allowlists `R/errors.R` `[safe_auto]`
  **Why**: `.collect_abort_calls()` only checks `"user_message" %in% names(node)`, so `piptm_abort("msg", user_message = NULL)` or `user_message = ""` would pass the static audit; additionally, `R/errors.R` is in `.INTERNAL_FILE_ALLOWLIST` instead of keying `piptm_abort`'s two internal `cli::cli_abort()` calls in `.MIXED_FILE_SITE_ALLOWLIST`.
  **Fix**: Check that `node[["user_message"]]` is not `NULL` (`quote(NULL)`) or an empty/whitespace string literal, move `R/errors.R` to `.MIXED_FILE_SITE_ALLOWLIST`, and verify in `test-error-coverage-audit.R`.

- **[P2.4]** [cg-testing] `tests/testthat/test-api-endpoints.R:912-928` — Weak assertion in `POST /table` parsed-list `filter_base` test `[safe_auto]`
  **Why**: The test allows `res$status %in% c(200L, 422L)` and only checks a negative regex (`!grepl("could not be read", ...)`), which would pass even on an unhandled 422 fallback error.
  **Fix**: Assert `expect_equal(res$status, 422L)` and `expect_identical(unlist(body$errors), "One or more selected sample filters are not valid for this data release.")`.

- **[P2.5]** [cg-testing] `tests/testthat/test-api-helpers.R:716-750` — Missing verification of `[piptm][unmapped_error]` `stderr` logging and weak assertion on `resolve_release()` `[safe_auto]`
  **Why**: The tests suppress messages with `suppressMessages()` instead of asserting `expect_message(..., "\\[piptm\\]\\[unmapped_error\\]")` on unmapped errors and `expect_no_message(...)` on mapped `piptm_abort()` errors, and `resolve_release()` only checks `nzchar(out$error)`.
  **Fix**: Use `expect_message()` / `expect_no_message()` and assert the exact `out$error` string on `resolve_release("this_is_not_a_real_release")`.

- **[P2.6]** [cg-architecture] `inst/plumber/helpers.R:115-123`, `R/manifest.R:326-336` — Non-scalar/NA `release` inputs fail `if (!release %in% ...)` before reaching `piptm_abort()` `[safe_auto]`
  **Why**: Evaluating `if (!release %in% known)` when `length(release) != 1L` throws base R's `"the condition has length > 1"` error before `piptm_abort()` is reached.
  **Fix**: Guard `!is.character(release) || length(release) != 1L || is.na(release) || !release %in% ...` before branching.

- **[P2.7]** [cg-reproducibility] `tests/testthat/test-error-coverage-audit.R:146-148` — Unescaped regex path prefix removal and missing `encoding = "UTF-8"` in `parse()` `[safe_auto]`
  **Why**: `sub(paste0("^", norm_root, "/?"), ...)` uses regex matching on file system paths (which can contain `+`, `(`, `)`), and `parse()` omits `encoding = "UTF-8"`.
  **Fix**: Strip `norm_root` with fixed string prefix comparison (`startsWith()` + `substr()`) and pass `encoding = "UTF-8"` to `parse()`.

- **[P2.8]** [cg-code-quality] `R/errors.R`, `R/description_builder.R`, `R/table_maker.R`, `R/measures.R`, `R/load_data.R`, `R/manifest.R`, `inst/plumber/` — Repeated user-facing error string literals across multiple files `[safe_auto]`
  **Why**: Several identical user-facing sentences (invalid description metadata, unreadable sample filter, invalid poverty line, invalid survey year, no active release) are repeated across multiple call sites and files.
  **Fix**: Define shared internal constants in `R/errors.R` for repeated user-facing sentences and reference them across call sites.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-reproducibility / cg-data-quality] `R/load_data.R:219-222,311-324` — Unsorted `available_ppp` in `user_message` and unguarded non-scalar `country_code`/`year` in `sprintf()` `[safe_auto]`
  **Why**: `available_ppp` order depends on manifest column order without `sort()`, and non-scalar `country_code`/`year` in `sprintf()` would produce a non-scalar `user_message`.
  **Fix**: Wrap `available_ppp` in `sort(unique(...))` and coerce `country_code`/`year` labels to scalar strings before `sprintf()`.

- **[P3.2]** [cg-data-quality] `R/table_maker.R:602-625` — `Inf`/`-Inf` in `filter_base` values causes `NA` in `vals_numeric != floor(vals_numeric)` `[safe_auto]`
  **Why**: `Inf != floor(Inf)` evaluates to `NA` (since `Inf - Inf` is `NaN`), causing `if (any(fractional_idx))` to throw `missing value where TRUE/FALSE needed`.
  **Fix**: Check `if (anyNA(vals_numeric) || any(!is.finite(vals_numeric)))` before checking `vals_numeric != floor(vals_numeric)`.

- **[P3.3]** [cg-testing] `tests/testthat/test-errors.R:40-64` — Missing edge-case tests for `NA_character_`, whitespace-only `user_message`, ANSI stripping, custom `fallback`, and non-condition inputs `[safe_auto]`
  **Why**: `R/errors.R` handles these edge cases, but `test-errors.R` does not yet assert them directly.
  **Fix**: Add unit assertions in `test-errors.R` for `NA_character_`, `"   "`, ANSI-wrapped `user_message`, custom `fallback`, and non-condition inputs.

- **[P3.4]** [cg-version-control] `inst/plumber/plumber.R:1` — Branch name `error_msg` and monolithic commit note `[advisory]`
  **Why**: Branch name `error_msg` does not use the `type/short-description` prefix (`feat/...`) and commit `eb912d7` combined all phases into one commit.
  **Fix**: Keep `error_msg` since it is already pushed to `origin/error_msg`; use `feat/<short-description>` and phase-scoped commits on future branches.

### ✅ Passed
- All 8 architecture-mode agents returned usable, detailed output (`@cg-code-quality`, `@cg-testing`, `@cg-documentation`, `@cg-version-control`, `@cg-reproducibility`, `@cg-performance`, `@cg-architecture`, `@cg-data-quality`).
