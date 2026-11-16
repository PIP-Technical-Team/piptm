---
date: 2026-11-08
title: "User-Facing API Error Messages"
status: completed
completed-date: 2026-11-08
scope: "Deep"
brainstorm: ".cg-docs/brainstorms/2026-11-08-user-facing-api-error-messages.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-11-08-user-facing-api-error-messages.md"
completed-phases: [1, 2, 3]
artifact-schema-version: 1
phases: 3
tags: [api, error-handling, plumber, ui-contract, cli_abort]
---

# Plan: User-Facing API Error Messages

## Objective

Prevent raw developer-facing `cli::cli_abort()` strings (containing `pip_id`,
`manifest`, `triplet`, glue/cli markup, or file paths) from leaking through the
Plumber API's `errors[]` response envelope. Introduce an internal condition
helper (`piptm_abort()`) that preserves the rich developer diagnostic message on
`conditionMessage(e)` for interactive R users and server logs while embedding a
plain-English, user-safe `user_message` attribute on the condition object for
the UI. Update the API boundary (`capture_with_warnings()` and
`POST /table/download`) to extract `user_message` when present and fall back to
a fixed generic safe message (`"Something went wrong processing this request."`)
while logging the raw developer diagnostic to `stderr()`.

## Context

In `{piptm}`, domain and computation errors are raised via `cli::cli_abort()`
across ~10 R files (`R/table_maker.R`, `R/load_data.R`, `R/measures.R`,
`R/manifest.R`, `R/description_builder.R`, `R/table_flatten.R`, etc.) and
`inst/plumber/`. Currently, `inst/plumber/helpers.R::capture_with_warnings()`
catches every error via
`tryCatch(..., error = function(e) structure(list(message = conditionMessage(e)), class = "cw_error"))`
and forwards `result$message` directly into `api_error(out$error, 422L, res)`.
Similarly, `POST /table/download` in `inst/plumber/plumber.R` catches
`flatten_table_response()` errors and echoes `cli::ansi_strip(conditionMessage(e))`.

As decided in `.cg-docs/brainstorms/2026-11-08-user-facing-api-error-messages.md`
and refined via `/cg-plan-review` (addressing findings P1.1, P1.2, P2.1, P2.2,
and P2.3), we will adopt **Approach 1** (custom condition class `piptm_error`
with embedded `user_message`), rolled out to the user-reachable error sites in
`table_maker()`, `pip_lookup()`, `load_survey_microdata()`, `load_surveys()`,
`measures.R`, `manifest.R`, `description_builder.R`, and `inst/plumber/`,
backed by a generic safe fallback and a static AST coverage guard.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Provide an internal condition helper `piptm_abort(message, ..., user_message = NULL, class = NULL, .envir = parent.frame(), call = rlang::caller_env())` that delegates to `cli::cli_abort()` with class `c(class, "piptm_error")` and attaches `user_message` (plain character scalar) to the condition while keeping `conditionMessage(e)` identical for R callers and existing `expect_error()` unit tests. | Brainstorm Next Step 1 |
| R2 | Define a single canonical fallback constant (`.PIPTM_GENERIC_USER_ERROR <- "Something went wrong processing this request."`) and extractor helper (`piptm_user_error_message(cnd)`) so fallback wording is defined in one place. | Brainstorm Next Step 6 |
| R3 | Update `inst/plumber/helpers.R::capture_with_warnings()` and `POST /table/download` in `inst/plumber/plumber.R` to return `e$user_message` when non-empty and otherwise return `.PIPTM_GENERIC_USER_ERROR` (or a dedicated user-safe download validation message for 400 payload rejections on `/table/download`), while logging unmapped developer messages (`conditionMessage(e)`) to `stderr()` with a `[piptm][unmapped_error]` prefix and recording `dev_error` on the `capture_with_warnings()` return list. | Brainstorm Next Step 2 + P2.1, P2.3 |
| R4 | Migrate user-reachable `cli_abort()` call sites in `R/table_maker.R`, `R/load_data.R` (`load_survey_microdata()` and `load_surveys()`), `R/measures.R`, `R/manifest.R`, `R/description_builder.R`, `inst/plumber/helpers.R`, and `inst/plumber/plumber.R` to supply clear, plain-English `user_message` strings free of internal jargon (`pip_id`, `manifest`, `triplet`, `filter_base`, `Parquet`, `Arrow`, `Approach C`). | Brainstorm Next Step 3 + P1.1, P1.2, P2.2 |
| R5 | Add a lightweight test/audit guard (`tests/testthat/test-error-coverage-audit.R`) that scans `R/` and `inst/plumber/` for `piptm_abort()` and `cli_abort()` calls, asserts every `piptm_abort()` supplies a non-empty `user_message`, forbids raw `cli_abort()` in `inst/plumber/`, `pip_lookup()`, `piptm_manifest()`, and `resolve_release()`, and verifies that remaining internal-only `cli_abort()` calls in mixed files (`R/table_maker.R`, `R/load_data.R`, `R/measures.R`, `R/manifest.R`, `R/description_builder.R`) match an explicit per-site allowlist keyed by `(file, enclosing_function, message_snippet)`. | Brainstorm Next Step 4 + P1.2 |
| R6 | Update `docs/api-contract.md` and `README.md` to document that `errors[]` entries are guaranteed user-safe sentences and that unmapped internal failures return the generic fallback string while logging diagnostics server-side. | Brainstorm Next Step 5 |

## Phase 1: Condition Helper & API Boundary

### 1. Create `R/errors.R` with `piptm_abort()` and `piptm_user_error_message()`
- **Requirements**: R1, R2
- **Files**: `R/errors.R`, `tests/testthat/test-errors.R`
- **Details**:
  - Define `.PIPTM_GENERIC_USER_ERROR <- "Something went wrong processing this request."` and `.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR <- "The table data could not be exported because the download payload is incomplete or invalid."`.
  - Implement `piptm_abort(message, ..., user_message = NULL, class = NULL, .envir = parent.frame(), call = rlang::caller_env())`:
    - Validate that `user_message`, when non-`NULL`, is a non-empty character scalar (`is.character(user_message) && length(user_message) == 1L && !is.na(user_message) && nzchar(trimws(user_message))`).
    - Delegate to `cli::cli_abort(message = message, ..., class = c(class, "piptm_error"), user_message = user_message, .envir = .envir, call = call)`.
  - Implement `piptm_user_error_message(cnd, fallback = .PIPTM_GENERIC_USER_ERROR)`:
    - Extract `um <- cnd$user_message`.
    - If `is.character(um) && length(um) == 1L && !is.na(um) && nzchar(trimws(um))`, return `cli::ansi_strip(trimws(um))`.
    - Otherwise return `fallback`.
- **Test Scenarios**:
  - Happy path: `piptm_abort("Dev msg {.val x}", user_message = "User msg.")` raises a condition inheriting from `c("piptm_error", "rlang_error")`, `conditionMessage(e)` contains the formatted developer message, and `piptm_user_error_message(e)` returns `"User msg."`.
  - Edge case: custom subclass `class = "piptm_no_surveys"` is preserved ahead of `"piptm_error"`.
  - Error/fallback path: plain `stop("boom")`, plain `cli::cli_abort("raw internal")`, or `piptm_abort("dev", user_message = NULL)` causes `piptm_user_error_message(e)` to return `.PIPTM_GENERIC_USER_ERROR`.
- **Tests**: `tests/testthat/test-errors.R`
- **Acceptance criteria**: All unit tests in `test-errors.R` pass; `conditionMessage()` for `piptm_abort()` matches `cli::cli_abort()` output.

### 2. Update `capture_with_warnings()`, `GET|POST /table` `filter_base` parsing, and `POST /table/download` to enforce user-safe errors
- **Requirements**: R2, R3
- **Files**: `inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-table-download.R`
- **Details**:
  - In `inst/plumber/helpers.R::capture_with_warnings()`:
    - On catching error `e` in the outer `tryCatch(error = ...)`, extract `dev_msg <- cli::ansi_strip(conditionMessage(e))`.
    - Check whether `e` carries a non-empty `e$user_message`.
    - When present, set `user_msg <- piptm:::piptm_user_error_message(e)`.
    - When absent, log `message(sprintf("[piptm][unmapped_error] %s: %s", paste(class(e), collapse = "/"), dev_msg))` inside the `tryCatch(error = ...)` handler (after `withCallingHandlers` has unwound, so it goes to server `stderr()` and is never captured into `collected` warnings), and set `user_msg <- piptm:::.PIPTM_GENERIC_USER_ERROR`.
    - Return `list(result = NULL, warnings = collected, error = user_msg, dev_error = dev_msg)`.
  - In `inst/plumber/plumber.R`:
    - **(P2.3 fix)** In `GET|POST /table` (lines 176–180), handle both query-string JSON (`is.character(filter_base)`) and already-parsed JSON request bodies (`is.list(filter_base)`): when `is.character(filter_base)`, parse via `tryCatch(jsonlite::fromJSON(filter_base), error = function(e) piptm:::piptm_abort("Malformed filter_base JSON: {conditionMessage(e)}", user_message = "The selected sample filter could not be read. Please reset your filters and try again."))`; when `is.list(filter_base)`, pass `filter_base` through directly to `table_maker()`.
    - **(P2.1 fix)** In `POST /table/download` (lines 310–316), update the `flatten_table_response(body$data)` `tryCatch` handler to extract `piptm:::piptm_user_error_message(e, fallback = piptm:::.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR)` and replace the fallback string ``"`data` must be a non-empty array of Approach C survey objects."`` with `piptm:::.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR` (`"The table data could not be exported because the download payload is incomplete or invalid."`), so client 400 payload rejections return a clear user-safe message without polluting server logs with `[piptm][unmapped_error]` alerts or leaking `"Approach C"` jargon.
  - Update existing tests in `tests/testthat/test-api-helpers.R` (lines 716–726) and `tests/testthat/test-api-endpoints.R` (lines 1198–1252) that previously asserted raw `stop()` / internal `format_table_output()` text in `out$error` / `body$errors` to assert the safe fallback in `error` / `body$errors` and the raw diagnostic in `out$dev_error`.
- **Test Scenarios**:
  - Happy path: `capture_with_warnings(piptm:::piptm_abort("internal {.val x}", user_message = "Friendly error."))` returns `out$error == "Friendly error."` and `out$dev_error == "internal \"x\""`.
  - Unmapped fallback path: `capture_with_warnings(stop("secret path /srv/arrow"))` returns `out$error == "Something went wrong processing this request."` and `out$dev_error == "secret path /srv/arrow"`, without adding the `[piptm][unmapped_error]` log line to `out$warnings`.
  - `filter_base` GET vs POST path (P2.3): `GET /table?filter_base={bad_json}` returns 422 with `"The selected sample filter could not be read. Please reset your filters and try again."`, while `POST /table` with `filter_base = list(...)` in the JSON body succeeds without triggering `fromJSON()` on a list.
  - Download 400 path (P2.1): invalid `POST /table/download` payloads return HTTP 400 with `"The table data could not be exported because the download payload is incomplete or invalid."` and never contain `"Approach C"`.
  - Endpoint regression path: mocked `format_table_output()` failure on `GET /table` returns HTTP 422 with `body$errors == "Something went wrong processing this request."` rather than leaking internal column names (`flag`).
- **Tests**: `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-table-download.R`
- **Acceptance criteria**: `test-api-helpers.R`, `test-api-endpoints.R`, and `test-api-table-download.R` pass with zero raw internal error strings leaked in 400 or 422 responses.

## Phase 2: Migrate High-Traffic User-Reachable Error Sites

### 3. Migrate survey lookup, availability, filter, and dimension errors in `R/table_maker.R` and `R/load_data.R`
- **Requirements**: R4
- **Files**: `R/table_maker.R`, `R/load_data.R`, `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`
- **Details**:
  - **(P1.2 fix)** Replace all 4 `cli_abort()` calls in `pip_lookup()` (`R/table_maker.R` lines 52, 62, 65, 69) with `piptm_abort()`:
    - Line 52 (unequal vector lengths): `user_message = "Country, year, and welfare type selections must have the same number of entries."`
    - Lines 62, 65, 69 (non-numeric, `NA`, or fractional `year`): `user_message = "Survey years must be valid whole-number years (for example, 2019)."`
  - Replace user-reachable `cli_abort()` calls in `table_maker()` (`R/table_maker.R`) with `piptm_abort()`:
    - Line 525 (`pip_id` empty after resolution): `user_message = "No surveys matched your selection. Please select at least one available country and survey year."`
    - Lines 547, 550, 555 (non-list, empty, or unnamed `filter_base`, e.g. `filter_base = "{}"` or `"[1,2]"`): `user_message = "The selected sample filter could not be read. Please reset your filters and try again."`
    - Line 564 (invalid `filter_base` variables): `user_message = "One or more selected sample filters are not valid for this data release."`
    - Line 576 (empty `filter_base` variable values): `user_message = "Each sample filter must include at least one selected category."`
    - Lines 582 & 592 (non-integer `filter_base` category codes): `user_message = "One or more selected sample filter values are invalid."`
    - Line 629 (no matching surveys found in manifest for release): `user_message = "The requested country or survey is not available in the selected data release."`
    - Line 702 (all requested surveys excluded by `filter_base` dimensions): `user_message = "None of the selected surveys contain the variables needed for your sample filters. Try removing a filter or selecting different surveys."`
    - Line 768 (all requested surveys excluded by `by` dimensions): `user_message = "None of the selected surveys support all of the requested breakdown variables. Try fewer breakdown dimensions or different surveys."`
    - Line 810 (`load_surveys` returned 0 rows): `user_message = "No survey observations matched the selected criteria."`
    - Line 860 (`poverty_line` required when `pov_status` in `by`): `user_message = "A valid positive poverty line is required when breaking down results by poverty status."`
  - **(P1.1 fix)** Replace user-reachable `cli::cli_abort()` calls in `load_survey_microdata()` and `load_surveys()` (`R/load_data.R`) with `piptm_abort()`:
    - Line 185 (`load_survey_microdata` no current release set): `user_message = "Survey data is temporarily unavailable because no active data release is loaded."`
    - Line 213 (`load_survey_microdata` no manifest entry found): `user_message = sprintf("Survey data for %s (%s) is not available in the selected data release.", country_code, year)`
    - Line 310 (`load_survey_microdata` PPP not available): `user_message = sprintf("PPP year %s is not available for the selected survey (%s %s). Available PPP years: %s.", ppp, country_code, year, paste(available_ppp, collapse = ", "))`
    - Line 501 (`load_surveys` no surveys remain after skipping those without PPP column): `user_message = sprintf("None of the selected surveys have data available for the %s PPP standard. Please switch to another PPP year.", effective_year)`
    - Line 596 (`load_surveys` 0 rows returned after query / `filter_base` filtering): `user_message = "No survey observations matched the selected sample filters. Try broadening your filter selection."` (leaving line 646 as an internal schema integrity check).
- **Test Scenarios**:
  - Happy path (API 422): calling `GET /table` with a valid-format `pip_id` that does not exist in the manifest (e.g. `"ZZZ_2020_FAKE_INC_ALL"`) returns HTTP 422 with `body$errors` equal to `"The requested country or survey is not available in the selected data release."`.
  - Happy path (PPP unavailable): calling `GET /table` with an unavailable PPP year (e.g. `ppp = 1999`) returns HTTP 422 with the user-friendly PPP message and no mention of `welfare_ppp_` column names.
  - Happy path (dimensions unavailable): calling `GET /table` with a breakdown dimension absent from all requested surveys returns HTTP 422 with the user-friendly breakdown message.
  - Happy path (0-row filter_base): calling `GET /table` with a `filter_base` value that matches 0 rows in `load_surveys()` (line 596) returns HTTP 422 with `"No survey observations matched the selected sample filters. Try broadening your filter selection."`.
  - R compatibility path: existing `test-table-maker-v2.R` and `test-load-data.R` `expect_error()` assertions against developer messages continue to pass unchanged.
- **Tests**: `tests/testthat/test-errors.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-table-maker-v2.R`, `tests/testthat/test-load-data.R`
- **Acceptance criteria**: All migrated sites return exact user-facing sentences via the API while all existing R package unit tests pass without modification.

### 4. Migrate measure, dimension, release, and `/description` error sites in `R/measures.R`, `R/manifest.R`, `R/description_builder.R`, and `inst/plumber/`
- **Requirements**: R4
- **Files**: `R/measures.R`, `R/manifest.R`, `R/description_builder.R`, `inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-description.R`
- **Details**:
  - In `R/measures.R`:
    - Line 110 (unknown measures): `user_message = "One or more requested statistics are not recognized."`
    - Line 170 (unknown dimensions in `.validate_by`): `user_message = "One or more requested breakdown variables are not available for the selected release."`
    - Line 181 (> 4 dimensions in `.validate_by`): `user_message = "At most 4 breakdown variables can be used in a single table."`
    - Lines 224, 234, 246 (`.validate_poverty_lines` missing/non-numeric/non-positive): `user_message = "A valid positive poverty line is required to compute poverty statistics."`
  - In `R/manifest.R`:
    - Line 315 (no current release set): `user_message = "Survey data is temporarily unavailable because no active data release is loaded."`
    - Line 326 (release not found in loaded manifests): `user_message = sprintf("The requested data release ('%s') is not available.", release)`
  - **(P2.2 fix)** In `R/description_builder.R`:
    - Lines 38, 49, and 69 (`build_description_model` top-level `description_metadata` validation for list type, missing required fields, and malformed top-level fields): migrate to `piptm_abort(..., user_message = "The table description could not be generated because the provided table metadata is incomplete or invalid.")`.
  - In `inst/plumber/helpers.R`:
    - Line 116 (`resolve_release` unknown release): use `piptm:::piptm_abort(..., user_message = sprintf("The requested data release ('%s') is not available.", release))`
  - In `inst/plumber/plumber.R`:
    - Lines 432 & 439 (`POST /description` invalid `description_metadata` / missing `params`): use `piptm:::piptm_abort(..., user_message = "The table description could not be generated because the provided table metadata is incomplete or invalid.")`
- **Test Scenarios**:
  - Bogus release on `/table`, `/lookup`, `/surveys`, `/countries`, `/regions`: returns HTTP 422 with `"The requested data release ('BOGUS_RELEASE_DOES_NOT_EXIST') is not available."`.
  - Invalid `description_metadata` on `POST /description` (missing `params`, invalid `provenance`, missing `surveys`, invalid `resolved_labels`): all return HTTP 422 with `"The table description could not be generated because the provided table metadata is incomplete or invalid."`.
  - Direct R calls to `resolve_release("bogus")`, `piptm_manifest("bogus")`, and `build_description_model()` still inherit from `rlang_error` and preserve their developer `conditionMessage()`.
- **Tests**: `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-description.R`, `tests/testthat/test-manifest.R`, `tests/testthat/test-measures-current.R`, `tests/testthat/test-description-builder-schema-validation.R`
- **Acceptance criteria**: All endpoint and package tests pass; 422 responses for release and description errors contain only user-safe copy.

## Phase 3: Coverage Audit Guard & API Contract Documentation

### 5. Add static `cli_abort` / `piptm_abort` coverage audit test
- **Requirements**: R5
- **Files**: `tests/testthat/test-error-coverage-audit.R`
- **Details**:
  - **(P1.2 fix)** Parse all `.R` files in `R/` and `inst/plumber/` using base R `parse(keep.source = TRUE)`.
  - Walk AST top-level function definitions and nested calls to inspect every call to `piptm_abort` / `piptm:::piptm_abort` and `cli_abort` / `cli::cli_abort`:
    1. Every `piptm_abort()` call must explicitly pass a `user_message` named argument (except the definition inside `R/errors.R`).
    2. Zero raw `cli_abort()` / `cli::cli_abort()` calls are permitted in `inst/plumber/*.R`, `pip_lookup()`, `piptm_manifest()`, or `resolve_release()`.
    3. Purely internal computation/formatter/startup files (`R/compute_inequality.R`, `R/compute_measures.R`, `R/compute_poverty.R`, `R/compute_shares.R`, `R/compute_summary_stats.R`, `R/format_table_output.R`, `R/table_response.R`, `R/table_flatten.R`, `R/schema.R`, `R/registry.R`, `R/api.R`) are listed in an explicit internal-file allowlist (protected at runtime by `.PIPTM_GENERIC_USER_ERROR` / `.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR`).
    4. For mixed files (`R/table_maker.R`, `R/load_data.R`, `R/measures.R`, `R/manifest.R`, `R/description_builder.R`), any remaining raw `cli_abort()` call must match an explicit per-site allowlist keyed by `(file, enclosing_function, message_snippet)` representing internal programmer-contract/schema-corruption checks. Any new unmigrated `cli_abort()` added to `table_maker()`, `load_survey_microdata()`, `load_surveys()`, `build_description_model()`, etc. that is not in the per-site allowlist fails the test with an actionable message instructing the author to use `piptm_abort(..., user_message = "...")`.
- **Test Scenarios**:
  - Happy path: current codebase after Phase 2 migration passes the AST audit cleanly with zero unallowlisted `cli_abort()` sites.
  - Regression detection: a simulated expression `quote(cli_abort("unmapped error in table_maker"))` inside `table_maker` or `quote(piptm_abort("missing user_message"))` is flagged by the audit helper.
- **Tests**: `tests/testthat/test-error-coverage-audit.R`
- **Acceptance criteria**: `test-error-coverage-audit.R` passes and guards all user-facing entry points against unmigrated `cli_abort()` calls.

### 6. Update `docs/api-contract.md` and `README.md` error conventions
- **Requirements**: R6
- **Files**: `docs/api-contract.md`, `README.md`
- **Details**:
  - Update `docs/api-contract.md` under `## Response Envelope` and `## Error Codes`:
    - Document the UI-safety guarantee: every string in `errors[]` (across 400, 413, 422, and 500 responses) is a plain-English, user-safe sentence that UI clients can display directly in a banner or toast without client-side parsing or sanitization.
    - Include a table of representative 422 and 400 `errors[]` messages (unavailable survey/country, unavailable PPP year, unsupported breakdown dimensions, empty sample-filter result, invalid sample-filter JSON, incomplete description metadata, invalid download payload, unknown data release, and the generic fallback `"Something went wrong processing this request."` for unexpected internal failures).
    - Note that detailed developer diagnostics for unmapped internal errors are written to server logs (`stderr`) with the `[piptm][unmapped_error]` tag.
  - Update `README.md` around lines 151–158 (`Error status conventions`) with a concise note on the user-safe `errors[]` guarantee.
- **Test Scenarios**:
  - Documentation accuracy: examples in `docs/api-contract.md` match the exact strings produced by `piptm_abort()` in Steps 2–4.
- **Tests**: Full `devtools::test()` suite.
- **Acceptance criteria**: `docs/api-contract.md` and `README.md` accurately document the new error message behavior; full test suite passes.

## Testing Strategy

- **Unit tests (`tests/testthat/test-errors.R`)**: Verify `piptm_abort()`, `piptm_error` S3 class hierarchy, `conditionMessage()` preservation for R users, and `piptm_user_error_message()` extraction and fallback behavior.
- **Plumber helper & integration tests (`tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-description.R`, `tests/testthat/test-api-table-download.R`)**: Verify `capture_with_warnings()` returns `user_message` for `piptm_error`, returns `.PIPTM_GENERIC_USER_ERROR` for unmapped `stop()`/`cli_abort()` while preserving `dev_error` and logging to `stderr()`, and verify end-to-end 422/400 responses for missing surveys, unavailable PPP years, excluded breakdown dimensions, invalid `filter_base` (both GET string and POST list), malformed `description_metadata`, malformed `/table/download` payloads, and unknown releases.
- **Static AST coverage audit (`tests/testthat/test-error-coverage-audit.R`)**: Ensure no `piptm_abort()` call omits `user_message` and no new unmapped `cli_abort()` is introduced into user-facing entry points.
- **Regression suite**: Run full `testthat` suite to confirm all existing `expect_error()` checks against developer messages continue to pass.

## Documentation Checklist

- [ ] `R/errors.R` internal roxygen2 documentation (`@keywords internal`, `@noRd`)
- [ ] `docs/api-contract.md` updated with user-safe `errors[]` guarantee and example 422/400 messages
- [ ] `README.md` updated under `Error status conventions`

## Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Existing unit tests use `expect_error(..., regexp = "...")` matching developer-facing wording. | High — could break dozens of tests if `conditionMessage()` changes. | Low | `piptm_abort()` passes `message` untouched to `cli::cli_abort()` and stores `user_message` as a separate condition field, keeping `conditionMessage(e)` 100% backward-compatible. |
| Two existing API tests (`test-api-helpers.R:716-726`, `test-api-endpoints.R:1198-1252`) explicitly assert raw `stop()` / internal `flag` column strings in `out$error` / `body$errors`. | Medium — those tests were written before the UI-safe fallback requirement. | Certain | Update those specific tests in Step 2 to assert the safe fallback in `out$error` / `body$errors` and assert the raw string in `out$dev_error`. |
| Server-side `message("[piptm][unmapped_error] ...")` inside `capture_with_warnings()` could be caught by `withCallingHandlers(message = ...)` and leaked into the API `warnings[]` array. | High — would leak internal text into `warnings[]`. | Medium | Log the unmapped error inside the outer `tryCatch(error = ...)` handler, *after* `withCallingHandlers()` has already unwound, and verify with an explicit test that `out$warnings` is empty. |
| `POST /table` receives `filter_base` as an already-parsed R `list` from Plumber's JSON body parser (P2.3). | High — calling `jsonlite::fromJSON()` on a list would break `POST /table` with `filter_base`. | Low (mitigated) | Step 2 explicitly checks `is.character(filter_base)` before calling `jsonlite::fromJSON()` and passes lists through directly. |
| Future contributors add new `cli_abort()` calls in `table_maker()`, `load_survey_microdata()`, `load_surveys()`, or `inst/plumber/` without `user_message`. | Medium — falls back to generic message instead of specific UX copy. | Medium | Step 5 adds an automated AST test (`test-error-coverage-audit.R`) keyed by `(file, enclosing_function, message_snippet)` that fails CI if a new raw `cli_abort()` is added to any user-facing entry point. |

## Out of Scope

- Machine-readable error codes, error-type enums, or per-error UI branching metadata.
- Multi-language / i18n translation keys.
- Changing domain error semantics (e.g., when a request errors vs. succeeds with a warning).
- Rewriting existing 400 validation messages in `validate_table_input()` / `validate_lookup_input()` (unless needed for consistency), as the brainstorm scoped this work to `cli_abort()` domain errors caught at the API boundary.
- Converting `warnings[]` messages (could be a separate follow-up brainstorm if UI later displays `warnings[]` directly).

## Completion Contract

### Outcome
`piptm_abort()` and `piptm_user_error_message()` are implemented in `R/errors.R`, `capture_with_warnings()` and `/table/download` never leak raw internal `cli_abort()` / `stop()` strings to API clients (returning authored `user_message` copy for migrated high-traffic domain errors, a clear payload error for 400 download rejections, and `"Something went wrong processing this request."` for unmapped internal errors while logging developer diagnostics to `stderr`), a per-site static AST coverage test guards against regressions, and `docs/api-contract.md` + `README.md` document the guarantee.

### Verification Surface
| ID | Phase | Evidence Required | Command/Artifact | Required |
|----|-------|-------------------|------------------|----------|
| V1 | 1 | `piptm_abort()`, `capture_with_warnings()`, `filter_base` GET/POST handling, and `/table/download` 400 error tests pass (including `user_message` extraction, generic fallback for unmapped errors, and no leak into `warnings[]`) | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-errors.R'); testthat::test_file('tests/testthat/test-api-helpers.R'); testthat::test_file('tests/testthat/test-api-table-download.R')"` | yes |
| V2 | 2 | Migrated high-traffic domain error sites in `R/table_maker.R`, `R/load_data.R` (`load_survey_microdata` and `load_surveys`), `R/measures.R`, `R/manifest.R`, `R/description_builder.R`, and `inst/plumber/` return plain-English `user_message` strings in 422 API responses while existing R `expect_error()` tests still pass | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-endpoints.R'); testthat::test_file('tests/testthat/test-api-description.R'); testthat::test_file('tests/testthat/test-table-maker-v2.R'); testthat::test_file('tests/testthat/test-load-data.R')"` | yes |
| V3 | 3 | Static AST coverage audit test passes and `docs/api-contract.md` + `README.md` document the user-safe `errors[]` guarantee | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-error-coverage-audit.R')"` | yes |
| V4 | final | Full package test suite passes with 0 failures | `Rscript -e "devtools::test()"` | yes |

### Constraints
| ID | Phase | Constraint | Check |
|----|-------|------------|-------|
| C1 | 1 | Developer-facing `conditionMessage(e)` in R sessions must remain unchanged so existing `expect_error()` unit tests and interactive R debugging do not regress | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-table-maker-v2.R'); testthat::test_file('tests/testthat/test-load-data.R'); testthat::test_file('tests/testthat/test-measures-current.R'); testthat::test_file('tests/testthat/test-manifest.R')"` |
| C2 | 2 | Domain error logic (when an error is thrown vs. when partial results + warnings are returned) must not change | Verify no control-flow conditions (`if (...)`) around abort sites are altered in `git diff R/` |
| C3 | final | Unmapped errors must never leak raw `conditionMessage(e)` into `errors[]` or `warnings[]` | Verified by `test-api-helpers.R` and `test-api-endpoints.R` unmapped-error assertions |

### Boundaries
- Allowed: Creating `R/errors.R` and new test files under `tests/testthat/`; updating `cli_abort()` call sites in `R/` and `inst/plumber/` to `piptm_abort()`; updating `inst/plumber/helpers.R` and `inst/plumber/plumber.R` error extraction; updating `docs/api-contract.md` and `README.md`.
- Out of scope: Adding error codes or i18n keys, changing HTTP status codes, changing partial-match vs. error control flow, or modifying Arrow/manifest schemas.

### Iteration Policy
1. Follow active `deviation-policy: ask` — pause and ask the user before deviating from the planned condition structure, fallback message wording, or file scope.
2. If an existing test fails because it asserted raw developer text on an API response (`body$errors` or `out$error`), update that test to assert the user-safe message on `error` and the developer string on `dev_error`.
3. Never weaken an R-level `expect_error()` test that checks `conditionMessage()` directly on `table_maker()`, `load_survey_microdata()`, `load_surveys()`, or `piptm_manifest()`.

### Blocked-Stop Conditions
- Required verification cannot be run through `Rscript`.
- Any required evidence item (V1–V4) fails after allowed recovery attempts.
- A required deviation is discovered under policy `ask` and user approval is unavailable.
- `/cg-work` cannot durably create or update the execution report.
- Completion would require marking evidence as passed from static inspection alone rather than an executed check.
