# Execution Report: User-Facing API Error Messages

- **Plan reference**: `.cg-docs/plans/2026-11-08-user-facing-api-error-messages.md`
- **Active deviation policy**: `ask` (stored: `ask`, runtime override: none)
- **Started**: 2026-11-08

## Completed Steps / Phases

- **Phase 1: Condition Helper & API Boundary** — Completed 2026-11-08
  - Step 1: Created `R/errors.R` (`piptm_abort()`, `piptm_user_error_message()`, `.PIPTM_GENERIC_USER_ERROR`, `.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR`) and `tests/testthat/test-errors.R` (15 passing assertions).
  - Step 2: Updated `capture_with_warnings()` in `inst/plumber/helpers.R`, `filter_base` parsing on `GET|POST /table`, and `POST /table/download` error handling in `inst/plumber/plumber.R`; updated `test-api-helpers.R`, `test-api-endpoints.R`, and `test-api-table-download.R`.
- **Phase 2: Migrate High-Traffic User-Reachable Error Sites** — Completed 2026-11-08
  - Step 3: Migrated user-reachable `cli_abort()` sites in `R/table_maker.R` (`pip_lookup()` and `table_maker()`) and `R/load_data.R` (`load_survey_microdata()` and `load_surveys()`).
  - Step 4: Migrated user-reachable `cli_abort()` sites in `R/measures.R`, `R/manifest.R`, `R/description_builder.R`, `inst/plumber/helpers.R`, and `inst/plumber/plumber.R`.
- **Phase 3: Coverage Audit Guard & API Contract Documentation** — Completed 2026-11-08
  - Step 5: Added static AST coverage audit test `tests/testthat/test-error-coverage-audit.R` (9 passing assertions).
  - Step 6: Updated `docs/api-contract.md` and `README.md` with the user-safe `errors[]` guarantee and representative error messages.

## Deviations

- None.

## Accepted Exceptions

- None.

## Evidence Table

| ID | Phase | Evidence Required | Command/Artifact | Required | Status |
|----|-------|-------------------|------------------|----------|--------|
| V1 | 1 | `piptm_abort()`, `capture_with_warnings()`, `filter_base` GET/POST handling, and `/table/download` 400 error tests pass (including `user_message` extraction, generic fallback for unmapped errors, and no leak into `warnings[]`) | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-errors.R'); testthat::test_file('tests/testthat/test-api-helpers.R'); testthat::test_file('tests/testthat/test-api-table-download.R')"` | yes | passed (15 + 173 + 103 + 250 PASS, 0 FAIL) |
| V2 | 2 | Migrated high-traffic domain error sites in `R/table_maker.R`, `R/load_data.R` (`load_survey_microdata` and `load_surveys`), `R/measures.R`, `R/manifest.R`, `R/description_builder.R`, and `inst/plumber/` return plain-English `user_message` strings in 422 API responses while existing R `expect_error()` tests still pass | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-endpoints.R'); testthat::test_file('tests/testthat/test-api-description.R'); testthat::test_file('tests/testthat/test-table-maker-v2.R'); testthat::test_file('tests/testthat/test-load-data.R')"` | yes | passed (258 + 53 + 13 + 128 + 49 + 11 + 24 PASS, 0 FAIL) |
| V3 | 3 | Static AST coverage audit test passes and `docs/api-contract.md` + `README.md` document the user-safe `errors[]` guarantee | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-error-coverage-audit.R')"` | yes | passed (9 PASS, 0 FAIL) |
| V4 | final | Full package test suite passes with 0 failures | `Rscript -e "devtools::test()"` | yes | passed (1747 PASS, 0 FAIL, 11 SKIP) |

## Constraints Check

| ID | Phase | Constraint | Check | Status |
|----|-------|------------|-------|--------|
| C1 | 1 | Developer-facing `conditionMessage(e)` in R sessions must remain unchanged so existing `expect_error()` unit tests and interactive R debugging do not regress | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-table-maker-v2.R'); testthat::test_file('tests/testthat/test-load-data.R'); testthat::test_file('tests/testthat/test-measures-current.R'); testthat::test_file('tests/testthat/test-manifest.R')"` | passed (13 + 128 + 11 + 49 PASS, 0 FAIL) |
| C2 | 2 | Domain error logic (when an error is thrown vs. when partial results + warnings are returned) must not change | Verify no control-flow conditions (`if (...)`) around abort sites are altered in `git diff R/` | passed (only `cli_abort` -> `piptm_abort(..., user_message = ...)` replacements) |
| C3 | final | Unmapped errors must never leak raw `conditionMessage(e)` into `errors[]` or `warnings[]` | Verified by `test-api-helpers.R` and `test-api-endpoints.R` unmapped-error assertions | passed |

## Remaining Uncertainty

- None.

## Final Status

- `completed`
