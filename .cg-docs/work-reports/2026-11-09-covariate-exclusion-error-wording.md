# Execution Report: Name Specific Covariates in Table Exclusion Error Messages

- **Plan reference**: `.cg-docs/plans/2026-11-09-covariate-exclusion-error-wording.md`
- **Active deviation policy**: `ask` (stored: `ask`, runtime override: none)
- **Started**: 2026-11-09

## Completed Steps

- Step 1: Added `.piptm_covariate_labels()` registry-lookup helper to `R/errors.R`, with unit tests covering known/unknown varnames and registry-unavailable fallback.
- Step 2: Updated the `filter_base`-exclusion and `by`-dimension-exclusion `user_message`s in `R/table_maker.R` to name specific covariate labels, with singular/plural wording.
- Step 3: Updated 3 existing test assertions (`test-api-endpoints.R`) to the new wording and added a 2-covariate pluralization/joining test case.
- Step 4: Updated `docs/api-contract.md`'s representative-messages table.

## Deviations

- Fix-attempt 1 (Test Failure Recovery): the new covariate-label happy-path test (Step 1) passed in isolation but errored in the full-suite run due to pre-existing cross-file ambient `piptm_current_release()` state ordering (other test files legitimately clear/restore this global as part of their own isolation, following the codebase's own established `skip_if(is.null(release) ..., "No current release set")` convention already used in `test-registry-structure.R`). Added the same `skip_if` guard to the new test rather than asserting ambient state. Full suite re-run confirmed 0 failures after the fix; no second attempt was needed.

## Accepted Exceptions

- None.

## Evidence Table

| ID | Evidence Required | Command/Artifact | Required | Status |
|----|--------------------|-------------------|----------|--------|
| V1 | `test-errors.R` and `test-api-endpoints.R` pass with new wording assertions, including the 2-covariate case | `Rscript -e "pkgload::load_all(quiet=TRUE); testthat::test_file('tests/testthat/test-errors.R'); testthat::test_file('tests/testthat/test-api-endpoints.R')"` | yes | passed (56 + 262 PASS, 0 FAIL) |
| V2 | Full test suite passes with 0 failures | `Rscript -e "devtools::test()"` | yes | passed (1782 PASS, 0 FAIL, 12 SKIP, 7 WARN pre-existing) |

## Constraints Check

| ID | Constraint | Check | Status |
|----|------------|-------|--------|
| C1 | Developer-facing `conditionMessage()` text for these two abort sites is unchanged | Diff review of `cli_abort()` message arguments in `R/table_maker.R` | passed (`git diff` confirms no `cli_abort`/`piptm_abort` developer-message lines changed) |
| C2 | `.piptm_covariate_labels()` never raises, even with no release set or unknown varnames | Covered by Step 1 test scenarios | passed |

## Remaining Uncertainty

- None.

## Final Status

- `completed`
