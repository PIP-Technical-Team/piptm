# Execution Report: Remove Unused `dimensions` Field from `surveys-ui` Endpoint

- **Plan reference**: `.cg-docs/plans/2026-11-11-remove-surveys-ui-dimensions.md`
- **Date**: 2026-11-11
- **Active deviation policy**: `ask` (stored: `ask`, runtime override: none)

## Completed Steps

- **Step 1**: Update `piptm_surveys_ui()` and documentation (`R/manifest.R`, `man/piptm_surveys_ui.Rd`) -- Done (2026-11-11)
- **Step 2**: Update `GET /surveys-ui` Plumber OpenAPI comments (`inst/plumber/plumber.R`) -- Done (2026-11-11)
- **Step 3**: Update unit and endpoint tests (`tests/testthat/test-surveys-ui.R`, verified `tests/testthat/test-api-endpoints.R`) -- Done (2026-11-11)

## Deviations

- None

## Accepted Exceptions

- None

## Evidence Table

| ID | Evidence Required | Command/Artifact | Required | Status |
|----|-------------------|------------------|----------|--------|
| V1 | `test-surveys-ui.R` passes and verifies `"dimensions"` is absent from `piptm_surveys_ui()` output | `testthat::test_file("tests/testthat/test-surveys-ui.R")` (8 passed, 0 failed) | yes | passed |
| V2 | API endpoint tests in `test-api-endpoints.R` pass | `testthat::test_file("tests/testthat/test-api-endpoints.R")` (809 passed, 0 failed) | yes | passed |

## Constraints Check

| ID | Constraint | Check | Status |
|----|------------|-------|--------|
| C1 | `piptm_manifest()` and `GET /surveys` continue to retain `dimensions` for backend intersection and developer inspection | Existing manifest and `/categories` / `/covariates` / `/surveys` tests in `test-api-endpoints.R` pass | passed |

## Remaining Uncertainty

- None

## Final Status

`completed`
