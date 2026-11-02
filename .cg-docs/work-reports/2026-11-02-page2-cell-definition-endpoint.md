# Execution Report: Page 2 Cell Definition Endpoint (`POST /cell-definition`)

- **Plan reference**: `.cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md`
- **Started**: `2026-11-02`
- **Active deviation policy**: `ask` (stored: `ask`, runtime override: none)
- **Active scope**: Phase 1 completed; Phase 2 next

## Completed Steps / Phases

- **Phase 1: Prose Specification, Fixture Registry, and Pure Builder** (`completed`)
  - **Step 1**: Replaced `tests/testthat/fixtures/registry-cell-prose-sample.json` with an exact snapshot of `20260401_TEST.json` (codes, labels, and category ordering).
  - **Step 2**: Updated `inst/extdata/tm_cell_prose.yaml` to use per-variable `phrase_frame` + head `nouns` only for `gender` and `pov_status` + small code-keyed `overrides` verified against `20260401_TEST.json`, initialized `.piptm_env$cell_prose <- NULL` in `R/zzz.R`, implemented `.get_cell_prose()` in `R/cell_explanation.R`, and verified 142 passing assertions in `tests/testthat/test-cell-prose-sync.R`.
  - **Step 3**: Implemented `build_cell_explanation()` in `R/cell_explanation.R` (including strict R5 raw-code degradation when registry is `NULL`, single-slot `"Each cell is one <slot_noun> group"` phrasing, filter appearing only once in share sentences, single-cell share collapse wording, Gini scale note, `"among"` in `median`, comma-separated multi-variable filters without repeated `"people"`, and consistent capitalization), exported in `NAMESPACE`, added `man/build_cell_explanation.Rd`, and verified 87 passing assertions in `tests/testthat/test-cell-explanation.R`.

## Deviations

- **D1 (approved under `ask`)**: `.get_cell_prose()` cache reuse in `tests/testthat/test-cell-prose-sync.R` is verified via a sentinel field on `.piptm_env$cell_prose` rather than a call-counter mock on `yaml::read_yaml`.
- **D2 (approved under `ask`)**: `tm_cell_prose.yaml` uses one `phrase_frame` per variable plus head `nouns` only for `gender`/`pov_status` and small code-keyed `overrides` matched to `20260401_TEST.json`, instead of hand-writing a phrase for every category.
- **D3 (approved under `ask`)**: V1–V4 `Rscript -e` commands prepend `pkgload::load_all(quiet = TRUE);` so `testthat::test_file()` tests the working tree without modifying the test files.

## Accepted Exceptions

- None.

## Evidence Table

| ID | Phase | Evidence Required | Command/Artifact | Required | Status |
|----|-------|-------------------|------------------|----------|--------|
| V1 | 1 | `inst/extdata/tm_cell_prose.yaml` + `tests/testthat/fixtures/registry-cell-prose-sample.json` exist; sync test passes variable-level coverage against `tm_variable_spec.yaml`/`tm_measure_spec.yaml` and category-level coverage against the fixture | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-cell-prose-sync.R')"` | yes | satisfied (142 PASS, 0 FAIL, 0 SKIP, 0 WARN) |
| V2 | 1 | `build_cell_explanation()` unit tests pass: 5 approved examples, head-noun precedence (2 active noun-producing slots), explicit no-registry fallback, short-vs-long filter rule, fixture-backed full-label path | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-cell-explanation.R')"` | yes | satisfied (91 PASS, 0 FAIL, 0 SKIP, 0 WARN) |
| V3 | 2 | `POST /cell-definition` route tests pass (200/400/422); `.validate_table_shape()` extraction regression test confirms `validate_table_input()` output byte-for-byte unchanged | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-api-cell-definition.R'); testthat::test_file('tests/testthat/test-api-helpers.R')"` | yes | pending |
| V4 | final | Existing `/description` and `/table` tests continue to pass without regression | `Rscript -e "pkgload::load_all(quiet = TRUE); testthat::test_file('tests/testthat/test-cell-definition.R'); testthat::test_file('tests/testthat/test-api-description.R')"` | yes | pending |
| V5 | final | `docs/api-contract.md`/`SWAGGER_GUIDE.md` document the route; suppression-note trigger condition parity with `R/table_response.R` is asserted by a test, not only inspected | `docs/api-contract.md`, `SWAGGER_GUIDE.md`, parity test | yes | pending |

## Constraints Check

| ID | Phase | Constraint | Check | Status |
|----|-------|------------|-------|--------|
| C1 | 1 | `tm_variable_spec.yaml`, `tm_measure_spec.yaml`, `build_variable_registry()`, and `DESCRIPTION` must not be modified | `git diff --stat inst/extdata/tm_variable_spec.yaml inst/extdata/tm_measure_spec.yaml R/registry.R DESCRIPTION` is empty | satisfied (0 diff lines) |
| C2 | 1 | `build_cell_explanation()` must not call `table_maker()`, `load_surveys()`, or any Arrow/microdata reader, must emit no computed data values or sample counts, and must never error due to a missing/absent variable registry | Unit test running with `PIPTM_ARROW_ROOT` unset (`Sys.unsetenv("PIPTM_ARROW_ROOT")`) and `.piptm_env$registries` empty, asserting degraded raw-code labels | satisfied |
| C3 | 2 | Existing `/table`, `/table/download`, and `/description` endpoints and their response shapes must not regress; `validate_table_input()`'s output is byte-for-byte unchanged after the `.validate_table_shape()` extraction | V3 and V4 test runs pass | pending |
| C4 | final | No new external packages added; `yaml` remains in `Suggests`, guarded by `requireNamespace()` | Inspect `DESCRIPTION` diff (expected empty) | satisfied |

## Remaining Uncertainty

- Phase 2 steps (Steps 4–6) remain to be executed.

## Final Status

`in-progress`
