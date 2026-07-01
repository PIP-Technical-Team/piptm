# Work Report — 2026-07-01 — table-endpoint-computation-refactor

## Plan Reference
- Plan: `.cg-docs/plans/2026-07-01-table-endpoint-computation-refactor.md`
- Started: 2026-07-01
- Mode: `review:manual`
- Deviation policy: `ask`

## Run Log

### Run 1 — Start
- Initialized phase execution for **Phase 1: Bug Fixes & Preparation**.
- Loaded charter, local config, and active plan.
- Shared contract files under `.github/shared/` were not present in this workspace path; proceeding with available project artifacts.
- Next: build Phase 1 test index, establish red-phase failures, then implement fixes for:
  1) `compute_measures()` shares dispatch slot/vector bug,
  2) family naming mismatch (`summary_stats` consistency),
  3) `compute_shares()` invalid condition and multi-measure behavior.

### Run 2 — Red phase confirmation
- Added focused red-phase tests in `tests/testthat/test-phase1-shares-and-family-routing.R`.
- Red-phase confirmed with expected failures:
  - `.classify_measures` did not return `summary_stats` family key.
  - `compute_measures()` failed due shares dispatch using wrong slot/vector.
  - `compute_shares()` rejected `measures` argument (unused argument error).

### Run 3 — Phase 1 implementation
- Updated `R/measures.R`:
  - Switched summary family mapping from `summary_statistics` to `summary_stats`.
  - Kept canonical order aligned with `summary_stats`.
- Updated `R/compute_measures.R`:
  - Fixed shares dispatch slot (`results$shares`).
  - Fixed shares measure vector (`classified$shares`).
  - Added summary target fallback to `"welfare"` for current pre-refactor signature safety.
- Updated `R/compute_shares.R`:
  - Fixed invalid condition.
  - Added `measures` parameter and validation.
  - Implemented multi-measure computation in one call (`pop_share`, `target_within_group_share`, `target_survey_share`).
  - Enforced target-variable requirement only for target-share measures.

### Run 4 — Green checks and diagnostics
- Targeted phase-1 tests now pass:
  - `test-phase1-shares-and-family-routing.R`: PASS (8 assertions).
- Additional focused runtime checks passed:
  - `.classify_measures(c("mean","gini","headcount"))` returns ordered families `poverty`, `inequality`, `summary_stats`.
  - `compute_measures(..., measures=c("mean","pop_share"))` returns both measures without overwrite.
- Diagnostics (`get_errors`) on touched files report **no errors**.

### Run 5 — Broader test signal
- Running broad existing files (`test-measures.R`, `test-compute-measures.R`) shows substantial pre-existing baseline failures unrelated to this phase's three fixes (e.g., missing `pip_tablemaker_measures()` in current load context).
- Phase 1 evidence requirements are satisfied via targeted red/green tests and direct behavior checks.

### Run 6 — Phase 2 red baseline
- Added focused Phase 2 tests in `tests/testthat/test-phase2-analysis-var-and-poverty-line.R`.
- Red-phase confirmed:
  - `validate_table_input()` accepted missing `analysis_var` (expected reject).
  - `validate_table_input()` did not accept `analysis_var` argument yet (unused argument errors).
  - `/table` returned non-400 responses for missing/unknown `analysis_var` and missing `poverty_line` with `pov_status`.

### Run 7 — Phase 2 implementation
- Updated `inst/plumber/helpers.R`:
  - `validate_table_input()` now accepts required `analysis_var` and optional scalar `poverty_line`.
  - Added `analysis_var` required check and allowlist validation.
  - Added scalar/finite/positive validation for `poverty_line`.
  - Added cross-rule: require `poverty_line` when `analysis_var == "pov_status"` or `"pov_status" %in% by`.
  - Excluded `pov_status` from `by` dimension allowlist checks.
  - Return payload now includes `poverty_line` (singular) instead of `poverty_lines`.
- Updated `inst/plumber/plumber.R`:
  - `/table` signature now accepts `analysis_var` and `poverty_line`.
  - Validation call updated to named arguments including `analysis_var`.
  - Normalized validated scalar `poverty_line` into downstream `table_maker()` call (`poverty_lines = poverty_line`) for Phase-2 compatibility prior to Phase-3 signature migration.

### Run 8 — Phase 2 green checks
- Focused Phase 2 tests pass:
  - `test-phase2-analysis-var-and-poverty-line.R`: PASS (15 assertions).
- Diagnostics (`get_errors`) on touched files report **no errors**:
  - `inst/plumber/helpers.R`
  - `inst/plumber/plumber.R`
  - `tests/testthat/test-phase2-analysis-var-and-poverty-line.R`
- Verification surface evidence met for Phase 2:
  - `V2`: `/table` accepts `analysis_var` + scalar `poverty_line` path.
  - `V3`: missing required `poverty_line` for `pov_status` usage rejected at 400-validation boundary.

### Run 9 — Phase 3 red baseline
- Added focused Phase 3 tests in `tests/testthat/test-phase3-core-signatures-and-routing.R`.
- Red-phase confirmed with expected failures:
  - `table_maker()` still exposed legacy `target_variable` / `poverty_lines` parameters.
  - `compute_measures()` still exposed legacy `target_variable` / `poverty_lines` parameters.
  - Calls using `analysis_var` failed with unused argument errors.

### Run 10 — Phase 3 implementation
- Updated `R/compute_measures.R`:
  - Signature changed to `compute_measures(dt, measures, analysis_var = NULL, poverty_line = NULL, by = NULL)`.
  - Dynamic required-column guard now depends on `analysis_var` type.
  - Poverty validation now uses scalar `poverty_line`.
  - Dispatch derives `target_variable` internally (`NULL` for `pov_status`, otherwise `analysis_var`).
- Updated `R/table_maker.R`:
  - Signature changed to `table_maker(pip_id = NULL, analysis_var, measures, poverty_line = NULL, by = NULL, filter_base = NULL, ppp = 2021L, release = NULL, pop_share_threshold = 0.01)`.
  - Removed triplet fallback path from runtime entry; `pip_id` is required in this phase.
  - Added internal derivation `target_variable <- if (analysis_var == "pov_status") NULL else analysis_var`.
  - Updated `needed_cols` to include only required columns by analysis mode.
  - Excluded `pov_status` from manifest dimension pre-filter checks.
  - Added `pov_status` derivation in loaded data when used in `by`, requiring scalar positive `poverty_line`.
  - Updated call into `compute_measures()` with named `analysis_var` and `poverty_line`.
- Updated `inst/plumber/plumber.R`:
  - Aligned `/table` to call `table_maker(analysis_var = ..., poverty_line = ...)`.

### Run 11 — Phase 3 green checks
- Focused Phase 3 tests pass:
  - `test-phase3-core-signatures-and-routing.R`: PASS (12 assertions).
- Diagnostics (`get_errors`) on touched files report **no errors**:
  - `R/compute_measures.R`
  - `R/table_maker.R`
  - `inst/plumber/plumber.R`
  - `tests/testthat/test-phase3-core-signatures-and-routing.R`
- Verification surface evidence met for Phase 3:
  - `V4`: `table_maker()` signature and `pov_status` handling updated.
  - `V5`: `compute_measures()` family routing and signature updated.

### Run 12 — Phase 4 red baseline
- Added focused Phase 4 tests in `tests/testthat/test-phase4-table-and-endpoint.R` covering:
  - welfare / poverty / continuous analysis paths in `table_maker()`
  - `pov_status` in `by` validation behavior
  - `/table` happy-path and required-parameter rejection behavior
- Red-phase confirmed with one failing scenario:
  - `table_maker()` rejected `by = "pov_status"` too early via `.validate_by()` instead of allowing derived `pov_status` logic to enforce `poverty_line`.

### Run 13 — Phase 4 implementation + green checks
- Updated `R/table_maker.R` to validate only non-derived dimensions:
  - introduced `by_validate <- setdiff(by, "pov_status")`
  - `.validate_by()` now runs on `by_validate` so derived `pov_status` is accepted for later guarded derivation.
- Focused Phase 4 tests pass:
  - `test-phase4-table-and-endpoint.R`: PASS (13 assertions).
- Diagnostics (`get_errors`) on touched files report **no errors**.
- Verification surface evidence met for this phase increment:
  - `V7`: `table_maker()` long-format multi-analysis scenarios validated in focused tests.
  - `V8`: `/table` validation and happy-path checks validated in focused tests.

### Run 14 — Phase 5 documentation + diagnostics
- Confirmed doc alignment for:
  - `R/table_maker.R`
  - `R/compute_measures.R`
  - `inst/plumber/plumber.R`
  - `inst/plumber/helpers.R`
- Added/normalized `NEWS.md` breaking-change notes for:
  - required `analysis_var`
  - `poverty_lines` → `poverty_line` migration
  - `pov_status`/`poverty_line` cross-validation rule
- Ran `devtools::check()` as a broad diagnostic pass.
  - Result: existing repository-wide baseline failures remain (legacy tests and historical NOTES/WARNINGS).
  - No new syntax/lint errors in touched Phase 2–5 files.

### Run 15 — Focused verification + Phase 5 closeout
- Focused checks pass on the Phase 3/4 verification surface:
  - `tests/testthat/test-phase3-core-signatures-and-routing.R` (PASS)
  - `tests/testthat/test-phase4-table-and-endpoint.R` (PASS)
- `get_errors` reports no problems in touched files:
  - `R/table_maker.R`
  - `R/compute_measures.R`
  - `inst/plumber/plumber.R`
  - `inst/plumber/helpers.R`
  - `NEWS.md`
- Phase 5 evidence satisfied for:
  - `V9`: documentation/signature consistency validated on touched surfaces; broad check executed with known baseline failures.
  - `V10`: `NEWS.md` includes breaking-change entry.
