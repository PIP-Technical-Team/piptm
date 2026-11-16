---
date: 2026-11-13
depth: standard
type: standard
plan: .cg-docs/plans/2026-11-13-layout-slot-dependency-rule.md
findings:
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
  P2.11: fixed
  P2.12: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  P3.5: fixed
  P3.6: fixed
  P3.7: fixed
  P3.8: skipped
  P3.9: skipped
  P3.10: skipped
---

## Review Report

**Review mode**: standard
**Files reviewed**: 12
**Findings**: 23 (P0: 0, P1: 1, P2: 12, P3: 10)

### P0 — BLOCKING (immediate remediation required)
*None.*

### P1 — CRITICAL (must fix before merge)
- **[P1.1]** [cg-architecture] `R/table_params.R:76-161` [safe_auto] — Direct callers passing layout slots without `by` bypass dimension validation and `poverty_line` checks.
  **Why**: When a direct R caller passes `column`, `row`, etc. without `by`, `by` is `NULL`, which skips `piptm_layout_covariates()` dimension validation and skips the `pov_status` poverty line requirement.
  **Fix**: When `is.null(by) && length(slot_dims) > 0L`, derive `by <- slot_dims`.

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-architecture] `R/table_params.R:42,145` [safe_auto] — Direct R callers can assign `analysis_var` to a layout slot or `by`.
  **Why**: `inst/plumber/helpers.R:405-414` rejects assigning `analysis_var` to any layout slot, but `.validate_and_normalize_table_params()` takes `analysis_var` and does not cross-check it against layout slots.
  **Fix**: Reject requests where `analysis_var %in% effective_by`.
- **[P2.2]** [cg-code-quality] `R/table_params.R:118,128` and `inst/plumber/helpers.R:348,357` [manual] — Layout slot dependency user error strings are duplicated across files.
  **Why**: User-facing error message strings are hardcoded verbatim across package and Plumber files rather than centralized.
  **Fix**: Centralize constants `.PIPTM_SUPERCOLUMN_DEPENDENCY_USER_ERROR` and `.PIPTM_SUPERROW_DEPENDENCY_USER_ERROR` in `R/errors.R`.
- **[P2.3]** [cg-testing] `tests/testthat/test-api-helpers.R:512` [safe_auto] — Weak assertion and ordering masking with `expect_setequal` on `by`.
  **Why**: `expect_setequal` masks the canonical visual-hierarchy order enforced by `validate_table_input()`.
  **Fix**: Replace with `expect_identical` asserting canonical slot order.
- **[P2.4]** [cg-testing] `tests/testthat/test-api-helpers.R:503` [safe_auto] — Incomplete argument order invariance test.
  **Why**: Test case 3 calls `.vti()` with `row, column` but does not assert identity against `column, row`.
  **Fix**: Test both argument orders explicitly and assert `identical(res_rc$by, res_cr$by)` and `identical(res_rc$slots, res_cr$slots)`.
- **[P2.5]** [cg-testing] `tests/testthat/test-api-helpers.R:524` [safe_auto] — Missing edge case test for `superRow` alone without any other slot.
  **Why**: `superRow` without `row` was only tested with `column` present; `superRow` alone was untested.
  **Fix**: Add an explicit test case for `superRow` alone.
- **[P2.6]** [cg-testing] `tests/testthat/test-api-helpers.R:580` [safe_auto] — Untested cascading error suppression for `row -> superRow`.
  **Why**: `column` arity error suppression was tested, but `row` arity error suppression was untested.
  **Fix**: Add a paired test verifying that arity errors on `row` suppress the `superRow` dependency error.
- **[P2.7]** [cg-testing] `tests/testthat/test-api-helpers.R:514` [safe_auto] — Untested simultaneous unmet dependencies (`superColumn` and `superRow` both supplied without parents).
  **Why**: When both child slots are supplied without parents, both error messages must be emitted.
  **Fix**: Add a test verifying both dependency error messages are returned together.
- **[P2.8]** [cg-testing] `tests/testthat/test-table-response.R:676` [safe_auto] — Unasserted intermediate serialization and missing Column+SuperColumn test.
  **Why**: `ser_rsr` was not inspected before flattening, and `column + superColumn` without `row` was not tested through the serializer/flattener.
  **Fix**: Add structural assertions for `ser_rsr` and add a test section for `column + superColumn`.
- **[P2.9]** [cg-testing] `tests/testthat/test-api-endpoints.R:1655` [safe_auto] — Incomplete endpoint coverage for newly valid relaxed layouts.
  **Why**: `GET /table` with `row + superRow` and `POST /metadata` with `row` alone were not tested for HTTP 200.
  **Fix**: Add tests for these newly valid endpoints returning 200.
- **[P2.10]** [cg-documentation] `inst/plumber/plumber.R:452` [safe_auto] — Stale "layout-slot ordering violation" in OpenAPI HTTP 400 response description.
  **Why**: Layout slots no longer have an ordering constraint; the error is a dependency violation.
  **Fix**: Update description to "layout-slot dependency violation".
- **[P2.11]** [cg-documentation] `SWAGGER_GUIDE.md:84-85` [safe_auto] — Parameter table for `/table` omits slot dependency requirements.
  **Why**: Table omitted `requires column` and `requires row`.
  **Fix**: Add dependency notes to the `/table` parameter table.
- **[P2.12]** [cg-documentation] `docs/api-contract.md:166-169` [safe_auto] — Parameter bullet points reference `/dimensions` instead of `/covariates` and omit dependency hints.
  **Why**: `GET /covariates` is the canonical source for layout slots.
  **Fix**: Change `/dimensions` to `/covariates` and add inline dependency hints.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-code-quality] `inst/plumber/plumber.R:112-114` [safe_auto] — Inconsistent leading indentation in OpenAPI description block.
- **[P3.2]** [cg-code-quality] `inst/plumber/helpers.R:344-360` [safe_auto] — Flatten nested conditionals and add parentheses `!(x %in% y)`.
- **[P3.3]** [cg-code-quality] `R/table_params.R:112,122` [safe_auto] — Parenthesize `%in%` expressions.
- **[P3.4]** [cg-testing] `tests/testthat/test-table-params.R:251` [safe_auto] — Missing valid relaxed slot combinations (`column + superColumn`, `row + superRow`) for direct callers.
- **[P3.5]** [cg-documentation] `SWAGGER_GUIDE.md:106-109` [safe_auto] — Error summary for `/table` omits layout slot dependency errors.
- **[P3.6]** [cg-documentation] `R/table_params.R:28-30` & `inst/plumber/helpers.R` [safe_auto] — Roxygen docstrings do not describe parent-child slot dependency.
- **[P3.7]** [cg-code-quality] `tests/testthat/*` [safe_auto] — Long assertion lines exceeding 80 characters.
- **[P3.8]** [cg-code-quality] `inst/plumber/helpers.R:23`, `R/table_params.R:85`, `R/table_response.R:292` [advisory] — Layout slot names definition duplication.
- **[P3.9]** [cg-architecture] `R/table_params.R:132-141` vs `inst/plumber/helpers.R:363-378` [advisory] — Divergence in user-facing error detail for duplicate layout slot assignments.
- **[P3.10]** [cg-documentation] `inst/plumber/plumber.R:1397-1398` [advisory] — Minor parameter description phrasing divergence on `/cell-definition`.

### ✅ Passed
- cg-reproducibility: No issues found
- cg-performance: No issues found
- cg-version-control: No issues found
- cg-data-quality: No issues found
