# Execution Report: Relax Layout-Slot Validation to a Parent-Child Dependency Rule

- **Plan**: `.cg-docs/plans/2026-11-13-layout-slot-dependency-rule.md`
- **Branch**: `fix/layout-params`
- **Started**: 2026-11-13
- **Completed**: 2026-11-13
- **Scope**: Phases 1 & 2 (Complete)
- **Deviation policy**: `ask`
- **Review mode**: `review:manual` (default)

## Run Log

### Phase 1: Research

#### Step 1 — Verify downstream slot-order independence
- Confirmed that downstream computation, serializer, layout builder, flattener, and description builders handle arbitrary slot combinations independently with zero contiguous prefix assumptions.
- Identified the two validator locations enforcing the old prefix rule: `inst/plumber/helpers.R` and `R/table_params.R`.

### Phase 2: Implement, document, and test

#### Step 2 — Implement the parent-dependency validation rule
- Replaced the contiguous prefix/gap check in `inst/plumber/helpers.R` with two independent parent-child dependency checks:
  - `superColumn` requires `column`
  - `superRow` requires `row`
  - Arity/type errors on parent slots are excluded from triggering cascading dependency errors on child slots.
- Replaced the prefix check in `R/table_params.R` (`.validate_and_normalize_table_params()`) with the same parent-child dependency checks for direct R callers of `build_description_model()`.
- Error messages updated to complete, actionable, user-friendly sentences suitable for direct display in the PIP UI:
  - ``"`superColumn` requires `column` to be filled. Please assign a variable to Columns before using Super Columns."``
  - ``"`superRow` requires `row` to be filled. Please assign a variable to Rows before using Super Rows."``

#### Step 3 — Update API documentation strings
- Updated OpenAPI spec description blocks, request schema descriptions, and Plumber `@param` annotations in `inst/plumber/plumber.R` (under `/table`, `/metadata`, and `/cell-definition`).
- Updated `docs/api-contract.md` and `SWAGGER_GUIDE.md` to document the parent-child dependency rules.

#### Step 4 — Update and extend tests
- `tests/testthat/test-api-helpers.R`: Added comprehensive tests covering all combinations of slots (row alone, column alone, row+column in either order, superColumn alone, column+superRow without row, column+superColumn+row, row+superRow, column+row+superRow, column+superColumn without row), isolation from parent arity failures, and multi-error validation (256 PASS).
- `tests/testthat/test-api-metadata.R`: Updated `/metadata` tests for dependency error responses and duplicate slot detection (64 PASS).
- `tests/testthat/test-api-cell-definition.R`: Updated `/cell-definition` tests for dependency error responses and confirmed row alone without column returns 200 (66 PASS).
- `tests/testthat/test-table-params.R`: Updated direct parameter validation tests for direct R callers (34 PASS).
- `tests/testthat/test-description-builder.R`: Updated `build_description_model()` tests and confirmed row-alone layout renders Rows metadata (106 PASS).
- `tests/testthat/test-table-response.R`: Added tests for `build_table_layout()`, `serialize_table_response()`, and `flatten_table_response()` with row-only and row+superRow layouts (215 PASS).
- `tests/testthat/test-api-endpoints.R`: Updated GET `/table` tests for 400 dependency failures and 200 responses on row-alone and column+superColumn without row (959 PASS).

## Verification Surface Assessment

- **V1** (`.validate_table_input()` slot combinations): Passed
- **V2** (Unmet dependencies rejected with user-friendly errors): Passed
- **V3** (Valid multi-slot combinations accepted): Passed
- **V4** (Non-layout error checks unchanged and isolated): Passed
- **V5** (Endpoint status codes 400/200): Passed
- **V6** (Row-only and row+superRow layouts in serializer/flattener): Passed
- **V7** (User-friendly error messages): Passed
- **V8** (Documentation updated): Passed
- **V9** (Targeted test suites pass): Passed (1700 tests passed across all touched suites)
