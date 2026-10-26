---
date: 2026-10-26
title: "Execution report: /table/download Approach C stateless flattening (Shape A)"
plan: ".cg-docs/plans/2026-10-26-table-download-approach-c-flattening.md"
status: completed
---

# Execution report

## Plan reference

`.cg-docs/plans/2026-10-26-table-download-approach-c-flattening.md`

## Active deviation policy

`ask`. No runtime override.

## Run 2026-10-26: All Phases Completed

### Completed steps

- **Step 1**: Implemented and exported `flatten_table_response()` in `R/table_response.R` with full roxygen2 documentation (`@examples`, `@section Errors:`), `man/flatten_table_response.Rd`, and `NAMESPACE` export. Verified with 34 unit test assertions in `tests/testthat/test-table-response.R` (190 passing total).
- **Step 2**: Wired `flatten_table_response()` into `POST /table/download` in `inst/plumber/plumber.R` (`simplifyVector = FALSE` parsing, HTTP 400 error mapping, removed obsolete `population` coercion loop).
- **Step 3**: Rewrote `tests/testthat/test-api-table-download.R` for Approach C Shape A CSV/RDS exports, 3-covariate and national fixture round-trips, multi-survey stacking, determinism, `"NR"`/scaled-percentage preservation, and HTTP 400 rejection of legacy flat-row and malformed payloads (93 passing assertions).
- **Step 4**: Updated `docs/api-contract.md` (`POST /table/download` section updated to Shape A; temporary incompatibility note removed) and `NEWS.md`; ran `devtools::document()` and full test suite (**1679 passed, 0 failed**).

### Deviations

None.

### Accepted exceptions

None.

### Evidence

| ID | Status | Note |
|----|--------|------|
| V1 | satisfied | `flatten_table_response()` flattens Approach C `data[]` into Shape A with exact column order (`test-table-response.R`) |
| V2 | satisfied | Formatted measure strings (`"4.50"`, `"NR"`, `NA_character_`) preserved verbatim; `poverty_line` numeric; national table has 0 dim cols (`test-table-response.R` + `test-api-table-download.R`) |
| V3 | satisfied | `POST /table/download` round-trips 3-covariate, national, and multi-survey Approach C payloads into CSV and RDS with HTTP 200 (`test-api-table-download.R`) |
| V4 | satisfied | Legacy flat-row payloads, empty `data`, missing fields, and mismatched multi-survey schemas rejected with HTTP 400 (`test-api-table-download.R`) |
| V5 | satisfied | `docs/api-contract.md`, `NEWS.md`, `NAMESPACE`, and `man/flatten_table_response.Rd` updated and consistent |
| V6 | satisfied | Full test suite passed (`1679 passed, 0 failed`) |
