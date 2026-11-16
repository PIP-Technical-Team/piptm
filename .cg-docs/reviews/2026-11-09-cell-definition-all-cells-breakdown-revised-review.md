---
date: 2026-11-09
depth: light
type: standard
plan: .cg-docs/plans/2026-11-09-cell-definition-all-cells-breakdown-revised.md
findings:
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: open
  P2.6: open
  P2.7: open
  P3.1: open
  P3.2: fixed
  P3.3: fixed
  P3.4: open
  P3.5: open
  P3.6: open
  P3.7: open
  P3.8: open
---

## Review Report

**Review mode**: light
**Files reviewed**: 5 (`R/cell_explanation.R`, `tests/testthat/test-cell-explanation.R`, `tests/testthat/test-api-cell-definition.R`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`)
**Findings**: 15 (P0: 0, P1: 0, P2: 7, P3: 8)

### P0 — BLOCKING (immediate remediation required)
- (None)

### P1 — CRITICAL (must fix before merge)
- (None)

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-code-quality] `R/cell_explanation.R:356` — Missing explicit `return()` in `.slot_effective_categories()` [safe_auto]
  **Why**: Project R style requires explicit `return()` in non-trivial functions; earlier guard branches use explicit `return()`.
  **Fix**: Add explicit `return()`.

- **[P2.2]** [cg-code-quality] `R/cell_explanation.R:402-406` — Missing explicit `return()` in `.compose_cell_group()` [safe_auto]
  **Why**: The terminal `if/else` block returns implicitly.
  **Fix**: Wrap both branches in `return()`.

- **[P2.3]** [cg-code-quality] `R/cell_explanation.R:430` — Missing explicit `return()` in `.slot_category_list()` [safe_auto]
  **Why**: The fallback branch returns implicitly while the noun branch returns explicitly.
  **Fix**: Add explicit `return()`.

- **[P2.4]** [cg-code-quality] `R/cell_explanation.R:320-329` — Unchecked scalar extraction in registry category vectorization [safe_auto]
  **Why**: `vapply(..., as.character(x$code)[[1L]], character(1))` throws `subscript out of bounds` if a category item has missing or null code/label.
  **Fix**: Add a safe length guard: `function(x) if (!is.null(x$code) && length(x$code) > 0L) as.character(x$code)[[1L]] else ""`.

- **[P2.5]** [cg-testing] `tests/testthat/test-cell-explanation.R:131` — Incomplete assertion on single-slot enumeration items [manual]
  **Why**: In scenario (B) (`res_lstatus`), the test asserts length 3 but does not check the resulting strings.
  **Fix**: Assert exact string values (`c("People who are employed", "People who are unemployed", "People outside the labor force")`).

- **[P2.6]** [cg-testing] `tests/testthat/test-cell-explanation.R:176` — Untested filter-by-label resolution on layout slot variables [manual]
  **Why**: `.slot_effective_categories()` matches on both code and label, but tests only exercise code-based filtering.
  **Fix**: Add a test verifying `filter_base = list(area = "Urban")` filters the layout slot cleanly.

- **[P2.7]** [cg-testing] `tests/testthat/test-api-cell-definition.R:371` — Missing API route test for 1-cell enumeration JSON array serialization [manual]
  **Why**: Route tests check 0, 1 (breakdown), 4, and 2 items, but not a 1-item combined enumeration array (`column = "area"` filtered to 1 category).
  **Fix**: Add a test verifying that a 1-cell combined enumeration serializes as a JSON array `["..."]`.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-code-quality] `R/cell_explanation.R` — Lines exceeding 80 characters (lines 612, 657, 664, etc.) [safe_auto]
- **[P3.2]** [cg-code-quality] `tests/testthat/test-cell-explanation.R:207-209` — Unlabeled assertion inside test loop [safe_auto]
- **[P3.3]** [cg-testing] `tests/testthat/test-cell-explanation.R:213` — Environment teardown in fallback test omits `cell_prose` cache reset [safe_auto]
- **[P3.4]** [cg-code-quality] `R/cell_explanation.R:420-428` — Noun detection logic mirrored across helper functions [advisory]
- **[P3.5]** [cg-code-quality] `R/cell_explanation.R:576-743` — Function complexity of `.build_what_a_cell_is()` [advisory]
- **[P3.6]** [cg-code-quality] `tests/testthat/test-api-cell-definition.R` — Repeated response body raw-to-character conversion [advisory]
- **[P3.7]** [cg-testing] `tests/testthat/test-cell-explanation.R:373` — Weak assertion for items weaving non-slot short filter phrases in Example 4 [advisory]
- **[P3.8]** [cg-testing] `tests/testthat/test-cell-explanation.R:107` — Untested fallback for unmatched filter codes on layout slot [advisory]

### ✅ Passed
- `@cg-code-quality`: Style, naming, contracts, and error handling passed cleanly with no P0/P1 issues.
- `@cg-testing`: Core test structure, Cartesian product ordering, adaptive thresholds, and Plumber serialization guards verified.
