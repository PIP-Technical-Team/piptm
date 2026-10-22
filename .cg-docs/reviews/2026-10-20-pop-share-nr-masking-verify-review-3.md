---
date: 2026-10-22
depth: light
parent-review: .cg-docs/reviews/2026-10-20-pop-share-nr-masking-review.md
type: verification
findings:
  P3.1: open
---

## Review Report

**Review mode**: verify (light, following fix-triage — third pass, covering the full cumulative P1/P2/P3 fix-triage diff)
**Parent review**: `.cg-docs/reviews/2026-10-20-pop-share-nr-masking-review.md`
**Resolved findings being verified**: all 30 original findings (26 fixed, 4 skipped) — this pass focuses primarily on P3.1, P3.2, P3.4, the newest changes since the round-2 verify pass
**Files reviewed**: 17 (full cumulative diff; primary focus: `R/measures.R`, `R/format_table_output.R`, `man/format_table_output.Rd`, `.gitignore`)
**Findings**: 1 (P0: 0, P1: 0, P2: 0, P3: 1)

Parsed 1 finding ID.

### P0 — BLOCKING (immediate remediation required)

None.

### P1 — CRITICAL (must fix before merge)

None. No cross-file breakage found.

### P2 — IMPORTANT (should fix)

None.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-testing] `tests/testthat/test-format-table-output.R:169` — No parallel registry-sync guard exists for the new `.WHOLE_NUMBER_MEASURES` constant (added in the P3.1 fix-triage item from the parent review).
  **Why**: The existing P2.12 test only asserts `.PCT_MEASURES %in% names(.MEASURE_REGISTRY)`. A future typo or stale entry in `.WHOLE_NUMBER_MEASURES` (e.g. a renamed `pop_poverty`) would silently misclassify rounding behavior with no test catching it — the exact risk class the P2.12 test was written to guard against, now reintroduced for the sibling constant.
  **Fix**: Add `expect_true(all(piptm:::.WHOLE_NUMBER_MEASURES %in% names(piptm:::.MEASURE_REGISTRY)))` alongside the existing test, or extend the existing test to loop over both constants.
  Tag: [advisory] — left open; low-priority coverage gap on a constant whose values were independently verified character-for-character identical to the prior inline literal, so no live regression risk today.

### ✅ Passed

- **cg-code-quality**: `.WHOLE_NUMBER_MEASURES` (`R/measures.R`) correctly mirrors `.PCT_MEASURES`'s placement, roxygen tags (`@keywords internal`, `@noRd`), and naming convention; its reference in `R/format_table_output.R` (`meas %chin% .WHOLE_NUMBER_MEASURES`) is idiomatically parallel to the existing `idx_pct` pattern with no inline literal remaining. The new `@section Architectural boundary with table_maker()` roxygen block is well-formed (no broken cross-references). The `.gitignore` additions are new, non-duplicated, syntactically valid entries.
- **cg-testing**: `.WHOLE_NUMBER_MEASURES <- c("pop_poverty", "nobs")` is character-for-character identical (same order, same values) to the removed inline literal — behavioral equivalence confirmed directly and via the still-passing whole-number formatting test. The roxygen `@section` addition is documentation-only with zero behavioral or coverage implications. The `.gitignore` and regenerated `.Rd` changes carry no test implications.

### Notes

The single finding in this pass is advisory-only and was not auto-applied, consistent with the verify-mode suppression policy (new, non-blocking coverage gap on fixed-scope-adjacent code, not a regression). No behavior changed across the full P1/P2/P3 fix-triage cycle: all constant/doc/config extractions were confirmed exactly equivalent to the code they replaced.

**The verification cycle has converged: no P0/P1/P2 findings remain from this pass, and the single P3 advisory note does not block merge.**
