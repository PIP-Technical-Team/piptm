---
date: 2026-11-03
depth: light
parent-review: .cg-docs/reviews/2026-11-02-page2-cell-definition-endpoint-review.md
type: verification
findings:
  P3.1: fixed
  P3.2: fixed
---

## Review Report

**Review mode**: verify (light, `@cg-code-quality` + `@cg-testing` only)
**Files reviewed**: 7 (`R/cell_explanation.R`, `inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `inst/extdata/tm_cell_prose.yaml`, `man/build_cell_explanation.Rd`, `SWAGGER_GUIDE.md`, `docs/api-contract.md`, plus the four test files)
**Findings**: 2 new (P0: 0, P1: 0, P2: 0, P3: 2), 4 suppressed per fix-triage scope

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-testing] `R/cell_explanation.R`:541-551 (`.build_who_is_in_table`) — No test exercises 3+ simultaneous `filter_base` variables.
  **Why**: The `tail_clauses <- vapply(var_phrases[-1L], .to_who_also_clause, character(1))` branch only produces a non-trivial multi-element join at 3+ filter variables (2 variables yields a single unchanged tail clause). This path has never been executed by a test, in either the pre-refactor monolith or the new split.
  **Fix**: Add a test with 3 filter variables of mixed phrase shapes (e.g. `filter_base = list(age_group = "3", educat7 = "4", area = "0")`) and assert the `who_is_in_table$text` / `cell_group` phrasing joins the 2nd and 3rd "who also" clauses correctly.

- **[P3.2]** [cg-testing] `inst/plumber/helpers.R`:798-806 (`validate_cell_definition_input`) — No test for logical-valued `filter_base` codes.
  **Why**: The validator explicitly rejects logical vectors via `is.logical(vals)`, distinguishing them from numeric/character codes, but no test supplies `filter_base = list(age_group = TRUE)` to confirm the guard actually fires with the expected error rather than silently coercing.
  **Fix**: Add a test asserting `valid == FALSE` and an error mentioning `filter_base$age_group` for a logical-valued filter code.

### Suppressed (per fix-triage suppression policy — inside explicitly `fixed` scope, 668-assertion suite passes 0 FAIL)

- `R/cell_explanation.R` — repeated `vapply` field-access patterns inside `.build_what_a_cell_is()` (P2.6 helper internals; not re-flagged).
- `inst/plumber/helpers.R`:527 — `ppp_safe` computed unconditionally before branch (negligible cost, inside P1.2/validator scope).
- `inst/plumber/plumber.R`:901-911 — multi-line `raw_body` handling duplication (this *is* the P1.1 fix).
- `R/cell_explanation.R` five new helpers (`.build_who_is_in_table`, `.build_what_a_cell_is`, `.build_what_is_shown`, `.build_cell_notes`) lack isolated unit tests; all coverage is indirect via `build_cell_explanation()`. Legitimate depth gap, but squarely inside the `fixed` P2.6 refactor scope and the full suite already exercises all observable behavior with 0 FAIL — reported for visibility only, no action required.

### Cross-file / argument-passing check (P2.6 decomposition)

Verified every call site between `build_cell_explanation()` and its five extracted helpers uses fully-named arguments matching each function signature exactly — no positional-order mismatches, no stale/shadowed variable names, no cross-scope variable reuse. Values threaded from `sec1`/`sec2` (`has_filter`, `is_short_filter`, `table_pop_short`/`table_pop_denom`, `cell_group`, `active_display_len`) match their pre-refactor single-function equivalents. **No regression found.**

### ✅ Passed
- `cg-code-quality`: Clean — no new issues beyond suppressed/negligible nits
- `cg-testing`: No P0/P1 — refactor is behavior-preserving; 2 minor coverage gaps noted above
