---
date: 2026-10-26
depth: architecture
type: standard
plan: .cg-docs/plans/2026-10-26-table-layout-cells-response-format.md
findings:
  P0.1: fixed
  P0.2: fixed
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: fixed
  P1.6: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: skipped
---

## Review Report

**Review mode**: architecture (explicit)
**Files reviewed**: `R/table_response.R` (new), `R/registry.R`, `inst/plumber/plumber.R`, `docs/api-contract.md`, `NEWS.md`, `tests/testthat/test-table-response.R` (new), `tests/testthat/test-api-endpoints.R`, `tests/testthat/test-api-table-download.R`, `tests/testthat/test-phase4-table-and-endpoint.R`, `tests/testthat/test-registry.R`, `tests/testthat/helper-fixtures.R`, baseline fixtures, `NAMESPACE`
**Findings**: 15 (P0: 2, P1: 6, P2: 7, P3: 0 standalone — minor items folded into Passed/Advisory)

### P0 — BLOCKING (immediate remediation required)

- **[P0.1]** [cg-reproducibility, cg-architecture] `R/table_response.R:40-50` — Registry error fallback silently substitutes an arbitrary, load-order-dependent registry
  **Why**: When `piptm_variable_registry(release)` errors, the code falls back to `.piptm_env$registries[[1L]]` — "whatever registry happened to load first" in the current R process — rather than failing or matching by release. This violates the charter's determinism constraint: identical requests can silently return category labels/codes from the wrong release depending on process/session state, and it reaches into the compute layer's private environment instead of using the public registry-resolution API, which is also a hidden architectural coupling.
  **Fix**: Remove the fallback entirely; let the error propagate via `cli::cli_abort(conditionMessage(e))`. If a fallback is truly needed, search loaded registries for one whose release actually matches the requested `release` and abort if none match.
  **Tag**: [manual]

- **[P0.2]** [cg-reproducibility] `R/table_response.R:138-143, 166-176, 349-354` — Unmatched/unregistered category values share one sort rank with no secondary tiebreaker, making cell order input-row-order-dependent
  **Why**: The "no categories defined" branch (`cat_idx = 999999L`) and the "NA/no-match" branches (`cat_idx = length(cats) + 1L`) assign the *same* rank to every distinct unmatched value for a slot. `order()` on tied integer keys falls back to the physical row order of `unique_cells_dt`, which depends on upstream row order. Two API calls with identical content but different upstream row arrival order can produce different `cells[]` ordering — a direct violation of "Identical inputs must always produce identical results."
  **Fix**: Add a stable secondary sort key for the unmatched bucket (e.g., the raw value's string form) so ties are broken deterministically by content, not input order.
  **Tag**: [manual]

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-architecture, cg-code-quality] `R/table_response.R:75-80` duplicates `R/registry.R`'s additive `pov_status` categories (`{0: Non-poor, 1: Poor}`) in two independent places
  **Why**: Two sources of truth for the same domain fact; they will silently diverge if one is edited without the other.
  **Fix**: Delete the duplicate block in `table_response.R:75-80`; rely solely on the registry.
  **Tag**: [safe_auto]

- **[P1.2]** [cg-reproducibility, cg-data-quality] `R/table_response.R:93-101, 145-176` — Category code matching mixes two different normalization paths (registry-side explicit rules vs. data-side blind `as.character()` coercion), risking silent mismatches for numeric edge cases
  **Why**: Floating-point artifacts, scientific-notation formatting, or leading-zero strings can silently fail to match and fall into the "unregistered" bucket without diagnostic.
  **Fix**: Factor a single `.normalize_category_code()` helper used by both functions.
  **Tag**: [manual]

- **[P1.3]** [cg-testing] `tests/testthat/test-table-response.R` (whole file) — No test exercises multiple surveys with different observed category sets
  **Why**: The per-survey loop is the most structurally complex part of the function and is never invoked with `length(unique_pips) > 1` in any unit test.
  **Fix**: Add a unit test with 2+ `pip_id`s differing in observed categories, asserting correct per-survey scoping.
  **Tag**: [manual]

- **[P1.4]** [cg-testing] `R/table_response.R:234-243` — "Slot variable missing from `dt`" error path is implemented but never tested
  **Fix**: Add a test passing a `slots` entry whose variable column is absent from `dt`.
  **Tag**: [manual]

- **[P1.5]** [cg-testing] `tests/testthat/test-api-table-download.R:181-213` — `/table/download` characterization test is tautological (passes for any of 200/400/422/500)
  **Fix**: Pin a single concrete expectation reflecting the actual current behavior.
  **Tag**: [manual]

- **[P1.6]** [cg-documentation] `R/table_response.R:179-199` — `serialize_table_response()` is the only exported function missing `@examples`; `@return`/error contract underdocumented
  **Fix**: Add `@examples`; document empty-input/no-slots edge cases, required `dt` columns, and abort conditions.
  **Tag**: [manual]

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-architecture] `inst/plumber/plumber.R:198-220` — `/table` returns two structurally different shapes depending on `include_metadata`; confirm this is documented clearly.
  **Tag**: [manual]
- **[P2.2]** [cg-performance] `R/table_response.R:301-346` — Per-cell row filtering is O(n_cells × nrow(sub_dt)); low-impact at current volumes but a real complexity smell.
  **Tag**: [advisory]
- **[P2.3]** [cg-data-quality] `R/table_response.R:198, 261-262` — `measures`/`analysis_var` accepted without validation.
  **Tag**: [manual]
- **[P2.4]** [cg-data-quality] `R/table_response.R:253` — `poverty_line` coercion result not checked for NA/non-scalar.
  **Tag**: [manual]
- **[P2.5]** [cg-testing] `tests/testthat/test-table-response.R:227-252` — Determinism test doesn't cover input-row-order invariance (would have caught P0.2).
  **Tag**: [manual]
- **[P2.6]** [cg-code-quality] `R/table_response.R:93-101` — Dense normalization ternary chain hard to read/test in isolation (overlaps P1.2).
  **Tag**: [manual]
- **[P2.7]** [cg-version-control] Working tree — change set spans 4 logically distinct concerns in one undifferentiated diff; recommend splitting commits.
  **Tag**: [manual]

### ✅ Passed / Advisory-only
- **cg-architecture**: Dependency direction confirmed correct; export scoping correct; file placement appropriate.
- **cg-code-quality**: `.Rbuildignore` already excludes `.cg-docs/`. Minor style nits (redundant list re-assignment, spacing, magic-number sentinel, dot-prefix naming inconsistency) filed as low-impact advisory/safe_auto only.
- **cg-version-control**: No secrets/credentials/leaked temp paths found. CRLF warnings on JSON fixtures are cosmetic.
- **cg-data-quality**: Initial "mismatched NA sentinel type" concern checked against `format_table_output.R:71` and found incorrect — `dt$value` is always character by contract; `NA_character_` is correct. Not included as a finding.
- **cg-performance**: No P0/P1 performance issues.

### ⚠️ Incomplete Reviews
None — all 8 dispatched agents returned usable, on-topic output.
