---
date: 2026-10-22
depth: light
parent-review: .cg-docs/reviews/2026-10-20-pop-share-nr-masking-review.md
type: verification
findings:
  P2.1: fixed
  P2.2: fixed
  P2.3: open
  P3.1: open
---

## Review Report

**Review mode**: verify (light, following fix-triage — second pass, covering the P1 and P2 fix-triage rounds cumulatively)
**Parent review**: `.cg-docs/reviews/2026-10-20-pop-share-nr-masking-review.md`
**Resolved findings being verified**: P0.1, P1.1–P1.9, P2.1–P2.6, P2.10–P2.14 (fixed); P2.7–P2.9 (skipped, branch already pushed)
**Files reviewed**: 13 (`R/table_maker.R`, `R/format_table_output.R`, `inst/plumber/plumber.R`, `README.md`, `SWAGGER_GUIDE.md`, `docs/api-contract.md`, `NEWS.md`, `man/table_maker.Rd`, `man/format_table_output.Rd`, `tests/testthat/test-table-maker-suppression.R`, `tests/testthat/test-format-table-output.R`, `tests/testthat/test-api-endpoints.R`, `tests/testthat/helper-fixtures.R`)
**Findings**: 4 (P0: 0, P1: 0, P2: 3, P3: 1)

Parsed 4 finding IDs.

### P0 — BLOCKING (immediate remediation required)

None.

### P1 — CRITICAL (must fix before merge)

None. No cross-file breakage was found: the P2.14 label-construction rewrite in `R/table_maker.R` was confirmed to produce byte-identical label text to the old per-row `vapply` version across single-key, multi-key, and NA-dimension cases, and the existing `expect_warning(..., "Reporting")` assertions in the P1.5/P1.6 tests still hold since only the per-cell detail bullet changed, not the primary warning message.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `tests/testthat/test-table-maker-suppression.R`, `tests/testthat/test-table-maker-v2.R`, `tests/testthat/test-api-endpoints.R` — Duplicated `reset_piptm_env_*`/`.reset_piptm_env()` helper body (identical 4-line `.piptm_env` reset) copy-pasted across three test files; a third near-identical copy was added by this fix-triage batch.
  **Why**: `helper-fixtures.R` already centralizes shared fixture helpers "so one change covers all test files"; the env-reset logic has the same cross-file-shared nature but was not consolidated there.
  **Fix**: Moved a single `reset_piptm_env_tm()` into `tests/testthat/helper-fixtures.R`; `reset_piptm_env_sup`, `reset_piptm_env_v2`, and `.reset_piptm_env` are now thin aliases of it. **FIXED** during this verify pass (safe_auto, whitespace/refactor-only, no behavior change — full suite still 1298 passed, 0 failed).

- **[P2.2]** [cg-code-quality] `tests/testthat/test-table-maker-suppression.R` (`make_sup_fixture`) vs. the new P2.13 end-to-end test in `tests/testthat/test-api-endpoints.R` — Near-identical 20-row `gender`-weighted Arrow/manifest fixture (same schema, same `weight = rep_len(c(0.01, 1000), n)` masking scenario) duplicated verbatim between the two files, both added in this fix-triage effort.
  **Why**: A change to the fixture schema would need to be kept in sync by hand in two places.
  **Fix**: Factored the shared "two-weight-group TST survey" builder into `tests/testthat/helper-fixtures.R` as `write_fixture_nr_masking_survey()`; both `make_sup_fixture()` and the P2.13 test now call it. **FIXED** during this verify pass (safe_auto; full suite still 1298 passed, 0 failed, confirming behavior-preserving).

- **[P2.3]** [cg-testing] `R/table_maker.R` (20-label suppression-warning cap) — The new cap branch (`if (length(sup_labels) > max_labels) ...`) added by P2.14 is not exercised by any existing or new test; no fixture currently produces more than 20 masked cells.
  **Why**: This is new conditional logic; an untested branch is a coverage gap (e.g. an off-by-one in `seq_len(max_labels)` or a wrong count in the `"... and %d more"` suffix could silently regress), though it affects only a diagnostic `cli_warn()` message, not masking correctness or data values.
  **Fix**: Add a test with a `by` grouping producing >20 masked cells and assert the warning output contains the correct `"... and N more"` suffix and exactly 20 individual labels before it.
  Tag: [advisory] (diagnostic-message-only; left open — not applied in this pass, as building a 25+-group synthetic fixture is disproportionate to the risk for a warning-text branch).

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/table_maker.R:916-935` (`pop_rows` three-way branch: `caller_requested_pop_share`, population-lookup reuse, `GRP()`/`compute_shares()` fallback) — No single comment documents the shared invariant that all three branches must produce identical semantics (population share of a cell within its survey).
  **Why**: Future maintenance (e.g. adding a fourth population-data source) risks changing semantics in only one branch without the others being updated to match, since the shared contract is implicit.
  **Fix**: Add a brief comment above the `if`/`else if`/`else` stating the shared output contract (columns, meaning, scale).
  Tag: [advisory] — touches the statistical masking/threshold code path, so left for manual review rather than auto-applied; not blocking.

### ✅ Passed

- **cg-code-quality**: No P0/P1 issues anywhere in the batch. The P2.6 runnable example in `R/format_table_output.R` runs standalone (fully-qualified `data.table::data.table()`, no missing `library()` call) and does not conflict with the `\dontrun{}` block beneath it.
- **cg-testing**: Exhaustive verification of the P2.14 label-construction rewrite confirms it is a genuine drop-in equivalent of the old nested-`vapply` version for single-key, multi-key, and `NA`-valued dimension cases (no reordering, no recycling risk, identical `paste0()`-based `NA` coercion). The P1.5/P1.6 multi-dimension NA test continues to exercise the new label-construction path correctly post-rewrite. The P2.11/P2.12 rounding-boundary and registry-sync tests are specific and non-tautological, with correct `piptm:::` internal-namespace access. The P2.13 end-to-end fixture is self-contained with correct `withr::defer()` cleanup, consistent with sibling tests.

### Notes

Both actionable P2 findings in this pass (duplicated env-reset helper, duplicated fixture builder) were fixed directly during this verify session: consolidated into `tests/testthat/helper-fixtures.R` as `reset_piptm_env_tm()` and `write_fixture_nr_masking_survey()` respectively, with the original per-file function names kept as aliases so no test call sites needed updating. Full regression suite confirmed behavior-preserving: `FAIL 0 | WARN 7 | SKIP 12 | PASS 1298` both before and after (identical pass count — pure refactor).

The two P3-level/advisory findings (untested 20-label cap branch; missing shared-contract comment on the `pop_rows` three-way branch) are left open as low-priority, non-blocking follow-ups; neither touches data correctness or the masking outcome itself.
