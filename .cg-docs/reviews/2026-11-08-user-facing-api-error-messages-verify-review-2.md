---
date: 2026-11-09
depth: light
parent-review: .cg-docs/reviews/2026-11-08-user-facing-api-error-messages-review.md
type: verification
findings:
  P3.1: fixed
---

## Review Report

**Review mode**: light (forced by `mode:verify`)
**Files reviewed**: 6 (changed in `4168044..115431a`)
**Findings**: 1 (P0: 0, P1: 0, P2: 0, P3: 1)

**Verification context**: Second verify pass following fix-triage of `.cg-docs/reviews/2026-11-08-user-facing-api-error-messages-verify-review.md` (5 findings fixed). Suppression policy applied: the single finding below targets new code introduced by the P3.3 fix itself (the `stopifnot()` fixture guard added in the prior verify pass), which was not itself covered by an explicit `fixed` entry's scope — it is new code from this pass, not suppressible.

### P0 — BLOCKING (immediate remediation required)
- None.

### P1 — CRITICAL (must fix before merge)
- None. All 5 fixes from the prior verify-review (`P2.1`, `P3.1`–`P3.4` by that report's numbering: docs table disambiguation, `.piptm_safe_scalar_label()` helper applied across `R/load_data.R`, tightened `.MIXED_FILE_SITE_ALLOWLIST` snippet, `stopifnot()` fixture guard, anchored `expect_message()` regex) were re-verified against current file content and converge correctly, except for the one item below.

### P2 — IMPORTANT (should fix)
- None.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-code-quality / cg-testing] `tests/testthat/test-api-endpoints.R:1021-1033` — The P3.3 fixture guard (added in the prior verify pass) has a dead fallback and only checks category *count*, not the actual code domain
  **Why**: The guard expression `area_entry[[1L]]$subcategories %||% area_entry[[1L]]$categories` includes a `%||% ...$categories` fallback for a field name (`categories`) that `piptm_filter_categories()` (`R/registry.R:569-573`) never actually returns — it always returns `subcategories` — so the fallback is dead and misleading about the real API shape. Separately, the guard's actual check (`999L >= length(subcategories)`) only validates the *count* of registered `area` categories (2), not that `999` is numerically outside the real code domain (`helper-fixtures.R:257-260` defines string codes `"urban"`/`"rural"`, not numeric `0`/`1` as the adjacent comment claims). If the fixture's codes were ever renumbered to include `"999"`, this guard would still pass while the test's "0 rows matched" assumption silently breaks.
  **Fix**: Either assert against the actual category `code` values directly (e.g. `!("999" %in% vapply(area_entry[[1L]]$subcategories, `[[`, character(1), "code"))`) instead of a length check, or at minimum drop the dead `%||% ...$categories` fallback and correct the adjacent comment to say the guard only checks category count, not code range.

### ✅ Passed
- `@cg-code-quality` (light, verify): usable output, confirmed 4 of 5 prior-verify-pass fixes converge correctly (`.piptm_safe_scalar_label()` extraction and reuse, `docs/api-contract.md` disambiguation, tightened `.MIXED_FILE_SITE_ALLOWLIST` snippet, anchored `expect_message()` regex); 1 finding surfaced (P3.1).
- `@cg-testing` (light, verify): usable output, independently confirmed the same P3.1 gap with additional detail on the weak domain check; confirmed the other 3 targeted fixes (P3.2/P3.3/P3.4 by prior numbering) are otherwise sound (full suite independently confirmed passing: 1,778 passed, 0 failed, 11 skipped).
