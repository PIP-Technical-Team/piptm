---
date: 2026-11-09
depth: light
parent-review: .cg-docs/reviews/2026-11-08-user-facing-api-error-messages-review.md
type: verification
findings:
  P2.1: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
---

## Review Report

**Review mode**: light (forced by `mode:verify`)
**Files reviewed**: 13 (changed in `eb912d7..4168044`)
**Findings**: 5 (P0: 0, P1: 0, P2: 1, P3: 4)

**Verification context**: Verify pass following fix-triage of `.cg-docs/reviews/2026-11-08-user-facing-api-error-messages-review.md` (13 findings fixed, 1 skipped advisory). Suppression policy applied: P2/P3 findings were only suppressed when squarely within the scope of a prior `fixed` entry; all findings below are either new gaps not covered by the prior fix scope, or genuine new issues introduced by the fixes themselves.

### P0 — BLOCKING (immediate remediation required)
- None.

### P1 — CRITICAL (must fix before merge)
- None. All 13 previously-fixed findings (P1.1, P1.2, P2.1–P2.8, P3.1–P3.3 from the prior review) were re-verified against current file content and converge correctly: the 0-row `filter_base` test, `parse_filter_base_input()` dedup across `/table` and `/description`, `piptm_user_error_message()` unification with single-pass ANSI-stripping, the AST audit's rejection of `NULL`/blank `user_message`, scalar `release` guards, deterministic `available_ppp` sorting, `Inf`/`-Inf` filter value guards, and the new edge-case unit tests all match their described intent.

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-code-quality] `docs/api-contract.md:59` — Inconsistent HTTP status enumeration between the new user-safe-guarantee prose and the error table beneath it
  **Why**: The prose states the guarantee covers "HTTP 422, 413, and 500 responses (as well as HTTP 400 payload rejections on `POST /table/download`)", but the representative-messages table has no row for "Malformed `filter_base` JSON" (422, confirmed correct against `inst/plumber/plumber.R:225`), only folded into prose. A reader cross-checking the table against Validation Rule §5 ("Invalid JSON returns HTTP 422") has no table row to confirm it.
  **Fix**: Add an explicit table row for "Malformed `filter_base` JSON" → `422` so the contract table is self-consistent with both the prose and §5.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-code-quality] `R/load_data.R:214-215,317-319` — Duplicated `cc_label`/`yr_label` scalar-guard pattern repeated verbatim at two call sites in the same function
  **Why**: The newly introduced scalar-guard pattern (`if (length(x) == 1L && !is.na(x)) as.character(x) else "the requested ..."`) is duplicated nearly identically between the "no manifest entry" branch and the "PPP not available" branch, ~100 lines apart. This DRY gap was introduced by the P3.1 fix itself (the prior fix only covered `manifest.R`'s `rel_label` pattern via `.piptm_unknown_release_user_error()`; this `load_data.R` duplication was not in scope).
  **Fix**: Extract a small internal helper (e.g. `.piptm_safe_scalar_label(x, fallback)`) and reuse it at both sites.

- **[P3.2]** [cg-code-quality] `tests/testthat/test-error-coverage-audit.R:95-96` — New allowlist entries use fragile substring snippets, one of which is overly generic
  **Why**: `snippet = "message = message"` (added as part of the P2.3 fix moving `R/errors.R` into `.MIXED_FILE_SITE_ALLOWLIST`) is a very generic token matched via `grepl()` against `deparse()`'d call text. It could silently match an unrelated future `cli_abort(message = message, ...)` call added elsewhere in `R/errors.R`, weakening the audit's precision for exactly the file this fix most needed to harden.
  **Fix**: Tighten the snippet to more specific context (e.g. `"class = unique(c(class"`) so the allowlist cannot accidentally swallow a new unmapped `cli_abort()` added later in the same file.

- **[P3.3]** [cg-testing] `tests/testthat/test-api-endpoints.R:1017-1031` — New 0-row `filter_base` test relies on an unverified comment-asserted fixture value
  **Why**: The test added for P1.1 relies on the inline comment `(COL_2010_ECH_INC_ALL has area = 0,1)` to justify why `area = 999L` yields zero matches, but never asserts the fixture's actual `area` domain before exercising the 999 code path. If the shared fixture generator's `area` value set changes in the future, this test could silently stop testing the "0 rows matched" branch without failing loudly.
  **Fix**: Add a one-line guard (e.g. `stopifnot(!(999L %in% unique(fixture$area)))`) or assert the fixture's `area` levels directly in the test body before issuing the request.

- **[P3.4]** [cg-testing] `tests/testthat/test-api-helpers.R:715-724` — `expect_message()` regex pattern added for P2.5 doesn't anchor the `dev_msg` boundary
  **Why**: `expect_message(out <- capture_with_warnings(stop("something broke")), "\\[piptm\\]\\[unmapped_error\\].*something broke")` verifies the prefix and substring are present, but the unanchored `.*` would still pass if `class(e)` happened to contain the literal text `something broke`, even if `dev_msg` were truncated or wrong.
  **Fix**: Tighten to anchor on the colon separator, e.g. `": .*something broke"`.

### ✅ Passed
- `@cg-code-quality` (light, verify): usable output, confirmed all 13 prior fixes converge correctly in current file content; 3 new findings surfaced (P2.1, P3.1, P3.2).
- `@cg-testing` (light, verify): usable output, confirmed all re-verified assertions match claimed behavior (full suite independently confirmed passing: 1,778 passed, 0 failed, 11 skipped); 2 new findings surfaced (P3.3, P3.4).
