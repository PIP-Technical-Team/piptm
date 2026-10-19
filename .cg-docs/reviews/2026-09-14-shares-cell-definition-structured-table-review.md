---
date: 2026-10-16
depth: light
type: standard
plan: .cg-docs/plans/2026-09-14-shares-cell-definition-structured-table.md
findings:
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P3.1: fixed
---

## Review Report

**Review mode**: light  
**Files reviewed**: 3  
**Findings**: 5 (P0: 0, P1: 0, P2: 4, P3: 1)

### P2 — IMPORTANT (should fix)
- **[P2.1]** [cg-code-quality] `inst/plumber/plumber.R:242` — Upload size cap relied only on `Content-Length` and could be bypassed when header is absent/invalid. **[safe_auto]**  
  **Why**: Oversized payloads could be parsed into memory despite the intended 25 MB guard.  
  **Fix**: Enforce size check on actual raw request body bytes (`nchar(..., type = "bytes")`) before JSON parse.

- **[P2.2]** [cg-testing] `tests/testthat/test-api-table-download.R:44` — Missing regression test for 413 payload-size protection. **[safe_auto]**  
  **Why**: Future regressions in size-limit logic would go undetected.  
  **Fix**: Add oversized request test asserting `413` and JSON error envelope.

- **[P2.3]** [cg-testing] `tests/testthat/test-api-table-download.R:72` — Error-path tests checked status only, not envelope/header contract. **[safe_auto]**  
  **Why**: API could regress from JSON-envelope error behavior without failing tests.  
  **Fix**: Assert error `Content-Type` and envelope fields (`status`, `warnings`, `errors`).

- **[P2.4]** [cg-testing] `tests/testthat/test-api-table-download.R:118` — Malformed-body branch coverage incomplete. **[safe_auto]**  
  **Why**: Parsing edge cases (`{"`, non-object JSON, missing `data`) were not all guarded by tests.  
  **Fix**: Add explicit 400 tests for invalid JSON and non-object/missing-data variants.

### P3 — MINOR (nice to have)
- **[P3.1]** [cg-code-quality] `R/api.R:14` — Endpoint inventory docs were stale relative to router endpoints. **[safe_auto]**  
  **Why**: Mismatched endpoint documentation increases maintenance and support risk.  
  **Fix**: Refresh endpoint table entries and wording to reflect current `inst/plumber/plumber.R` routes.

### ✅ Passed
- cg-code-quality: No additional issues beyond findings above.
- cg-testing: No additional issues beyond findings above.
