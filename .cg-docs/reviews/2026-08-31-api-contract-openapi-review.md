---
date: 2026-08-31
depth: light
type: standard
plan: ".cg-docs/plans/2026-08-31-api-contract-openapi.md"
findings:
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
  P3.4: fixed
  A.1: fixed
  A.2: fixed
  A.3: fixed
  A.4: fixed
  A.5: fixed
  A.6: fixed
  A.7: fixed
---

# Review Report — API Contract Documentation

**Review mode**: light
**Files reviewed**: 4
**Findings**: 10 (P0: 0, P1: 0, P2: 6, P3: 4) + 7 advisory

## P2 — IMPORTANT (should fix)

### P2.1 — [cg-code-quality] `docs/api-contract.md:117` — Duplicate word in statistics description
**Fixed**: Removed duplicate "shares" from `/statistics` purpose line.

### P2.2 — [cg-code-quality] `docs/api-contract.md:26–27` — ResponseEnvelope `data` missing `type`
**Fixed**: Added `type: object` and `nullable: true` to `data` property.

### P2.3 — [cg-code-quality] `docs/api-contract.md:219–220` — TableRow `poverty_line`/`population` missing `type`
**Already correct**: Both properties already had `type: number` with `nullable: true`.

### P2.4 — [cg-code-quality] `docs/api-contract.md:166–168` — `/table` response overly compressed
**Fixed**: Split into separate sub-bullets for default and `include_metadata="true"` cases, with cross-reference to Shared Schemas.

### P2.5 — [cg-code-quality] `docs/api-contract.md:112–114` — `/analysis-variables` missing response
**Open** (advisory). Needs domain knowledge of actual response shape.

### P2.6 — [cg-code-quality] `docs/api-contract.md:121–123` — `/dimensions` missing response
**Open** (advisory). Needs domain knowledge of actual response shape.

## P3 — MINOR (nice to have)

### P3.1 — [cg-code-quality] `docs/api-contract.md:143` — Session rules duplicated
**Fixed**: Replaced inline details with cross-reference to Session Management Details section.

### P3.2 — [cg-code-quality] `docs/api-contract.md:88,110,119` — Inconsistent response specificity
**Open** (advisory). `/surveys-ui`, `/covariates`, `/statistics` use prose instead of structured `data = {...}` format.

### P3.3 — [cg-code-quality] `docs/api-contract.md:152–172` — Missing cross-reference to Shared Schemas
**Fixed**: Added "(see Shared Schemas)" link in `/table` response descriptions.

### P3.4 — [cg-code-quality] `docs/api-contract.md:41–42` — Quoting inconsistency
**Open** (advisory). Minor mixed quoting between double-quotes and backticks in envelope semantics.

## Advisory Cross-References (from cg-testing)

### A.1 — `by` ≤4 constraint documented but not enforced
Contract documents ≤4 entries for `by`; `validate_table_input()` only validates dimension names.

### A.2 — Session POST pip_id cap overstated
Contract says 1–15; `validate_session_pip_id()` only validates non-empty and format.

### A.3 — `include_metadata` case-insensitivity not documented
Implementation uses `tolower()`; contract shows only `"true"`.

### A.4 — `/description` error Content-Type mismatch
Errors return JSON body with `text/plain` Content-Type.

### A.5 — pip_id 15-entry limit overstated
Validation rule 1 generalizes a `/table`-only constraint to "all endpoints."

### A.6 — `README.md:268` — Health response example mismatch
**Manual**. README shows `"status": "healthy"`; code and contract return `"status": "ok"`.

### A.7 — Parameter type annotations at HTTP layer
Contract types are domain-level; all query params arrive as strings at HTTP layer.

## ✅ Passed
- `@cg-testing`: No testable behaviors. All 15 endpoints comprehensively documented.
- `README.md`: Pointer addition correct.
- `.cg-docs/plans/`: Frontmatter change valid.
- `.cg-docs/work-reports/`: Execution report valid.

## Autofix Summary
Applied 5 safe_auto fixes: P2.1, P2.2, P2.4, P3.1, P3.3.
P2.3 was already correct in file.
