---
date: 2026-10-23
depth: light
parent-review: .cg-docs/reviews/2026-10-22-table-explicit-slot-parameters-revised-review.md
type: verification
findings:
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
---

## Verification Review Report (Pass 2)

**Review mode**: light (`mode:verify`)
**Files reviewed**: current working-tree object-slot fix in `inst/plumber/helpers.R`, `tests/testthat/test-api-helpers.R`, and `tests/testthat/test-api-endpoints.R`
**Findings**: 3 (P0: 0, P1: 3, P2: 0, P3: 0)

### P1 — CRITICAL

- **[P1.1]** [cg-code-quality] `inst/plumber/helpers.R:407` — Singleton nested JSON arrays can still bypass validation after jsonlite simplification.
  **Why**: JSON `[ ["gender"] ]` may simplify to a one-element character matrix. `is.list(value)` is false, so the dimensioned value can pass scalar checks.
  **Fix**: Reject non-NULL `dim(value)` before list unboxing. Add a raw POST regression test for `{"column":[["gender"]]}`.

- **[P1.2]** [cg-code-quality] `inst/plumber/helpers.R:404` — Empty and empty-key JSON objects can evade object detection.
  **Why**: `{}` is treated as omitted because zero-length handling precedes object detection. A list with any non-NULL `names()` is object-valued even when names are empty strings.
  **Fix**: Detect list objects before the zero-length omission check and classify any list with non-NULL `names()` as an object.

- **[P1.3]** [cg-testing] `tests/testthat/test-api-endpoints.R:1401` — Nested JSON arrays are not covered through the Plumber request helper.
  **Why**: The current direct validator test uses an R nested list, but request parsing can transform that JSON shape differently.
  **Fix**: Add a POST endpoint case encoding a nested array and assert HTTP 400 with the error envelope.

### Passed

- Scalar strings, flat arrays, and named object handling otherwise remain covered.
