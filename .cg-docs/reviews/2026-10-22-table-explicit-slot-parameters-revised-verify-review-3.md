---
date: 2026-10-23
depth: light
parent-review: .cg-docs/reviews/2026-10-22-table-explicit-slot-parameters-revised-review.md
type: verification
findings:
  P2.1: open
  P2.2: open
  P3.1: open
---

## Verification Review Report (Pass 3)

**Review mode**: light (`mode:verify`)
**Files reviewed**: current uncommitted working-tree diff (parent commit `83acfef`) — `inst/plumber/helpers.R`, `tests/testthat/test-api-helpers.R`, `tests/testthat/test-api-endpoints.R`
**Findings**: 3 (P0: 0, P1: 0, P2: 2, P3: 1)

**Correctness confirmed**: Both agents independently confirmed the reordered validation branch (`dim()` check → list detection → object/nested detection → length-0 check) is logically sound. It correctly closes all three gaps raised in verify pass 2: matrix/dim bypass, premature `length==0` short-circuit letting `{}` and `{"":...}` evade object detection, and the previously untested nested-array decode shape. No previously-fixed P1/P2 finding was reintroduced. Direct `jsonlite::fromJSON` inspection also confirmed the fix correctly rejects an array-of-objects (which decodes to a `data.frame`, caught by the `dim()` guard) and a bare boolean scalar (caught by the existing character-type guard) — these are real-shape gaps only in **test coverage**, not in the implementation.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-testing] `tests/testthat/test-api-helpers.R` (new block, ~L601-660) — Missing coverage for an array of JSON objects, which `jsonlite::fromJSON(simplifyVector = TRUE)` decodes to a `data.frame` (e.g. `[{"a":1},{"b":2}]`).
  **Why**: The implementation's `!is.null(dim(value))` guard does correctly reject a `data.frame` (non-NULL `dim()`), so this is not a correctness gap — but it is exactly the class of "simplification can differ subtly from manual construction" shape the new tests explicitly call out as their motivation, and it is untested.
  **Fix**: Add `jsonlite::fromJSON('{"column": [{"a":1},{"b":2}]}', simplifyVector = TRUE)$column` as a `.vti()` input; assert `valid` is `FALSE` with the "character string or flat array" error.

- **[P2.2]** [cg-testing] `tests/testthat/test-api-helpers.R` (new block) — Missing coverage for a bare boolean scalar slot value (e.g. `{"column": true}`, which decodes to `logi TRUE`).
  **Why**: This falls through to the existing `!is.character(value) && !all(is.na(value))` check and is correctly rejected, but only via code-reading inference — the real jsonlite-decoded boolean shape is untested, and it exercises a different error-message branch ("must be a character string") than the new flat-array/object tests.
  **Fix**: Add `jsonlite::fromJSON('{"column": true}', simplifyVector = TRUE)$column` as a `.vti()` input; assert `valid` is `FALSE` with the "must be a character string" error (not the "flat array" message).

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality / cg-testing] `tests/testthat/test-api-endpoints.R` (new block, ~L1408-1433) — Endpoint-level (real Plumber-decoder) coverage was added only for the nested-array (`[["gender"]]`) and empty-object (`{}`) cases, not for the blank-key-object (`{"":"gender"}`) or empty-array (`[]`) cases, which are currently verified only at the `validate_table_input()` unit level via direct `jsonlite::fromJSON` calls.
  **Why**: Plumber's actual request-body parser is what determines the decoded shape in production; the blank-key-object rejection path and the empty-array acceptance (unfilled/omitted) path are not end-to-end verified through `.ep_router$call()`.
  **Fix**: Add two more endpoint cases mirroring the existing pattern: a POST with `column = setNames(list("gender"), "")` (serializes to `{"":"gender"}`) expecting 400, and a POST with `column = list()` (serializes to `[]`) expecting 200/valid.

### Passed

- Both reviewers independently confirm: no P0/P1 findings; no regression of any previously-fixed P1/P2 finding from the standard review or prior verify passes.
