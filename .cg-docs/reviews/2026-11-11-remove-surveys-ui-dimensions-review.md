---
date: 2026-11-11
depth: light
type: standard
plan: .cg-docs/plans/2026-11-11-remove-surveys-ui-dimensions.md
findings:
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: fixed
---

## Review Report

**Review mode**: light
**Files reviewed**: 4 (`R/manifest.R`, `man/piptm_surveys_ui.Rd`, `inst/plumber/plumber.R`, `tests/testthat/test-surveys-ui.R`)
**Findings**: 7 (P0: 0, P1: 0, P2: 4, P3: 3)

Explicit route `light` kept. The diff is an API response-shape change, but the user requested light review.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `inst/plumber/plumber.R:1070` — GET `/surveys` OpenAPI text listed region labels, survey acronym, and `dimensions`, then pointed the Step 1 grid at `GET /surveys-ui`.
  **Why**: Those fields belong to `/surveys`, not `/surveys-ui`. A reader could think the UI catalog still returns them.
  **Fix**: Keep the `/surveys` field list and state that `/surveys-ui` returns only the seven catalog fields. **Applied** [manual].

- **[P2.2]** [cg-code-quality] `R/manifest.R:343` — roxygen still described a `countrycode` ISO3 fallback the function does not implement.
  **Why**: `country_label` is `mf$country_name`, or `NA` when that field is absent. The same sentence was in `man/piptm_surveys_ui.Rd`.
  **Fix**: Document the actual source. **Applied** [manual].

- **[P2.3]** [cg-testing] `tests/testthat/test-surveys-ui.R:1` — the only test is one `INC` row.
  **Why**: Empty-manifest, `CON` → `"Consumption"`, unknown welfare type, and case-insensitive `pip_id` order branches are untested.
  **Fix**: Add those cases. Not applied. [manual]

- **[P2.4]** [cg-testing] `tests/testthat/test-api-endpoints.R` — no response assertion for `GET /surveys-ui`.
  **Why**: The path is only in the OpenAPI allowlist. A regression that put `dimensions` back on the HTTP payload would not fail this file. `GET /surveys` still correctly asserts `dimensions`.
  **Fix**: Call `GET /surveys-ui` and assert the seven field names. Not applied. [manual]

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/manifest.R` — stale note said country name was not yet on the manifest.
  **Why**: `country_name` is already built when the manifest is loaded.
  **Fix**: Removed the note. **Applied** [advisory].

- **[P3.2]** [cg-testing] `tests/testthat/test-surveys-ui.R` — comment and assertion did not lock `country_label` to `country_name`.
  **Why**: A missing name becomes the string `"NA"`, which still passed `nzchar()`.
  **Fix**: Fixture now sets `country_name = "Testland"` and the test expects that label. Exact `expect_named()` also replaces the superset check. **Applied** [advisory].

- **[P3.3]** [cg-testing] `R/manifest.R` roxygen `countrycode` sentence — same contract drift as P2.2, noted again by testing.
  **Why**: Duplicate of P2.2.
  **Fix**: Covered by the P2.2 doc edit. Left open as a duplicate. [advisory]

### Passed

- `@cg-code-quality`: field removal matches the `/surveys-ui` comment and the unit test. `.Rbuildignore` already excludes `.cg-docs/`.
- `@cg-testing`: `expect_false("dimensions" %in% names(srv))` is a real negative check. Manifest fixtures correctly still carry `dimensions`.

Parsed 7 finding IDs.
