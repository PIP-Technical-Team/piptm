---
date: 2026-11-11
depth: light
parent-review: .cg-docs/reviews/2026-11-11-remove-surveys-ui-dimensions-review.md
type: verification
findings:
  P2.1: fixed
  P2.2: fixed
---

## Review Report

**Review mode**: light
**Files reviewed**: 5 (`R/manifest.R`, `inst/plumber/plumber.R`, `man/piptm_surveys_ui.Rd`, `tests/testthat/test-surveys-ui.R`, `tests/testthat/test-api-endpoints.R`)
**Findings**: 2 (P0: 0, P1: 0, P2: 2, P3: 0)

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `docs/api-contract.md:111` and `SWAGGER_GUIDE.md:247` — `/surveys-ui` documentation still advertises `dimensions`.
  **Why**: The implementation and plumber comments no longer return that field, so clients following these documents can expect data the endpoint does not provide.
  **Fix**: Document only `pip_id`, `country_code`, `country_label`, `year`, `welfare_type`, `welfare_label`, and `version`, and point variable availability to `/categories` and `/covariates`. [manual]

- **[P2.2]** [cg-testing] `tests/testthat/test-api-endpoints.R:1174` — HTTP `/surveys-ui` test checks field names but not serialized label values.
  **Why**: A regression returning `NA` or raw welfare codes would still pass, even though the fixture contains known country and welfare values.
  **Fix**: Assert the fixture country labels and `"Income"` welfare labels on the HTTP response. [manual]

### Passed

- `@cg-code-quality`: Prior findings P2.1–P2.4 and P3.1–P3.3 are fixed in the reviewed implementation and plumber files. No P0 or P1.
- `@cg-testing`: The new unit tests are not tautological. `GET /surveys` still retains `dimensions`, while `GET /surveys-ui` omits it.

Parsed 2 finding IDs.
