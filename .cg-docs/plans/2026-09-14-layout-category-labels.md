---
title: Add category labels to layout dimensions in description
date: 2026-09-14
status: completed
completed-date: 2026-09-14
deviation-policy: ask
---

## Completion Contract

**Outcome**: The description endpoint's layout section shows both the count and
the category labels for each "by" dimension. Example: `gender` currently
renders as `2` under Categories; after this change it renders as
`2: male, female`. `pov_status` renders as `2: poor, non-poor`.

**Verification Surface**:
- `tests/testthat/test-description-metadata.R` and
  `tests/testthat/test-description-builder-schema-validation.R` continue to pass
  (with updates for the new n_categories string format).
- New tests in `tests/testthat/test-description-builder-schema-validation.R`
  (or `test-description-helpers.R`) assert that `.build_layout_content()`
  produces the combined `"N: label1, label2"` string per covariate, using a
  mocked `piptm_variable_registry`.
- New test asserts pov_status yields `"2: poor, non-poor"` without any registry
  entry.
- New test asserts fallback: if registry unavailable, falls back to bare count
  (e.g. `"2"`) and does not error.

## Steps

1. Modify `.build_layout_content()` in `R/description_builder.R` to look up
   categories via `piptm_variable_registry(meta$provenance$release)` per
   covariate varname, format `n_categories` as `"N: label1, label2"`, special-
   case `pov_status` -> `"2: poor, non-poor"`, fall back to bare count on
   registry failure.
2. Update the two existing schema-validation tests that assert `n_categories`
   is integer type to reflect the new character semantics.
3. Add new tests covering: registry-resolved labels, pov_status hardcoding,
   registry failure fallback.
4. Run the full-file suite for description tests.
