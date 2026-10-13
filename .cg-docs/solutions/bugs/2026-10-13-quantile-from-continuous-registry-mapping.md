---
date: 2026-10-13
title: "Registry build abort on factor + quantile_from_continuous (wquintile)"
category: "bugs"
type: "bug"
language: "R"
tags: [registry, build_variable_registry, recode_spec, wquintile, quantile_from_continuous, tm_type]
root-cause: "`.build_registry_entry()` had an incomplete factor recode-type mapping table and did not recognize `quantile_from_continuous`, causing `wquintile` to abort as an invalid type combination once pipdata mappings were prioritized."
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
red-phase-confirmed: "yes"
expected-behavior-source: "documentation"
test-gap: "edge-case-gap"
---

# Registry build abort on factor + quantile_from_continuous (wquintile)

## Symptom

`build_variable_registry()` aborted during registry generation for release `20260401_TEST` with:

- `Invalid combination of "factor" and "quantile_from_continuous" for variable "wquintile".`

This blocked writing/updating the release registry JSON on disk.

## Expected Behavior Source

The expected behavior for `.build_registry_entry()` is defined by: **documentation**.

Specifically: `{pipdata}` recode spec (`recode_spec.yml`) defines `wquintile` as:

- `type: factor`
- `recode_type: quantile_from_continuous`
- `mapping: 1..5`

Therefore piptm registry build must classify this variable as `tm_type = "categorical"` and map categories from the recode-spec mapping, not abort.

## Root Cause

The root cause appears to be an incomplete recode-type classification table in
`R/registry.R` (`.build_registry_entry()`) because the `factor` branch recognized
`binary_map`, `haven_labels`, and `binned_from_continuous`, but omitted
`quantile_from_continuous`.

As a result, valid `wquintile` metadata from pipdata fell through to `tm_type <- NULL`
and triggered the invalid-combination `cli_abort()`.

## Reproduction Test

New regression test written at:

- `tests/testthat/test-registry-quantile-from-continuous.R`

It constructs `spec_var` + `pip_vars` for `wquintile` with
`recode_type = "quantile_from_continuous"` and asserts:

- `entry$tm_type == "categorical"`
- `entry$n_categories == 5L`
- category codes are `c("1","2","3","4","5")`

Red-phase failure (pre-fix) matched symptom exactly:

- `.build_registry_entry(...)` aborted with invalid combination error for
  `factor + quantile_from_continuous`.

## Test Gap

Test gap classification: **edge-case-gap** — existing registry tests covered common
registry structures and other recode types but did not include this specific
`factor + quantile_from_continuous` boundary case. The suite had no fixture
that exercised this exact type pair, so the missing mapping branch was not
caught.

## Fix

Updated `R/registry.R` in `.build_registry_entry()`:

1. Extended recognized factor recode types to include
   `quantile_from_continuous` as categorical.
2. Kept `binned_from_continuous` categorical handling intact.
3. Updated error help text to include both:
   - `factor + binned_from_continuous -> categorical`
   - `factor + quantile_from_continuous -> categorical`

Code change (core logic):

```r
} else if (pip_type == "factor") {
  if (recode_type == "binary_map") {
    tm_type <- "binary"
  } else if (recode_type %in% c("haven_labels", "binned_from_continuous", "quantile_from_continuous")) {
    tm_type <- "categorical"
  }
}
```

Validation performed:

- Reproduction test green:
  - `devtools::test(filter='registry-quantile-from-continuous')` → pass.
- Relevant regression sweep:
  - `devtools::test(filter='(registry|categories-current|registry-binned-categories-source|registry-quantile-from-continuous)')`
  - Result: `PASS 582`, `FAIL 0`.
- Operational check:
  - `pipfun::setup_working_release("20260401", "TEST")`
  - `build_variable_registry("20260401_TEST")` now succeeds.
  - Rebuilt registry JSON contains `wquintile.tm_type = "categorical"` and
    category codes `1,2,3,4,5`.

## Lessons Learned

`edge-case-gap` issues often arise when type dispatch tables evolve in upstream
metadata contracts (here: recode types from pipdata) but downstream validation
lists are not updated. For registry builders, each supported recode type should
have an explicit regression test that binds `(type, recode_type)` to expected
`tm_type` so contract drift is caught immediately.

## Related

- `.cg-docs/solutions/bugs/2026-10-13-hsize-group-filter-category-codes.md`
