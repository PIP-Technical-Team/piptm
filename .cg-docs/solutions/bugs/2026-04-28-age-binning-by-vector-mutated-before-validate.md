---
date: 2026-04-28
title: "Age binning: mutating 'by' vector before compute_measures causes unknown-dimension error"
category: "bugs"
type: "bug"
language: "R"
tags: [age-binning, by-vector, compute_measures, validate_by, dimension, age_group, table_maker]
root-cause: "table_maker() replaced 'age' with 'age_group' in the `by` vector before calling compute_measures(), but compute_measures/.validate_by() only accepts the canonical dimension names ('age'), not derived column names ('age_group')"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# Age Binning: Mutating `by` Before `compute_measures` Causes Unknown-Dimension Error

## Symptom

Calling `table_maker(..., by = "age")` threw:

```
Error: Unknown dimension: "age_group".
ℹ Valid dimensions: "gender", "area", "educat4", "educat5", "educat7", and "age".
```

The error originated inside `compute_measures()` → `.validate_by()`.

## Root Cause

The original implementation of step 5 (age binning) in `table_maker()` did two
things in sequence:

1. Called `.bin_age(dt)` — adds an `age_group` column to `dt`
2. Immediately replaced `"age"` with `"age_group"` in the `by` vector

```r
# ORIGINAL — WRONG
if (!is.null(by) && "age" %in% by) {
  .bin_age(dt)
  by <- c(setdiff(by, "age"), "age_group")  # ← by is now c("age_group")
}
```

When `compute_measures()` was then called with `by = c("age_group")`, it
reached `.validate_by()`, which checks against `.VALID_DIMENSIONS`:
`c("gender", "area", "educat4", "educat5", "educat7", "age")`. `"age_group"`
is not in this list, so it errored.

The root conceptual error: the `by` vector is both a **user-facing API
parameter** (validated against canonical names) and an **internal column
selector** (used after transformation). They need to be kept separate.

## Fix

Keep `by` containing `"age"` throughout the compute step. Instead of renaming
the element in `by`, overwrite the **`age` column in `dt`** with the binned
factor values, then rename in the **result** after `rbindlist`:

```r
# Step 5 — bin age but keep column name as "age"
age_was_binned <- FALSE
if (!is.null(by) && "age" %in% by) {
  .bin_age(dt)                    # adds age_group column
  dt[, age := age_group]          # overwrite raw integer with binned factor
  dt[, age_group := NULL]         # drop helper column
  age_was_binned <- TRUE
  # by is NOT changed here — still contains "age"
}

# ...compute_measures() called with by = c(..., "age") ← valid ✓...

# Step 7 — rename in result after rbindlist
if (age_was_binned) {
  data.table::setnames(result, "age", "age_group")
  by <- c(setdiff(by, "age"), "age_group")  # update for column reorder only
}
```

This pattern:
- Passes `compute_measures()` the canonical `"age"` name it validates against
- Preserves the binned factor as the column's content during computation
- Renames to the more descriptive `"age_group"` in the final output only

## Prevention

**Rule**: Transformation column names (e.g. `"age_group"` derived from `"age"`)
must not be introduced into the `by` vector before any function that validates
`by` against a fixed allowed-names list.

**General pattern for any post-load column transformation in a pipeline**:
1. Apply transformation in-place, keeping the canonical column name
2. Pass the canonical name through all intermediate functions
3. Rename to the descriptive output name only in the final result

**Test requirement**: Always write a test that exercises the full path
`table_maker(..., by = "age")` end-to-end and asserts that the output contains
`age_group` (not `age`) — this catches both the rename and the
validate_by error.

## Related

- `R/measures.R` — `.bin_age()` and `.VALID_DIMENSIONS` definitions
- `R/compute_measures.R` — `.validate_by()` implementation
- `.cg-docs/solutions/testing-patterns/2026-04-14-partial-miss-regression-test-batch-loaders.md`
  — similar pattern of late-binding column validation
