---
date: 2026-05-21
title: "collapse::fsum/fcumsum silently drops NA rows, biasing weighted statistics"
category: "data-quality"
language: "R"
tags: [collapse, fsum, fcumsum, NA, gini, mld, silent-bias, na.rm, compute_inequality, fail-loudly]
root-cause: "collapse aggregate functions default to na.rm = TRUE, silently excluding NA welfare/weight rows from denominators and producing statistically biased results with no warning"
severity: "P1"
fix-confirmed: "yes"
---

# collapse silent NA drop biases Gini and MLD

## Problem

`compute_inequality()` calls `collapse::fsum()`, `collapse::fcumsum()`, and
`collapse::fmean()` on `welfare` and `weight` vectors. When either vector
contains `NA`, the functions silently exclude those rows from all calculations
because `collapse` defaults to `na.rm = TRUE` everywhere.

The visible symptom is wrong — but numerically plausible — Gini and MLD
values. No error, no warning. The bias is proportional to the share of NA rows
and their welfare values. On a survey with 0.5% NA rows near the top of the
distribution the Gini can be understated by ~0.01 without any diagnostic
signal.

### Why collapse is different from base R here

Base R aggregate functions (`sum()`, `mean()`) default to `na.rm = FALSE` and
propagate `NA` to the result — making the problem visible. `collapse` defaults
to `na.rm = TRUE` for performance reasons. This is correct behaviour for
exploratory analysis but dangerous in a pipeline that assumes clean input.

## Root Cause

The `collapse` package's philosophy is "fast, no copies, na.rm = TRUE by
default". In `.gini_sorted()`:

```r
ww  <- weight * welfare          # NA * anything = NA
sw  <- collapse::fsum(weight)    # silently skips NA rows — denominator too small
swy <- collapse::fsum(ww)        # silently skips NA rows
v   <- (collapse::fcumsum(collapse::flag(ww, fill = 0)) + ww / 2) * weight
auc <- collapse::fsum(v) / sw / swy  # computed on a biased subset
```

`fcumsum()` propagates NA forward from the first NA position (correct
behaviour) — but the cumulative sum is then divided by `swy` and `sw`, both of
which were computed without NA rows. The result is a Gini for a different
(smaller, biased) population than the one requested.

For MLD the same issue applies: `fmean()` with `na.rm = TRUE` computes the
weighted mean over non-NA rows only, then the log ratio is evaluated at those
rows. The excluded zero-welfare rule adds further complexity that masks the NA
bias.

## Solution

Guard at the entry point of `compute_inequality()`, before any
collapse-aggregate call, using `base::anyNA()` (O(n) scan, early-exit on
first NA):

```r
if (anyNA(welfare_v))
  cli::cli_abort(
    "{.arg dt}$welfare contains {sum(is.na(welfare_v))} NA value(s). \\
    Remove or impute before calling {.fn compute_inequality}."
  )
if (anyNA(w))
  cli::cli_abort(
    "{.arg dt}$weight contains {sum(is.na(w))} NA value(s). \\
    Remove or impute before calling {.fn compute_inequality}."
  )
```

Place this block **before** `fsum(w, g = grp)` (the population calculation)
so it fires early and clearly, not midway through a grouped computation.

### Why not pass `na.rm = FALSE` to every collapse call?

- `collapse::fcumsum()` does not accept `na.rm`.
- `collapse::flag()` does not propagate NA in a way that would surface the
  problem at the aggregate level.
- Passing `na.rm = FALSE` to `fsum()` returns `NA` for the whole group when
  any row is NA, making it impossible to distinguish "data has NA" from
  "correct NA result" (e.g., all-zero welfare group → `NA_real_`).
- An explicit guard is clearer, more diagnosable, and easier to test.

## Prevention

### Rule: guard before collapse aggregates in statistics functions

Any function that applies `collapse::fsum`, `fmean`, `fcumsum`, or similar to
user-supplied data should guard for NA **before** the first aggregate call:

```r
# ALWAYS add this pattern before collapse aggregates on user data:
if (anyNA(data_vec))
  cli::cli_abort("NA values in {.field column_name} — remove before calling {.fn fn_name}.")
```

### Do not rely on the upstream pipeline to be NA-free

Even though `pipdata` write-side validation checks for `welfare` and `weight`
quality, the guard must exist in `{piptm}` too. `compute_inequality()` is
exported and can be called directly with any data.

### Tests to write

```r
test_that("NA welfare raises error before any computation", {
  dt <- data.table(welfare = c(1, NA, 3), weight = rep(1, 3))
  expect_error(compute_inequality(dt, measures = "gini"), regexp = "NA value")
})

test_that("NA weight raises error before any computation", {
  dt <- data.table(welfare = c(1, 2, 3), weight = c(1, NA, 1))
  expect_error(compute_inequality(dt, measures = "mld"), regexp = "NA value")
})
```

Both tests are now in `tests/testthat/test-compute-inequality.R`.

## Related

- `.cg-docs/solutions/bugs/2026-04-14-load-surveys-silent-partial-miss.md` — similar "silent omission" failure mode, different layer
- `.cg-docs/solutions/data-quality/2026-05-21-arrow-multifile-partition-sort-violation.md` — sibling guard added in the same review pass
- `R/compute_inequality.R` lines containing `anyNA(welfare_v)` / `anyNA(w)`
