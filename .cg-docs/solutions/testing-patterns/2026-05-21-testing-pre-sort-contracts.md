---
date: 2026-05-21
title: "Testing pre-sort contracts: verify the contract is required, not just that it holds"
category: "testing-patterns"
language: "R"
tags: [gini, sort-contract, testing, compute_inequality, pre-sort, contract-testing, is.unsorted, welfare]
root-cause: "After removing an internal sort from a statistics function, no test verified that passing unsorted data actually breaks results — the contract existed only in documentation"
severity: "P2"
fix-confirmed: "yes"
---

# Testing pre-sort contracts

## Problem

`compute_inequality()` was refactored to remove its internal `setorder()` call,
relying instead on a documented pre-sort contract: callers must supply welfare
data sorted ascending. All existing tests passed — but none of them would have
caught a regression where the upstream pipeline stopped sorting welfare.

If `pipdata::generate_arrow_dataset()` ever dropped its sort, `load_surveys()`
would return unsorted data, and `compute_inequality()` would produce silently
wrong Gini values with every existing test still green.

## Root Cause

Tests for statistics functions typically verify that **correct input produces
correct output**. They rarely verify that **incorrect input produces wrong
output** (the contract's negative case). For invariants held by trusted
upstream layers this is often reasonable — but when the invariant becomes
invisible (guard removed), the negative case becomes the only safety net.

## Solution

Two complementary test patterns:

### Pattern A: negative-case test — unsorted input MUST error

When the function exposes a runtime guard (`is.unsorted()` check with
`cli_abort()`), add a test that explicitly verifies the error fires:

```r
test_that("gini: unsorted input raises an error (pre-sort contract, by = NULL)", {
  dt_unsorted <- data.table(
    welfare = as.numeric(c(5, 1, 8, 3, 10, 2, 7, 4, 9, 6)),
    weight  = rep(1, 10)
  )
  expect_error(
    compute_inequality(dt_unsorted, measures = "gini"),
    regexp = "Pre-sort contract violated"
  )
})

test_that("gini: sorted input does NOT trigger the pre-sort guard", {
  dt_sorted <- data.table(welfare = as.numeric(1:10), weight = rep(1, 10))
  expect_no_error(compute_inequality(dt_sorted, measures = "gini"))
})
```

This locks in the guard's existence. If someone removes the `is.unsorted()`
check, the first test fails immediately.

### Pattern B: correctness-divergence test — unsorted input gives WRONG results

Use this when the function has no guard (e.g., the grouped `by != NULL` path
where `is.unsorted()` on the flattened vector is not meaningful). Instead of
asserting an error, assert that sorted and unsorted inputs produce different
results:

```r
test_that("gini: unsorted input produces wrong result (contract unguarded)", {
  set.seed(1)
  dt_sorted   <- data.table(welfare = as.numeric(1:10), weight = rep(1, 10))
  dt_unsorted <- data.table(welfare = sample(1:10), weight = rep(1, 10))
  res_sorted   <- compute_inequality(dt_sorted,   measures = "gini")$value
  res_unsorted <- compute_inequality(dt_unsorted, measures = "gini")$value
  # Sorted is correct; unsorted should produce a different (wrong) value.
  expect_false(isTRUE(all.equal(res_sorted, res_unsorted)),
    label = "unsorted input must not accidentally equal sorted result")
})
```

This is weaker than Pattern A (it does not confirm the result is wrong, only
that it differs), but it serves as an upstream-regression detector: if
`load_surveys()` ever returns unsorted data and the Gini happens to be
computed correctly, this test fails.

## When to apply each pattern

| Situation | Pattern |
|---|---|
| Function has `is.unsorted()` guard + `cli_abort()` | A — assert the error fires |
| Function has no guard (grouped path, external pre-sort relied on) | B — assert divergence |
| Both paths present | Both A and B |
| Cross-package contract (sort done in `{pipdata}`) | Integration test in `{pipdata}` that asserts sorted output; Pattern B in `{piptm}` as regression detector |

## Broader principle: contracts need negative-case tests

Any time a function removes an internal defensive operation (sort, dedup,
type-cast) in favour of a documented contract, add a test that will fail if the
contract is violated. This converts a documentation-only invariant into an
enforced invariant.

Checklist for removing an internal guard:

- [ ] Document the contract in `@section` and `@param`, not just in comments
- [ ] Add a runtime guard (`is.unsorted()`, `anyNA()`, `stopifnot()`) where
  feasible
- [ ] Add Pattern A test (error fires) or Pattern B test (divergence)
- [ ] Add guard at the upstream data boundary (e.g., multi-file check in
  `.build_parquet_paths()`)
- [ ] Record the decision in a `.cg-docs/brainstorms/` file

## Related

- `.cg-docs/brainstorms/2026-05-21-remove-redundant-welfare-sort.md` — decision
  record for the sort removal that motivated these patterns
- `.cg-docs/solutions/data-quality/2026-05-21-arrow-multifile-partition-sort-violation.md` — the multi-file guard that protects the same invariant at the loader layer
- `.cg-docs/solutions/testing-patterns/2026-04-14-partial-miss-regression-test-batch-loaders.md` — prior pattern for testing silent-miss regressions
- `tests/testthat/test-compute-inequality.R` — contains both Pattern A tests and NA-guard tests added in this session
