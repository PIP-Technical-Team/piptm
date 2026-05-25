---
date: 2026-05-21
title: "Arrow open_dataset on multiple Parquet files silently breaks within-survey welfare sort"
category: "data-quality"
language: "R"
tags: [arrow, parquet, sort-contract, open_dataset, hive-partitioning, gini, compute_inequality, multi-file, pre-sort-contract]
root-cause: "open_dataset() concatenates multiple Parquet files in file-system order, producing two sorted segments end-to-end rather than a single sorted sequence; after sort removal from compute_inequality() this silently produces wrong Gini values"
severity: "P1"
fix-confirmed: "yes"
---

# Arrow multi-file partition silently breaks the welfare pre-sort contract

## Problem

The `{piptm}` pipeline relies on a **pre-sort contract**: welfare values in
each survey's microdata are sorted ascending before `compute_inequality()` is
called. `compute_inequality()` was refactored to remove its internal
`setorder()`, trusting this contract entirely.

The contract holds when each partition directory contains exactly one Parquet
file — the `{pipdata}` pipeline writes exactly one file per partition under
normal operation. However, if a partition directory were to contain two files
(retried write, manual copy, pipeline shard, test artefact), `open_dataset()`
on that directory concatenates the files in file-system order. The result is:

```
[file1: welfare 1…5000 sorted] ++ [file2: welfare 1…4800 sorted]
= welfare 1, 2, …, 5000, 1, 2, …, 4800   ← NOT globally sorted
```

`compute_inequality()` receives this combined vector, the `is.unsorted()` guard
fires only on the `by = NULL` path (aggregate), so grouped Gini computation
(the most common call-site) would compute silently wrong values.

**Empirically confirmed**: unsorted welfare produces Gini errors up to 0.39
(mean absolute difference ~0.18 across 16 groups in a 10,000-row test).

## Root Cause

`.build_parquet_paths()` uses `list.files()` to discover all `.parquet` files
in the Hive leaf directory and returns the full vector to `open_dataset()`.
Before the sort-removal refactor, `setorder()` inside `compute_inequality()`
corrected this silently. After sort removal, no layer caught multiple files.

The one-file-per-partition assumption was implicit and unenforceable.

## Solution

Add a hard error in `.build_parquet_paths()` when more than one file is found:

```r
if (length(files) > 1L) {
  cli::cli_abort(
    c(
      "Multiple Parquet files found for {.val {country_code}} / {year} / \\
      {.val {welfare_type}} / {.val {version}}.",
      "i" = "Expected exactly one file per partition. Found: {.val {basename(files)}}",
      "i" = "Partition path: {.path {leaf}}"
    )
  )
}
```

This makes the one-file invariant **explicit and enforced** rather than
implicit and assumed. Any future pipeline change that shards surveys into
multiple files will need to update `.build_parquet_paths()` consciously,
not discover the breakage through wrong statistics.

### Why an error rather than a sort?

Adding `setorder(dt, welfare)` as a "safety net" in `load_surveys()` or
`compute_inequality()` is tempting, but was measured at **537% overhead** on a
network share (1.658s → 3.337s for 15 surveys). See L5 in
`.cg-docs/solutions/performance-issues/2026-04-30-arrow-io-and-compute-lessons.md`.

An error at the I/O layer is free and forces the operator to fix the root cause
(duplicate file) rather than masking it with an expensive sort.

## When this invariant might legitimately change

If the `{pipdata}` pipeline ever shards large surveys across multiple Parquet
files (e.g., surveys > 1M rows), the correct fix is:

1. Update `write_survey_parquet()` to write a single sorted file regardless of
   size (Arrow handles multi-gigabyte single files fine).
2. OR: update `.build_parquet_paths()` to sort row groups explicitly across
   files before returning them to `open_dataset()` — and re-establish and test
   the sort contract for the multi-file case.

The current guard is the correct position: **fail loudly until the invariant is
deliberately re-established**.

## Prevention

### Rule: after removing an internal sort, enumerate all callers that could
### supply unsorted data and guard each one explicitly

The removal of `setorder()` from `compute_inequality()` was correct, but it
transferred the correctness responsibility to every caller. Whenever a sort
assumption is removed:

1. Document the pre-sort contract in `@section Pre-sort contract:` in the
   function's roxygen block.
2. Add `is.unsorted()` or equivalent guard at the aggregate call-site.
3. Audit every upstream data source that feeds the function and add a guard
   at any layer that could produce unsorted data.

### Tests to write

```r
# In the loader tests — verify that duplicate-file partitions are caught:
test_that(".build_parquet_paths errors on multiple Parquet files", {
  tmp <- withr::local_tempdir()
  dir <- file.path(tmp, "country_code=COL", "surveyid_year=2010",
                   "welfare_type=INC", "version=v01_v01")
  dir.create(dir, recursive = TRUE)
  writeLines("dummy", file.path(dir, "part-0.parquet"))
  writeLines("dummy", file.path(dir, "part-1.parquet"))
  expect_error(
    piptm:::.build_parquet_paths(tmp, "COL", 2010L, "INC", "v01_v01"),
    regexp = "Multiple Parquet files"
  )
})
```

## Related

- `.cg-docs/solutions/data-quality/2026-05-21-collapse-silent-na-drop-in-statistics.md` — sibling guard, same review pass
- `.cg-docs/solutions/performance-issues/2026-04-30-arrow-io-and-compute-lessons.md` — L5: never add Arrow sort as safety net (537% overhead)
- `.cg-docs/brainstorms/2026-05-21-remove-redundant-welfare-sort.md` — decision record for sort removal
- `R/load_data.R` — `.build_parquet_paths()` implementation
