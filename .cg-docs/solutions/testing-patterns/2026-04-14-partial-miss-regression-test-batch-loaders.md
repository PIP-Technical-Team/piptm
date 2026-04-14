---
date: 2026-04-14
title: "Partial-miss regression test pattern for batch data loaders"
category: "testing-patterns"
type: "pattern"
language: "R"
tags: [testthat, arrow, parquet, batch-loading, regression-test, partial-miss, fixtures, piptm]
root-cause: "Batch loading functions can silently drop entries when one of N requested items is missing; a partial-miss test catches this by constructing a manifest where exactly one entry has no on-disk fixture"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# Partial-Miss Regression Test for Batch Data Loaders

## Problem

A batch loading function (`load_surveys()`) silently dropped surveys when their
partition directories were missing from disk. The full-happy-path test suite
passed because every fixture was present. There was no test that intentionally
omitted one fixture to verify the error path.

## Pattern

For any function that loads N items from disk given a list/manifest of N keys,
always write a **partial-miss regression test**: a test where N-1 items exist on
disk and 1 does not. The test asserts that the function **errors** (not silently
drops or returns a partial result).

### Fixture Setup

Use `withr::local_tempdir()` so the temp directory is cleaned up automatically
at the end of the test block. Create only the fixtures that *should* exist.

```r
test_that("load_surveys() errors when a manifest entry has no Parquet files on disk", {
  tmp        <- withr::local_tempdir()
  arrow_root <- file.path(tmp, "arrow")

  # ---- Fixture 1: EXISTS on disk (COL / 2010 / INC) ----
  col_leaf <- file.path(
    arrow_root,
    "country_code=COL", "surveyid_year=2010",
    "welfare_type=INC", "version=v01_v01"
  )
  dir.create(col_leaf, recursive = TRUE)
  arrow::write_parquet(
    data.frame(
      welfare = 1, weight = 1,
      pip_id  = "COL_2010_ENCV_INC_ALL",
      ...     # include all columns expected by the schema
    ),
    file.path(col_leaf, "part-0.parquet")
  )

  # ---- Fixture 2: INTENTIONALLY ABSENT (BOL / 2015 / CON) ----
  # No dir.create / write_parquet for this entry

  # ---- Manifest includes both entries ----
  mf <- data.table::data.table(
    country_code = c("COL",                    "BOL"),
    year         = c(2010L,                    2015L),
    welfare_type = c("INC",                    "CON"),
    version      = c("v01_v01",               "v01_v01"),
    pip_id       = c("COL_2010_ENCV_INC_ALL", "BOL_2015_EH_CON_ALL"),
    dimensions   = list(c("area"), c("area"))
  )

  piptm::set_arrow_root(arrow_root)
  expect_error(piptm::load_surveys(mf), regexp = "No Parquet files found")
})
```

### Key Elements

| Element | Why |
|---|---|
| `withr::local_tempdir()` | Auto-cleanup; no `on.exit()` boilerplate needed |
| Exactly 1 of N entries missing | Tests the partial-miss path specifically |
| `expect_error(regexp = ...)` | Verifies the right error, not just any error |
| Real Parquet fixture (not a mock) | Ensures the read path is tested end-to-end |
| `dir.create(recursive = TRUE)` | Creates the full Hive directory tree in one call |

### What This Test Catches

- A batch function that silently ignores missing entries (returns partial result).
- A batch function that calls `unlist()` on a list including `character(0)`,
  collapsing gaps.
- A helper that only errors in the single-item variant but not the batch variant.

## Related

- `.cg-docs/solutions/bugs/2026-04-14-load-surveys-silent-partial-miss.md` —
  the specific bug this pattern caught
- `.cg-docs/solutions/bugs/2026-04-07-partition-key-prefix-mismatch.md` —
  related Arrow partition path bugs
