---
date: 2026-05-20
title: "Column pruning in load_surveys() for table_maker performance"
status: completed
completed-date: 2026-05-20
scope: Lightweight
estimated-effort: small
tags: [inline, performance, arrow, column-pruning]
brainstorm: .cg-docs/brainstorms/2026-05-20-load-surveys-column-pruning.md
---

# Column Pruning in load_surveys()

## Context

Benchmarking showed loading only needed columns instead of all 14 gives a 68%
I/O improvement (1.658s → 0.524s) over UNC share. The fix is to add a `cols`
parameter to `load_surveys()` and pass the needed columns from `table_maker()`.

## Steps

### 1. Refactor `load_surveys()`: move PPP target_col determination before `collect()`

The PPP column name (`target_col`) is currently resolved after `collect()`.
Move it before the `open_dataset()` call so it is available when building the
Arrow `select()`. Use `entries_dt$welfare_vars` (manifest data) — no file read
needed.

**Acceptance criteria:**
- All existing `load_surveys()` tests still pass (no behaviour change when
  `cols = NULL`).

### 2. Add `cols` parameter to `load_surveys()`

Signature change: `load_surveys(entries_dt, ppp = NULL, cols = NULL, release = NULL)`.

When `cols` is non-NULL:
- Translate logical `"welfare"` → physical `target_col` (or keep `"welfare"` for
  legacy surveys without `welfare_vars`).
- Always include `"pip_id"` (required for the integrity check).
- Use `intersect(physical_cols, ds$schema$names)` so columns absent from some
  survey files are skipped here and NA-filled downstream by `table_maker()`.
- Insert `dplyr::select(dplyr::all_of(safe_cols))` in the Arrow pipeline
  **before** `dplyr::collect()`.
- Update `@param cols` roxygen.

When `cols = NULL` (default): no change to current behaviour.

**Acceptance criteria:**
- `load_surveys(..., cols = c("welfare", "weight", "pip_id", "gender"))` returns
  a `data.table` with only those 4 columns (plus any that schema unification
  adds as NA for partial surveys).
- New-schema survey: correct `welfare_ppp_*` column loaded and renamed to
  `"welfare"`; other PPP columns absent.
- `cols = NULL`: all columns returned as before.

### 3. Update `table_maker()` to pass `cols` to `load_surveys()`

Compute needed columns from `table_maker()`'s arguments and pass them:

```r
needed_cols <- unique(c(
  "pip_id", "country_code", "surveyid_year", "welfare_type",
  "welfare", "weight",
  by   # NULL is silently dropped by c()
))
dt <- load_surveys(entries, ppp = ppp, cols = needed_cols, release = release)
```

**Acceptance criteria:**
- Existing `table_maker()` tests all pass.
- A new integration test confirms only the expected columns are loaded when
  `by = c("gender")` (verifying no extra columns appear in the intermediate
  `dt` prior to the metadata join).

### 4. Add / update tests

- `test-load-data.R`: 3 new tests for `cols` behaviour (cols subset, new-schema
  with cols, cols=NULL no-op).
- `test-table-maker.R`: 1 new test verifying column pruning reaches
  `load_surveys()` (spy via fixture parquet with extra columns that should
  not appear in output).
