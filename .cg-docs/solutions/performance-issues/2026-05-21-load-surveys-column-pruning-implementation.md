---
date: 2026-05-21
title: "Column Pruning in load_surveys(): Implementation Pattern"
category: "performance-issues"
language: "R"
tags: [arrow, parquet, column-pruning, load_surveys, table_maker, io, network-io, select-before-collect]
root-cause: "load_surveys() loaded all 14 Parquet columns regardless of which columns table_maker() actually needed, wasting ~68% of I/O bandwidth on a UNC share."
severity: "P1"
---

# Column Pruning in load_surveys(): Implementation Pattern

## Problem

`load_surveys()` always called `open_dataset() |> collect()` without any
`select()`, loading all 14 schema columns into memory. `table_maker()` then
used only the handful it needed (welfare, weight, by-dimensions). The unused
columns were transferred over the UNC network share but immediately discarded.

Benchmark result (50-iteration resample, 15 surveys):
- All 14 columns: **1.658s** median I/O
- 6 needed columns: **0.524s** median — **68% faster**

## Root Cause

Two sub-problems:

1. **`load_surveys()` had no mechanism to restrict columns** — no `cols`
   parameter existed.

2. **PPP target-column resolution happened *after* `collect()`**, so the
   physical column name (e.g. `welfare_ppp_2017_01_02`) wasn't known at
   query-build time and couldn't be used in a `select()`.

## Solution

### 1. Move PPP target-column determination before `open_dataset()`

Resolve the physical welfare column name from `entries_dt$welfare_vars`
(manifest metadata, already in memory — no file read needed) before building
the Arrow query:

```r
# Resolved entirely from manifest metadata — no I/O
target_col       <- "welfare"          # default for legacy surveys
all_welfare_vars <- character(0L)

if (any(new_schema_mask)) {
  # ... validation of effective_year, diverging column names, etc. ...
  target_col       <- target_cols[[1L]]              # e.g. "welfare_ppp_2017_01_02"
  all_welfare_vars <- unique(unlist(entries_dt$welfare_vars[new_schema_mask]))
}
```

### 2. Add `cols` parameter; translate "welfare" → physical name; select before collect

```r
load_surveys <- function(entries_dt, ppp = NULL, cols = NULL, release = NULL)
```

```r
ds <- arrow::open_dataset(parquet_files, format = "parquet")

if (!is.null(cols)) {
  physical_cols <- cols
  # Translate logical "welfare" → physical PPP column name
  physical_cols[physical_cols == "welfare"] <- target_col
  # pip_id is always needed for the integrity check
  physical_cols <- union(physical_cols, "pip_id")
  # intersect() makes this safe for partial-match surveys that lack some
  # dimension columns — they are silently omitted here and NA-filled later
  # by table_maker()
  safe_cols <- intersect(physical_cols, ds$schema$names)
  ds <- dplyr::select(ds, dplyr::all_of(safe_cols))
}

dt <- ds |> dplyr::collect() |> data.table::as.data.table()
```

Post-collect, rename `target_col` → `"welfare"` and drop other welfare_* columns
(same as before, but now `target_col` is the only such column when `cols` was
non-NULL):

```r
if (target_col != "welfare") {
  welfare_data_cols <- intersect(all_welfare_vars, names(dt))
  data.table::setnames(dt, target_col, "welfare")
  drop_cols <- setdiff(welfare_data_cols, target_col)
  if (length(drop_cols) > 0L) dt[, (drop_cols) := NULL]
}
```

### 3. `table_maker()` computes needed columns and passes them

```r
needed_cols <- unique(c(
  "pip_id", "country_code", "surveyid_year", "welfare_type",
  "welfare", "weight",
  by   # NULL is silently dropped by c()
))
dt <- load_surveys(entries, ppp = ppp, cols = needed_cols, release = release)
```

`by` expands to whatever disaggregation dimensions were requested
(e.g. `c("gender", "area")`). If `by = NULL`, the minimum set of 6 columns
is loaded.

## Prevention

**Pattern to follow in any Arrow-backed loader that has callers with different
column needs:**

1. Accept a `cols` argument (default `NULL` = load all).
2. Resolve any physical-column aliases (like PPP column names) from metadata
   *before* calling `open_dataset()`.
3. Translate logical names → physical names, then `select()` before `collect()`.
4. Use `intersect(physical_cols, ds$schema$names)` to guard against schemas
   that lack some optional columns — never let a missing column abort the load.
5. Always force-include columns that the function itself requires internally
   (here: `pip_id` for the integrity check).

**Anti-patterns:**
- Resolving column names after `collect()` and then trying to `select()` — zero
  I/O benefit, all bytes already transferred.
- Hard-coding a fixed column list inside the loader — breaks when callers need
  different subsets or when the schema gains new columns.

## Related

- `2026-04-30-arrow-io-and-compute-lessons.md` — full benchmark theory and
  lesson set (L1: select-before-collect rule; L2: scan-count matters)
- `2026-04-29-arrow-vs-collapse-results.md` — raw benchmark numbers
- `R/load_data.R` — implementation (search for `cols` parameter)
- `R/table_maker.R` — caller (search for `needed_cols`)
