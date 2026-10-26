---
date: 2026-10-26
title: "data.table by= silently uses names(), not values(), of a named character vector"
category: "bugs"
language: "R"
tags: [data.table, by, named-vector, vapply, grouping, silent-bug, serializer, table-response]
root-cause: "vapply() preserved input names onto its result; passing that named character vector as by= made data.table group by the *names* instead of the *values*, throwing 'columns not found' only at a later unrelated line"
severity: "P1"
---

# data.table `by=` silently uses `names()`, not values, of a named character vector

## Problem

While optimizing `serialize_table_response()` in `R/table_response.R` (piptm)
to replace an O(n_cells × nrow) per-cell boolean-mask rescan with a single
grouped `data.table` split, the refactor introduced:

```r
dim_varnames <- vapply(active_slots, function(k) trimws(slots[[k]]), character(1L))
grouped <- sub_dt[, list(.rows = list(.I)), by = dim_varnames]
unique_cells_dt <- grouped[, dim_varnames, with = FALSE]
```

Running the test suite immediately failed, but at a *different* line than
the actual root cause:

```
Error in `[.data.table`(grouped, , dim_varnames, with = FALSE):
  columns not found: [gender, area, pov_status]
```

The error message named the exact columns expected — making it look like
`grouped` simply didn't have them, which pointed suspicion at the `by=`
grouping step. But a standalone, minimal reproduction of the grouping step
alone (in an isolated script, same data.table version) worked fine with
an **unnamed** character vector of the same values. The failure only
reproduced when the vector carried names.

## Root Cause

`active_slots` was built via `Filter(function(k) ..., slot_names)`, where
`slot_names <- c("column", "row", "superColumn", "superRow")`. Filtering a
named-conceptually vector of slot *keys* (not variable names) through
`Filter()` preserves those key strings as the vector's values.

```r
dim_varnames <- vapply(active_slots, function(k) trimws(slots[[k]]), character(1L))
```

`vapply()` (like `sapply()`) **names its result after the input `X` it
iterated over** when `X` itself is a plain character vector — i.e. for a
character vector, each element becomes both the index *and* (by default)
the name of the corresponding position in the output, unless explicitly
suppressed. So `dim_varnames` ended up as:

```r
# dim_varnames
  column         row  superColumn
 "gender"      "area"  "pov_status"
```

A named character vector, not a plain one. `data.table`'s `by=` (and `j`
column-selection via `with = FALSE`) special-cases named vectors: **when a
vector passed to `by=` has names, data.table groups by the vector's VALUES
but renames the resulting grouping columns to the vector's NAMES.** So
`grouped` ended up with columns `column`, `row`, `superColumn` — not
`gender`, `area`, `pov_status` — even though grouping itself "worked"
silently with no warning. The failure only surfaced two lines later, at
`grouped[, dim_varnames, with = FALSE]`, which tried to select columns by
the (irrelevant) *values* `"gender"`/`"area"`/`"pov_status"` that no longer
existed under those names in `grouped`.

This is exactly why the error pointed at the wrong line: the `by=` call
itself never errored — it silently did something different from what was
intended, and the consequence only became visible at the next unrelated
read.

## Solution

Strip names before passing a programmatically-built variable-name vector to
`by=`, column selection, or anywhere `data.table` NSE might interpret names
specially:

```r
dim_varnames <- unname(vapply(active_slots, function(k) trimws(slots[[k]]), character(1L)))
```

Verified with a minimal reproduction:

```r
library(data.table)
dt <- data.table(a = c(1, 1, 2), b = c("x", "y", "x"))

dv_named   <- c(column = "a", row = "b")   # BUG: has names
dv_unnamed <- unname(dv_named)             # FIX

dt[, list(.rows = list(.I)), by = dv_named]
#>    column    row  .rows   <-- wrong column names! (should be a, b)
#>  1      1      x      1
dt[, list(.rows = list(.I)), by = dv_unnamed]
#>      a      b  .rows   <-- correct
#>  1    1      x      1
```

## Prevention

- **Any time a character vector is built via `vapply()`/`sapply()` over
  another named or keyed structure and will later be passed to
  `data.table`'s `by=`, `.SDcols=`, or `j` with `with = FALSE`, call
  `unname()` on it first** unless the names are deliberately being used
  to rename output columns (a legitimate, different use case — e.g.
  `by = c(new_name = "old_col")` is intentional data.table syntax for
  renaming during grouping, which is exactly the feature that silently
  fired here unintentionally).
- When a `data.table` error names columns that look like they *should*
  exist, don't only inspect the line the error fired on — print
  `names()`/`str()` of the upstream object immediately before that line;
  the actual defect is often one or two steps earlier where names were
  attached unexpectedly.
- This class of bug is easy to miss because the "buggy" grouping step
  produces a syntactically valid, silently-wrong result with no warning —
  it only surfaces downstream when something expects the original names.
  Prefer adding a quick `stopifnot(is.null(names(x)))` assertion (or just
  always `unname()`) at the point a vector is handed to `by=` whenever it
  was constructed programmatically rather than written as a literal.
- This was caught by test coverage, not by casual code review — it is a
  strong argument for running the full `test-table-response.R` suite
  (`devtools::test_file()`, not just visual inspection) after *any*
  `data.table` grouping refactor, even one framed as "performance only."

## Related

- Plan: `.cg-docs/plans/2026-10-26-table-layout-cells-response-format.md`
  (Phase 2, Step 4) — the broader `/table` Approach C response reshape this
  bug was found during.
- Review: `.cg-docs/reviews/2026-10-26-table-layout-cells-response-format-review.md`
  — finding `[P2.2]` (performance refactor) is what triggered writing the
  code that exposed this bug; the fix is recorded there as part of that
  finding's resolution.
- `cg-skill-r-datatable` — grouped/by= patterns; this gotcha is a candidate
  addition to that skill's anti-patterns catalog.
