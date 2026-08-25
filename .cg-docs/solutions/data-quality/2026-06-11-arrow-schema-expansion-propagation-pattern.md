---
date: 2026-06-11
title: "Arrow schema expansion: all 5 propagation points must be updated together"
category: "data-quality"
language: "R"
tags: [arrow, parquet, schema, pip_arrow_schema, prepare_for_arrow, validate_for_write, validate_pre_write, optional_dim_cols, int32, pipdata, piptm, schema-evolution]
root-cause: "Adding new optional columns to the Arrow schema requires updating 5 separate allow-lists across {piptm} and {pipdata}; missing any one causes false 'extra column' errors at write or validation time"
severity: "P2"
fix-confirmed: "yes"
---

# Arrow schema expansion: all 5 propagation points must be updated together

## Problem

When new optional columns are added to the PIP Arrow schema (e.g. household
size, infrastructure indicators, labour variables), the pipeline raises false
"extra column" errors like:

```
Input contains column(s) not in the Arrow schema: "hsize", "lstatus", …
Run `prepare_for_arrow()` first, or drop these columns manually.
```

This happens even when the column is genuinely schema-conformant, because the
new name was added to `pip_arrow_schema()` but not propagated to every
allow-list that enforces it.

## Root Cause

The schema's single source of truth (`pip_arrow_schema()` in `{piptm}`) is
**authoritative** but not **automatically consumed** by every enforcement point.
Five locations hard-code or derive an allow-list independently:

| # | Location | What it guards |
|---|---|---|
| 1 | `piptm/R/schema.R` → `pip_arrow_schema()` | Source of truth; also drives `pip_allowed_cols()` |
| 2 | `piptm/R/validate_parquet.R` → `.vp_canonical_schema()` | Schema type-check in `validate_parquet_schema()` |
| 3 | `pipdata/R/arrow_prep.R` → `validate_pre_write()` §4.8 `optional_dim_cols` | Pre-write column allow-list |
| 4 | `pipdata/R/arrow_prep.R` → `prepare_for_arrow()` `optional_dim_cols` | Column selection / all-NA drop |
| 5 | `pipdata/R/arrow_generation.R` → `.validate_for_write()` `optional_dims` | Write-time column allow-list |
| 6 | `pipdata/R/arrow_generation.R` → `write_survey_parquet()` `dim_cols` | Available-dimensions summary in result row |

Points 3–6 do **not** call `pip_allowed_cols()` at runtime — they use local
`c(...)` vectors. This is intentional (no runtime cross-package call) but means
a schema change must be manually mirrored.

## Solution

For every new optional column group, update all 5 (or 6) locations atomically:

```r
# 1. piptm/R/schema.R — pip_arrow_schema()
new_col = list(type = arrow::int32(), required = FALSE)

# 2. piptm/R/validate_parquet.R — .vp_canonical_schema()
arrow::field("new_col", arrow::int32())

# 3 & 4. pipdata/R/arrow_prep.R — BOTH optional_dim_cols vectors
optional_dim_cols <- c(...existing..., "new_col")

# 5. pipdata/R/arrow_generation.R — .validate_for_write() optional_dims
optional_dims <- c(...existing..., "new_col")

# 6. pipdata/R/arrow_generation.R — write_survey_parquet() dim_cols intersect
dim_cols <- intersect(c(...existing..., "new_col"), names(dt))
```

### For pass-through int32 columns (no standardisation helper needed)

Add a cast step in `prepare_for_arrow()` between the standardisation block
(Step 3) and column selection (Step 4):

```r
# Step 3b: cast pass-through int32 columns where present
.new_int_cols <- c("hsize", "imp_wat_rec", "imp_san_rec", ...)
int_cols_present <- intersect(.new_int_cols, names(dt))
if (length(int_cols_present) > 0L) {
  dt[, (int_cols_present) := lapply(.SD, as.integer), .SDcols = int_cols_present]
}
```

This is necessary because survey microdata often stores these as `numeric`
(double) rather than integer; Arrow will fail the `as_arrow_table(schema)` call
if the R type does not match the schema-declared `int32`.

### All-NA drop is free

`prepare_for_arrow()` already drops optional columns that are entirely `NA`
after standardisation. Because the new columns are included in
`optional_dim_cols`, they participate in this drop automatically — no new code
needed for the NA-handling path.

## When Applied

This pattern was used to add 18 new `int32` optional columns:

| Group | Columns |
|---|---|
| Household | `hsize` |
| Infrastructure | `imp_wat_rec`, `imp_san_rec`, `electricity` |
| Labour — lstatus | `lstatus`, `lstatus_year` |
| Labour — empstat | `empstat`, `empstat_2`, `empstat_year`, `empstat_2_year` |
| Labour — industrycat10 | `industrycat10`, `industrycat10_2`, `industrycat10_year`, `industrycat10_2_year` |
| Labour — industrycat4 | `industrycat4`, `industrycat4_2`, `industrycat4_year`, `industrycat4_2_year` |

`pip_allowed_cols()` count: 12 → 30. `pip_arrow_schema()$fields` count: 12 → 30.

## Prevention

### Test checklist for schema expansion PRs

1. `pip_allowed_cols()` returns the expected count.
2. `pip_required_cols()` count unchanged (still 6).
3. `prepare_for_arrow()` on a data.table carrying the new columns produces
   them in the output as the correct R type.
4. `prepare_for_arrow()` on a data.table with one of the new columns entirely
   `NA` drops that column without error.
5. `validate_parquet_schema()` on a Parquet file with the new columns passes.
6. `.validate_for_write()` on a prepared data.table with the new columns passes.

Tests 3–4 were added to `pipdata/tests/testthat/test-arrow-prep.R` as part of
this change.

### Schema expansion PR template (minimal)

```r
# Check 1: count
expect_length(pip_allowed_cols(), 30L)           # update expected count

# Check 2: new col in allowed, not in required
expect_true("hsize" %in% pip_allowed_cols())
expect_false("hsize" %in% pip_required_cols())

# Check 3: round-trip type
dt[, hsize := 4L]
result <- prepare_for_arrow(dt, pip_id = ...)
expect_true(is.integer(result$hsize))

# Check 4: all-NA drop
dt[, hsize := NA_integer_]
result <- prepare_for_arrow(dt, pip_id = ...)
expect_false("hsize" %in% names(result))
```

## Context.md Impact

After a schema expansion, update the "Physical layout" note in
`compound-gpid.context.md` (Arrow / Parquet Data Repository section) to
reflect the new base column count and column list.

> **After this change**: base column count = 30 (was 14/12 in prior notes).

## Related

- `.cg-docs/solutions/bugs/2026-04-03-null-allowed-cols-gen-validate-for-write.md` —
  sibling bug: when `.ALLOWED_COLS_GEN` is `NULL`, the same allow-list in
  `.validate_for_write()` silently accepts *all* columns instead of the schema
  set. The lazy-accessor pattern in `arrow_generation.R` is the fix.
- `.cg-docs/solutions/data-quality/2026-08-24-metadata-schema-execution-truth-pattern.md` —
  defensive validation pattern: check schema boundaries (NULL, data.table type, required columns) before consuming metadata
- `piptm/R/schema.R` — `pip_arrow_schema()` (source of truth)
- `pipdata/R/arrow_prep.R` — `prepare_for_arrow()`, `validate_pre_write()`
- `pipdata/R/arrow_generation.R` — `.validate_for_write()`, `write_survey_parquet()`
