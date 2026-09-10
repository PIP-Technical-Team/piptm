---
title: "Expand Allowed Column Set in PIP Arrow Pipeline"
date: "2026-06-11"
status: completed
completed-date: 2026-06-11
completed-phases: []
failing-steps: []
---

# Expand Allowed Column Set in PIP Arrow Pipeline

Add 18 new socioeconomic indicator columns (all `int32`, no extra
standardisation required) to the canonical Arrow schema and propagate
the change through every place that enforces the column allow-list.

## Columns to Add

| Group | Columns |
|-------|---------|
| Household | `hsize` |
| Infrastructure | `imp_wat_rec`, `imp_san_rec`, `electricity` |
| Labour — lstatus | `lstatus`, `lstatus_year` |
| Labour — empstat | `empstat`, `empstat_2`, `empstat_year`, `empstat_2_year` |
| Labour — industrycat10 | `industrycat10`, `industrycat10_2`, `industrycat10_year`, `industrycat10_2_year` |
| Labour — industrycat4 | `industrycat4`, `industrycat4_2`, `industrycat4_year`, `industrycat4_2_year` |

All 18 columns are `arrow::int32()`, `required = FALSE`, and are treated
as **pass-through** in `{pipdata}` — no additional standardisation step is
applied; they are cast to `integer` and written as-is.

## Requirements

- `pip_arrow_schema()` is the single source of truth for types.
- All places that hard-code allowed or required column lists must derive
  from (or mirror) `pip_arrow_schema()`.
- Existing required-column count (6) must not change.
- `pip_allowed_cols()` base count increases from 12 to 30.
- Tests must be updated to reflect the new counts and column names.
- No new standardisation helpers are needed in `{pipdata}` — the caller
  is responsible for ensuring the columns are already integer-coded.

## Phase 1: Schema & validation in {piptm}

### 1.1 Update `piptm/R/schema.R` — `pip_arrow_schema()`

Add all 18 new fields as `list(type = arrow::int32(), required = FALSE)`
to the `fields` list in `pip_arrow_schema()`.  Place them after the
existing optional fields (`educat7`, `age`), grouped by semantic family
and preceded by comments.

Acceptance: `length(pip_arrow_schema()$fields) == 30`,
`pip_required_cols()` still returns exactly 6 names,
`pip_allowed_cols()` returns 30 names.

### 1.2 Update `piptm/R/validate_parquet.R`

1. **`.vp_canonical_schema()`** — add `arrow::field("hsize", int32())` etc.
   for all 18 new columns (after the `age` field).

2. **`validate_parquet_data()` check 8** — the education factor check
   already iterates `c("educat4", "educat5", "educat7")`; the new int32
   columns need no fixed-level check.  However, add a type-conformance
   guard: if any of the 18 new columns is present in `dt`, warn (not
   error) if it cannot be coerced to integer (unlikely after pipeline
   enforcement, but safe to audit).

   Simpler approach (preferred): just ensure the new columns are listed in
   `optional_dims` inside `validate_parquet_data()` check 7
   (partition-key consistency) so they are not flagged as extra columns.
   No data-range checks are required.

### 1.3 Update `piptm` tests

File: `tests/testthat/test-schema.R`

- `"pip_allowed_cols returns 12 base columns total"` → expect 30.
- `"pip_allowed_cols includes all required and optional base columns"` →
  add all 18 new column names to `expected`.
- `"pip_allowed_cols with welfare_vars appends welfare columns"` →
  expect 32 (30 + 2 welfare).
- `"pip_required_cols does not include optional columns"` → add the 18
  new names to the loop that asserts they are not in required.

## Phase 2: Allow-list in {pipdata}

### 2.1 Update `pipdata/R/arrow_prep.R`

1. **`prepare_for_arrow()`** — extend `optional_dim_cols` to include all
   18 new columns.  After `standardize_age(dt)` and before column
   selection, add a **`cast_int_cols()`** step (inline or a tiny helper)
   that casts any of the 18 new columns present in `dt` to `integer`.

2. **`validate_pre_write()`** — extend `optional_dim_cols` inside §4.8
   so the new columns are not flagged as extra.

### 2.2 Update `pipdata/R/arrow_generation.R`

In `.validate_for_write()`, extend `optional_dims` to include all 18
new column names so they are not flagged as extra columns.

### 2.3 Update `pipdata` tests

File: `tests/testthat/test-arrow-prep.R`

Add a test that a `data.table` carrying, e.g., `lstatus` and `hsize`
(as raw integer-ish numerics) passes through `prepare_for_arrow()` and
that both columns appear in the output as `integer` with the correct
Arrow type.
