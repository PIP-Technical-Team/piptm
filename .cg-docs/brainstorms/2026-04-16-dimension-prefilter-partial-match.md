---
date: 2026-04-16
title: "Dimension Pre-Filter: Partial Match with NA Fill"
status: decided
scope: "Lightweight"
chosen-approach: "Per-survey NA fill in table_maker()"
tags: [computation, dimensions, table-maker, pre-filter]
---

# Dimension Pre-Filter: Partial Match with NA Fill

## Context

The computation engine plan (Step 7, `table_maker()`) originally specified
a strict dimension pre-filter: surveys missing **any** requested `by`
dimension are dropped entirely before loading. This was deemed too strict —
it silently excludes surveys that have partial dimension coverage, losing
data unnecessarily.

## Requirements

- User requests `by = c("gender", "area")`.
- Some surveys have both dimensions, some have only `gender`, some have only
  `area`.
- **Inclusion threshold**: Keep any survey that has **at least 1** of the
  requested dimensions. Only drop surveys with **zero** overlap.
- **Missing dimension handling**: For surveys missing a requested dimension,
  add the column as `NA` before computing. GRP naturally produces a single
  NA group for that dimension.
- **NA semantics**: `NA` means "dimension not collected in this survey."
  No sentinel value — plain `NA` is sufficient.
- **Output shape**: Rectangular. Every result row has every `by` column.
  Surveys with missing dimensions have `NA` in those columns.
- **Warning**: Emit `cli_warn()` listing which dimensions are missing for
  which surveys. One consolidated warning, not per-survey.

## Approaches Considered

### Approach 1: Per-survey NA fill in table_maker() (chosen)

After loading, in the per-survey loop, check which `by` columns are missing
from the survey slice and add them as `NA_character_` via `data.table::set()`.
Pass the full `by` vector to `compute_measures()` unchanged. The compute
layer is completely unaware of missing dimensions.

- **Pros**: Simple. Compute layer unchanged. Output always rectangular.
- **Cons**: Produces a single NA group per missing dimension (minor row
  inflation). Downstream must understand `NA` = "not collected."
- **Effort**: Small — changes only `table_maker()` step 4 + per-survey loop.

### Approach 2: Per-survey by reduction

Compute each survey with only its available dimensions, then pad results
with NA columns before `rbindlist()`.

- **Pros**: Only groups by real data.
- **Cons**: Each survey gets a different `by` vector. GRP differs per survey.
  Result padding is fiddly. More complex for negligible benefit.
- **Effort**: Medium.

## Decision

**Approach 1** — Per-survey NA fill. The NA group from a missing dimension
is a single extra cross-tab level, not a correctness issue. The compute
layer stays clean and unaware of dimension availability.

### Concrete flow

1. Check manifest `dimensions` for each entry against user's `by`.
2. Drop entries with **zero** overlap (not ≥1 overlap). Warn.
3. Load all remaining entries via `load_surveys()`.
4. In per-survey loop, before `compute_measures()`:
   ```r
   missing_dims <- setdiff(by, names(survey_dt))
   for (d in missing_dims) {
     data.table::set(survey_dt, j = d, value = NA_character_)
   }
   ```
5. Pass full `by` to `compute_measures()`. GRP groups by NA naturally.

## Next Steps

- Update `table_maker()` Step 7 in the computation engine plan to reflect
  partial-match inclusion and NA fill.
- Update related test cases in `test-table-maker.R` plan section.
- Update the "Missing dimension" risk row in the plan's risk table.
