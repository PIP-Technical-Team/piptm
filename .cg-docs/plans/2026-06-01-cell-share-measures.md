---
date: 2026-06-01
title: "Add obs_share and pop_share measures"
status: completed
completed-date: 2026-06-01
scope: Lightweight
estimated-effort: small
tags: [inline, measures, welfare]
---

# Add Cell Observation Share and Population Share Measures

Based on brainstorm: `.cg-docs/brainstorms/2026-06-01-cell-share-measures.md`

## Requirements

- `obs_share` = unweighted row count of cell / unweighted row count of survey
- `pop_share` = weighted population of cell / weighted population of survey
- Both registered in `.MEASURE_REGISTRY` under `"welfare"` family
- Computed inside `compute_welfare()` using existing GRP + a coarser pip_id-only GRP for denominators
- When `by = NULL`, both trivially return 1.0

## Steps

### 1. Register measures in `.MEASURE_REGISTRY`

Add `obs_share = "welfare"` and `pop_share = "welfare"` to the registry in `R/measures.R`.
Update `pip_measures()` docstring (length 19 → 21).

**Acceptance**: `pip_measures()` returns 21 entries including `obs_share` and `pop_share`.

### 2. Implement computation in `compute_welfare()`

Add `obs_share` and `pop_share` to `all_welfare_measures` vector.
Compute survey-level denominators using a coarser grouping on `pip_id` only (when batch_by includes pip_id) or no grouping (single survey).
Divide cell-level values by survey totals, broadcasting via GRP group indices.

**Acceptance**: `compute_welfare(dt, by = "gender", measures = c("obs_share", "pop_share"))` returns correct proportions summing to 1.0 within each survey.

### 3. Add unit tests

Add tests in the existing test file for `compute_welfare`. Verify:
- Shares sum to 1.0 per survey when grouped
- Shares equal 1.0 when `by = NULL`
- Correct values for a known fixture
- Works in multi-survey batch via `compute_measures()`

**Acceptance**: All new tests pass.

### 4. Update documentation

Regenerate roxygen docs (`devtools::document()`). Ensure `compute_welfare` Rd reflects new measures.

**Acceptance**: `devtools::document()` runs without error.
