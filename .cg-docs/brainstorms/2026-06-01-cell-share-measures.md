---
date: 2026-06-01
title: "Cell observation share and population share measures"
status: decided
scope: "Lightweight"
chosen-approach: "Add to welfare family in compute_welfare()"
tags: [measures, welfare, shares, compute-engine]
---

# Cell Observation Share and Population Share Measures

## Context

Need to add two new measures to the computation engine:
- `obs_share` — proportion of total survey observations in each cross-tabulated cell
- `pop_share` — proportion of total weighted survey population in each cell

## Requirements

- Denominator is the per-survey total (not batch-wide)
- `obs_share` = `nobs_cell / nobs_survey` (unweighted row count ratio)
- `pop_share` = `population_cell / population_survey` (weighted population ratio)
- When `by = NULL`, both return 1.0 (no special-casing needed)
- Must reuse the existing GRP infrastructure for efficiency

## Approaches Considered

### Approach 1: Add to welfare family in `compute_welfare()` *(Chosen)*

Register in `.MEASURE_REGISTRY` under `"welfare"` family. Compute inside
`compute_welfare()` using the existing `grp` object. Survey-level denominator
is one additional `fsum`/`fnobs` call with a coarser `pip_id`-only grouping,
then broadcast back to cells.

**Pros**: Reuses existing GRP, melt-to-long, test infrastructure. Zero new
dependencies. O(n) single-pass. Minimal code change.

**Cons**: Slightly overloads "welfare" family semantically — shares aren't
welfare statistics per se. Minor naming quibble only.

**Effort**: Small (~1–2 hours including tests).

### Approach 2: New `"share"` computation family

New `compute_shares()` function with its own dispatch branch.

**Pros**: Cleaner conceptual separation.
**Cons**: Over-engineered for two simple ratios. More boilerplate.
**Effort**: Small-medium.

### Approach 3: Post-hoc in `table_maker()`

Compute shares after `compute_measures()` returns.

**Pros**: Doesn't touch engine internals.
**Cons**: Breaks measure-registry contract. Harder to test.
**Effort**: Small.

## Decision

Approach 1 — add to welfare family. Minimal disruption, follows the
established pattern exactly, negligible performance cost.

## Next Steps

1. Add `obs_share` and `pop_share` to `.MEASURE_REGISTRY` (welfare family)
2. Add computation logic in `compute_welfare()` — coarser `pip_id`-only GRP for denominators, then divide
3. Update `all_welfare_measures` vector in `compute_welfare()`
4. Add unit tests
5. Update `pip_measures()` docstring (count will change from 19 to 21)
