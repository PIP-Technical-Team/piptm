---
date: 2026-06-01
title: "Pop share threshold suppression for small cells"
status: decided
scope: "Lightweight"
chosen-approach: "Post-processing step in table_maker()"
tags: [measures, pop_share, suppression, threshold, data-quality]
---

# Pop Share Threshold Suppression for Small Cells

## Context

After adding `pop_share` and `obs_share` measures, we need a configurable
suppression rule: if a cell's population share falls below a threshold, all
other measures (except `pop_share` and `obs_share`) for that cell should be
dropped from the output, with a warning explaining what was suppressed.

## Requirements

- `pop_share_threshold` parameter on `table_maker()` (default `0.01`, `NULL` to disable)
- Suppression only triggers when `"pop_share"` is in the requested `measures`
- Below-threshold cells: drop rows for all measures EXCEPT `pop_share` and `obs_share`
- `pop_share` and `obs_share` rows are always retained (user can see the distribution)
- Emit a `cli_warn()` listing suppressed cells (pip_id + dimension values + pop_share value)
- If `pop_share` is not requested, suppression does not apply regardless of threshold

## Approaches Considered

### Approach 1: Post-processing step in table_maker() *(Chosen)*

After `compute_measures()` returns the full long-format result, apply suppression
as a final filter. Identify below-threshold cells via the `pop_share` rows,
then anti-join to drop non-share measures for those cells.

**Pros**: Zero impact on computation engine internals. Easy to test. Easy to
disable. Clean separation — suppression is a policy concern, not a computation
concern.

**Cons**: Computes measures that are then discarded (negligible cost for tiny cells).

**Effort**: Small (~1 hour).

### Approach 2: Early suppression inside compute_measures()

Identify below-threshold cells before dispatching to family functions and
exclude them from computation.

**Pros**: Avoids computing measures for suppressed cells.

**Cons**: Requires pop_share to be computed first (breaks parallel family dispatch).
Adds conditional logic to orchestrator. Negligible performance savings. Couples
policy into the engine.

**Effort**: Medium.

## Decision

Approach 1 — post-processing in `table_maker()`. Suppression is a display/policy
concern that belongs at the API boundary, not inside the computation engine.

## Next Steps

1. Add `pop_share_threshold` parameter to `table_maker()` (default `0.01`, `NULL` disables)
2. After result assembly, if `"pop_share" %in% measures && !is.null(pop_share_threshold)`:
   - Extract pop_share values per cell (keyed by pip_id + by columns)
   - Identify cells below threshold
   - Drop rows where `measure` not in `c("pop_share", "obs_share")` for those cells
   - Emit `cli_warn()` with details
3. Add unit tests
4. Update `validate_table_input()` in the API to accept/validate the threshold param
