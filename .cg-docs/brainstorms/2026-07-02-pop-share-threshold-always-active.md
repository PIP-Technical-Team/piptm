---
date: 2026-07-02
title: "Pop-share threshold as always-active suppression guard"
status: decided
scope: "Lightweight"
chosen-approach: "Always-active internal pop_share evaluation"
tags: [measures, pop_share, suppression, threshold, data-quality, table_maker]
---

# Pop-Share Threshold as Always-Active Suppression Guard

## Context

Follow-up to `2026-06-01-pop-share-threshold-suppression.md`.  That brainstorm
implemented `pop_share_threshold` as a post-processing step in `table_maker()`
Step 10.  However the implementation gated activation on
`"pop_share" %in% measures`, making the safeguard accidentally opt-in: callers
requesting only `"mean"` or `"gini"` by a dimension where one cell contains
0.2% of the survey receive unsuppressed results with no warning.

## Requirements

- `pop_share_threshold` activates whenever it is non-NULL, regardless of
  whether `"pop_share"` is in the requested `measures`.
- If `"pop_share"` is not requested, compute it internally in Step 10 using
  `compute_shares()` with the same `c("pip_id", by)` batch grouping — used
  only for threshold evaluation, never appended to the output.
- If `"pop_share"` is requested, include it in output as usual.
- Suppression behaviour (drop non-share measures for small cells, emit
  `cli_warn()`) is unchanged.
- ALL shares-family measures (`pop_share`, `target_within_group_share`,
  `target_survey_share`) are retained for below-threshold cells (fix to the
  previous `share_measures <- "pop_share"` hardcoding).
- Threshold validation is unconditional — not contingent on requested measures.
- `by = NULL` case is explicitly guarded: suppress block is skipped when
  `length(dim_cols) == 0L` (each cell is a full survey with pop_share = 1.0;
  threshold can never trigger).
- Only Step 10 of `table_maker()` changes.  `compute_shares()` and all other
  downstream functions are untouched.

## Bugs Fixed Alongside

| # | Bug |
|---|-----|
| 1 | Primary: threshold gated on `"pop_share" %in% measures` |
| 2 | Threshold validation skipped when `pop_share` not requested |
| 3 | `share_measures <- "pop_share"` — `target_within_group_share` and `target_survey_share` wrongly suppressed |
| 4 | `by = NULL` ran full suppression logic unnecessarily |

## Approaches Considered

### Approach 1: Always-active internal pop_share evaluation *(Chosen)*

Widen the outer guard to `!is.null(pop_share_threshold)`.  When
`"pop_share"` is not in `measures`, call `compute_shares(dt, by =
c("pip_id", dim_cols), measures = "pop_share", grp = GRP(dt, ...))` inside
Step 10 and use the result solely for cell identification.

**Pros**: Minimal blast radius (Step 10 only), no downstream changes, fixes
all four bugs as natural co-riders, negligible cost.

**Cons**: One extra GRP + fsum when threshold is active and pop_share not
requested — unavoidable and acceptable.

**Effort**: Small.

### Approach 2: Reject threshold if pop_share not requested

Keep the existing activation guard but move validation earlier and emit
`cli_abort()` if `pop_share_threshold` is set without `"pop_share"` in
`measures`.

**Pros**: No hidden computation.

**Cons**: Does not solve the primary problem — safeguard still opt-in by
accident.  Rejected.

## Decision

Approach 1.  Suppression is a data-quality policy that should be
unconditional at the API boundary.  The existing `compute_shares()` interface
handles internal pop_share computation cleanly.

## Next Steps

1. ~~Implement revised Step 10 in `table_maker.R`~~ (done in this session)
2. Update `@param pop_share_threshold` documentation to reflect new semantics
3. Add unit tests:
   - Threshold fires when `pop_share` NOT in measures; small cell suppressed
   - `target_within_group_share` retained for suppressed cells
   - Invalid threshold throws error even when `pop_share` not in measures
   - `by = NULL` — no suppression regardless of threshold value
