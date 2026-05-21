---
date: 2026-05-21
title: "Remove redundant welfare sort in compute_inequality()"
status: decided
scope: "Lightweight"
chosen-approach: "Remove sort, rely on upstream contract"
tags: [performance, gini, sorting, contract, arrow]
---

# Remove Redundant Welfare Sort in compute_inequality()

## Context

`compute_inequality()` sorts data by welfare within each group before computing
Gini. However, the upstream data pipeline guarantees welfare-sorted row order:

1. `pipload::load_pip_deflated_data()` returns data sorted by welfare.
2. `prepare_for_arrow()` in {pipdata} performs only by-reference column
   mutations — no reordering.
3. `write_survey_parquet()` writes rows in order; Arrow preserves row order.
4. `load_surveys()` in {piptm} reads via `arrow::open_dataset()` +
   `dplyr::collect()` — row order within each Parquet file is preserved.
5. `data.table`'s `[, ..., by=]` passes each group's rows in their original
   row order — a subsequence of a sorted sequence is still sorted.

Empirically verified: Gini results are identical (difference = 0) with or
without `setorder()` on 10,000-row datasets with 16 groups. Unsorted data
produces errors up to 0.39 in Gini, confirming the sort matters — but the
upstream contract guarantees it.

## Requirements

- Remove `setorder(work_g, .grp_id, welfare)` from the `by != NULL` Gini path.
- Remove `ord <- order(welfare_v)` from the `by == NULL` Gini path.
- Document the pre-sort contract in function documentation.
- Existing tests must continue to pass (they use pre-sorted inputs).

## Approaches Considered

### Approach 1: Remove sort, rely on upstream contract (Chosen)

Remove the sort entirely. Document the contract (welfare must be pre-sorted)
in the function's `@param dt` documentation and in a comment.

**Pros**: Eliminates O(n log n) sort per group per call — meaningful for large
batch runs.
**Cons**: Silent wrong results if contract is violated upstream.
**Effort**: Small.

### Approach 2: Keep sort as defensive safety net

Leave code unchanged.

**Pros**: Bulletproof regardless of upstream changes.
**Cons**: Redundant work on every call; sort cost accumulates across surveys.
**Effort**: None.

## Decision

Approach 1 — remove sort, rely on contract. The pipeline is fully controlled
and the sort contract is validated end-to-end (pipload → pipdata → piptm).
The performance benefit across hundreds of surveys justifies the removal.

## Next Steps

1. Remove `setorder()` and `order()` calls in `compute_inequality()`.
2. Add contract documentation to the `@param dt` roxygen.
3. Run full test suite to confirm no regressions.
