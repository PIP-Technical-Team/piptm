---
date: 2026-05-20
title: "Column pruning in load_surveys() for table_maker performance"
status: decided
scope: "Lightweight"
chosen-approach: "Add cols parameter to load_surveys()"
tags: [performance, arrow, load_surveys, table_maker, column-pruning]
---

# Column Pruning in load_surveys()

## Context

Benchmarking (50-iteration resample, 15 surveys over UNC share) showed that
loading only the 6 needed columns instead of all 14 reduces Arrow I/O time
from 1.658s to 0.524s — a **68% improvement**. Currently `load_surveys()`
always loads all columns and `table_maker()` uses only what it needs.

## Requirements

- `table_maker()` needs: `welfare`, `weight`, `pip_id`, plus whatever `by`
  dimensions are requested (e.g. `gender`, `area`, `educat4`).
- Column selection must happen *before* `collect()` to skip network bytes.
- PPP welfare column selection logic (already in `load_surveys()`) must
  interact correctly with column pruning.
- Backward compatible: existing callers that don't pass `cols` get current
  behavior (all columns).

## Approaches Considered

### Approach A: Add `cols` parameter to `load_surveys()`

Add an optional `cols` argument. When non-NULL, insert `dplyr::select()`
before `collect()`. `table_maker()` computes the needed columns from its
arguments and passes them through.

- **Pros**: Clean abstraction, all callers benefit, PPP logic stays
  co-located, backward compatible (`cols = NULL` loads everything).
- **Cons**: Minor complexity in coordinating `cols` with PPP column
  renaming (caller asks for `"welfare"` but the file has
  `welfare_ppp_2017_01_02`).
- **Effort**: Small (< 1 day)
- **Recommended**: Yes

### Approach B: table_maker() calls Arrow directly

Bypass `load_surveys()` and replicate path-building + Arrow query with
baked-in `select()`.

- **Pros**: Full control in `table_maker()`.
- **Cons**: Duplicates I/O logic, breaks layered design, maintenance risk.
- **Effort**: Medium
- **Recommended**: No

## Decision

**Approach A** — add a `cols` parameter to `load_surveys()`. The function
already encapsulates path resolution, PPP selection, and integrity checks.
Column pruning is a natural one-line extension (`select()` before
`collect()`).

## Next Steps

1. Add `cols` parameter to `load_surveys()` with default `NULL` (no change).
2. When `cols` is non-NULL, expand it to include any welfare columns needed
   for PPP resolution (so the PPP logic still works), then `select()` before
   `collect()`.
3. In `table_maker()`, compute `needed_cols` from `measures`/`by` args and
   pass to `load_surveys(..., cols = needed_cols)`.
4. Update tests to cover both `cols = NULL` and explicit column subsets.
5. Verify benchmark improvement matches prior results (~68% I/O reduction).
