---
date: 2026-06-01
title: "Pop share threshold suppression"
status: active
scope: Lightweight
estimated-effort: small
tags: [inline]
---

# Pop Share Threshold Suppression

Based on brainstorm: `.cg-docs/brainstorms/2026-06-01-pop-share-threshold-suppression.md`

## Steps

### 1. Add `pop_share_threshold` parameter to `table_maker()`

Add parameter (default `0.01`, `NULL` to disable). Add input validation:
must be NULL or a single numeric in (0, 1).

**Acceptance**: `table_maker()` accepts the new parameter without breaking existing calls.

### 2. Implement suppression logic

After the result is assembled in `table_maker()`, if `"pop_share" %in% measures`
and threshold is non-NULL:
- Extract pop_share values per cell (keyed by pip_id + by columns)
- Identify cells where pop_share < threshold
- Drop rows for those cells where measure is NOT in c("pop_share", "obs_share")
- Emit `cli_warn()` listing suppressed cells

**Acceptance**: Below-threshold cells have non-share measures dropped; pop_share/obs_share rows retained.

### 3. Add unit tests

Test:
- Suppression drops correct rows when pop_share < threshold
- pop_share and obs_share rows are retained for suppressed cells
- Warning is emitted listing suppressed cells
- No suppression when pop_share not in measures
- No suppression when threshold is NULL
- Works with multi-survey batch

**Acceptance**: All tests pass.

### 4. Update API endpoint validation

Add `pop_share_threshold` to `validate_table_input()` in helpers.R and pass
through in plumber.R.

**Acceptance**: API accepts the parameter and passes it to table_maker().
