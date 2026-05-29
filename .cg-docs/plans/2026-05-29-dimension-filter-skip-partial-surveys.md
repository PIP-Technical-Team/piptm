---
date: 2026-05-29
title: "Dimension filter: skip partial-match surveys"
status: completed
completed-date: 2026-05-29
scope: Lightweight
estimated-effort: small
tags: [inline]
---

# Dimension Filter: Skip Partial-Match Surveys

## Objective

Change `table_maker()` so that surveys missing *any* requested breakdown
dimension are skipped entirely (not loaded), rather than loaded with the
missing dimension filled as NA.

## Steps

### 1. Update `table_maker()` dimension pre-filter (Step 3)

In `R/table_maker.R`, change the `partial_idx` branch:
- Old: warn that missing dims will be NA, keep survey
- New: include partial-match surveys in the dropped set (same warn+drop path
  as zero-overlap surveys), with a message that reflects "missing some
  requested dimensions"

### 2. Remove NA-fill loop (Step 6)

Delete or no-op the Step 6 `for (d in setdiff(by, names(dt)))` loop — it is
no longer needed since all surviving surveys now carry every requested
dimension.

### 3. Update `@return` doc

Remove the sentence "`NA` when the dimension is absent for a survey
(partial-match)" from the `@return` block.

### 4. Update tests

Rewrite the "fills missing dim with NA for partial match survey" test:
- Expect a warning (survey is *excluded*)
- Expect only the full-match survey to appear in results
- Add a test that *all* surveys being partial/zero yields an error
