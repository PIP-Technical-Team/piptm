---
plan: .cg-docs/plans/2026-04-07-computation-engine.md
review-date: 2026-04-20
scope: compute_inequality.R (Step 4)
findings:
  P0.1: fixed
  P1.1: fixed
  P1.2: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: skipped
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: fixed
  P3.1: fixed
  P3.2: skipped
  P3.3: fixed
  P3.4: skipped
# --- previous findings (earlier review) ---
  prev-P1.1: resolved
  prev-P2.1: resolved
  prev-P2.2: resolved
  P2.3: resolved
  P3.1: deferred
  P3.2: resolved
---

## Review Report

**Review depth**: targeted (performance + correctness focus on `compute_poverty.R`)
**Files reviewed**: `R/compute_poverty.R`, `tests/testthat/test-compute-poverty.R`
**Findings**: 0 P0, 1 P1, 3 P2, 2 P3

Auto-escalation applied: file calls `fsum` (statistical function) → `@cg-data-quality` + `@cg-reproducibility` added.

---

### P1 — CRITICAL

- **[P1.1]** `[data-quality]` `compute_poverty.R:115–118` — `fsum(w * poor, ...)` allocates a **second** temporary `n`-length vector for `w * poor` when computing `pop_poverty`, duplicating the allocation already made for `headcount`
  **Why**: `hc` and `pp` both call `collapse::fsum(w * poor, g = grp)`. The product `w * poor` is computed twice — two full `n`-length multiplications. With 100K rows and 5 poverty lines, that is 10 redundant multiplications. The product can be stored once.
  **Fix**:
  ```r
  w_poor <- w * poor   # computed once
  hc <- collapse::fsum(w_poor, g = grp) / w_total
  # ...
  pp <- collapse::fsum(w_poor, g = grp)
  ```
  Drop the separate `collapse::fsum(w * poor, ...)` call for `pp`.

---

### P2 — IMPORTANT

- **[P2.1]** `[performance]` `compute_poverty.R:119–121` — `(poor & welfare_v > 0)` re-evaluated on every loop iteration; `welfare_v > 0` is loop-invariant
  **Why**: `welfare_v > 0` doesn't change across poverty lines. Re-computing it each iteration is an n-length logical comparison per poverty line — entirely avoidable.
  **Fix**: Hoist before the loop:
  ```r
  pos <- welfare_v > 0   # computed once, before the loop
  # Inside loop:
  watts_contrib <- (poor & pos) * (log(z) - logw)
  ```

- **[P2.2]** `[performance]` `compute_poverty.R:125–134` — `as.data.table(grp$groups)` called inside the loop on every iteration
  **Why**: `grp$groups` is constant across all poverty lines. Calling `as.data.table()` on it `n_pl` times allocates `n_pl` identical data.tables. For `n_pl = 10` with 4 dimension groups, that is 10 unnecessary allocations.
  **Fix**: Extract once before the loop:
  ```r
  grp_dt <- if (!is.null(grp)) as.data.table(grp$groups) else NULL
  # Inside loop:
  results[[i]] <- if (!is.null(grp_dt)) cbind(grp_dt, pl_row) else pl_row
  ```

- **[P2.3]** `[performance]` `compute_poverty.R:88–92` — full n×2 working copy of `dt` allocated even when `by = NULL`
  **Why**: When `by = NULL`, `cols_needed = c("welfare", "weight")` and the copy is only used to extract two vectors. A ~8 MB copy for a 500K-row survey that is immediately discarded.
  **Fix**:
  ```r
  welfare_v <- dt[["welfare"]]
  w         <- dt[["weight"]]
  if (!is.null(by)) work <- dt[, c("welfare", "weight", by), with = FALSE]
  ```
  When `by = NULL` no copy is needed at all.

---

### P3 — MINOR

- **[P3.1]** `[code-quality]` `compute_poverty.R:108` — `gap <- poor * (z - welfare_v) / z` computes `z - welfare_v` for all n rows including non-poor (immediately zeroed)
  **Why**: Minor vectorisation overhead. For low poverty rates, most arithmetic is discarded. Only worth addressing if profiling confirms it is a hot path.
  **Note**: Leave as-is unless benchmarks show >5% contribution from this line.

- **[P3.2]** `[testing]` `test-compute-poverty.R` — `grp` parameter (caller-supplied GRP path) is not tested
  **Why**: The GRP-sharing feature is a key performance contract. If a mismatched GRP produces wrong output, no test catches it.
  **Fix**: Add two tests — one verifying pre-built GRP gives identical results to internally-built GRP, one verifying `grp` is silently ignored when `by = NULL`.

---

### ✅ Passed

- **Algorithmic correctness**: O(n) memory loop with `log(welfare)` precomputed outside the loop — correct and efficient. No cross-join.
- **GRP reuse**: Single `collapse::GRP()` call before the loop, reused across all `n_pl` iterations. Structurally correct.
- **Zero-welfare rule**: `logw = 0` for `welfare == 0` combined with `(poor & welfare_v > 0)` mask correctly prevents `+Inf`.
- **Numerical correctness**: All 32 existing tests pass. Hand-computed fixture values are verified.
- **Data isolation**: `dt` is not modified.
- **`fsum` for headcount/pop_poverty**: Computing `fsum(w * poor) / fsum(w)` is correct and `w_total` is already held.
