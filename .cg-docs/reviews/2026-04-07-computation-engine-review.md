---
plan: .cg-docs/plans/2026-04-07-computation-engine.md
review-date: 2026-04-28
scope: compute_inequality.R (Step 4), compute_measures.R (Step 6), table_maker.R (Step 7)
findings:
  # --- 2026-04-28 Step 7 review (table_maker.R, thorough) ---
  s7-P0.1: false-positive
  s7-P1.1: fixed
  s7-P1.2: fixed
  s7-P2.1: fixed
  s7-P2.2: fixed
  s7-P2.3: false-positive
  s7-P3.1: open
  s7-P3.2: open
  # --- 2026-04-22 Step 6 review (compute_measures.R, thorough) ---
  # --- 2026-04-27 Step 6 light verification review ---
  s6v-P0.1: open
  s6v-P2.1: fixed
  s6-P0.1: fixed
  s6-P1.1: fixed
  s6-P1.2: fixed
  s6-P2.1: fixed
  s6-P2.2: fixed
  s6-P2.3: fixed
  s6-P2.4: fixed
  s6-P3.1: fixed
  s6-P3.2: fixed
  s6-P3.3: fixed
  s6-P3.4: fixed
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
# --- 2026-04-21 re-review (Step 4 readiness check, light) ---
  re-P2.1: open
  re-P2.2: open
  re-P2.3: open
  re-P3.1: open
  re-P3.2: open
  re-P3.3: open
# --- 2026-04-21 Step 5 review (compute_welfare.R, thorough) ---
  s5-P1.1: fixed
  s5-P2.1: fixed
  s5-P2.2: open
  s5-P2.3: open
  s5-P2.4: open
  s5-P3.1: open
  s5-P3.2: open
  s5-P3.3: open
  s5-P3.4: open
  s5-P3.5: open
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

---

## 2026-04-21 Re-review: Step 4 Readiness Check (light)

**Review depth**: light (explicit override)
**Scope**: Step 4 final state — `R/compute_inequality.R`, `tests/testthat/test-compute-inequality.R`
**Test result**: FAIL 0 | WARN 1 | SKIP 0 | PASS 34 ✅
**Verdict**: ✅ Step 4 is complete — proceed to Step 5

### P2 — IMPORTANT

- **[re-P2.1]** [cg-code-quality] `test-compute-inequality.R:1-2` — `library(testthat)` and `library(data.table)` called at file top-level outside any `test_that()` block.
  **Why**: Causes the `package 'data.table' was built under R version 4.5.3` warning visible in the test run. More broadly, explicit `library()` calls in test files are unnecessary — `devtools::test()` attaches the package under test, which brings all Imports into scope.
  **Fix**: Remove lines 1–2 (`library(testthat)` and `library(data.table)`).

- **[re-P2.2]** [cg-testing] `compute_inequality.R` / `test-compute-inequality.R` — `grp` is silently ignored when `by = NULL`, but this contract is not tested.
  **Why**: `compute_measures()` will pass a pre-computed GRP down. If `by` is ever NULL in that path, the GRP is silently dropped with no diagnostic. Unclear whether this is intentional.
  **Fix**: Add a test for this case, or add a `cli_warn()` in the function body.

- **[re-P2.3]** [cg-testing] `test-compute-inequality.R` — `mld: zero-welfare rows` test comment says "Negative MLD is expected" but only tests equality to `expected_mld`, not the sign.
  **Why**: A sign error in the implementation would still pass the test.
  **Fix**: Add `expect_lt(res[measure == "mld", value], 0)`.

### P3 — MINOR

- **[re-P3.1]** [cg-code-quality] `compute_inequality.R:95` — `grp$groups` accesses a collapse internal slot directly.
  **Fix**: Replace with `collapse::GRP_groups(grp)`.

- **[re-P3.2]** [cg-code-quality] `compute_inequality.R` — Comment "Subset to requested measures + melt to long" is misleading; subsetting is implicit via `measure.vars`.
  **Fix**: Update to "melt only the requested measure columns to long".

- **[re-P3.3]** [cg-code-quality] `compute_inequality.R:85` — `result <- grp_dt` is a data.table reference alias, not a copy; `:=` modifies in place.
  **Fix**: Add comment clarifying the reference semantics.

### ✅ Passed

- **Algorithmic correctness**: O(n) memory loop with `log(welfare)` precomputed outside the loop — correct and efficient. No cross-join.
- **GRP reuse**: Single `collapse::GRP()` call before the loop, reused across all `n_pl` iterations. Structurally correct.
- **Zero-welfare rule**: `logw = 0` for `welfare == 0` combined with `(poor & welfare_v > 0)` mask correctly prevents `+Inf`.
- **Numerical correctness**: All 32 existing tests pass. Hand-computed fixture values are verified.
- **Data isolation**: `dt` is not modified.
- **`fsum` for headcount/pop_poverty**: Computing `fsum(w * poor) / fsum(w)` is correct and `w_total` is already held.

---

## Step 6 Review — `compute_measures.R` (thorough, 2026-04-22)

**Review depth**: thorough (all 10 agents)
**Files reviewed**: `R/compute_measures.R`, `tests/testthat/test-compute-measures.R`
**Findings**: 11 (P0: 1, P1: 2, P2: 4, P3: 4)

Auto-escalation applied: files dispatch statistical family functions (`GRP`, `fsum` via family calls) → `@cg-data-quality` + `@cg-reproducibility` + `@cg-performance` (all already in thorough tier).

---

### P0 — BLOCKING

- **[s6-P0.1]** [cg-adversarial] `R/compute_measures.R:109` + `tests/test-compute-measures.R:90` — `poverty_line` column silently absent from output when no poverty measures requested; `@return` contract broken; covering test passes vacuously.
  **Why**: `rbindlist(results, fill=TRUE)` only creates `poverty_line` when at least one source table carries it. When only inequality/welfare families run, `poverty_line` never materialises. Test `expect_true(all(is.na(res$poverty_line)))` passes because `res$poverty_line` is `NULL`, `is.na(NULL)` is `logical(0)`, and `all(logical(0))` is `TRUE`. Confirmed in console: `all(is.na(NULL))` → `TRUE`.
  **Fix** (two parts):
  ```r
  # R/compute_measures.R — after rbindlist
  result <- data.table::rbindlist(results, fill = TRUE)
  if (!"poverty_line" %in% names(result)) result[, poverty_line := NA_real_]
  result
  ```
  ```r
  # test file — replace vacuous assertion
  expect_true("poverty_line" %in% names(res))
  expect_equal(sum(!is.na(res$poverty_line)), 0L)
  ```

---

### P1 — CRITICAL

- **[s6-P1.1]** [cg-data-quality] `R/compute_measures.R` (between steps 3 and 4) — `.validate_by(by)` is never called; `by` argument is entirely unvalidated at the orchestrator level.
  **Why**: `.classify_measures()` and `.validate_poverty_lines()` are both called, but `.validate_by()` is missing. Passing `by = c("gender", "area", "educat4", "educat5")` (two education columns, which is forbidden) silently proceeds to `GRP()`. Passing `by = c("invalid_dim")` proceeds with an opaque collapse error.
  **Fix**: Add after `.validate_poverty_lines()`:
  ```r
  .validate_by(by)
  ```

- **[s6-P1.2]** [cg-data-quality] `R/compute_measures.R:53` — No guard for missing `pip_id` (or `welfare`/`weight`) columns; missing `pip_id` produces a misleading "0 surveys" abort.
  **Why**: `dt[["pip_id"]]` returns `NULL` when absent. `uniqueN(NULL)` = `0L`, triggering "received data for 0 surveys" with `Found: NULL`. The real error — a missing required column — is hidden.
  **Fix**: Add before the uniqueN check:
  ```r
  required <- c("pip_id", "welfare", "weight")
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols)) {
    cli_abort(
      c("Required column{?s} missing from {.arg dt}: {.col {missing_cols}}."),
      call = NULL
    )
  }
  ```

---

### P2 — IMPORTANT

- **[s6-P2.1]** [cg-learnings-researcher] `tests/test-compute-measures.R:24` — `piptm:::.MEASURE_REGISTRY` accessed via `:::` when the exported `pip_measures()` already surfaces the same data.
  **Why**: `:::` couples the test to a private symbol. If `.MEASURE_REGISTRY` is renamed, refactored, or the package is byte-compiled with symbol hiding, tests break with a cryptic error.
  **Fix**: `ALL_MEASURES <- names(pip_measures())`

- **[s6-P2.2]** [cg-testing] `tests/test-compute-measures.R:57,62,68` — Row counts `18L`, `23L`, `13L` are hardcoded literals that will silently break if the registry grows.
  **Why**: The registry grew from 17 to 18 measures during Step 1 development; the plan's text still says "17". A future measure addition breaks three tests with no diagnostic.
  **Fix**: Drive from `ALL_MEASURES`:
  ```r
  n_all  <- length(ALL_MEASURES)              # 18 today, dynamic tomorrow
  n_pov  <- sum(pip_measures() == "poverty")  # 5
  n_ineq_welf <- n_all - n_pov               # 13
  expect_equal(nrow(res), n_all)
  expect_equal(nrow(res), n_all + n_pov)      # 2 PL: +5 extra poverty rows
  expect_equal(nrow(res), n_ineq_welf)
  ```

- **[s6-P2.3]** [cg-testing] `tests/test-compute-measures.R:90` — Vacuous `all(is.na(...))` assertion (independent of P0.1; the logical flaw persists even after the contract fix).
  **Why**: `all(is.na(x))` gives a single `TRUE`/`FALSE` with no diagnostic on failure. Standard pattern for "all NA" in testthat:
  **Fix**: `expect_equal(sum(!is.na(res$poverty_line)), 0L)` — fails with count information.

- **[s6-P2.4]** [cg-testing] `tests/test-compute-measures.R` — No numerical correctness test; all assertions are structural only.
  **Why**: `compute_measures()` is the integration layer. A dispatch bug (e.g., wrong `grp` passed, wrong `classified$family`) could produce wrong values that pass all structural tests. One cross-check against direct family function calls would catch this.
  **Fix**:
  ```r
  test_that("compute_measures welfare value matches compute_welfare directly", {
    dt  <- make_single_dt(10L)
    direct <- compute_welfare(dt, measures = "mean")$value
    via_cm <- compute_measures(dt, measures = "mean")$value
    expect_equal(via_cm, direct)
  })
  ```

---

### P3 — MINOR

- **[s6-P3.1]** [cg-code-quality] `R/compute_measures.R:54` — `unique(dt[["pip_id"]])` uses base R `unique()` in a data.table+collapse codebase; also not truncated — could print hundreds of pip_ids in an error message.
  **Fix**: `collapse::funique(dt[["pip_id"]])` or truncate: `head(unique(dt[["pip_id"]]), 5L)`.

- **[s6-P3.2]** [cg-documentation] `R/compute_measures.R:45` — Missing `@family compute` tag; all three exported family functions declare it but the orchestrator does not.
  **Fix**: Add `@family compute` below `@keywords internal`.

- **[s6-P3.3]** [cg-testing] `tests/test-compute-measures.R` — No test for duplicate measure names (e.g., `c("mean", "mean")`).
  **Fix**:
  ```r
  test_that("duplicate measure names produce no duplicate rows", {
    dt  <- make_single_dt(10L)
    res <- compute_measures(dt, measures = c("mean", "mean", "gini"))
    expect_equal(nrow(res), 2L)
  })
  ```

- **[s6-P3.4]** [cg-performance] `R/compute_measures.R:73` — `GRP(dt, by = by)` built on full `dt` (all columns) rather than on the `by` subset. Functionally identical (confirmed: `group.id` and `groups` are identical), but increases GRP object memory footprint unnecessarily for wide tables.
  **Fix** (advisory): `collapse::GRP(dt[, by, with = FALSE], by = by)`

---

### ✅ Passed

| Agent | Area |
|---|---|
| **cg-version-control** | No secrets, credentials, or new path gaps; `.Rbuildignore` already contains `.cg-docs/` |
| **cg-reproducibility** | Deterministic; no seeds required; no lockfile changes |
| **cg-performance** | GRP sharing across all three family dispatches correctly implemented and measured; `rbindlist` merge is idiomatic |
| **cg-architecture** | `compute_measures` correctly unexported; naming, dispatch pattern, and call-chain position are consistent with plan contract |
| **cg-code-quality** (overall) | Section headers, alignment, step comments, and NULL-guard patterns consistent with rest of package |

---

## Step 6 Light Verification — fix-triage correctness check (2026-04-27)

**Review depth**: light
**Files reviewed**: `R/compute_measures.R`, `tests/testthat/test-compute-measures.R`, `R/compute_welfare.R` (out-of-scope change detected)
**Test result before this review**: FAIL 0 | WARN 1 | SKIP 0 | PASS 30
**Test result after**: FAIL 0 | WARN 0 | SKIP 0 | PASS 30 ✅

### P0 — BLOCKING

- **[s6v-P0.1]** [cg-code-quality] `R/compute_welfare.R:72-77` — `"sum"` added to `all_welfare_measures` and dispatched via `fsum`, but `sum` is **absent from `.MEASURE_REGISTRY`**. Out-of-scope change not part of any s6 finding.
  **Why**: `pip_measures()` / `names(pip_measures())` / `ALL_MEASURES` do not include `"sum"`. Any call to `compute_measures(dt, measures="sum")` will error with "Unknown measure". The `@param measures` docstring also incorrectly says "all twelve" and adds `sum` to the mapping table without registry backing.
  **Fix — two options (requires your decision)**:
  - **Option A (add to registry)**: Add `sum = "welfare"` to `.MEASURE_REGISTRY` in `R/measures.R`, add a test in `test-compute-welfare.R`, update `pip_measures()` docs.
  - **Option B (revert)**: Remove `"sum"` from `all_welfare_measures`, remove the `fsum` dispatch line, and revert the docstring changes in `compute_welfare.R`.

### P2 — IMPORTANT

- **[s6v-P2.1]** [cg-testing] `tests/test-compute-measures.R` — Hardcoded `%in% c("gini","mld","mean",...)` list in the `n_all-n_pov` row-count test would silently under-select if registry grows. **Fixed** — replaced with `ALL_MEASURES[pip_measures()[ALL_MEASURES] != "poverty"]`.

### ✅ Passed

| Check | Result |
|---|---|
| All 11 s6 fixes implemented as specified | ✅ |
| `funique` in `@importFrom` | ✅ |
| `.validate_by(by)` call order | ✅ |
| `GRP(dt[, by, with=FALSE], by=by)` syntax | ✅ |
| `poverty_line` guarantee position (after `rbindlist`, before return) | ✅ |
| `n_all`/`n_pov` derivation from `pip_measures()` correct | ✅ |
| `n_all + n_pov` math for 2-PL all-measures test | ✅ |
| 3 numerical cross-checks isolated correctly | ✅ |

---

## Step 7 review � `table_maker.R` (2026-04-28, thorough)

**Scope**: `R/table_maker.R` � `pip_lookup()` + `table_maker()` + `%||%`
**Depth**: thorough
**Auto-escalation**: `@cg-data-quality`, `@cg-reproducibility` (statistical functions present)

### Findings

#### P0 � BLOCKING

- **[s7-P0.1] open** � [cg-code-quality] `table_maker.R:214`: bare `piptm_manifest` symbol at file top level outside any function body. Evaluated on `load_all()` / `library()`. Dead/stray code; delete line 214.

#### P1 � CRITICAL

- **[s7-P1.1] open** � [cg-data-quality] No guard when `load_surveys()` returns 0 rows. `lapply` iterates 0 times ? `rbindlist(list(), fill=TRUE)` returns a 0-column `data.table` ? `setcolorder` silently no-ops or errors. Add explicit `nrow(dt) == 0L` abort after Step 4.

- **[s7-P1.2] open** � [cg-adversarial] `sdt\[[1L]]` (and `surveyid_year`, `welfare_type`) called without asserting the columns exist on `dt`. If `load_surveys()` changes its contract the error surfaces inside `table_maker` with a misleading traceback. Add a `required_meta` column assertion after Step 4 load.

#### P2 � IMPORTANT

- **[s7-P2.1] open** � [cg-code-quality] `by_check` construction (`c(setdiff(by, "age"), "age")`) is a no-op: produces the same set as `by` with `"age"` reordered last. The manifest stores `"age"` (pre-binning name) already. Replace with `by_check <- by` and add an explanatory comment.

- **[s7-P2.2] open** � [cg-testing] No test coverage for `by = NULL` (aggregate mode). Step 6a `if (!is.null(by))` branch and Step 7 `dim_cols <- character(0L)` branch are untested.

- **[s7-P2.3] open** � [cg-code-quality] `.cg-docs/` not excluded from package build via `.Rbuildignore`.

#### P3 � MINOR

- **[s7-P3.1] open** � [cg-code-quality] `%||%` defined at bottom of file but used at top. Move to `utils-internal.R` or place above `pip_lookup`.

- **[s7-P3.2] open** � [cg-documentation] `@return` for `population` column does not note that for partial-match surveys, population reflects only non-NA cell counts.

### Passed (Step 7)

| Check | Result |
|---|---|
| Clean separation `pip_lookup` / `table_maker` / internals | pass |
| `.` prefix on all internal helpers | pass |
| No hardcoded paths; `release` defaults to `piptm_current_release()` | pass |
| Arrow partition pushdown before load | pass |
| Per-survey loop over `unique(dt\)` (keyed) | pass |
| `rbindlist(fill=TRUE)` for final bind | pass |
| Metadata attached after `compute_measures()` | pass |
| Loud failures � no silent fallbacks | pass |
