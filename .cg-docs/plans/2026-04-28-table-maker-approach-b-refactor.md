---
date: 2026-04-28
title: "Refactor table_maker() to Approach B — Grouped Collapse Orchestration"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-04-28-orchestration-benchmark.md"
language: "R"
estimated-effort: "medium"
tags: [performance, orchestration, table-maker, collapse, refactor]
---

# Plan: Refactor table_maker() to Approach B — Grouped Collapse Orchestration

## Objective

Replace the per-survey `lapply()` loop in `table_maker()` (Steps 6–7) with
Approach B's grouped collapse strategy: build a single compound
`GRP(c("pip_id", by))` across the full batch and dispatch directly to family
functions. This eliminates 15× repeated GRP construction, dispatch overhead,
and thread-lock acquisition, yielding a 72% speedup (0.12s vs 0.43s) as
demonstrated by the orchestration benchmark.

## Context

**What exists today:**

- `table_maker()` Steps 0–5 are unchanged (resolve IDs → validate → manifest
  lookup → dimension pre-filter → load → age binning). Step 6 splits by
  `pip_id` via `lapply()`, calls `compute_measures()` per slice, then
  `rbindlist()` in Step 7.
- `compute_measures()` enforces a **single-survey guard**
  (`uniqueN(pip_id) == 1L`) and pre-computes `GRP(dt, by)` for a single
  survey, sharing it across family functions.
- Family functions (`compute_poverty()`, `compute_inequality()`,
  `compute_welfare()`) already accept optional `grp` argument and are
  naturally multi-group capable — they do not assume single-survey data.
- The benchmark (`benchmarks/orchestration-strategy.R`) proved Approach B
  produces **identical results** to Approach A (verified via `all.equal()`
  with tolerance 1e-10).

**Benchmark results** (15 surveys, 336K rows, 4 measures, 3 poverty lines,
3 dimensions):

| Approach | nthreads=1 | nthreads=4 | Memory |
|----------|-----------|-----------|--------|
| A (current) | 0.43s | 0.36s | 164 MB |
| B (grouped) | 0.13s | 0.12s | 123 MB |

**Why B is faster:** The arithmetic in each family function is identical.
The difference is fixed overhead paid per `compute_measures()` call: GRP
construction, input validation/dispatch, and thread-lock acquisition. A pays
this 15× (once per survey); B pays it once.

**Key design constraints from the benchmark prototype:**

1. `compute_poverty()` **cannot reuse** the shared compound GRP — its internal
   cross-join with poverty lines changes the row count, invalidating the GRP.
   It builds its own GRP on `c("pip_id", by)` internally. This is already how
   the benchmark prototype works.
2. `compute_inequality()` and `compute_welfare()` **can and should** receive
   the pre-computed compound GRP. The benchmark confirmed this works correctly
   (Gini's per-group sort uses `grp$group.id` which already encodes
   `pip_id × by_dims`).
3. Metadata (`country_code`, `surveyid_year`, `welfare_type`) is attached via
   a keyed join from a `pip_id → metadata` lookup table, not scalar assignment.
4. Batch-level NA-fill for missing dimension columns happens **before** GRP
   construction, not per-survey.

## Requirements

| ID  | Requirement | Source |
|-----|-------------|--------|
| R1  | Remove per-survey `lapply()` loop from `table_maker()` | user |
| R2  | Adopt compound `GRP(c("pip_id", by))` strategy from benchmark Approach B | benchmark |
| R3  | Batch-level NA-fill for missing dimension columns before GRP | benchmark prototype |
| R4  | Metadata attachment via keyed join (not per-survey scalar assignment) | benchmark prototype |
| R5  | `compute_measures()` must accept multi-survey data (lift single-survey guard) | R2 implies |
| R6  | `compute_poverty()` builds its own GRP internally (no shared GRP for poverty) | benchmark finding |
| R7  | All existing tests must continue to pass | correctness |
| R8  | Set `collapse::set_collapse(nthreads = 4L)` as the default in `.onLoad()` | benchmark finding |
| R9  | Result must be numerically identical to current implementation | correctness |

## Implementation Steps

### Step 1: Lift the single-survey guard in `compute_measures()`

- **Requirements**: R5
- **Files**: `R/compute_measures.R` (modify)
- **Details**:
  1. Remove the `uniqueN(dt$pip_id) == 1L` assertion (lines ~67–78).
  2. Change GRP construction from `GRP(dt[, by, with = FALSE], by = by)` to
     `GRP(dt, by = c("pip_id", by))` when `by` is non-NULL. When `by` is
     NULL, build `GRP(dt, by = "pip_id")` — each survey is its own group.
  3. Pass the compound GRP to `compute_inequality()` and `compute_welfare()`.
  4. For `compute_poverty()`: pass `by = c("pip_id", by)` but do **not**
     pass the shared `grp` — poverty builds its own GRP after the cross-join.
  5. Keep the `"pip_id"` guard but change it to: `assert pip_id column exists`
     (not uniqueN == 1).
  6. Update the roxygen documentation to reflect multi-survey capability.
     Remove the "must never receive multi-survey data" warning. Document that
     `pip_id` is included in the grouping structure.
- **Test Scenarios**:
  - ✅ Single-survey call still works (backward compatible)
  - ✅ Multi-survey call produces correct grouped results
  - 🛑 Missing `pip_id` column → informative error
  - ❌ Empty `dt` → graceful handling
- **Tests**: Update `tests/testthat/test-compute-measures.R`:
  - Remove/update the "multi-survey guard" test
  - Add: multi-survey input with 3 surveys produces correct per-survey results
  - Add: multi-survey with `by = NULL` (aggregate per survey, not pooled)
  - Add: verify output identical to per-survey `lapply()` approach
- **Acceptance criteria**: `compute_measures()` accepts multi-survey data and
  produces results identical to calling it per-survey via `lapply()`.

### Step 2: Refactor `table_maker()` Steps 6–7

- **Requirements**: R1, R2, R3, R4
- **Files**: `R/table_maker.R` (modify)
- **Details**:
  1. **Replace Step 6 (per-survey compute)** with batch compute:
     ```r
     # ── 6. Batch-level NA-fill for missing dimension columns ──────────
     if (!is.null(by)) {
       for (d in setdiff(by, names(dt))) {
         data.table::set(dt, j = d, value = NA_character_)
       }
     }

     # ── 7. Batch compute ─────────────────────────────────────────────
     result <- compute_measures(dt, measures, poverty_lines, by)
     ```
  2. **Replace Step 7 (rbindlist + metadata)** with a keyed join:
     ```r
     # ── 8. Attach survey metadata ────────────────────────────────────
     meta <- unique(dt[, .(pip_id, country_code, surveyid_year, welfare_type)])
     result <- meta[result, on = "pip_id"]
     ```
  3. **Keep Steps 0–5 unchanged** (resolve IDs, validate, manifest lookup,
     dimension pre-filter, load, age binning).
  4. **Keep column reordering** (current Step 7 → renumbered Step 9) as-is.
  5. Step numbering in comments should be updated to reflect the new flow:
     - Steps 0–5: unchanged
     - Step 6: batch NA-fill (was per-survey in the loop)
     - Step 7: `compute_measures()` (single call, not lapply)
     - Step 8: metadata join
     - Step 9: column reorder (was Step 7)
- **Test Scenarios**:
  - ✅ End-to-end with fixture data: identical output to current implementation
  - ✅ Surveys with partial dimension overlap: NA-filled correctly
  - ✅ Single survey input: still works
  - 🛑 All surveys missing a dimension: NA group present in output
  - ❌ Empty load result: error message unchanged
- **Tests**: Update `tests/testthat/test-table-maker.R`:
  - Existing tests should pass without modification (same output contract)
  - Add: verify multi-survey batch produces same result as sequential calls
- **Acceptance criteria**: `table_maker()` produces numerically identical
  output to the current implementation. The `lapply()` loop is removed.

### Step 3: Set default `nthreads` in `.onLoad()`

- **Requirements**: R8
- **Files**: `R/zzz.R` (modify)
- **Details**:
  1. In `.onLoad()`, after manifest scanning, add:
     ```r
     # Set collapse threading — benchmark showed optimal at nthreads = 4
     # for typical 15-survey workloads. Falls back silently if OpenMP
     # unavailable.
     tryCatch(
       collapse::set_collapse(nthreads = 4L),
       error = function(e) NULL
     )
     ```
  2. Cap at physical core count if available:
     ```r
     n <- min(4L, max(1L, parallel::detectCores(logical = FALSE)))
     ```
  3. Document this in the package-level documentation or a comment in `zzz.R`.
- **Test Scenarios**:
  - ✅ Package loads without error when OpenMP available
  - ✅ Package loads without error when OpenMP unavailable
  - 🛑 System with 2 cores → nthreads = 2 (not 4)
- **Tests**: No formal test needed (`.onLoad()` side effect).
  Verify manually that `collapse::get_collapse("nthreads")` returns expected
  value after `devtools::load_all()`.
- **Acceptance criteria**: `nthreads` is set automatically on package load.

### Step 4: Update computation engine plan Step 8

- **Requirements**: Documentation
- **Files**: `.cg-docs/plans/2026-04-07-computation-engine.md` (modify)
- **Details**:
  1. Update Step 8 (Performance Optimization and Benchmarking) to note:
     - The orchestration benchmark has been completed
     - Approach B was selected (72% faster, identical correctness)
     - Link to this plan for the implementation details
     - Link to `benchmarks/orchestration-strategy.R` and the results document
  2. Update the "Critical data flow boundary" section in Context to remove
     the statement that `compute_measures()` "always operates on a
     single-survey slice" — it now operates on multi-survey batches.
- **Acceptance criteria**: Plan accurately reflects the current state.

### Step 5: Regression validation

- **Requirements**: R7, R9
- **Files**: All test files
- **Details**:
  1. Run the full test suite: `devtools::test()`.
  2. Run `devtools::check()` — ensure no new warnings or errors.
  3. Run the benchmark script on real data to confirm the speedup is
     preserved after refactoring: `source("benchmarks/orchestration-strategy.R")`.
  4. Verify the A-vs-B comparison is now moot (both paths produce the same
     code) — the benchmark can be kept as a historical artifact.
- **Test Scenarios**:
  - ✅ All 53+ existing tests pass
  - ✅ `R CMD check` passes cleanly
  - ✅ Benchmark timing for the refactored `table_maker()` matches Approach B
    timings (0.12–0.13s for 15 surveys)
- **Acceptance criteria**: Zero test regressions. Performance matches benchmark.

## Testing Strategy

### Correctness Approach

The primary correctness guarantee is **regression testing**: the refactored
`table_maker()` must produce byte-identical output to the current
implementation for any given input. This is verified by:

1. Existing unit tests (which test the full pipeline end-to-end)
2. A new explicit regression test: call `table_maker()` with a fixture,
   compare to a saved reference result via `all.equal()`
3. The benchmark's `all.equal()` pre-check (already passing for Approach B)

### Key Edge Cases

- Single survey input (degenerate batch of 1)
- Survey with all dimension columns missing → full NA cross-tab
- Survey with zero rows after loading (should error in Step 4, before compute)
- Mixed welfare types across surveys in the same batch
- `by = NULL` (aggregate mode, no disaggregation) — compound GRP is just
  `GRP(dt, by = "pip_id")`

## Documentation Checklist

- [ ] `compute_measures()` roxygen updated (multi-survey capable)
- [ ] `table_maker()` internal comments updated (new step numbering)
- [ ] `.onLoad()` nthreads setting commented
- [ ] Computation engine plan Step 8 updated with links
- [ ] `compound-gpid.md` Current Focus updated if needed

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `compute_poverty()` correctness with compound GRP | Low | Wrong poverty results | Already validated in benchmark. `compute_poverty()` builds its own GRP internally — no change to its logic. |
| Existing tests assume single-survey `compute_measures()` | Medium | Test failures | Step 1 explicitly updates tests. Single-survey case is a valid degenerate batch. |
| `.onLoad()` nthreads conflicts with user's global collapse settings | Low | Unexpected threading | Cap at physical cores. Document the setting. Users can override with `set_collapse(nthreads = n)`. |
| Memory regression from batch GRP on very large batches (60 surveys, 14M rows) | Low | Higher peak memory | Benchmark showed B uses **less** memory (123 MB vs 164 MB for 15 surveys). GRP object is smaller than 15 separate GRP objects + 15 intermediate copies. |

## Out of Scope

- Modifying `load_surveys()` or the I/O layer
- Parallel processing across surveys (`future`, `parallel`)
- Changing the output schema or column order
- Modifying family function internals (`compute_poverty`, etc.)
- CRAN-compatible performance tests
- Removing the benchmark script (kept as historical artifact)
