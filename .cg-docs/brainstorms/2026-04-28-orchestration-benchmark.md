---
date: 2026-04-28
title: "Orchestration Strategy Benchmark — Per-Slice vs Grouped vs nthreads"
status: decided
scope: "Standard"
chosen-approach: "Benchmark-driven selection (A → D → E → B → C)"
tags: [performance, benchmarking, orchestration, table-maker, collapse, nthreads]
---

# Orchestration Strategy Benchmark

## Context

Step 8 of the computation engine plan (`2026-04-07-computation-engine.md`)
proposes a per-slice vs grouped benchmark but is overengineered — it requires
building a full alternative prototype just for comparison, uses an arbitrary
2× decision threshold, and may be answering the wrong question if I/O
dominates runtime.

This brainstorm refines Step 8 into a practical, incremental benchmark that:

1. Profiles I/O vs compute separately (to identify the real bottleneck)
2. Tests approaches in order of increasing implementation effort
3. Includes `collapse::nthreads` as an orthogonal "free" optimization axis
4. Uses a clear decision rule tied to the 3-second wall-clock target

**Current implementation**: `table_maker()` loads all surveys via
`load_surveys()` into a single `data.table`, splits by `pip_id` using
`unique() + lapply()`, calls `compute_measures()` per slice, then
`rbindlist()` at the end. GRP sharing across family functions is already
implemented.

## Requirements

- **Target**: ≤3 seconds wall-clock for the full pipeline (load → compute →
  return final `data.table`) for 15 surveys
- **Typical workload**: 15 surveys, ~50K–250K rows each (heavy right tail up
  to ~784K), 3 poverty lines, up to 4 dimensions
- **Deliverable**: A standalone benchmark script (`benchmarks/orchestration-strategy.R`)
  that produces a results table and/or graph showing per-step and total
  timings for each approach
- **Scope**: Single-threaded orchestration strategies only (no `future` /
  `parallel`). `collapse::nthreads` is in scope as an orthogonal axis.
  I/O layer (`load_surveys()`) is measured but not modified.
- **Decision rule**:
  1. If any approach meets ≤3s end-to-end: pick the simplest one that does
  2. If multiple meet it: pick the simplest unless a more complex one is ≥40%
     faster
  3. If none meets it: the bottleneck is I/O, and orchestration optimization
     is the wrong lever — pivot to I/O optimization

## Approaches Considered

### Approach A: Per-Slice `lapply()` (Current Baseline)

The existing implementation — `unique(dt$pip_id)` + `lapply()` + `dt[pip_id == pid]`
subsetting + `compute_measures()` per slice + `rbindlist()`.

**Pros**:
- Already implemented and tested (53 passing tests)
- Simple NA-fill logic per survey (missing dims handled individually)
- Each slice is independent — easy to reason about and debug
- Trivially parallelizable later with `future_lapply()`

**Cons**:
- `dt[pip_id == pid]` subsetting repeats for every survey (copies data)
- `GRP()` called N times (once per survey)
- `rbindlist()` assembles N result tables at the end

### Approach B: Grouped `collapse` — Single `GRP(c("pip_id", by))`

Skip `lapply()` entirely. Build a compound `GRP(dt, by = c("pip_id", by))`
across the full batch and pass it to family functions.

**Pros**:
- Single `GRP()` call; no subsetting overhead; no `rbindlist()` of N results

**Cons**:
- Dimension NA-fill must happen batch-level before grouping
- Poverty cross-join inflates the entire batch (e.g. 3.6M × 3 = 10.8M rows)
- Gini's per-group sort operates over compound grouping — more complex
- `compute_poverty()` builds its own GRP on the cross-joined table (includes
  `poverty_line` in grouping), so the shared `grp` from the caller **cannot**
  be reused for poverty anyway
- Metadata attachment requires a join back from manifest

### Approach C: Pure `data.table` Grouped — `dt[, .(...), by = pip_id]`

Use data.table's native `by` grouping instead of `lapply()`.

**Pros**:
- Idiomatic data.table; leverages internal C-level grouping

**Cons**:
- `.SD` creates a copy per group — potentially slower than explicit subsetting
- GForce won't help (we call {collapse} functions, not data.table-native ones)
- Same cross-join and Gini complications as Approach B
- `compute_measures()` returns variable-length results per group

### Approach D: `collapse::nthreads` (Orthogonal Axis)

`set_collapse(nthreads = N)` before the compute loop. Combinable with any of
A/B/C/E.

**Pros**:
- Zero code changes — one-line configuration
- Accelerates `fsum`, `fmean`, `fmedian`, `fnth`, `fsd`, `fvar` at C level
- Benefits scale with data size

**Cons**:
- Only helps OpenMP-supporting functions (Gini's cumsum may not benefit)
- Thread overhead may hurt for small surveys (< ~10K rows)
- Not all systems have OpenMP enabled

### Approach E: `split()` + `lapply()` (Variant of A)

Replace repeated `dt[pip_id == pid]` with `split(dt, by = "pip_id")` which
produces all slices in a single pass, then `lapply()` over the list.

**Pros**:
- Avoids repeated `[` subsetting (single pass over data)
- Same simplicity as Approach A

**Cons**:
- All slices in memory simultaneously
- Marginal difference expected

## Decision

**Chosen approach**: Benchmark-driven selection, testing in order of increasing
effort: A (baseline) → A+D (nthreads) → E+D (split+nthreads) → B → C.
Stop as soon as one meets the ≤3s target. The benchmark script profiles I/O
vs compute separately to identify whether orchestration optimization is even
the right lever.

The benchmark should test `nthreads` at 1, 2, and 4 as an orthogonal axis
across whichever structural approach is being tested.

## Next Steps

1. Create `benchmarks/orchestration-strategy.R` — standalone script that:
   - Generates or loads a realistic 15-survey fixture dataset
   - Profiles `load_surveys()` I/O separately from compute
   - Implements each approach as a self-contained function
   - Runs `bench::mark()` across all approaches × nthreads settings
   - Produces a summary table and visualization (timing breakdown by phase)
   - Records results to `.cg-docs/solutions/performance-issues/`
2. Update Step 8 of the computation engine plan to reference this brainstorm
   and replace the old benchmark design
3. If a non-A approach wins: refactor `table_maker()` accordingly (separate step)
