---
date: 2026-04-28
title: "Orchestration Strategy Benchmark"
status: completed
completed-date: 2026-04-28
scope: "Lightweight"
brainstorm: ".cg-docs/brainstorms/2026-04-28-orchestration-benchmark.md"
language: "R"
estimated-effort: "small"
tags: [performance, benchmarking, orchestration, table-maker, collapse, nthreads]
---

# Plan: Orchestration Strategy Benchmark

## Objective

Create a standalone benchmark script that compares orchestration strategies
for `table_maker()`'s per-survey computation loop, measures I/O vs compute
time separately, and determines whether the current per-slice `lapply()`
approach should be kept or replaced. This plan supersedes Step 8 of the
computation engine plan (`2026-04-07-computation-engine.md`).

## Context

**What exists today:**

- `table_maker()` — full pipeline: manifest → load → split by `pip_id` →
  `lapply()` over `compute_measures()` per slice → `rbindlist()` → reorder
- `compute_measures()` — orchestrator that pre-computes `GRP(dt, by)` once
  and shares it across `compute_poverty()`, `compute_inequality()`,
  `compute_welfare()`. All three family functions accept optional `grp` arg.
- `load_surveys()` — batch Parquet loader via exact Hive partition paths.
  Returns a single flat `data.table` with `pip_id` column identifying surveys.

**Brainstorm decision:**

Benchmark five approaches (A through E) in order of increasing effort. Use
`collapse::nthreads` as an orthogonal axis. Stop as soon as one meets the
≤3s wall-clock target for 15 surveys. Decision rule:

1. If any approach meets ≤3s: pick the simplest
2. If multiple meet it: pick the simplest unless another is ≥40% faster
3. If none meets it: bottleneck is I/O — pivot to I/O optimization

**Data characteristics** (from `dt_batch` in session):

- 60 surveys, median ~49K rows, mean ~241K, max ~784K
- Benchmark target: 15 surveys
- 3 poverty lines, up to 4 dimensions
- Total batch: ~14.5M rows

## Requirements

| ID  | Requirement                                              | Source    |
|-----|----------------------------------------------------------|-----------|
| R1  | Profile I/O (`load_surveys`) vs compute separately       | brainstorm |
| R2  | Benchmark 5 orchestration approaches (A–E)               | brainstorm |
| R3  | Test `collapse::nthreads` at 1, 2, 4 as orthogonal axis | brainstorm |
| R4  | Produce timing breakdown table and visualization         | user      |
| R5  | Use realistic data (15 surveys from `dt_batch` or synthetic equivalent) | user |
| R6  | Script is standalone and re-runnable (`benchmarks/`)     | user      |
| R7  | Record results in `.cg-docs/solutions/performance-issues/` | brainstorm |

## Implementation Steps

### Step 1: Create benchmark script skeleton

- **Requirements**: R5, R6
- **Files**: `benchmarks/orchestration-strategy.R` (new)
- **Details**:
  1. Create `benchmarks/` directory at package root.
  2. Script preamble: load `{piptm}` (via `devtools::load_all()`), `{bench}`,
     `{data.table}`, `{collapse}`, `{ggplot2}`.
  3. **Data setup section**: Two modes:
     - **Live mode**: Load 15 surveys from the real Arrow repository using
       `load_surveys()` with a fixed set of `pip_id`s sampled from the
       current manifest. Save the loaded `data.table` to a local `.qs2`
       cache so subsequent runs skip I/O.
     - **Synthetic mode** (fallback if no Arrow access): Generate a
       synthetic 15-survey `data.table` matching the observed distribution
       (median ~49K rows, some large surveys up to ~400K). Include columns:
       `pip_id`, `welfare`, `weight`, `gender`, `area`, `age`,
       `country_code`, `surveyid_year`, `welfare_type`.
  4. Define benchmark parameters:
     ```r
     POVERTY_LINES <- c(2.15, 3.65, 6.85)
     BY_DIMS       <- c("gender", "area", "age")
     ALL_MEASURES  <- pip_measures()
     NTHREADS      <- c(1L, 2L, 4L)
     ```
- **Tests**: Script runs without error on synthetic data.
- **Acceptance criteria**: Script loads or generates a realistic 15-survey
  dataset ready for benchmarking.

### Step 2: Implement the five approach wrappers

- **Requirements**: R1, R2
- **Files**: `benchmarks/orchestration-strategy.R` (append)
- **Details**:
  Each approach is implemented as a self-contained function that takes the
  pre-loaded `dt` (already in memory — I/O excluded) and returns the final
  long-format result. All must produce **identical output** (verified by
  `bench::mark(check = TRUE)` or a manual `all.equal()` pre-check).

  1. **`approach_a_baseline(dt, measures, poverty_lines, by)`**
     - Current implementation: `unique(dt$pip_id)` → `lapply()` →
       `dt[pip_id == pid]` → `compute_measures()` → `rbindlist()`.
     - Extract the per-survey loop from `table_maker()` lines 335–365
       (Steps 6–7) into a standalone function, stripping the manifest/load
       layers.

  2. **`approach_b_collapse_grouped(dt, measures, poverty_lines, by)`**
     - No `lapply()`. Build `GRP(dt, by = c("pip_id", by))` once.
     - For **welfare** and **inequality**: pass the compound GRP to family
       functions directly. Family functions already accept `grp` — they'll
       compute across all surveys simultaneously.
     - For **poverty**: the cross-join must happen per-survey (poverty line
       expansion changes row count per survey differently if surveys have
       different sizes). Use a grouped poverty approach: loop over poverty
       lines (as current `compute_poverty()` does), but compute `fsum()`
       with the compound `GRP` that includes `pip_id`.
     - Batch-level NA-fill for missing dimensions before building GRP.
     - Metadata attachment via join from a `pip_id → metadata` lookup table
       built before computation.
     - **Gini complication**: `compute_inequality()` sorts by
       `(.grp_id, welfare)` for Gini. With compound GRP, `.grp_id` already
       encodes `pip_id × by`, so the sort and per-group `.gini_sorted()`
       call work as-is — just with more groups.

  3. **`approach_c_datatable_by(dt, measures, poverty_lines, by)`**
     - `dt[, compute_measures(.SD, measures, poverty_lines, by), by = pip_id]`
     - Requires adding metadata columns after the fact (same join as B).
     - `compute_measures()` already has the single-survey guard
       (`uniqueN(pip_id) == 1L`). Each `.SD` slice contains one `pip_id`,
       so the guard passes.

  4. **`approach_d_nthreads(dt, measures, poverty_lines, by, nthreads)`**
     - Wrapper: `set_collapse(nthreads = nthreads)`, then call
       `approach_a_baseline()`, then `set_collapse(nthreads = 1L)` to reset.
     - This is tested at nthreads = 1, 2, 4 for each structural approach.

  5. **`approach_e_split_lapply(dt, measures, poverty_lines, by)`**
     - `split(dt, by = "pip_id")` → `lapply()` over the resulting list →
       `compute_measures()` per slice → `rbindlist()`.
     - One-line change from A: replaces repeated `dt[pip_id == pid]` with
       a single `split()` call.

- **Test Scenarios**:
  - ✅ All five approaches produce identical output on a small 3-survey
    fixture (verified with `all.equal()` before running full benchmark)
  - 🛑 Approach B handles surveys with missing dimension columns (NA-fill)
  - ❌ Approach C: if `.SD` overhead is extreme, note it but still record
- **Tests**: Pre-benchmark correctness check (small fixture, `all.equal`).
- **Acceptance criteria**: All five wrappers produce identical results.

### Step 3: Run benchmarks and produce output

- **Requirements**: R1, R3, R4, R7
- **Files**: `benchmarks/orchestration-strategy.R` (append)
- **Details**:
  1. **I/O profiling** (R1):
     ```r
     io_time <- bench::mark(
       load = load_surveys(entries_15, release = release),
       iterations = 3L
     )
     ```
     Record median I/O time separately. This establishes the floor — if I/O
     alone exceeds 3s, no orchestration change can meet the target.

  2. **Compute benchmarks** (R2, R3):
     Build a benchmark grid: 5 approaches × 3 nthreads settings = 15 runs.
     For approaches A/B/C/E, wrap each in a `set_collapse(nthreads = n)`
     call. Approach D is redundant with "A + nthreads" — it's the same
     thing. So effectively:

     | Approach | nthreads=1 | nthreads=2 | nthreads=4 |
     |----------|-----------|-----------|-----------|
     | A (baseline) | ✓ | ✓ | ✓ |
     | B (collapse grouped) | ✓ | ✓ | ✓ |
     | C (data.table by) | ✓ | ✓ | ✓ |
     | E (split+lapply) | ✓ | ✓ | ✓ |

     Use `bench::mark()` with `iterations = 5L`, `check = FALSE` (already
     verified in Step 2), `memory = TRUE`.

  3. **Output** (R4):
     - **Summary table**: approach × nthreads → median time, memory
       allocated, `itr/sec`. Print with `knitr::kable()` or plain text.
     - **Visualization**: `ggplot2` faceted bar chart:
       - x = approach, y = median time (seconds)
       - facet by nthreads
       - horizontal line at 3s target
       - second panel: memory allocation
     - Save plot to `benchmarks/orchestration-results.png`.

  4. **Results document** (R7):
     Script writes a summary to
     `.cg-docs/solutions/performance-issues/2026-04-28-orchestration-benchmark-results.md`
     with:
     - Machine specs (via `sessionInfo()` or `benchmarkme::get_cpu()`)
     - The timing table
     - I/O time vs compute time breakdown
     - Decision recommendation per the decision rule

- **Acceptance criteria**: Script produces a timing table, visualization,
  and results document. Decision is clear from the output.

## Testing Strategy

This is a benchmark script, not package code — no formal testthat tests.
Correctness is verified by the pre-benchmark `all.equal()` check in Step 2.
The script itself is validated by running it end-to-end on both synthetic
and live data.

## Documentation Checklist

- [ ] Benchmark script has a header comment explaining purpose and usage
- [ ] Results document in `.cg-docs/solutions/performance-issues/`
- [ ] Step 8 of `2026-04-07-computation-engine.md` updated to reference
      this plan and brainstorm (after benchmark completes)

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| I/O dominates runtime (>2s of 3s budget) | Medium | Orchestration optimization irrelevant | Profile I/O first (R1); if it dominates, document and pivot |
| Approach B correctness issues (batch NA-fill, compound GRP) | Medium | Invalid benchmark | `all.equal()` pre-check before timing |
| `nthreads` not available (no OpenMP) | Low | Incomplete benchmark | Detect with `collapse::get_collapse("nthreads")` and skip if unavailable |

## Out of Scope

- Modifying `load_surveys()` or the I/O layer
- Parallel processing (`future`, `parallel`)
- Refactoring `table_maker()` — that's a follow-up if a non-A approach wins
- CRAN-compatible test for performance (timing-sensitive tests are unreliable)
- Benchmarking per-family functions individually (already known to be fast)
