---
date: 2026-04-29
title: "Arrow vs Collapse End-to-End Benchmark"
status: decided
scope: "Standard"
chosen-approach: "Benchmark all three approaches (Arrow-Select, Arrow-Split, Arrow-Sort)"
tags: [performance, benchmarking, arrow, collapse, table-maker, io, parquet]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Arrow vs Collapse End-to-End Benchmark

## Context

The current `table_maker()` pipeline loads all survey data from Parquet via
`open_dataset() |> collect() |> as.data.table()`, then computes everything
in-memory using collapse + data.table. The Approach B refactor
(`.cg-docs/plans/2026-04-28-table-maker-approach-b-refactor.md`) optimised
the compute stage to 0.12s for 15 surveys. Total end-to-end is ~0.28s
(0.16s I/O + 0.12s compute).

This brainstorm explores whether a more Arrow-native pipeline can improve
overall performance by:

- Reducing data transfer across the Arrow→R boundary (column pruning)
- Pushing simple aggregations into Arrow's C++ engine
- Pre-sorting data in Arrow for Gini's sorted-cumsum algorithm

### What Arrow can and cannot compute

**Arrow-computable** (via `group_by() |> summarize()`):
- Weighted mean: `sum(welfare * weight) / sum(weight)`
- Sum, nobs, min, max
- Headcount: `sum(cast(welfare < pl, float64) * weight) / sum(weight)`
- pop_poverty: `sum(cast(welfare < pl, float64) * weight)`
- Population: `sum(weight)`

**Must stay in R** (no Arrow expression support):
- Gini — requires sorted cumulative sum with lag within groups (Brown 1994)
- MLD — requires weighted group mean broadcast back to each row (`TRA = "replace_fill"`)
- Weighted median / percentiles (p10, p25, p75, p90) — no grouped weighted quantile
- Weighted SD / variance
- Poverty gap, severity — expressible but requires per-poverty-line cross-join
- Watts — conditional log masking gets complex

**Summary**: 7 of 19 measures can be fully pushed to Arrow. But the measures
that *can* be pushed are the cheapest in collapse already (simple `fmean`,
`fsum`). The expensive measures (gini, weighted median) *must* stay in R.

## Requirements

- **Benchmark scenario**: `measures = c("headcount", "gini", "mean", "median")`,
  `poverty_lines = c(2.15, 3.65)`, `by = c("gender", "area")`.
  Two easy measures (mean, headcount), two hard (gini, median).
- **Iterations**: 50, each drawing a random sample of 15 surveys from the
  full manifest. Random seed set for reproducibility.
- **Data**: Live Arrow repository, but Parquet file paths cached per sample
  to reduce network latency noise. Actual file reads are not cached.
- **Metrics**: Total wall-clock time (load → compute → return), peak memory.
- **Correctness**: Each approach must produce results identical to the
  baseline (verified via `all.equal()` with tolerance 1e-10).
- **Minimum improvement threshold**: Approach 2 (Arrow-Split) must beat
  Approach 1 (Arrow-Select) by ≥30% to justify its added complexity.
- **Out of scope**: Modifying `compute_measures()` or family functions.
  Benchmark builds self-contained wrapper functions. Production code changes
  happen only if a strategy wins.

## Approaches Considered

### Approach 1: Column-Pruned I/O ("Arrow-Select")

Replace `collect()` all columns with
`select(welfare, weight, pip_id, by_cols) |> collect()`. Compute stays 100%
collapse. Metadata extracted via a second tiny Arrow query:
`select(pip_id, country_code, surveyid_year, welfare_type) |> distinct() |> collect()`.

**Pros**:
- Universal — works for any measure mix, no branching logic
- Minimal code change (just `select()` before `collect()`)
- Reduces bytes transferred from Parquet (skip version, survey_acronym, etc.)
- Big table never carries metadata columns

**Cons**:
- Parquet is already columnar; Arrow's reader may already partially prune
- Savings may be modest (~40–50% fewer columns)
- Compute stage unchanged — no speedup there

**Effort**: Small
**Recommended?**: Yes — low risk, universally applicable

### Approach 2: Arrow-Compute Hybrid ("Arrow-Split")

Split measures into "easy" (Arrow-computable) and "hard" (needs microdata).
Compute easy measures entirely in Arrow C++ via `group_by() |> summarize()`.
Collect microdata only for hard measures, then collapse for those.
`rbindlist()` the two result sets.

```
ds <- open_dataset(files) |> select(needed_cols)

# Easy path (Arrow C++, returns ~60-row grouped table):
easy_result <- ds |>
  group_by(pip_id, gender, area) |>
  summarize(
    mean       = sum(welfare * weight) / sum(weight),
    headcount  = sum(cast(welfare < 2.15, float64) * weight) / sum(weight),
    population = sum(weight)
  ) |> collect()

# Hard path (collect microdata, then collapse):
hard_dt <- ds |> collect() |> as.data.table()
hard_result <- compute_measures(hard_dt, hard_measures, poverty_lines, by)

rbindlist(easy_result, hard_result, fill = TRUE)
```

**Pros**:
- Easy measures never materialize row-level data in R (336K rows → ~60 groups)
- If all-easy request, entire pipeline stays in Arrow — no microdata collected
- Arrow C++ handles full scan + group + aggregate in one pass

**Cons**:
- Two code paths + branching logic (classify easy vs hard, merge results)
- Poverty measures need per-poverty-line handling in Arrow expressions
- Mixed requests still collect full microdata for the hard path
- More complex to maintain and test

**Effort**: Medium
**Recommended?**: Benchmark to find out — must beat Approach 1 by ≥30%

### Approach 3: Column-Pruned I/O + Arrow Pre-Sort ("Arrow-Sort")

Like Approach 1 but also push the Gini per-group sort into Arrow.
Currently `compute_inequality()` does `setorder(work_g, .grp_id, welfare)`.
Arrow can do `arrange(pip_id, gender, area, welfare)` before `collect()`, so
data arrives pre-sorted.

**Pros**:
- Arrow's sort is multi-threaded C++ — may beat data.table `setorder`
- Rest of pipeline stays identical — no branching
- Gini's `.gini_sorted()` already requires sorted input

**Cons**:
- Requires `compute_inequality()` to accept a "pre-sorted" flag
- Only helps when Gini is requested
- Arrow `arrange()` across multiple files may be slower than in-memory sort

**Effort**: Medium
**Recommended?**: Include in benchmark; only adopt if sort is measurable

## Decision

Benchmark all three approaches against the current baseline. Priority order:

1. **Approach 1 (Arrow-Select)** — universal, low-risk
2. **Approach 2 (Arrow-Split)** — tests the Arrow-compute ceiling
3. **Approach 3 (Arrow-Sort)** — targeted Gini optimization

Decision rule:
- If Approach 1 wins or ties: adopt it (simplest)
- If Approach 2 beats Approach 1 by ≥30%: adopt Approach 2
- If Approach 3 beats Approach 1 and Gini sort is ≥20% of compute time: adopt

### Devil's Advocate Notes

1. **Problem validation**: Current E2E is 0.28s, 10× under the 3s target.
   This is exploratory — the value ceiling is ~0.05–0.10s savings.
2. **Simplicity check**: Parquet is columnar; Arrow's reader already does
   row-group-level column pruning. The explicit `select()` tells Arrow
   *which* columns to decode, but savings depend on whether unused columns
   (survey_acronym, version, string dictionaries) are significant.
3. **Effort-value check**: Approach 2 adds real maintenance complexity.
   The ≥30% threshold guards against adopting complexity for marginal gain.
4. **Charter alignment**: No conflicts — "Scalability constraints: Code must
   handle large, partitioned datasets efficiently (Arrow/Parquet)."

## Next Steps

1. Create `benchmarks/arrow-vs-collapse.R` implementing all three approaches
   as self-contained functions, plus the baseline
2. Run 50 iterations × 4 approaches with random 15-survey samples
3. Produce summary table + visualization (timing distribution, memory)
4. Record results to `.cg-docs/solutions/performance-issues/`
5. If a non-baseline approach wins: create a plan to refactor production code
