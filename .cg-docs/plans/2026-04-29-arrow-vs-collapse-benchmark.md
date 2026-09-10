---
date: 2026-04-29
title: "Arrow vs Collapse End-to-End Benchmark"
scope: standard
status: completed
completed-date: 2026-04-29
brainstorm: .cg-docs/brainstorms/2026-04-29-arrow-vs-collapse-benchmark.md
---

## Objective

Measure whether pushing column selection, filtering, or partial aggregation
into Arrow (before `collect()`) speeds up the current collapse-based pipeline
enough to justify the added complexity. The benchmark produces a data-driven
recommendation with clear thresholds.

## Schema Context

Parquet columns (from `.vp_canonical_schema()`):

| Column | Type | Role |
|--------|------|------|
| country_code | utf8 | partition key |
| surveyid_year | int32 | partition key |
| welfare_type | utf8 | partition key |
| version | utf8 | partition key |
| pip_id | utf8 | ID |
| survey_acronym | utf8 | ID |
| welfare | float64 | **computation** |
| weight | float64 | **computation** |
| gender | dict | dimension |
| area | dict | dimension |
| educat4 | dict | dimension |
| educat5 | dict | dimension |
| educat7 | dict | dimension |
| age | int32 | dimension |

Column-pruning surface: 8 metadata/unused columns can be dropped at I/O,
keeping only `welfare`, `weight`, and the requested `by` dimensions.

## Approaches

### Approach 1 — Baseline (current pipeline)

```
open_dataset(files) |> collect() |> as.data.table()
→ compute_measures(dt, measures, poverty_lines, by)
```

No column selection; reads all columns. This is what `load_surveys()` +
`compute_measures()` does today.

### Approach 2 — Arrow-Select (column-pruned I/O)

```
open_dataset(files) |> select(welfare, weight, !!!by_cols) |> collect() |> as.data.table()
→ compute_measures(dt, measures, poverty_lines, by)
```

Only read required columns. Computation unchanged. Tests whether I/O
bandwidth is the bottleneck.

### Approach 3 — Arrow-Push (partial Arrow aggregation)

```
# Arrow-computable measures (mean, headcount, pop_poverty, nobs, sum, min, max):
open_dataset(files) |> select(...) |> group_by(!!!by_cols) |>
  summarise(mean = ..., headcount = ...) |> collect()

# R-only measures (gini, mld, median, quantiles, sd, var, poverty_gap, severity, watts):
open_dataset(files) |> select(welfare, weight, !!!by_cols) |> collect() |> as.data.table()
→ collapse path for R-only measures
→ rbind Arrow results + collapse results
```

Hybrid: push simple aggregations into Arrow, keep complex ones in collapse.
Tests whether avoiding full materialisation for simple measures helps.

### Approach 4 — Arrow-Sort (pre-sorted for Gini)

```
open_dataset(files) |> select(welfare, weight, !!!by_cols) |>
  arrange(!!!by_cols, welfare) |> collect() |> as.data.table()
→ compute_measures() [skip internal setorder for Gini]
```

Tests whether Arrow's parallel sort (C++) beats data.table's `setorder()`.
Requires a modified `compute_inequality()` that skips sorting when data is
pre-sorted. **Only implement if Approach 2 shows I/O is not the bottleneck.**

## Benchmark Design

| Parameter | Value |
|-----------|-------|
| Measures | `c("headcount", "gini", "mean", "median")` |
| Poverty lines | `c(2.15, 3.65)` |
| By dimensions | `c("gender", "area")` |
| Survey sample | 15 random surveys per iteration |
| Iterations | 50 |
| Timer | `bench::mark()` with `check = FALSE`, `min_iterations = 1` |
| Correctness | `all.equal(baseline, approach_n, tolerance = 1e-10)` after first iteration |

### Decision Rules

1. **Approach 2 vs 1**: adopt if ≥30% faster (median time)
2. **Approach 3 vs 2**: adopt if ≥20% faster AND correctness passes
3. **Approach 4**: only benchmark if sort accounts for ≥20% of compute time in profiling

## File Plan

| File | Action | Description |
|------|--------|-------------|
| `benchmarks/arrow-vs-collapse.R` | create | Main benchmark script (~400 lines) |
| `benchmarks/arrow-vs-collapse-results.png` | output | Comparative visualisation |
| `.cg-docs/solutions/performance-issues/2026-04-29-arrow-vs-collapse-results.md` | output | Results document |

### Script Structure (follows `orchestration-strategy.R` conventions)

```
1. Config block (LIVE_MODE, CACHE_PATH, measures, dims, poverty_lines, iterations)
2. devtools::load_all()
3. Data setup: load manifest, sample 15 surveys, cache paths to RDS
4. Approach functions (baseline, arrow_select, arrow_push, arrow_sort)
5. Correctness check (all.equal on first-run results)
6. bench::mark() loop across approaches
7. Results table + ggplot2 visualisation + ggsave
8. Decision logic + recommendation message
9. Write results markdown
```

## Implementation Steps

### Step 1 — Script skeleton + config (est. 15 min)

Create `benchmarks/arrow-vs-collapse.R` with:
- Config block matching orchestration-strategy.R conventions
- `devtools::load_all()`
- Manifest loading + survey sampling
- RDS cache for Parquet file paths

### Step 2 — Approach functions (est. 30 min)

Implement the 4 approach functions:
- `approach_1_baseline()` — current pipeline
- `approach_2_arrow_select()` — column-pruned I/O
- `approach_3_arrow_push()` — hybrid Arrow + collapse
- `approach_4_arrow_sort()` — pre-sorted I/O (conditional)

### Step 3 — Correctness + benchmarking (est. 20 min)

- `all.equal()` correctness gate
- `bench::mark()` with 50 iterations
- Results aggregation into data.table

### Step 4 — Visualisation + output (est. 15 min)

- ggplot2 bar chart (median time by approach, faceted by nthreads if relevant)
- Decision logic with threshold checks
- Write results markdown to `.cg-docs/solutions/performance-issues/`

## Risks

| Risk | Mitigation |
|------|------------|
| Network I/O variance swamps compute differences | Use median over 50 iterations; report IQR |
| Arrow push correctness drift | Gate with `all.equal()` before benchmarking |
| Approach 3 complexity not worth marginal gain | Decision rule requires ≥20% improvement |
| dict type mismatch on collect | Explicit column types in Arrow schema |

## Out of Scope

- Modifying production code (`load_surveys.R`, `compute_measures.R`)
- Benchmarking write paths
- Multi-node parallelism
- Age binning dimensions (separate benchmark)
