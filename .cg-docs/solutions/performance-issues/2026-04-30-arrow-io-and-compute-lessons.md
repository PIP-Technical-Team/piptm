---
date: 2026-04-30
title: "Arrow I/O and Compute Strategy: Lessons from Benchmarking"
category: "performance-issues"
language: "R"
tags: [arrow, parquet, io, push-down, column-pruning, compute, benchmark, network-io, unc-share]
root-cause: "Unclear separation between I/O cost and compute cost led to flawed push-down designs and untested assumptions about Arrow performance on a network share."
severity: "P1"
---

# Arrow I/O and Compute Strategy: Lessons from Benchmarking

## Background

Three benchmark scripts were developed on branch `bm-arrow` to investigate
performance of the `table_maker()` pipeline, which reads PIP survey microdata
from a UNC-mounted Arrow/Parquet repository and computes poverty, inequality,
and welfare statistics:

| Script | Focus | Key finding |
|---|---|---|
| `benchmarks/orchestration-strategy.R` | In-memory compute orchestration (Approach A vs B) | Batch GRP approach (B) is 72% faster than per-survey lapply (A); I/O was only 157ms (warm cache, not representative) |
| `benchmarks/arrow-vs-collapse.R` | I/O strategy isolation: column pruning, Arrow sort, naïve push-down | Column pruning: 68% I/O speedup, 32% E2E speedup; Arrow sort: 77% overhead; naïve push-down (multiple full scans): 70% slower |
| `benchmarks/pipeline-comparison.R` | End-to-end pipeline comparison with I/O and compute timed separately | A2 (column-pruned I/O) confirmed 29% E2E win; A3 (genuine Arrow push-down) to be evaluated |

Results are on the `bm-arrow` branch. Benchmark data files:
- `.cg-docs/solutions/performance-issues/2026-04-28-orchestration-benchmark-results.md`
- `.cg-docs/solutions/performance-issues/2026-04-29-arrow-vs-collapse-results.md`
- `.cg-docs/solutions/performance-issues/2026-04-29-pipeline-comparison-results.md`

---

## Problem

The `table_maker()` pipeline was meeting a 3s compute target in isolation but
its real-world performance was unknown when I/O (network reads from a UNC
share) was included. Multiple optimization ideas existed (column pruning, Arrow
aggregation push-down, Arrow pre-sort) but none had been measured.

---

## Root Cause

Two separable costs were conflated:

1. **I/O cost** — bytes transferred from Parquet files over the network
2. **Compute cost** — R/collapse processing of the loaded data

Without isolating them, it was impossible to know which was worth attacking, or
whether an "Arrow optimization" would help or hurt.

---

## Lessons Learned

### L1 — I/O dominates on a UNC share; column pruning is the primary lever

**Observation**: Reading all 14 schema columns (IO-1) took 1.658s median.
Reading only the 6 needed columns (IO-2) took 0.524s — a **68% reduction** in
I/O time and **32% reduction** in E2E time.

**Why it works**: Parquet is a columnar format. Each column is stored in
separate byte ranges. A `select()` before `collect()` tells Arrow to skip the
byte ranges for unneeded columns entirely — the bytes never cross the network.

**Critical detail**: The `select()` must happen BEFORE `collect()`. Subsetting
columns after `collect()` provides zero I/O benefit because all bytes were
already transferred.

```r
# ✅ Correct — select before collect: skips bytes on network
open_dataset(files) |>
  select(all_of(NEEDED_COLS)) |>   # ← Arrow skips other column byte ranges
  collect()

# ❌ Wrong — select after collect: all bytes already transferred
open_dataset(files) |>
  collect() |>
  select(all_of(NEEDED_COLS))      # ← too late, no I/O saving
```

**Actionable**: Add `select(needed_cols)` inside `load_surveys()` before
`collect()`. The `NEEDED_COLS` set is `c("pip_id", "welfare", "weight")` plus
whatever `by` dimensions are requested. The 8 dropped columns
(`country_code`, `surveyid_year`, `welfare_type`, `version`, `survey_acronym`,
`educat5`, `educat7`, `age`) are never consumed by `compute_measures()`.

---

### L2 — Each Arrow scan over a UNC share costs ~500–900ms regardless of column count

**Observation**: IO-2 (6 columns) = 0.524s. E2E-3 (3 scans × 6 columns) =
4.196s vs E2E-2 (1 scan × 6 columns) = 2.473s. The two extra scans added
1.723s ≈ **860ms per scan** — similar to the cost of a single full scan,
despite reading the same narrow column set.

**Why**: On a UNC network share, each `open_dataset() → collect()` call incurs
a fixed overhead: connection negotiation, file metadata reads, row group
scheduling, data transfer setup. This fixed overhead (~400–600ms) dominates
over the marginal cost of additional bytes for narrow column sets.

**Implication**: Scan count is as important as byte count. An optimization that
reduces bytes-per-scan but doubles the number of scans will almost certainly be
slower, not faster.

---

### L3 — "Arrow push-down" requires aggregation BEFORE collect(), not separate collects per measure

**Observation**: `arrow-vs-collapse.R` E2E-3 was described as "Arrow push-down"
but actually called `collect()` three times (once per measure group), each
returning full microdata. Result: 70% **slower** than E2E-2.

**The correct definition of push-down**:

```r
# ❌ Wrong — "push-down" that isn't: 3 full scans, aggregate in R
.arrow_mean <- function(files, by) {
  open_dataset(files) |> select(...) |> collect() |>   # full scan 1
    as.data.table() |> _[, sum(welfare*weight)/sum(weight), by = by]
}
.arrow_headcount <- function(files, by, pl) {
  open_dataset(files) |> select(...) |> collect() |>   # full scan 2 (per pl)
    as.data.table() |> _[, sum(weight[welfare < pl])/sum(weight), by = by]
}

# ✅ Correct — genuine push-down: Arrow aggregates before collect()
#   Returns ~hundreds of rows instead of millions
open_dataset(files) |>
  select(all_of(NEEDED_COLS)) |>
  mutate(across(all_of(dict_cols), as.character)) |>
  group_by(across(all_of(batch_by))) |>
  summarise(
    sum_w  = sum(weight, na.rm = TRUE),
    sum_ww = sum(welfare * weight, na.rm = TRUE),
    poor_w_1 = sum(if_else(welfare < 2.15, weight, 0), na.rm = TRUE),
    poor_w_2 = sum(if_else(welfare < 3.65, weight, 0), na.rm = TRUE)
  ) |>
  collect()   # ← only aggregated rows cross the network
```

**Rule**: "Push-down" only saves time if the data volume crossing the
network is smaller. Aggregation that returns 1 row per group (hundreds) instead
of 1 row per person (millions) is genuine push-down. Compute that runs after
`collect()` is just R compute, regardless of what you call it.

---

### L4 — Arrow's aggregation engine cannot hash-aggregate dictionary-encoded columns across multiple Parquet files

**Observation**: When `group_by + summarise` is pushed into Arrow across
multiple Parquet files, dict-encoded columns (`gender`, `area`, `educat4`,
`educat5`, `educat7`) raise:

```
NotImplemented: Unifying differing dictionaries
```

**Why**: Dictionary encoding stores category strings once per file (the
dictionary) and each row stores a small integer index. Different files may
encode `"male"` as index 0 or index 1. Arrow's lazy query engine cannot
reconcile differing integer-to-string mappings across files during aggregation.

**Fix**: Cast dict-encoded columns to plain character strings inside the Arrow
query, before `group_by`:

```r
mutate(across(all_of(dict_cols), as.character))
```

This is only required when aggregation happens inside Arrow (genuine push-down).
After `collect()` into R, Arrow automatically converts dict columns to R
factors, and R's grouping functions handle factors natively — no cast needed.

**Dict-encoded columns in the PIP Arrow schema** (as of 2026-04-30):
`gender`, `area`, `educat4`, `educat5`, `educat7`

---

### L5 — Arrow CAN sort rows, but pre-sorting in Arrow over a network share is not cost-effective

**Observation**: IO-3 (column-pruned + Arrow pre-sort by welfare) took 3.337s
vs IO-2's 0.524s — a **537% overhead** for sorting alone. The sort step added
2.813s, which is 77% of the full E2E-1 baseline time.

**Clarification on "sort vs order-dependent algorithms"**: Arrow can physically
rearrange rows (`arrange()`). What Arrow cannot compute are *statistics that
require an ordered pass* over the data — e.g., Gini (cumulative Lorenz sum),
weighted median (quantile algorithm). Sorting is a row rearrangement; Gini is
an algorithm that requires the sorted order to exist at computation time.

**Why Arrow sort is slow here**: The sort must be executed and materialized
server-side (or during transfer) before rows are sent. On a network share,
this means the Arrow process holds open the connection longer while sorting
millions of rows. R's `setorder()` on already-loaded in-memory data is far
cheaper.

**Implication**: Arrow pre-sort is not worth pursuing unless the data already
arrives pre-sorted (i.e., Parquet files are written in welfare order). This is
a write-time optimization, not a read-time one.

---

### L6 — Arrow-feasibility of measures in the registry

Which measures can be computed via Arrow `group_by + summarise` (push-down)
vs which require full microdata in R:

| Feasibility | Measures | Reason |
|---|---|---|
| ✅ Full Arrow push-down | `headcount`, `poverty_gap`, `severity`, `watts`, `pop_poverty`, `mean`, `sd`, `var`, `min`, `max`, `nobs`, `sum`, `mld` | Closed-form scalar aggregates: one pass over rows, each row contributes independently |
| ⚠️ Arrow unweighted only (wrong for PIP) | `p10`, `p25`, `p75`, `p90`, `median` | Arrow has `quantile()` / `median()` but they are **unweighted**. PIP uses weighted quantiles via `collapse::fmedian()` / `collapse::fnth()` — requires full microdata in R |
| ❌ Must be in R | `gini` | Requires a sorted welfare vector for the Lorenz curve integration: $G = 1 - 2\int_0^1 L(p)\,dp$. Not expressible as a single-pass scalar aggregate |

**Note on `mld`**: Mathematically Arrow-feasible (`sum(weight * log(welfare)) / sum(weight)`), but currently computed in R alongside Gini for implementation consistency. No barrier to push-down if needed.

---

### L7 — Fixed-survey benchmarks understate real-world variance; resample surveys each iteration

**Observation**: `arrow-vs-collapse.R` used `bench::mark()` on a fixed set of
15 surveys. This captures OS/scheduling noise but not the variance from "which
surveys happened to be in the batch" — which in production is the dominant
source of run-to-run variance (survey sizes range from ~7,500 to ~525,000 rows).

**Better design**: Resample N surveys each iteration with `set.seed(BASE_SEED + i)`.
The per-iteration scatter plot then shows the full distribution of run times
across the realistic range of survey size combinations, not just a narrow band
around one fixed point.

---

### L8 — The orchestration benchmark's 157ms I/O time was a warm-cache single measurement, not a production baseline

**Risk**: The orchestration benchmark reported `load_surveys()` time as 157ms,
leading to the impression that I/O is negligible (5.2% of 3s target). The
arrow benchmarks showed real repeated I/O is 1.6–2.4s per 15-survey batch.

**Why the discrepancy**: The 157ms measurement was:
1. A single `bench::mark()` call after the data was already partially cached by
   the OS file cache
2. Measured against a different (smaller) set of surveys than the arrow benchmark

**Lesson**: Single I/O measurements on a network share are unreliable. Always
use repeated measurements with fresh survey samples to characterize I/O cost.
The arrow benchmark's 50-iteration resample design is the correct approach.

---

### L9 — `!!!` (bang-bang-bang) splicing: building multi-poverty-line summarise() programmatically

When pushing headcount for N poverty lines into a single Arrow scan, the
`summarise()` arguments must be built dynamically to avoid N separate scans:

```r
# Build unevaluated expressions — one per poverty line
hc_exprs <- setNames(
  lapply(POVERTY_LINES, function(pl) {
    rlang::expr(sum(dplyr::if_else(welfare < !!pl, weight, 0), na.rm = TRUE))
  }),
  paste0("poor_w_", seq_along(POVERTY_LINES))
)

# Splice into summarise() — Arrow sees one scan with N conditional sums
summarise(sum_w = sum(weight), !!!hc_exprs)
```

`!!pl` injects the numeric value of `pl` into the expression at build time.
`!!!hc_exprs` unpacks the named list as individual named arguments, equivalent
to writing them by hand. Arrow executes the entire `summarise()` in one scan.

Without splicing, you would need one `summarise()` call per poverty line, each
requiring a separate Arrow scan — defeating the purpose.

---

## Prevention

1. **Always `select()` before `collect()`** in `load_surveys()`. Never subset
   columns after collect.

2. **Count scans, not just bytes**. On a UNC share, each scan costs ~500ms
   fixed overhead. An approach that adds a scan to save compute time is almost
   always a net loss unless the scan returns aggregated data (<<1% of microdata
   volume).

3. **"Push-down" = aggregation inside Arrow, not compute after collect**. If
   `collect()` returns millions of rows, no push-down occurred.

4. **Cast dict-encoded columns before Arrow `group_by`**: always apply
   `mutate(across(all_of(DICT_SCHEMA_COLS_USED), as.character))` before any
   `group_by + summarise` pushed into Arrow across multiple Parquet files.

5. **Don't push `gini` or weighted quantiles into Arrow** — they cannot be
   computed as single-pass scalar aggregates.

6. **Benchmark I/O with repeated fresh samples**. Single measurements on a
   network share are unreliable. Use 30+ iterations with survey resampling.

---

## Related

- `benchmarks/arrow-vs-collapse.R` — I/O isolation benchmark (source data for L1–L5, L8)
- `benchmarks/pipeline-comparison.R` — E2E pipeline comparison (source data for L3 genuine push-down, L7)
- `benchmarks/orchestration-strategy.R` — In-memory compute benchmark (source data for L8)
- `.cg-docs/solutions/performance-issues/2026-04-29-arrow-vs-collapse-results.md` — Numeric results
- `.cg-docs/solutions/performance-issues/2026-04-29-pipeline-comparison-results.md` — Numeric results
- `.cg-docs/solutions/performance-issues/2026-04-28-orchestration-benchmark-results.md` — Numeric results
