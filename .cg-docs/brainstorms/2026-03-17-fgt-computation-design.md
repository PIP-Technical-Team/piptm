---
date: 2026-03-17
title: "Efficient FGT Poverty Measure Computation Design"
status: decided
chosen-approach: "Unified FGT with data.table Grouped Aggregation"
depends-on: "2026-03-16-data-pipeline-architecture.md"
tags: [computation, poverty, fgt, data.table, performance, phase-1]
---

# Efficient FGT Poverty Measure Computation Design

## Context

Before implementing the computation engine, we needed to design the most efficient strategy for computing Foster-Greer-Thorbecke (FGT) poverty measures. These are the core poverty indicators: headcount (α=0), poverty gap (α=1), and poverty severity (α=2). The design must handle multiple poverty lines, multiple breakdown dimensions (cross-tabulated), and integrate cleanly with the broader measure orchestrator.

## Requirements Established Through Q&A

### Input Data Contract
- By the time data reaches FGT computation, pre-processing guarantees:
  - One `welfare` column (PPP-adjusted)
  - One `weight` column (always present, always positive)
  - Zero or more categorical breakdown dimension columns
- Pre-processing (welfare selection, PPP adjustment, weight validation) happens upstream of FGT computation

### FGT Formula
$$FGT_\alpha = \frac{1}{N} \sum_{i=1}^{q} w_i \left( \frac{z - y_i}{z} \right)^\alpha$$

Where:
- $z$ = poverty line
- $y_i$ = welfare value for individual $i$
- $w_i$ = survey weight
- $q$ = number of poor (those with $y_i < z$ — **strictly less than**)
- $N = \sum_{i=1}^{n} w_i$ (total weighted population)
- $\alpha \in \{0, 1, 2\}$ only — **arbitrary α values are not supported**

### Grouped Computation
- Up to 4 categorical breakdown dimensions per call
- Output is a full cross-tabulation of all dimension combinations
- Realistic upper bound on groups: well under 1,000 (typically ~120 for 4 dimensions)
- **Totals and marginals**: deferred for future decision. The design should allow adding them later by calling the same core function on different groupings, not as a structural change.

### Reference Implementation
- {wbpip} has existing FGT implementations that can be used as a reference for formula correctness
- The grouped/cross-tabulated computation is new — no existing benchmark
- {piptm} should not depend on {wbpip} but can reuse logic patterns
- Validation benchmarks will need to be built from scratch

### Performance Context
- Survey data: thousands of rows × 10–20 columns
- Poverty lines per call: typically 3–5
- Groups per call: typically <1,000
- Cross-join expansion: thousands of rows × 3–5 poverty lines = trivial memory cost
- data.table GForce optimization applies to `sum()` operations

## Approaches Considered

### Approach 1: Unified FGT with data.table Grouped Aggregation ✅

A single `compute_fgt()` function that computes all three FGT measures (α=0,1,2) simultaneously for all poverty lines and all group combinations in one data.table operation.

**How it works**:
```r
compute_fgt <- function(dt, poverty_lines, by = NULL) {
  # 1. Cross-join data with poverty lines
  pl_dt <- data.table(poverty_line = poverty_lines)
  work <- dt[, c("welfare", "weight", by), with = FALSE]
  work <- work[pl_dt, on = .NATURAL, allow.cartesian = TRUE]

  # 2. Compute gap vectorized
  work[, gap := pmax((poverty_line - welfare) / poverty_line, 0)]
  work[, is_poor := welfare < poverty_line]

  # 3. Single grouped aggregation
  result <- work[, .(
    headcount    = sum(weight * is_poor) / sum(weight),
    poverty_gap  = sum(weight * gap) / sum(weight),
    severity     = sum(weight * gap^2) / sum(weight),
    population   = sum(weight)
  ), by = c("poverty_line", by)]

  result
}
```

**Pros**:
- All 3 FGT measures in a single grouped aggregation — no repeated scans
- Multiple poverty lines via cross-join — one `[, .(), by]` call
- Pure data.table — leverages GForce optimization
- Simple, readable, testable
- Cross-tabulation is just adding columns to `by`
- `population` comes for free

**Cons**:
- Cross-join multiplies memory by number of poverty lines (3–5x on thousands of rows — trivial)
- Gap recomputed per row per poverty line (but vectorized)

**Effort**: Small

### Approach 2: Sort-and-Cumsum Optimization

Pre-sort data by welfare, compute cumulative weighted sums, use binary search to locate poverty line cutoffs, derive FGT measures algebraically.

**Pros**:
- No cross-join — constant memory regardless of poverty line count
- Theoretically faster for very large datasets with many poverty lines

**Cons**:
- Significantly more complex to implement and debug
- Sorting per group adds overhead — may negate savings for small groups
- Harder to test and validate
- Overkill at this data scale

**Effort**: Medium–Large

### Approach 3: collapse-Based Vectorized Aggregation

Use {collapse} (`fsum`, `GRP()`) for grouped aggregation instead of data.table.

**Pros**:
- {collapse} often faster than data.table for grouped aggregations
- `GRP()` reusable across multiple measure types
- Already in Imports

**Cons**:
- Mixes data.table and collapse idioms — less readable
- Marginal speed benefit at this data scale

**Effort**: Small–Medium

## Decision

**Approach 1: Unified FGT with data.table Grouped Aggregation** was chosen, with the understanding that optimization may be revisited during implementation if benchmarking reveals performance issues.

Rationale:
- At the expected data scale (thousands of rows × 3–5 poverty lines × <1K groups), the cross-join + grouped `sum()` approach is more than fast enough
- Most readable and maintainable — critical for team collaboration
- Easiest to test — straightforward input → output mapping
- Approach 2's sort-and-cumsum is premature optimization at this scale
- Approach 3 ({collapse}) remains available as a drop-in optimization if needed later

## Function Signature

```r
compute_fgt(dt, poverty_lines, by = NULL)
```

**Parameters**:
- `dt`: data.table with at minimum `welfare` (numeric) and `weight` (numeric) columns, plus optional breakdown dimension columns
- `poverty_lines`: numeric vector of poverty line thresholds
- `by`: character vector of column names to group by (0–4 columns), or NULL for overall computation

**Returns**: data.table with columns:
- `poverty_line`: the threshold used
- `[by columns]`: one column per breakdown dimension (if any)
- `headcount`: FGT(0) — share of weighted population below the line
- `poverty_gap`: FGT(1) — average normalized shortfall
- `severity`: FGT(2) — average squared normalized shortfall
- `population`: total weighted population in the group

## Integration with Orchestrator

The `compute_measures()` orchestrator will call `compute_fgt()` for poverty measures alongside separate functions for other measure types:

```
compute_measures()
├── compute_fgt(dt, poverty_lines, by)       → poverty measures
├── compute_gini(dt, by)                     → inequality
├── compute_welfare_stats(dt, by)            → mean, median, percentiles
└── rbindlist() or merge results             → unified output table
```

Each measure function receives the same pre-processed data.table and `by` specification. This keeps measure functions independent and composable.

## Open Items

- **Totals and marginals**: Whether to include overall and single-dimension marginals alongside the full cross-tabulation — deferred for future decision
- **Optimization**: May revisit with {collapse} or sort-and-cumsum if benchmarking during implementation reveals bottlenecks
- **Validation benchmarks**: Need to be built from scratch since no existing grouped FGT benchmarks exist; can validate ungrouped FGT against {wbpip} outputs

## Next Steps

1. **Implement `compute_fgt()`** — core function with tests
2. **Build validation fixtures** — small test datasets with hand-computed expected FGT values (ungrouped and grouped)
3. **Cross-validate with {wbpip}** — ensure ungrouped FGT matches existing implementation
4. **Design output schema** — standardized output format shared across all measure functions
5. **Implement remaining measure functions** — Gini, welfare stats, following the same pattern
