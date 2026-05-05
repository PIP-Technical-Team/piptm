---
date: 2026-04-29
title: "Pipeline Comparison Benchmark Results"
status: completed
tags: [performance, arrow, io, benchmark, pipeline]
---

# Pipeline Comparison Benchmark Results

## Environment

- **R version**: R version 4.5.2 (2025-10-31 ucrt)
- **Platform**: Windows Server 2022 x64 (build 20348) / x86_64-w64-mingw32/x64
- **Logical cores**: 16
- **arrow version**: 23.0.1.2
- **collapse version**: 2.1.6
- **data.table version**: 1.18.2.1

## Configuration

- **Surveys per iteration**: 10
- **Iterations**: 50
- **Measures**: `headcount`, `sum`, `median`, `gini`
- **Poverty lines**: 3, 4.1
- **By dimensions**: `age`, `gender`, `educat4`
- **Survey pool size**: 60 surveys (filtered from 60 total to those with all BY_DIMS)

## Approaches

| | I/O | Compute |
| --- | --- | --- |
| **A1 Baseline** | `load_surveys()` -- all 14 schema cols | `compute_measures()` for headcount, sum, median, gini |
| **A Column-pruned** | Arrow `select(6 cols)` before `collect()` | `compute_measures()` for headcount, sum, median, gini |
| **B Arrow push-down** | Scan 1: Arrow `group_by+summarise` -> tiny aggregated table (one row/group, 6 cols); Scan 2: Arrow `select(6 cols)` -> full microdata | headcount + sum: R arithmetic on aggregated table; gini + median (not Arrow-feasible for PIP): `compute_measures()` |

## Arrow Push-Down Feasibility

| Measure | Arrow-feasible? | Reason |
| --- | --- | --- |
| headcount | YES | Conditional sum / total weight -- scalar aggregate |
| sum | YES | sum(welfare * weight) -- scalar aggregate |
| median | NO (Arrow unweighted only) | Arrow median() is unweighted; PIP requires collapse::fmedian |
| gini | NO | Lorenz curve requires sorted welfare vector; not a scalar aggregate |

## Results

```
                  approach I/O med (s) CMP med (s) Total med (s) Total p25 (s) Total p75 (s)
                    <char>       <num>       <num>         <num>         <num>         <num>
1: A1: Baseline (all cols)        0.82       1.385         2.240         1.638         2.655
2:   A:  Column-pruned I/O        0.28       1.200         1.460         1.213         1.712
3:     B:  Arrow push-down        1.12       0.470         1.575         1.363         1.810
4:    C:  DuckDB push-down        0.96       0.475         1.530         1.145         1.807
```

## Speedup Summary

| Comparison | I/O speedup | Total speedup | Decision |
| ---------- | ----------- | ------------- | -------- |
| A vs A1 | 66% faster | 35% faster | ADOPT |
| B vs A | same as A | 8% slower | KEEP A |
| C vs A | same as A | 5% slower | KEEP A |

## Decisions

**A vs A1**: ADOPT column pruning in load_surveys(): Approach A is **35% faster** total vs A1 (I/O alone: 66% faster).

**B vs A**: KEEP Approach A compute path: Approach B is only 8% slower total vs A -- below the 15% threshold.

**C vs A**: KEEP Approach A compute path: Approach C is only 5% slower total vs A -- below the 15% threshold.

## Correctness

- A  vs A1 (full result):  PASS
- B  vs A1 (all measures): PASS
- C  vs A1 (all measures): PASS

*Generated: 2026-05-04 14:05*
