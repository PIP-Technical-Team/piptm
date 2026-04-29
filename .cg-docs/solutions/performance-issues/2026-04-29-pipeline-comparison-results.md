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

- **Surveys per iteration**: 15
- **Iterations**: 50
- **Measures**: `headcount`, `gini`, `mean`, `median`
- **Poverty lines**: 2.15, 3.65
- **By dimensions**: `gender`, `area`, `educat4`
- **Survey pool size**: 59 surveys (filtered from 60 total to those with all BY_DIMS)

## Approaches

| | I/O | Compute |
| --- | --- | --- |
| **A1 Current** | `load_surveys()` — all 14 schema columns | `compute_measures()` for headcount, gini, mean, median |
| **A2 Column-pruned** | Arrow `select(6 cols)` before `collect()` | `compute_measures()` for headcount, gini, mean, median |
| **A3 Hybrid** | Arrow `select(6 cols)` before `collect()` | data.table for mean + headcount; `compute_measures()` for gini + median |

## Results

```
                 approach I/O med (s) CMP med (s) Total med (s) Total p25 (s) Total p75 (s)
                   <char>       <num>       <num>         <num>         <num>         <num>
1: A1: Current (all cols)       1.030       1.315         2.370         1.965         2.905
2:  A2: Column-pruned I/O       0.405       1.270         1.675         1.442         2.005
3:     A3: Hybrid compute       0.400       1.195         1.625         1.430         1.867
```

## Speedup Summary

| Comparison | I/O speedup | Total speedup | Decision |
| ---------- | ----------- | ------------- | -------- |
| A2 vs A1 | 61% faster | 29% faster | ✅ ADOPT |
| A3 vs A2 | same as A2 | 3% faster | ❌ KEEP A2 |

## Decisions

**A2 vs A1**: ADOPT column pruning in load_surveys(): A2 is **29% faster** total vs A1 (I/O alone: 61% faster).

**A3 vs A2**: KEEP A2 compute path: A3 is only 3% faster total vs A2 — below the 15% threshold.

## Correctness

- A2 vs A1 (full result):  ✅ PASS
- A3 vs A1 (all measures): ✅ PASS

*Generated: 2026-04-29 16:16*
