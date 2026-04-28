---
date: 2026-04-28
title: "Orchestration Strategy Benchmark Results"
status: completed
tags: [performance, orchestration, table-maker, benchmark]
---

# Orchestration Strategy Benchmark Results

## Environment

- **R version**: R version 4.5.2 (2025-10-31 ucrt)
- **Platform**: Windows Server 2022 x64 (build 20348) / x86_64-w64-mingw32/x64
- **Logical cores**: 16
- **Physical cores**: 16
- **collapse version**: 2.1.6
- **data.table version**: 1.18.2.1
- **bench version**: 1.1.4

## Dataset

- **Mode**: Live (Arrow repository)
- **Surveys**: 15
- **Total rows**: 336,316
- **Measures**: 4
- **Poverty lines**: 2.15, 3.65, 6.85
- **Dimensions**: gender, area, educat4

## I/O Profile

Median `load_surveys()` time for 15 surveys: **0.157 seconds**.

> I/O budget used: 5.2% of the 3s target. 2.84 seconds remain for compute.

## Compute Benchmark Results

```
    approach nthreads median (s) mem (MB) itr/sec meets_3s
      <char>    <int>      <num>    <num>   <num>   <lgcl>
 1:        A        1      0.432    164.6    2.37     TRUE
 2:        B        1      0.127    122.7    7.86     TRUE
 3:        C        1      0.334    136.4    3.01     TRUE
 4:        E        1      0.331    162.8    2.96     TRUE
 5:        A        2      0.358    163.3    2.77     TRUE
 6:        B        2      0.124    122.7    8.02     TRUE
 7:        C        2      0.338    136.4    2.88     TRUE
 8:        E        2      0.368    162.8    2.73     TRUE
 9:        A        4      0.359    163.3    2.80     TRUE
10:        B        4      0.120    122.7    8.33     TRUE
11:        C        4      0.335    136.4    2.99     TRUE
12:        E        4      0.356    162.8    2.82     TRUE
```

## Correctness Check

- A vs B: **PASS** (identical)
- A vs C: **PASS** (identical)
- A vs E: **PASS** (identical)

## Decision

RECOMMENDATION: Use Approach B (nthreads = 4, 0.12s). It is 72% faster than the simpler Approach A (0.43s) and exceeds the 40% speedup threshold.

## Notes

- Iterations per approach × nthreads cell: 5
- nthreads tested: 1, 2, 4
- `check = FALSE` in `bench::mark()` — correctness verified separately.
- Preference order (simplest → most complex): A > E > B > C.
- See `benchmarks/orchestration-results.png` for the timing chart.

