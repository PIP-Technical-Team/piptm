---
date: 2026-04-29
title: "Arrow I/O Strategy Benchmark Results"
status: completed
tags: [performance, arrow, io, benchmark]
---

# Arrow I/O Strategy Benchmark Results

## Environment

- **R version**: R version 4.5.2 (2025-10-31 ucrt)
- **Platform**: Windows Server 2022 x64 (build 20348) / x86_64-w64-mingw32/x64
- **Logical cores**: 16
- **Physical cores**: 16
- **arrow version**: 23.0.1.2
- **collapse version**: 2.1.6
- **data.table version**: 1.18.2.1
- **bench version**: 1.1.4

## Dataset

- **Mode**: Live (Arrow repository)
- **Surveys**: 15
- **Parquet files**: 15
- **Measures**: `headcount`, `gini`, `mean`, `median`
- **Poverty lines**: 2.15, 3.65
- **By dimensions**: `gender`, `area`
- **Iterations**: 50

## Column Pruning Surface

Schema has 14 columns. This benchmark requires 5 (`pip_id`, `welfare`, `weight`, `gender`, `area`). 9 columns dropped by IO-2/IO-3: `country_code`, `surveyid_year`, `welfare_type`, `version`, `survey_acronym`, `educat4`, `educat5`, `educat7`, `age`.

## Results

```
         approach    phase median (s) p25 (s) p75 (s) mem (MB)
           <char>   <char>      <num>   <num>   <num>    <num>
1:  IO-1 Baseline I/O only      1.658   1.640   1.689    808.1
2:    IO-2 Select I/O only      0.524   0.536   0.620    319.5
3:      IO-3 Sort I/O only      3.337   3.306   3.374    395.8
4: E2E-1 Baseline      E2E      3.648   3.605   3.680   2217.8
5:   E2E-2 Select      E2E      2.473   2.345   2.517   1729.4
6:     E2E-3 Push      E2E      4.196   4.070   4.239   2011.5
```

## Speedup Summary

| Comparison | Speedup | Decision |
| ---------- | ------- | -------- |
| IO-2 vs IO-1 (Select vs Baseline I/O) | 68% faster | ✅ ADOPT |
| E2E-2 vs E2E-1 (Select vs Baseline E2E) | 32% faster | ✅ ADOPT |
| E2E-3 vs E2E-2 (Push vs Select E2E) | 70% slower | ❌ SKIP |
| Arrow sort overhead (IO-3 − IO-2) / E2E-1 | 77% |  ⚠️ INVESTIGATE |

## Decisions

**I/O Select**: ADOPT column pruning: IO-2 is **68% faster** than IO-1 (1.658s → 0.524s), exceeding the 30% threshold.

**E2E Select**: ADOPT column pruning in load_surveys(): E2E-2 is **32% faster** than E2E-1 (3.648s → 2.473s).

**Arrow Push**: SKIP Arrow push: E2E-3 is only 70% slower than E2E-2 (2.473s → 4.196s), below the 20% threshold. Performs 1 + 2 Arrow scans.

**Arrow Sort**: WORTH INVESTIGATING: Arrow sort + cast overhead is 77% of E2E-1 (2.812s extra over IO-2). Note: this is an upper bound — IO-3 also casts dict cols to string before sorting; true sort-only overhead is lower. Next step: implement pre-sorted I/O and skip setorder() in compute_inequality().

## Correctness

- E2E-2 vs E2E-1 (full result): ✅ PASS
- E2E-3 vs E2E-1 (mean+headcount only): ✅ PASS

*Generated: 2026-04-29 14:39*
