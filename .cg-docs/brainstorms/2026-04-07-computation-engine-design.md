---
date: 2026-04-07
title: "Computation Engine Design — Family-Based Orchestrator"
status: decided
chosen-approach: "Family-Based Orchestrator with long-format output"
depends-on:
  - "2026-03-16-data-pipeline-architecture.md"
  - "2026-03-17-fgt-computation-design.md"
tags: [computation, engine, orchestrator, measures, phase-1]
---

# Computation Engine Design

## Context

With Phase 0 (Arrow dataset generation, manifest system, data loading) largely
in place, the project needs to design the computation engine that takes
partitioned survey microdata as input and produces poverty, inequality, and
welfare statistics disaggregated across available dimensions.

The manifest → load → compute pipeline already exists:

1. User requests data for a specific (country_code, year, welfare_type, release)
2. The manifest for that release is filtered to retain relevant entries
3. `load_surveys()` loads the corresponding partitioned data
4. **The computation engine processes this data and returns indicators** ← this brainstorm

## Requirements

### Indicator Set (9 measures, 3 families)

**Poverty Measures** (all require external poverty lines):
- `headcount` — FGT(α=0): share of weighted population below the line
- `poverty_gap` — FGT(α=1): average normalized shortfall
- `severity` — FGT(α=2): average squared normalized shortfall
- `watts` — Watts index: weighted average of log(z/y) for the poor
- `pop_poverty` — total weighted population below the line

**Inequality Measures** (no poverty line):
- `gini` — Gini coefficient
- `mld` — Mean Log Deviation (Theil-L)

**Welfare Measures** (no poverty line):
- `mean` — weighted mean (`fmean`)
- `median` — weighted median (`fmedian`)
- `sd` — weighted standard deviation (`fsd`)
- `var` — weighted variance (`fvar`)
- `min` — minimum observed welfare (`fmin`)
- `max` — maximum observed welfare (`fmax`)
- `nobs` — unweighted observation count (`fnobs`)
- `p10` — weighted 10th percentile (`fnth(0.10)`)
- `p25` — weighted 25th percentile (`fnth(0.25)`)
- `p75` — weighted 75th percentile (`fnth(0.75)`)
- `p90` — weighted 90th percentile (`fnth(0.90)`)

**Deferred measures** (out of scope for this project phase): phase):
- `societal_poverty_rate` — depends on median + PPP-year-specific constants.
  Requires intra-computation dependency chain.
- `prosperity_gap` — average shortfall from prosperity standard. Depends on
  PPP-year-specific prosperity standard.

### Multi-Measure API

Users can request **multiple measures** in a single call. The engine computes
all requested measures and returns a unified result. This supersedes the earlier
project-context doc statement of "one measure at a time."

### Disaggregation

- Up to 4 breakdown dimensions per request
- Available dimensions: `gender`, `area`, `educat4`, `educat5`, `educat7`, `age`
- Education: only one education column per request (educat4 OR educat5 OR educat7)
- Output is **full cross-tabulation only** — no marginals, no totals
- Age bins (computed at query time, not stored in data):
  - 0–14 (children)
  - 15–24 (youth)
  - 25–64 (working age)
  - 65+ (elderly)

### Pre-processing Contract

- Welfare is already PPP-adjusted when loaded from Arrow
- Weights are already clean and positive
- Dimension columns are already standardized
- **Only transformation needed**: age → age bins (via `.bin_age()` helper)

### Output Format

- **Long format throughout** — both internal computation and final
  `table_maker()` output. One row per (survey × poverty_line × dimension_combo
  × measure).
- Long format maps directly to JSON for the API layer with zero transformation
  (`jsonlite::toJSON()` on the result data.table produces the API payload).
- `poverty_line` column is always present. Set to `NA_real_` (serializes to
  JSON `null`) for non-poverty measures. This keeps the schema consistent
  across all rows without redundancy.
- No wide-format pivot is needed anywhere in the pipeline.

### Zero-Welfare Handling

Both Watts and MLD require `log()`, which is undefined at zero. A documented
rule is needed. Options:
- Exclude zero-welfare rows from Watts/MLD computation
- Return NA for groups where zero-welfare rows exist
- **Decision: TBD during implementation — must be consistent across both
  `compute_poverty()` (Watts) and `compute_inequality()` (MLD)**

### Per-Survey Computation

Measures are computed **per survey** (per `pip_id`), not pooled across surveys.
The output contains one block of cross-tabulated rows per survey.

## Approaches Considered

### Approach 1: Monolithic Orchestrator

A single `compute_measures()` dispatches to individual `compute_*()` functions
(one per measure), each independently processing the data.

**Pros:**
- Simple control flow
- Easy to understand

**Cons:**
- Redundant computation: all 5 poverty measures share the same cross-join,
  gap, and is_poor flag, but each function recomputes them independently
  (~5 cross-joins + 5 grouped aggregations vs. 1 of each)
- Mixing poverty measures (with poverty_line dimension) and non-poverty
  measures (without) in one output is structurally awkward
- ~9 full data passes for a typical multi-measure request vs. ~3

**Effort:** Medium

### Approach 2: Family-Based Orchestrator ✅

Three family-level compute functions (`compute_poverty()`,
`compute_inequality()`, `compute_welfare()`), each computing all measures
in its family in a single pass. The orchestrator dispatches to relevant
families and merges results.

**Pros:**
- Computational efficiency: measures within a family share intermediate state
  (cross-join, gap, sort) — computed once, not per-measure
- ~3 full data passes for a typical multi-measure request (vs. ~9 for Approach 1)
- Clean separation of concerns by computational pattern
  (gap-based / sort-based / simple aggregation)
- Natural handling of poverty_line asymmetry between families
- Extensible: new measures added to existing families; new families added
  with one function + one dispatch line
- Each family independently testable

**Cons:**
- More functions to maintain (3 families + orchestrator vs. 1 monolithic)
- Long-to-wide pivot required before returning (trivial cost)
- Gini requires sorting per group — could surface as bottleneck with many
  groups (unlikely at <1K groups)
- Zero-welfare rule must be consistent across compute_poverty() and
  compute_inequality() — risk of drift

**Effort:** Medium

### Approach 3: Arrow-Native Lazy Pipeline

Keep data in Arrow format, use dplyr grouped summarization on Arrow Tables
directly, only collect the final aggregated result.

**Pros:**
- Lower memory footprint (data not fully materialized)

**Cons:**
- Arrow compute kernels do NOT support weighted median, Gini
  (sort + cumulative sums), Watts (conditional log), or MLD — must collect()
  for 7/9 measures anyway, eliminating the memory benefit
- Significantly more complex — fighting Arrow limitations
- Premature optimization — survey data is thousands to ~100K rows, not millions
- Breaks the data.table idiom the project is built on

**Effort:** Large
**Recommended?** No

## Decision

**Approach 2: Family-Based Orchestrator** was chosen.

Rationale:
- At this data scale, the primary efficiency concern is minimizing redundant
  computation, not raw throughput. Family grouping achieves ~3× fewer data
  passes than the monolithic approach for typical multi-measure requests.
- The three computational patterns (gap-based, sort-based, simple aggregation)
  are genuinely different — separating them makes each family function focused
  and testable.
- Arrow-native computation is infeasible for most measures and unnecessary
  at this data scale.
- Long-format output maps directly to JSON for the API layer with zero
  transformation — no pivot needed.

## Architecture

### Function Hierarchy

```
table_maker(country_code, year, welfare_type, measures, poverty_lines, by, release)
│
├─ Validate inputs (measures, dimensions, poverty_lines)
├─ Resolve manifest → filter entries
├─ load_surveys(entries_dt)
│     → data.table [welfare, weight, dimension cols, pip_id, ...]
│
├─ .bin_age(dt)  (if "age" in by)
│
├─ For each pip_id in dt:
│   └─ compute_measures(survey_dt, measures, poverty_lines, by)
│       │
│       ├─ Classify measures into families
│       ├─ if poverty family needed:
│       │     compute_poverty(dt, poverty_lines, by)
│       │       → long: poverty_line × by-combos × 5 measures
│       │
│       ├─ if inequality family needed:
│       │     compute_inequality(dt, by)
│       │       → long: by-combos × 2 measures
│       │
│       ├─ if welfare family needed:
│       │     compute_welfare(dt, by)
│       │       → long: by-combos × 2 measures
│       │
│       └─ rbindlist(family_results, fill = TRUE) → long format
│
├─ rbindlist() across surveys
└─ Return long-format data.table (JSON-ready)
```

### Output Schema (long format)

Every row represents one (survey × poverty_line × dimension_combo × measure)
tuple. Columns:

- `pip_id` (character) — survey identifier
- `country_code` (character)
- `surveyid_year` (integer)
- `welfare_type` (character)
- `poverty_line` (numeric) — the threshold; `NA_real_` for non-poverty measures
- 0–4 dimension columns, depending on request:
  - `gender` (character)
  - `area` (character)
  - `educat4` / `educat5` / `educat7` (character) — at most one
  - `age_group` (character) — binned from raw age
- `measure` (character) — canonical measure name
- `value` (numeric) — the computed statistic
- `population` (numeric) — total weighted population in the group

**JSON serialization example** (direct output of `jsonlite::toJSON()`):

```json
[
  {
    "country_code": "COL",
    "surveyid_year": 2010,
    "gender": "female",
    "poverty_line": 2.15,
    "measure": "headcount",
    "value": 0.15,
    "population": 5400
  },
  {
    "country_code": "COL",
    "surveyid_year": 2010,
    "gender": "female",
    "poverty_line": null,
    "measure": "gini",
    "value": 0.42,
    "population": 5400
  }
]
```

No redundancy: non-poverty measures appear once per group, not repeated
across poverty lines.

### Family Function Signatures

```r
compute_poverty(dt, poverty_lines, by = NULL)
# Returns: data.table (long) with columns:
#   poverty_line, [by cols], measure, value, population

compute_inequality(dt, by = NULL)
# Returns: data.table (long) with columns:
#   [by cols], measure, value, population

compute_welfare(dt, by = NULL)
# Returns: data.table (long) with columns:
#   [by cols], measure, value, population
```

### Inside compute_poverty()

All 5 poverty measures computed in a single grouped aggregation:

```r
# 1. Cross-join data × poverty_lines
# 2. Vectorized intermediates (one pass):
#      gap      = pmax((z - y) / z, 0)
#      is_poor  = (y < z)          # strictly less than
#      log_ratio = ifelse(is_poor & y > 0, log(z / y), 0)  # zero-welfare rule TBD
# 3. Single grouped aggregation:
#      headcount   = sum(w * is_poor) / sum(w)
#      poverty_gap = sum(w * gap) / sum(w)
#      severity    = sum(w * gap^2) / sum(w)
#      watts       = sum(w * log_ratio) / sum(w)
#      pop_poverty = sum(w * is_poor)
#      population  = sum(w)
# 4. melt() to long format
```

### Inside compute_inequality()

Per group, sorted welfare:

```r
# Gini: sort welfare, cumulative weight sums, trapezoid/covariance formula
# MLD:  sum(w * log(wmean / y)) / sum(w)   # zero-welfare rule TBD
```

### Inside compute_welfare()

Per group:

```r
# mean:   sum(w * y) / sum(w)
# median: sorted welfare, cumulative weight, interpolate at 0.5
```

## Open Items

1. **Zero-welfare handling for Watts and MLD** — must define a consistent rule
   (exclude zeros? return NA? cap at epsilon?) and apply it identically in
   `compute_poverty()` and `compute_inequality()`. Needs its own decision.

2. **Education dimension constraint** — at most one education column per request
   (educat4 OR educat5 OR educat7). Engine must validate this and error if
   multiple education columns are requested.

3. **Missing dimension handling** — when a survey lacks a requested dimension
   (manifest says it's unavailable), the engine should: skip that survey?
   return NA for that dimension? error? Needs a decision.

4. **Societal Poverty Rate and Prosperity Gap** — deferred. Both require
   methodology-specific constants (PPP-year-dependent) and intra-computation
   dependencies (SPR needs median first). Separate brainstorm needed.

5. **Validation benchmarks** — no existing grouped benchmarks for Gini, MLD,
   Watts, or median. {pipapi} may provide reference values for ungrouped
   measures. All grouped benchmarks must be built from scratch using
   hand-computed fixtures. No dependency on {wbpip}.

6. **Measure name registry** — need a canonical mapping from user-facing
   measure names (e.g., "headcount") to internal function dispatch. Should be
   a package-level constant.

## Next Steps

1. **Proceed to `/cg-plan`** to create a detailed implementation plan for the
   computation engine based on this design
2. **Brainstorm zero-welfare handling** — define the rule for Watts and MLD
3. **Brainstorm SPR and Prosperity Gap** — design the dependency chain and
   parameterization
4. **Implement `compute_poverty()`** — first family function, extends the
   existing FGT design with Watts and pop_poverty
5. **Implement `compute_inequality()`** — Gini + MLD
6. **Implement `compute_welfare()`** — mean + median
7. **Implement `compute_measures()`** — orchestrator with family dispatch
8. **Implement `table_maker()`** — top-level API with long-format output
