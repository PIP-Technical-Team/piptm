---
date: 2026-06-01
title: "Adding new measures to the computation engine via the registry pattern"
category: "testing-patterns"
language: "R"
tags: [measures, registry, extensibility, compute-welfare, collapse, GRP]
root-cause: "N/A — pattern documentation, not a bug fix"
severity: "P3"
---

# Adding New Measures to the Computation Engine

## Problem

Need to extend the computation engine with new measures (`obs_share`,
`pop_share`) without breaking existing consumers (API endpoints, validators,
tests, orchestrator).

## Root Cause

N/A — this documents the successful extensibility pattern.

## Solution

The `.MEASURE_REGISTRY` in `R/measures.R` is the **single source of truth**.
All downstream consumers reference it dynamically:

1. **Register** in `.MEASURE_REGISTRY` (family assignment).
2. **Implement** in the family function (`compute_welfare()` for welfare family).
3. **Add to `all_welfare_measures`** vector inside the family function.
4. **Tests**: update the "computes all N measures" test + add measure-specific tests.

No changes needed in:
- `compute_measures()` — dispatches by family automatically.
- API endpoints (`/table`, `/measures`) — use `piptm::pip_measures()` dynamically.
- `validate_table_input()` — validates against `names(piptm::pip_measures())`.
- `table_maker()` — passes measures through unchanged.

### Share measures implementation detail

For ratio measures (cell value / survey total), the pattern is:

```r
# Coarser GRP for survey-level denominators
grp_survey <- collapse::GRP(dt, by = "pip_id")
# Map each cell-group to its survey
survey_of_cell <- collapse::GRP(grp$groups, by = "pip_id")
# Broadcast survey totals to cell groups via index lookup
vals[["obs_share"]] <- nobs_cell / nobs_survey[survey_of_cell$group.id]
```

Key insight: `GRP(grp$groups, by = "pip_id")` creates a mapping from cell
groups to their parent survey, enabling vectorized broadcast of survey-level
totals without any row-level expansion.

### Reuse existing computations

When a measure depends on a value already computed for another measure (e.g.,
`obs_share` needs `nobs`), check `vals[["nobs"]]` before recomputing:

```r
nobs_cell <- if (!is.null(vals[["nobs"]])) vals[["nobs"]] else as.double(collapse::fnobs(welfare_v, g = grp))
```

## Prevention

- Never hardcode measure lists outside `.MEASURE_REGISTRY`.
- Never hardcode measure counts in docstrings (use generic `@return A named character vector.`).
- When adding ratio measures, always define the denominator scope clearly (per-survey vs per-batch).
- Always run `test-measures.R` after registry changes — it guards the count and set.

## Related

- `.cg-docs/brainstorms/2026-06-01-cell-share-measures.md` — decision record
- `docs/project-context.md` — architecture overview
- `.cg-docs/brainstorms/2026-04-07-computation-engine-design.md` — original engine design
