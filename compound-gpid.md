---
project-name: "Table Maker"
created: "2026-03-31"
last-reviewed: "2026-08-20"
---

# Table Maker

## Objective

This project is building a fast, efficient computation engine that generates poverty, inequality, and welfare statistics from harmonized PIP survey data, producing cross-tabulated results across dimensions like gender, education, and geography. It is designed for the PIP platform and is intended for researchers, policymakers, and analysts who need reliable, disaggregated socioeconomic indicators.

## Key Deliverables

- An R package ({piptm}) implementing the computation engine
- A Plumber API service exposing `table_maker()` with session surveys, categories/covariates endpoints, and insomnia test collections
- New data preparation functions in the {pipdata} package to clean survey data and generate Arrow/Parquet partitions
- A user interface in the PIP platform to display and interact with the generated tables

## Constraints

- Reproducibility: All computations must be fully reproducible from the underlying survey data and parameter inputs
- Methodological consistency: Poverty, inequality, and welfare measures must strictly follow PIP/World Bank methodological standards
- Deterministic outputs: Identical inputs must always produce identical results
- Scalability constraints: Code must handle large, partitioned datasets efficiently (Arrow/Parquet) and support batch execution across many surveys
- Interoperability: Outputs must conform to schemas expected by downstream consumers

## Current Focus

Phase 1 (Computation Engine) and Phase 2 (API service) are **complete**. The full `table_maker()` pipeline is implemented, tested (414 passing), and optimised. The Plumber API exposes `table_maker()` with session surveys, categories/covariates endpoints, and insomnia test collections (validated on `test-endpoints` branches).

Key optimisations already landed:
- `compute_measures()` handles multi-survey batches using a compound `GRP(c("pip_id", by))` — single GRP construction regardless of batch size.
- `table_maker()` uses a single batch call + keyed metadata join (Approach B). Verified 72% faster (0.12 s vs 0.43 s for 15 surveys).
- `collapse::set_collapse(nthreads = min(4L, physical_cores))` set on load.
- Column-pruned Arrow reads (6 cols vs 14) — 68% faster I/O.
- Pop-share threshold suppression and survey-specific variable filtering implemented.

Next priorities:
- Description file implementation (brainstormed 2026-08-13)
- Validation & documentation pass (`devtools::check()` clean, CRAN-ready)
- {pipdata} harmonization pipeline alignment
