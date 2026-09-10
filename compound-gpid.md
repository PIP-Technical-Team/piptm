---
project-name: "Table Maker"
created: "2026-03-31"
---

# Table Maker

## Objective

This project is building a fast, efficient computation engine that generates poverty, inequality, and welfare statistics from harmonized PIP survey data, producing cross-tabulated results across dimensions like gender, education, and geography. It is designed for the PIP platform and is intended for researchers, policymakers, and analysts who need reliable, disaggregated socioeconomic indicators.

## Key Deliverables

- An R package ({piptm}) implementing the computation engine
- New data preparation functions in the {pipdata} package to clean survey data and generate Arrow/Parquet partitions
- An API service enabling communication between the PIP platform and the computation engine
- A user interface in the PIP platform to display and interact with the generated tables

## Constraints

- Reproducibility: All computations must be fully reproducible from the underlying survey data and parameter inputs
- Methodological consistency: Poverty, inequality, and welfare measures must strictly follow PIP/World Bank methodological standards
- Deterministic outputs: Identical inputs must always produce identical results
- Scalability constraints: Code must handle large, partitioned datasets efficiently (Arrow/Parquet) and support batch execution across many surveys
- Interoperability: Outputs must conform to schemas expected by downstream consumers

## Architecture Notes

The {piptm} package follows a **Manifest-First with Lazy Validation** architecture. On load, it reads all `manifest_*.json` files from `PIPTM_MANIFEST_DIR` into memory (keyed by release ID). No microdata is loaded at startup — all loading is on-demand per `table_maker()` call.

The data pipeline is:

```
Raw survey microdata
→ {pipdata} harmonization
→ Clean survey datasets (.qs2)
→ Arrow/Parquet partitions (partitioned by country_code / year / welfare_type)
→ Release manifest (reproducibility contract)
→ {piptm} computation engine
→ Structured cross-tabulated measure outputs
```

Key internal components:
- `load_survey_microdata()` — manifest lookup + lazy file validation + Arrow loading
- `compute_fgt()`, `compute_gini()`, `compute_mean_welfare()`, etc. — core measure functions
- `compute_measures()` — orchestrator across surveys and breakdown dimensions
- `table_maker()` — top-level API function

## Current Focus

Phase 1 (Computation Engine) is **complete**. The full `table_maker()` pipeline
is implemented, tested (414 passing), and optimised:

- `compute_measures()` now handles multi-survey batches using a compound
  `GRP(c("pip_id", by))` — single GRP construction regardless of batch size.
- `table_maker()` uses a single batch call + keyed metadata join (Approach B),
  replacing the per-survey `lapply()` loop. Verified 72% faster (0.12 s vs
  0.43 s for 15 surveys).
- `collapse::set_collapse(nthreads = min(4L, physical_cores))` set on load.

Next priorities (Phase 2 / Phase 3):
- API service layer connecting PIP platform to `table_maker()`
- Validation & documentation pass (`devtools::check()` clean, CRAN-ready)
- {pipdata} harmonization pipeline alignment

## Roadmap

- **Phase 0** — Arrow dataset generation: Master Arrow repository, partition pipeline in {pipdata}, release manifest generation (2–3 weeks)
- **Phase 1** — Computation engine: `.onLoad()` manifest system, measure functions (FGT, Gini, welfare stats), `compute_measures()` orchestrator (4–6 weeks)
- **Phase 2** — Data integration & release management: `load_survey_microdata()`, manifest inspection utilities, `table_maker()` top-level API (2–3 weeks)
- **Phase 3** — Validation & documentation: unit tests (≥90% coverage), performance benchmarks, developer and user docs

## Related Resources

- `docs/project-context.md` — Detailed architecture, manifest schema, computation engine design
- `docs/roadmap.md` — Phased implementation plan
- `inst/schema/arrow-schema.json` — Arrow partition schema
- PIP platform: <https://pip.worldbank.org>
