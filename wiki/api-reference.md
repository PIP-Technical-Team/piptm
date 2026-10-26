# API Reference

<!-- cg:auto:functions -->
## Core Functions

- `table_maker()` — top-level API function. Orchestrates manifest lookup,
  lazy microdata loading, and computation of poverty, inequality, and
  welfare measures across one or more surveys and breakdown dimensions.
- `compute_measures()` — orchestrator across surveys and breakdown
  dimensions; batches multi-survey computation using a compound
  `GRP(c("pip_id", by))`.
- `compute_fgt()` — computes Foxy-Greer-Thorbecke (FGT) poverty measures.
- `compute_gini()` — computes the Gini inequality coefficient.
- `compute_mean_welfare()` — computes mean welfare statistics.
- `load_survey_microdata()` — manifest lookup, lazy file validation, and
  Arrow-backed loading of survey microdata.
- `serialize_table_response()` (`R/table_response.R`) — reshapes `/table`
  API endpoint output into the per-survey, dense `cells[]` layout (Approach
  C): one object per survey with a dense `cells` array instead of flat
  rows. Uses a grouped `data.table` split (`by = dim_varnames`) to build
  unique cell groupings efficiently.
<!-- cg:auto:end -->

<!-- cg:auto:parameters -->
## Parameters

Common parameters across measure functions:

- `survey_id` — identifier(s) of the survey(s) to compute measures for.
- `by` — character vector of breakdown dimensions (e.g. `gender`,
  `education`, `geography`) to cross-tabulate results against.
- `measures` — character vector naming which measures to compute (e.g.
  `"fgt"`, `"gini"`, `"mean_welfare"`).

Refer to each function's roxygen2 documentation (`?table_maker`,
`?compute_measures`, etc.) for full parameter definitions.
<!-- cg:auto:end -->

<!-- cg:auto:return-values -->
## Return Values

`table_maker()` and `compute_measures()` return structured, cross-tabulated
measure outputs conforming to the schemas expected by downstream PIP
platform consumers (see `inst/schema/arrow-schema.json`).
<!-- cg:auto:end -->

← [Home](README.md)
