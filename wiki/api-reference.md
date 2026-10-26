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
- `flatten_table_response()` (`R/table_flatten.R`) — validates and flattens
  an Approach C `/table` payload into deterministic Shape A rows for
  `POST /table/download`. It preserves formatted measure strings (including
  `"NR"` and JSON `null`), validates headers, layouts, cell slots, name
  collisions, and cross-survey schema equality, and is registry-free.
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
platform consumers (see `inst/schema/arrow-schema.json`). The `/table` API
uses Approach C (per-survey `layout` plus dense `cells[]`), while
`POST /table/download` accepts that payload and exports deterministic Shape A
rows. Malformed payloads are rejected at the boundary with the standard HTTP
400 JSON error envelope rather than being silently coerced.
<!-- cg:auto:end -->

← [Home](README.md)
