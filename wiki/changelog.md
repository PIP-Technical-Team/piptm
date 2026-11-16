# Changelog

<!-- cg:auto:version-history -->
## Version History

See `NEWS.md` in the package root for the authoritative, maintained version
history. This section mirrors high-level, user-facing changes as they are
captured through the compound-docs workflow.

Current state (as of this wiki's initialization): Phase 1 (Computation
Engine) is complete — the full `table_maker()` pipeline is implemented,
tested (414 passing at time of writing), and optimized (multi-survey batch
computation via compound `GRP()`, single batch call + keyed metadata join in
`table_maker()`, `collapse::set_collapse(nthreads = ...)` configured on
load).

- 2026-10-26 — `/table` API endpoint response reshaped to a per-survey,
  dense `cells[]` layout (Approach C) via the new `serialize_table_response()`
  function in `R/table_response.R`, replacing flat rows. During this work a
  `data.table` `by=` gotcha was found and fixed: passing a *named* character
  vector to `by=` silently groups by the vector's values but renames the
  resulting columns to the vector's names, rather than erroring — always
  `unname()` a programmatically-built variable-name vector before using it
  as `by=`.
- 2026-10-26 — `POST /table/download` now accepts Approach C `/table` payloads
  and exports deterministic Shape A rows through the stateless
  `flatten_table_response()` module in `R/table_flatten.R`. Boundary
  validation rejects malformed scalar headers, duplicate or reserved names,
  invalid cell slots, and cross-survey schema mismatches; formatted measure
  strings (including `"NR"` and JSON `null`) are preserved and malformed
  requests receive the standard HTTP 400 JSON error envelope.
- 2026-11-09 — `GET /cell-definition` and `build_cell_explanation()` in
  `R/cell_explanation.R` adopt bounded adaptive prose enumeration for
  `what_a_cell_is$items`. Tables with 4 or fewer cells emit full Cartesian
  cell descriptions, while tables with more than 4 cells factorize into one
  line per active layout slot (guaranteeing <= 4 items and preventing text
  bloat). Plumber array unboxing is guarded via `as.list()` so single-element
  lists serialize as JSON arrays, slot categories intersect with `filter_base`,
  and unresolved registry fallbacks omit false cell counts.
- 2026-11-13 — metadata direct callers and compute paths now share a single
  table-parameter validation boundary through
  `.validate_and_normalize_table_params()` in `R/table_params.R`. Slot
  guardrails (`column`/`row`/`superColumn`/`superRow`) now enforce scalar
  non-empty values, contiguous left-to-right fill, and duplicate rejection at
  shared-validation time; `by` values containing `pov_status` require a single
  positive finite `poverty_line`; and `filter_base = list()` normalizes to
  `NULL` as intentional no-filter semantics. `build_description_model()` now
  reuses one-time role-specific registry slices to avoid repeated full-registry
  scans, and regression coverage was expanded in
  `tests/testthat/test-table-params.R` and
  `tests/testthat/test-description-builder.R`.
<!-- cg:auto:end -->

← [Home](README.md)
