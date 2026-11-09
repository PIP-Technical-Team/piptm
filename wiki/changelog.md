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
<!-- cg:auto:end -->

← [Home](README.md)
