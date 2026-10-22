---
date: 2026-10-22
depth: data-risk
type: standard
plan: .cg-docs/plans/2026-10-20-pop-share-nr-masking.md
findings:
  P0.1: fixed
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P1.5: fixed
  P1.6: fixed
  P1.7: fixed
  P1.8: fixed
  P1.9: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P2.5: fixed
  P2.6: fixed
  P2.7: skipped
  P2.8: skipped
  P2.9: skipped
  P2.10: fixed
  P2.11: fixed
  P2.12: fixed
  P2.13: fixed
  P2.14: fixed
  P3.1: fixed
  P3.2: fixed
  P3.3: skipped
  P3.4: fixed
  P3.5: skipped
---

## Review Report

**Review mode**: data-risk
**Scope**: diff `e6eae4b..HEAD` (branch `new_pop_threshold`), implementing `.cg-docs/plans/2026-10-20-pop-share-nr-masking.md` (Phases 1 and 2)
**Files reviewed**: 27 (core: `R/table_maker.R`, `R/format_table_output.R`, `R/measures.R`, `inst/plumber/plumber.R`; tests: 5 files; docs: `docs/api-contract.md`, `SWAGGER_GUIDE.md`, `NEWS.md`, `man/*.Rd`; plan/roadmap/active-state artifacts)
**Findings**: 30 (P0: 1, P1: 9, P2: 14, P3: 6)

### P0 — BLOCKING (immediate remediation required)

- **[P0.1]** [cg-data-quality] `inst/plumber/plumber.R:290-291` — `/table/download` silently corrupts data when a client round-trips a `/table` JSON response directly into it.
  **Why**: `/table` now returns `value` as a formatted **character string** (`"NR"` for masked cells; percentage-family measures pre-scaled ×100, e.g. `"45.67"`; `null`/`NA` for genuine missing). `/table/download` does `data.table::set(tbl, j = "value", value = suppressWarnings(as.numeric(tbl[["value"]])))`. `as.numeric("NR")` silently becomes `NA` — indistinguishable from genuine missing data, destroying the disclosure-control signal. For non-masked percentage rows, the re-materialized numeric value is the already-×100-scaled percentage (e.g. `45.67`), not the raw proportion (`0.4567`) `table_maker()` produces — a silent unit/scale corruption for any consumer who treats `/table/download` output as raw statistical data. This is new: before this diff `/table`'s `value` was always a raw numeric, so this round-trip was safe.
  **Fix**: Either (a) make `/table/download` reject/pass-through the `"NR"` sentinel literally instead of coercing to numeric, and stop re-scaling already-formatted percentage strings, or (b) explicitly document that `/table/download` only accepts raw (pre-`format_table_output()`) `table_maker()` payloads, not the public `/table` JSON contract, and validate that at the endpoint boundary.

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-reproducibility] `R/table_maker.R` + `inst/plumber/plumber.R` — Default `pop_share_threshold` silently changed `0.01` → `0.001` (10× more permissive) for every caller/consumer that doesn't pass an explicit value.
  **Why**: Per the project charter ("Deterministic outputs: Identical inputs must always produce identical results"), this is a breaking reproducibility change: cells with pop_share between `0.001` and `0.01` flip from masked to exposed for identical input data. `NEWS.md` lists it as one bullet among several unrelated changes without flagging the statistical/reproducibility impact or a migration note.
  **Fix**: Add a dedicated `NEWS.md` "Breaking changes" entry explaining the statistical rationale for the new default and advising downstream pipelines to pin `pop_share_threshold` explicitly rather than rely on the default.

- **[P1.2]** [cg-reproducibility] `R/table_maker.R` — Masking scope reversed: share-family measures (`pop_share`, `target_within_group_share`, `target_survey_share`) were previously exempted from suppression (retained so callers could inspect the suppressed group's distribution); this diff removes that exemption and masks them like every other measure.
  **Why**: This is a second, independent behavioral reversal bundled into the same change as the threshold-value change, which complicates auditing which change caused any observed output difference, and is not called out separately from the threshold-default change in `NEWS.md`.
  **Fix**: Add an explicit `NEWS.md` bullet describing the share-measure masking reversal separately from the threshold-default bullet, and confirm the plan/brainstorm captured this as an intentional decision.

- **[P1.3]** [cg-reproducibility] [cg-architecture] `R/table_maker.R` — `population` column removed entirely from `table_maker()`'s returned table (not just from the formatted API response).
  **Why**: This changes the computation engine's output schema/contract for every direct caller (scripts, other packages), not only the `/table` API layer. Removing a previously documented output column with no backward-compatible fallback or deprecation period is a breaking change to the computation engine's interface, not merely a display/formatting change.
  **Fix**: Add an explicit `NEWS.md` "Breaking changes" bullet for the column removal; confirm (already partially verified: no internal `compute_*` function reads `table_maker()`'s *output* `population` column) that no other first-party caller depends on it before merge.

- **[P1.4]** [cg-architecture] `R/table_maker.R`, `R/format_table_output.R` — The two-step contract (`table_maker()` returns raw `value = NA_real_` + `flag = "NR"`; `format_table_output()` must be called separately to turn that into the `"NR"` display string) is enforced only by convention — `table_maker()`'s own roxygen doesn't state that downstream consumers must call `format_table_output()` before treating `NA` values as genuinely missing vs. masked.
  **Why**: Any new direct caller of `table_maker()` (batch scripts, other R packages, a future CLI) that doesn't also call `format_table_output()` will see `value = NA_real_` for suppressed cells and may silently treat masked cells as ordinary missing data in downstream aggregation or plots — a correctness risk for welfare/poverty statistics.
  **Fix**: Add a `@seealso` cross-reference between the two Rd files and a `@section Downstream formatting` note on `table_maker()` stating that raw `NA` + `flag = "NR"` output requires `format_table_output()` (or equivalent) before user-facing display.

- **[P1.5]** [cg-testing] `tests/testthat/test-table-maker-suppression.R` — No test for the "all cells suppressed" edge case (every group in a `by`-disaggregated result falls below threshold).
  **Why**: Every existing fixture leaves at least one unmasked cell. A regression that assumed at least one surviving row (e.g., in `setcolorder`/column-ordering logic) would not be caught.
  **Fix**: Add a fixture where all groups are masked and assert all rows are retained (not dropped) with `flag == "NR"` and `value` all `NA`.

- **[P1.6]** [cg-testing] [cg-data-quality] `tests/testthat/test-table-maker-suppression.R` — No multi-dimension `by` test with partial-NA keys across two or more dimensions simultaneously (only a single-dimension NA-key case is tested).
  **Why**: The masking join relies on `data.table`'s NA-matches-NA join semantics across `cell_keys`; a regression in multi-column NA join matching (a known `data.table` pitfall) could silently over-mask or under-mask cells and would not be caught by current tests.
  **Fix**: Add a fixture with `by = c("gender", "area")` where NA appears independently in each dimension and assert only the intended cells are masked, with no cross-contamination between independent NA-dimension cells.

- **[P1.7]** [cg-testing] `tests/testthat/test-api-endpoints.R` — The "GET /table returns 422 envelope when formatter fails" test only exercises a *mocked* `format_table_output()` that throws an injected error string; it never exercises a real failure path through the actual endpoint code.
  **Why**: This proves the plumber error-handling wrapper converts any thrown error into a 422 envelope, but does not prove `format_table_output()` itself ever throws for real, naturally-occurring malformed input from `table_maker()`. A future `table_maker()` schema change that should trigger this path could go uncaught.
  **Fix**: Add a second, non-mocked test that drives a genuinely malformed/incomplete `data.table` through the real endpoint path and confirms a real 422 results.

- **[P1.8]** [cg-documentation] `R/table_maker.R` (roxygen for `pop_share_threshold`), `man/table_maker.Rd`, `docs/api-contract.md` — The behavioral change where cells with `NA` population share (not just below-threshold) are now also masked (`is.na(value) | value < pop_share_threshold`) is undocumented.
  **Why**: A caller cannot infer from current docs why a cell with no computed `pop_share` (e.g., an all-NA group) is flagged `"NR"`.
  **Fix**: Add a sentence to `@param pop_share_threshold` and mirror in `man/table_maker.Rd` and `docs/api-contract.md`: "Cells whose population share cannot be computed (`NA`) are also masked as `NR`, in addition to cells below the threshold."

- **[P1.9]** [cg-performance] [cg-data-quality] `R/table_maker.R` — Potential duplicate `GRP()`/`compute_shares()` aggregation pass: when `pop_share` isn't among the requested `measures`, it is recomputed solely for threshold evaluation, duplicating a full grouped-aggregation pass over `dt` that may already have been computed in `compute_measures()`.
  **Why**: This compounds the stated "batch execution across many surveys" scalability requirement; escalated to manual because it touches the population/share statistical computation path, not purely a mechanical optimization.
  **Fix**: Evaluate whether `population`/`pop_share` from the initial `compute_measures()` pass can be reused here instead of recomputed, to avoid the duplicate aggregation cost at scale. Benchmark before and after on a representative multi-survey batch.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] [cg-documentation] [cg-data-quality] `R/table_maker.R` (function signature) — Misaligned indentation on the `pop_share_threshold = 0.001` parameter line relative to sibling parameters. **FIXED** during this review (whitespace-only, verified safe).

- **[P2.2]** [cg-code-quality] [cg-data-quality] `inst/plumber/plumber.R:290` (`/table/download`) — Stale reference to a `population` column that `table_maker()` no longer emits (`intersect(c("value", "population", "poverty_line"), names(tbl))`); harmless no-op today but is the exact drift signal that produced **[P0.1]**.
  **Why**: Left as-is it silently no-ops, but signals this endpoint was not audited against the schema change.
  **Fix**: Remove `"population"` from the intersect list, or add a comment noting it is retained only for legacy client payloads.

- **[P2.3]** [cg-code-quality] `README.md:57,70` — Still documents `pop_share_threshold` default as `0.01` and shows an example passing `0.01`, inconsistent with the new `0.001` default everywhere else (code, `docs/api-contract.md`, `man/table_maker.Rd`).
  **Why**: Doc drift on a threshold parameter is itself a data-risk — integrators following the README will assume the old 1% suppression threshold.
  **Fix**: Update both the default-value prose and example call to `0.001`.

- **[P2.4]** [cg-documentation] `R/table_maker.R` (`@return` doc, `value` field) — Stale/incomplete: doesn't mention `value` can be `NA_real_` when `flag == "NR"`.
  **Fix**: Update the `value` `@return` item to state this explicitly.

- **[P2.5]** [cg-documentation] `SWAGGER_GUIDE.md:74`, `docs/api-contract.md:166` — Still describe `pop_share_threshold` as a "cell-suppression threshold," inconsistent with the new masking (not row-dropping) semantics used elsewhere in the same docs (e.g., the Table Output Rows section correctly says "masked").
  **Fix**: Reword to "cell-masking threshold (`NR`)" in both locations.

- **[P2.6]** [cg-documentation] `R/format_table_output.R` `@examples` — Entirely wrapped in `\dontrun{}`, so it is never checked by `R CMD check` and doesn't verify the documented `"6.70"`-style conversion claim.
  **Fix**: Add a small, self-contained runnable example using a hand-built `data.table` outside `\dontrun`.

- **[P2.7]** [cg-version-control] commit `54eb4ef` ("brainsotrm on new NR cells") — Not conventional-commit format; contains a typo.
  **Fix**: Reword as e.g. `docs(brainstorm): capture NR-cell masking notes` (only if branch history can still be amended; otherwise note as a convention reminder).

- **[P2.8]** [cg-version-control] commit `0cde2f0` ("plan implementation") — Not conventional-commit format and too vague.
  **Fix**: Reword as e.g. `docs(plan): add pop-share NR-masking implementation plan`.

- **[P2.9]** [cg-version-control] `.kilo/commands/cg-plan.md` — Unrelated prompt-tooling change bundled into this feature branch.
  **Fix**: Split into a separate commit/PR, or note explicitly in the PR description why it's bundled.

- **[P2.10]** [cg-testing] `tests/testthat/test-table-maker-suppression.R:145,173` — Test name/comment ("retained for suppressed cells") is stale relative to the new "all measures masked" behavior (share measures used to be exempted; they no longer are).
  **Fix**: Rename to describe the new contract explicitly (e.g., "share-family measures are masked, not exempted, for suppressed cells").

- **[P2.11]** [cg-testing] `tests/testthat/test-format-table-output.R` — No test for the `x.xx5` half-way rounding boundary (floating-point representation can make `formatC()` round either direction at the boundary).
  **Fix**: Add explicit boundary-value regression tests (e.g., `0.145`, `1.005`) pinning the currently observed behavior so an engine/locale change is caught.

- **[P2.12]** [cg-testing] [cg-architecture] `R/measures.R`, `tests/testthat/test-format-table-output.R` — No test asserting `.PCT_MEASURES` stays consistent with `.MEASURE_REGISTRY` (every registered measure resolves to exactly one formatting bucket); `.PCT_MEASURES` is a manually curated exception list (not cleanly derivable from registry families, since the `poverty` and `inequality` families each mix percentage and non-percentage measures) and could silently drift if a new measure is added to the registry but omitted here.
  **Fix**: Add a registry-synchronization unit test asserting `.PCT_MEASURES ⊆ names(.MEASURE_REGISTRY)` plus a test for an unregistered/future measure name falling through to the generic 2-decimal formatting path.

- **[P2.13]** [cg-testing] `tests/testthat/test-api-endpoints.R` — No end-to-end API-level test that forces a masked cell (low threshold + tiny-weight fixture) and asserts the live JSON response shows `"NR"` with `population`/`flag` both absent, in one pass through the full request → `table_maker()` → `format_table_output()` → JSON pipeline.
  **Fix**: Add such an integration test; current coverage verifies masking and formatting only in isolation.

- **[P2.14]** [cg-performance] `R/table_maker.R` (suppression warning-message construction) — Nested `vapply` loop builds a per-suppressed-cell diagnostic label purely for a `cli_warn()` message; at scale (many small masked cells across a large batch) this row-wise construction could dominate the cost of an otherwise fully vectorized suppression step.
  **Fix**: Vectorize the label construction, or cap the number of labels shown in the warning (e.g., first 20).

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/format_table_output.R`, `R/measures.R` — Whole-number measure set `c("pop_poverty", "nobs")` is an inline magic literal, while the percentage set was promoted to `.PCT_MEASURES`.
  **Fix**: Add a `.WHOLE_NUMBER_MEASURES` constant beside `.PCT_MEASURES` for discoverability/consistency.

- **[P3.2]** [cg-architecture] — The principle for *why* masking lives inside `table_maker()` (needs row-level access to per-cell `pop_share`/grouping context) while formatting lives in a separate `format_table_output()` module is not documented anywhere.
  **Fix**: Add a short note (e.g., in the function family docs or `compound-gpid.md`) stating this boundary explicitly so future contributors don't assume all presentation-like logic belongs in `format_table_output()`.

- **[P3.3]** [cg-version-control] Branch name `new_pop_threshold` does not follow the `type/short-description` convention used elsewhere in the repo.
  **Fix**: Not blocking since already in use; consider `feat/pop-share-threshold-masking` for future branches.

- **[P3.4]** [cg-version-control] `.gitignore` has no explicit Python/OS-file patterns (not currently exploited by this diff).
  **Fix**: Proactively add `.DS_Store`, `Thumbs.db`, and relevant data-file patterns if ever produced locally.

- **[P3.5]** [cg-testing] `tests/testthat/test-format-table-output.R` — Input-validation tests use loose substring matching on error messages rather than also asserting condition class.
  **Fix**: Optionally assert on error condition class per `cg-skill-r-shared` conventions.

### ✅ Passed

- **cg-code-quality**: `.Rbuildignore` already excludes `.cg-docs/` — no package-build hygiene issue.
- **cg-version-control**: No hardcoded secrets (`api_key`, `password`, `secret`, `token`, `AWS_`, `OPENAI_`) found anywhere in the diff. No hardcoded absolute/local paths in `R/*.R` or `inst/plumber/plumber.R`. No stray/leftover files in the working tree.
- **cg-reproducibility**: No RNG/seed dependency, no new package dependencies, no hardcoded absolute paths introduced by this diff.
- **cg-performance**: `format_table_output()` is fully vectorized (no `apply`/`sapply`/row-wise loop); the masking join's in-place `:=` update on matched row indices is a genuine improvement over the prior filter-and-copy approach.
- **cg-architecture**: No circular-dependency or layering violation between `format_table_output.R`, `table_maker.R`, and `measures.R`.
- **cg-data-quality**: Percentage scaling order is correct (×100 applied before rounding, verified with `gini = 0.4567 → "45.67"`, not the reverse); `mld`/`watts` correctly excluded from `.PCT_MEASURES`; `table_maker()`'s `value` column remains numeric/double throughout — only `format_table_output()` introduces character coercion, on a `copy()`, never mutating the caller's table by reference.
