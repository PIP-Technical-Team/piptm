---
date: 2026-09-14
title: "Structured Cell Definition table for share measures"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-09-14-shares-cell-definition-structured-table.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
tags: [description-endpoint, cell-definition, shares, table-maker, api, ux, accuracy]
---

# Plan: Structured Cell Definition Table for Share Measures

## Objective

Replace the ambiguous, near-identical prose sentences generated for the three share measures (`pop_share`, `target_within_group_share`, `target_survey_share`) in the `/description` endpoint's Cell Definition section with a structured four-column table (Measure, Denominator, Numerator, Plain meaning), rendered in both Markdown and HTML, while leaving non-share measures and all other description sections unchanged.

## Context

`build_cell_definition()` (`R/description_builder.R`) generates one prose sentence per requested measure. For poverty/inequality/summary-statistics measures this reads well. For the three share measures, all three sentences reference the same base population and grouping and fuse denominator + numerator + grouping into one clause, making them nearly indistinguishable and factually hard to parse (see brainstorm for the reported example).

This is a follow-up to `2026-09-08-description-html-renderer-and-wording.md` (wording-first prose fixes + HTML renderer, both already shipped) and `2026-06-01-cell-share-measures.md` (introduced the three share measures in `compute_shares.R`). The UI has not integrated the endpoint yet, so the response shape for the shares case can change freely; non-share measures must stay backward-compatible in shape.

**Source-of-truth check completed during planning**: `R/compute_shares.R` confirms
- `pop_share = cell_wpop / denom_survey`
- `target_within_group_share = targ_in_group / cell_wpop`
- `target_survey_share = targ_in_group / denom_survey`
- Algebraic identity: `pop_share × target_within_group_share = target_survey_share` — holds exactly, confirmed from source (not just from measure semantics).
- `denom_survey` is computed from `dt` **as already filtered** by any `filter_base` (filtering happens upstream of `compute_measures()`/`compute_shares()`), not the raw unfiltered survey. So the correct Denominator/Numerator wording for `pop_share` and `target_survey_share` must reuse the builder's existing `base_pop` concept (filter-only, no `by` grouping) rather than a hardcoded "All survey-weighted individuals" string. When no filter is applied, `base_pop` already reads as "survey-weighted individuals in the selected survey" (existing code, `description_builder.R:542`), which is equivalent in meaning.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `build_cell_definition()` returns list-shaped `measure_interpretation` (`prose`, `shares_table`, `shares_footer`) when the requested measure set includes ≥1 share measure; unchanged character vector otherwise | Brainstorm Approach A |
| R2 | `shares_table` is a `data.table` with columns `measure`, `denominator`, `numerator`, `plain_meaning`, one row per requested share measure, in the order requested | Brainstorm Q2/Q3 |
| R3 | Denominator/Numerator text reuses `base_pop`/`full_pop` composition already present in `build_cell_definition()`, correctly reflecting filter + grouping per measure per the source-of-truth check above | Planning research (compute_shares.R) |
| R4 | Plain-meaning text is hand-authored per share measure, matching the templates specified in the brainstorm, with a `by = NULL` fallback wording branch matching the existing prose code's pattern | Brainstorm Q3 |
| R5 | `shares_footer` (identity note) present iff ≥2 share measures requested; text is `"Target share in sample base = Population share × Target share within cell."` | Brainstorm Q4a |
| R6 | Non-share measures continue through the existing `switch(stat_group, ...)` prose path, unchanged in output | Brainstorm scope |
| R7 | `render_description_markdown()` emits prose lines then a Markdown pipe-table then footer, when `measure_interpretation` is list-shaped | Brainstorm Approach A |
| R8 | `render_description_html()` emits prose lines then an inline-styled `<table>` then footer, with all dynamic cell values HTML-escaped | Brainstorm constraints |
| R9 | Suppression is never mentioned in the shares table | Brainstorm Q4b(3) |
| R10 | `by = NULL` degeneracy: render all three rows unchanged (no collapsing/merging), even though two rows will show identical Denominator text | Brainstorm Q4b(1) |

## Implementation Steps

### 1. Extend `build_cell_definition()` to produce structured shares payload
- **Requirements**: R1, R2, R3, R4, R5, R6, R9, R10
- **Files**: `R/description_builder.R`
- **Details**:
  - Partition `measures` into `shares_measures <- measures[measures_dt$stat_group[match(measures, measures_dt$measure)] == "shares"]` and `nonshare_measures <- setdiff(measures, shares_measures)`.
  - Keep existing `vapply(...)` prose generation for `nonshare_measures` only (reuse existing `switch` logic minus the `"shares"` branch; the fallback/other branches are untouched). Result: `prose` character vector (possibly length 0).
  - Add a new internal helper `.build_shares_table(shares_measures, analysis_var_label, base_pop, full_pop, by)` that returns a `data.table` with one row per measure in `shares_measures`, using:
    - `pop_share`: denominator = `base_pop` (filter-only), numerator = `full_pop` phrased as "Individuals in this cell (<filter+group description>)" — reuse the existing `group_qualifier`/`full_pop` string construction, plain meaning = `"What share of the total survey population does this cell represent?"`.
    - `target_within_group_share`: denominator = `full_pop`-as-cell-numerator text (same phrase as `pop_share`'s numerator, referenced consistently), numerator = `sprintf("Cell members for whom %s is true", analysis_var_label)`, plain meaning = `sprintf("Within this cell, what share have %s?", analysis_var_label)` with a `by = NULL` branch reusing the existing degenerate-case wording pattern (~line 674).
    - `target_survey_share`: denominator = `base_pop`, numerator = same "Cell members for whom `<var>` is true" phrase, plain meaning = `sprintf("What share of the total survey population is in this cell AND has %s?", analysis_var_label)`.
  - Build `shares_footer`: `if (length(shares_measures) >= 2) "Target share in sample base = Population share \u00d7 Target share within cell." else NULL`.
  - Return shape: if `length(shares_measures) == 0`, return `prose` (character vector) exactly as today — no wrapping list, no behavior change. Otherwise return `list(prose = prose, shares_table = shares_table, shares_footer = shares_footer)`.
  - Update the function's roxygen `@return` doc to describe both possible shapes.
- **Test Scenarios**: happy path (shares-only, `by != NULL`, filter present); edge case (`by = NULL`, all 3 shares — assert two rows share identical Denominator text and identity still verifiable); error path (unknown share measure key falls back gracefully, matching existing fallback behavior for other families).
- **Tests**: `tests/testthat/test-description-model-interactive.R` — add `describe("build_cell_definition - shares table")` block covering: shares-only, mixed measures (assert `prose` non-empty and `shares_table` present), non-share-only (assert character-vector return, byte-identical to pre-change baseline captured before edits), footer presence/absence at 1 vs 2 vs 3 shares requested.
- **Acceptance criteria**: All new tests pass; existing non-share tests in the same file pass unmodified; `shares_table` column values verified algebraically consistent with `compute_shares.R` semantics for a representative filtered+grouped case (R7/V7 style assertion using `all.equal()`).

### 2. Update Markdown renderer for list-shaped `measure_interpretation`
- **Requirements**: R6, R7, R9
- **Files**: `R/description_renderer.R`
- **Details**:
  - In the existing `cell_definition` special-case block (around line 335 / wherever `measure_interpretation` content is rendered inside `.render_section_content()`), detect `is.list(value) && !is.data.frame(value)` (list-but-not-data.table) for the `measure_interpretation` key specifically.
  - If list-shaped: emit `value$prose` as today's sentence lines (if non-empty), then render `value$shares_table` as a Markdown pipe-table using the existing table-rendering helper pattern already used elsewhere in this file for `data.table`/`data.frame` content (reuse `.display_table_header_markdown()`-style column labeling — add mappings for `measure`, `denominator`, `numerator`, `plain_meaning` → `"Measure"`, `"Denominator"`, `"Numerator"`, `"Plain meaning"`), then emit `value$shares_footer` as an italicized line if non-NULL.
  - If character-vector-shaped (today's default): render exactly as today — no behavior change.
- **Test Scenarios**: happy path (shares-only markdown output contains valid pipe-table syntax with correct headers); edge case (mixed measures — prose lines appear before the table); error path (shares-only with only 1 share measure — footer line absent).
- **Tests**: extend or add to `tests/testthat/test-api-description.R` (or the renderer-specific test file if one exists) asserting on rendered Markdown string structure (header row, separator row, one data row per share measure, footer presence/absence).
- **Acceptance criteria**: Rendered Markdown parses as a valid table (manual pipe-count check per row); non-share-only descriptions produce byte-identical Markdown output to pre-change baseline.

### 3. Update HTML renderer for list-shaped `measure_interpretation`
- **Requirements**: R6, R8, R9
- **Files**: `R/description_renderer_html.R`
- **Details**:
  - Mirror Step 2's detection logic in `.render_section_content_html()`'s `cell_definition` branch (around line 549).
  - Emit prose lines as today (if any), then an inline-styled `<table>` (reuse whatever inline style constants/helpers the existing HTML table-rendering path for other `data.table` content already uses, per the `2026-09-08` inline-styles-only convention), then the footer as an inline-styled `<p>`/`<em>` if non-NULL.
  - **Critical**: every cell value (measure label, denominator, numerator, plain-meaning text, all of which interpolate `analysis_var_label` and filter/group text derived from user-supplied labels) must go through the same HTML-escaping utility already used elsewhere in this file for dynamic content — locate and reuse it; do not introduce a second escaping implementation (per plan's Blocked-Stop condition).
- **Test Scenarios**: happy path (HTML output contains a `<table>` with 4 `<th>` cells and correct row count); edge case (a filter value containing `<`, `&`, or `"` renders escaped, e.g. `&lt;`); error path (no shares requested — HTML output unchanged from baseline).
- **Tests**: extend HTML renderer test file (locate existing `test-*html*` test or add one alongside Step 2's test file) asserting escaped output and table structure.
- **Acceptance criteria**: No unescaped `<`/`&`/`"` from dynamic values appears in output for a crafted test case; non-share-only HTML output byte-identical to pre-change baseline.

### 4. Cross-check API contract and update docs
- **Requirements**: R1 (documentation of the polymorphic shape)
- **Files**: `.cg-docs/brainstorms/2026-08-31-api-contract-openapi.md` (read-only reference), any OpenAPI spec file this brainstorm points to (if it exists under version control)
- **Details**: Read the referenced brainstorm/spec to determine whether `/description`'s response schema has been formalized outside this package (e.g. in a spec file). If a formal spec exists and is in this repo, flag the shape change (`cell_definition.measure_interpretation`: `string[]` or `object`) as needing a spec update — do not silently edit a spec file outside this plan's file permissions without confirming with the user first, since this plan's `Files` boundary is limited to the R package source and tests. If no in-repo spec artifact is found, note this as an open follow-up only.
- **Test Scenarios**: N/A (documentation/discovery step).
- **Tests**: N/A.
- **Acceptance criteria**: A clear statement in the plan's completion notes (or a follow-up ticket) of whether a spec update is needed, and if so, where.

## Testing Strategy

- Unit tests at the builder level (`build_cell_definition()`) are the primary correctness gate — they verify the four-column content is textually and numerically consistent with `compute_shares.R` semantics, independent of rendering.
- Renderer-level tests verify presentation (valid Markdown table syntax, valid HTML table structure, escaping) without re-verifying business logic already covered at the builder level.
- One end-to-end regression test per renderer confirms non-share descriptions are pixel/byte-identical to the pre-change baseline (captured by running the existing test suite before any edits and diffing snapshot output, or by asserting against the current literal expected strings already present in the test files).
- `devtools::check()` run at the end to confirm no new NOTES/WARNINGs from roxygen changes or new internal helpers.

## Documentation Checklist

- [ ] Update `build_cell_definition()` roxygen `@return` to document the polymorphic return shape.
- [ ] Update the package-level description-model docs (`R/description_builder.R` top-of-file roxygen, ~line 13/32) that enumerate section shapes, to note `cell_definition$measure_interpretation` may be a list when shares are requested.
- [ ] No `NEWS.md`/changelog entry unless the project maintains one (not found in this scan — skip unless project convention says otherwise).

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Denominator/Numerator wording is subtly wrong for some filter/grouping combination not covered by existing tests | Reuse the builder's already-tested `base_pop`/`full_pop`/`group_qualifier` construction rather than writing new interpolation logic; add explicit filtered + `by != NULL` test case |
| Two renderers drift out of sync (one updated, one not, or divergent table formatting) | Implement Step 2 and Step 3 back-to-back in the same session; write near-identical test assertions for both, checked in the same PR |
| HTML escaping regresses (a new code path bypasses the existing escaping utility) | Explicit test with `<`/`&`/`"` in a filter label value; Blocked-Stop condition requires reusing the existing utility, not writing a new one |
| `shares_table`/`shares_footer` polymorphism silently breaks a currently-passing test that assumed `measure_interpretation` is always a character vector | Run full existing test suite before any edits to capture baseline; run again after each step; treat any newly-failing non-shares test as a regression to fix, not to suppress |
| API/OpenAPI contract drift if `/description` schema has been formalized elsewhere | Step 4 explicitly checks for this and surfaces it as a follow-up rather than silently shipping an undocumented breaking shape change |
| `by = NULL` degeneracy renders two identical-looking Denominator rows, which could read as a bug to a future maintainer | Add a code comment at the `.build_shares_table()` degenerate branch explaining this is intentional per the brainstorm decision, referencing the footer as the disambiguating context |

## Out of Scope

- Any of the other 8 description-model sections.
- `build_description_model()` orchestration changes.
- A generalized four-column structured table for poverty/inequality/summary-statistics measures.
- Grammar handling for continuous (non-boolean) analysis variables in share-measure wording.
- Editing any OpenAPI/spec file directly (Step 4 only checks and flags).
- Suppression-related wording changes.

## Completion Contract

### Outcome
For any `/description` request whose measure set includes one or more share measures (`pop_share`, `target_within_group_share`, `target_survey_share`), the `cell_definition` section renders a four-column table (Measure, Denominator, Numerator, Plain meaning) instead of ambiguous prose, with an optional identity footer when ≥2 share measures are present. Non-share measures are unaffected. Both Markdown and HTML renderers support this consistently.

### Verification Surface
| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | `build_cell_definition()` returns list-shaped `measure_interpretation` (`prose`, `shares_table`, `shares_footer`) when any share measure is requested; character vector otherwise | `devtools::test(filter = "description-model-interactive")` | yes |
| V2 | `shares_table` has correct columns and correct denominator/numerator/plain-meaning text for all 3 share measures, under both `by = NULL` and `by != NULL`, and under filtered and unfiltered `filter_base` | New/updated tests in `test-description-model-interactive.R` | yes |
| V3 | `shares_footer` present iff ≥2 share measures requested; absent otherwise | Same test file | yes |
| V4 | Markdown renderer emits a valid pipe-table for shares and unchanged prose for non-shares, including mixed-measure descriptions | `test-api-description.R` (or renderer-specific test) | yes |
| V5 | HTML renderer emits a `<table>` with inline styles only, all dynamic values HTML-escaped | Renderer test asserting escaped output for a value containing `<`/`&`/`"` | yes |
| V6 | Non-share-only descriptions produce byte-identical `measure_interpretation` character vector output vs. pre-change baseline | Regression test comparing rendered Markdown/HTML for a non-share description | yes |
| V7 | Algebraic identity (`target_survey_share = pop_share × target_within_group_share`) holds in a live `table_maker()` example with `by != NULL` and a filter applied | Integration test computing all 3 shares and asserting `all.equal()` on the ratio | yes |
| V8 | `devtools::check()` clean (or existing CI-equivalent) after changes | `devtools::check()` | yes |

### Constraints
| ID | Constraint | Check |
|----|------------|-------|
| C1 | No new package dependencies | Diff of `DESCRIPTION` Imports/Suggests |
| C2 | `build_description_model()` orchestration and the 9-section shape unchanged | Diff review of `description_builder.R` outside `build_cell_definition()` |
| C3 | Non-share prose path (`switch(stat_group, ...)` for poverty/inequality/summary_statistics) untouched | Diff review + V6 |
| C4 | HTML output uses inline styles only, no CSS classes | Diff review of `description_renderer_html.R` |
| C5 | All dynamic content HTML-escaped in table cells | V5 |
| C6 | Shares table never mentions suppression | Manual read of generated table content in tests |

### Boundaries
- **Allowed**: Changes to `R/description_builder.R` (`build_cell_definition()` only), `R/description_renderer.R` and `R/description_renderer_html.R` (`cell_definition` rendering branch and any new helper functions they need), associated test files, and roxygen docs for touched functions.
- **Out of scope**: Any other of the 9 description sections; `build_description_model()` orchestration; OpenAPI spec file (flagged as a follow-up only); a generalized structured-table format for other measure families; grammar handling for continuous analysis variables; suppression-related wording.

### Iteration Policy
1. Implement builder changes first (Step 1), verify with unit tests in isolation before touching renderers.
2. Implement Markdown renderer changes (Step 2), then HTML renderer changes (Step 3), reusing shared helper logic where the two renderers already parallel each other.
3. If the `compute_shares.R` cross-check (done during planning, confirmed above) is contradicted by any newly discovered edge case (e.g. a future `pip_id`-only `by` degeneracy), pause and re-derive wording rather than shipping an incorrect footer note.
4. Add tests alongside each step rather than batching all tests at the end.

### Blocked-Stop Conditions
- If `filters_dt` or `resolved_labels$measures` schema differs from what `build_cell_definition()` currently assumes (e.g. missing `stat_group` column), stop and report — do not guess at a fallback schema.
- If any existing test asserting on `measure_interpretation` as a character vector for a **shares-containing** description already exists and can't be safely updated without losing coverage intent, stop and ask before overwriting.
- If HTML escaping utilities used elsewhere in `description_renderer_html.R` are not directly reusable for table cell values, stop and ask rather than introducing a second escaping implementation.
