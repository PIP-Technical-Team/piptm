---
date: 2026-09-14
title: "Structured Cell Definition table for share measures (revised)"
status: completed
completed-date: 2026-09-14
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-09-14-shares-cell-definition-structured-table.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
phases: 2
completed-phases: [1, 2]
execution-report: ".cg-docs/work-reports/2026-09-14-shares-cell-definition-structured-table-revised.md"
artifact-schema-version: 1
tags: [description-endpoint, cell-definition, shares, table-maker, api, ux, accuracy]
---

# Plan: Structured Cell Definition Table for Share Measures (Revised)

## Objective

Replace ambiguous prose for `pop_share`, `target_within_group_share`, and `target_survey_share` in the `/description` endpoint's Cell Definition section with a structured four-column table: Measure, Denominator, Numerator, and Plain meaning. Render the table in Markdown and HTML while preserving existing non-share behavior.

This plan supersedes `.cg-docs/plans/2026-09-14-shares-cell-definition-structured-table.md` and incorporates its plan-review decisions.

## Context

`build_cell_definition()` currently creates one sentence per measure. Share sentences combine denominator, numerator, filtering, and grouping in similar clauses, making the three measures difficult to distinguish accurately.

`R/compute_shares.R` establishes:

- `pop_share = cell_wpop / denom_survey`
- `target_within_group_share = targ_in_group / cell_wpop`
- `target_survey_share = targ_in_group / denom_survey`

`denom_survey` is based on data already restricted by `filter_base`, so descriptions must use the filter-only `base_pop` rather than imply an unfiltered survey universe. The multiplication identity holds when the cell weighted population is positive. For a zero-weight cell, the within-cell share is `NA`, so the footer must not state the identity unconditionally.

Plan-review decisions:

- Accept the assumption that requests do not repeat a measure; no duplicate-preservation work is required.
- Qualify the identity footer for positive weighted populations and test zero-weight behavior.
- Remove the unreachable unknown-share test.
- Use `tests/testthat/test-cell-definition.R` for builder tests.
- Defer Markdown escaping for pipes, newlines, and backslashes.
- Update `docs/description-endpoint-spec.md`; remove the irrelevant OpenAPI polymorphism follow-up.
- Insert renderer logic in `.render_named_list()` and `.render_named_list_html()`, not display-label helpers.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | When any share is requested, `measure_interpretation` contains `prose`, `shares_table`, and `shares_footer`; without shares it remains the existing character vector | Brainstorm Approach A |
| R2 | `shares_table` has `measure`, `denominator`, `numerator`, and `plain_meaning`, with one row per requested share | User requirement |
| R3 | Definitions use existing `base_pop` and `full_pop` composition and match `compute_shares.R` semantics under filters and grouping | Source review |
| R4 | Non-share measures retain their existing prose and appear before the share table for mixed requests | User decision |
| R5 | The footer is additional to all measure rows, appears only for two or more requested shares, and says the identity applies to cells with positive weighted population | Plan review P2.1 |
| R6 | All three rows remain present when `by = NULL`, including equivalent target-share rows | Brainstorm decision |
| R7 | Markdown renders prose, table, then footer through `.render_named_list()` | Plan review P3.1 |
| R8 | HTML renders prose, escaped inline-styled table, then footer through `.render_named_list_html()` | Plan review P3.1 |
| R9 | The internal description-model specification documents the new share-specific shape | Plan review P2.5 |
| R10 | Suppression remains documented separately and is not mentioned in the shares table | Brainstorm decision |

## Phase 1: Structured Model and Correctness

### 1. Build the structured share interpretation
- **Requirements**: R1, R2, R3, R4, R5, R6, R10
- **Files**: `R/description_builder.R`
- **Details**:
  - Classify each requested measure from `resolved_labels$measures` and separate share and non-share processing. Duplicate measures are outside the supported request contract; `setdiff()` is acceptable under that explicit assumption.
  - Preserve the current prose switch for non-share measures without changing its templates.
  - Build a `data.table` for requested shares in request order. Use resolved UI labels in `measure`.
  - Reuse `base_pop` for survey-base denominators and `full_pop` for cell membership. Avoid hardcoded wording that implies filters are excluded from the denominator.
  - Define `pop_share` as cell weighted population over the filtered survey base.
  - Define `target_within_group_share` as target-positive cell members over all cell members.
  - Define `target_survey_share` as target-positive cell members over the filtered survey base.
  - Hand-author concise plain-meaning questions per measure using `analysis_var_label` for target measures.
  - Return the existing character vector when no shares are requested. Otherwise return `list(prose, shares_table, shares_footer)`.
  - Use footer text equivalent to: `For cells with positive weighted population: Target share in sample base = Population share × Target share within cell.` only when at least two shares are requested.
  - Update relevant roxygen return documentation.
- **Test Scenarios**: shares-only with filter and grouping; mixed shares/non-shares; no shares; one/two/three shares; `by = NULL`; filtered and unfiltered bases; zero-weight cell semantics.
- **Tests**: `tests/testthat/test-cell-definition.R`; run `testthat::test_file("tests/testthat/test-cell-definition.R")`.
- **Acceptance criteria**: Payload columns and rows match R2; text matches source formulas; footer condition and qualification are correct; existing non-share exact-output tests remain unchanged and pass.

### 2. Strengthen measure-definition regression coverage
- **Requirements**: R3, R5, R6
- **Files**: `tests/testthat/test-cell-definition.R`, existing share computation test file if required for a zero-weight fixture
- **Details**:
  - Replace the existing share-prose assertions in Example 3 with structured-payload assertions.
  - Add exact expectations for all four fields for each share measure.
  - Add filtered/grouped and unfiltered/ungrouped examples.
  - Add a zero-weight computation fixture demonstrating that `target_within_group_share` is `NA` while `target_survey_share` may be zero, justifying the footer qualification.
  - Do not add an unknown-share test; unknown public measures are rejected before this branch and cannot be classified as shares.
- **Test Scenarios**: positive denominator identity; zero cell population; no grouping equivalence.
- **Tests**: `testthat::test_file("tests/testthat/test-cell-definition.R")` plus the targeted existing `compute_shares` test file if the numeric fixture belongs there.
- **Acceptance criteria**: Tests prove both the normal identity and its zero-weight limitation without inventing unreachable metadata states.

## Phase 2: Rendering and Documentation

### 3. Render structured interpretations in Markdown
- **Requirements**: R4, R7
- **Files**: `R/description_renderer.R`, relevant renderer/API tests
- **Details**:
  - Add handling for `cell_definition` + `measure_interpretation` inside `.render_named_list()` around the actual nested-value dispatch (`R/description_renderer.R:148-219`).
  - Render non-share prose first, then `shares_table` through the existing `.render_table()`, then the italicized footer.
  - Add display-header mappings for `measure`, `denominator`, `numerator`, and `plain_meaning`.
  - Preserve the existing character-vector path when there are no shares.
  - Do not implement Markdown escaping for pipes, line breaks, or backslashes in this iteration; record it as deferred risk.
- **Test Scenarios**: shares-only table; mixed prose followed by table; one share without footer; two shares with qualified footer; non-share output unchanged.
- **Tests**: locate and extend the existing Markdown renderer/API test file; run its matching targeted test command.
- **Acceptance criteria**: Valid expected pipe-table output for supported labels; correct ordering; unchanged non-share rendering.

### 4. Render structured interpretations in HTML
- **Requirements**: R4, R8
- **Files**: `R/description_renderer_html.R`, relevant HTML renderer tests
- **Details**:
  - Add handling inside `.render_named_list_html()` around `R/description_renderer_html.R:263-379`.
  - Flush pending list items before table output, render prose first, then reuse `.render_table_html()`, and append an inline-styled footer.
  - Reuse `.html_escape()` through the existing table implementation for every dynamic header and cell value. Do not create a second escaping function.
  - Preserve existing character-vector behavior for descriptions without shares.
- **Test Scenarios**: shares-only; mixed output ordering; escaped analysis/filter labels containing `<`, `&`, and quotes; footer condition; non-share regression.
- **Tests**: extend the existing HTML renderer/API test file and run its matching targeted command.
- **Acceptance criteria**: Four escaped headers/cells render in an inline-styled `<table>`; no raw hostile dynamic text appears; non-share HTML remains unchanged.

### 5. Update the internal model specification
- **Requirements**: R9
- **Files**: `docs/description-endpoint-spec.md`
- **Details**:
  - Replace the unconditional `measure_interpretation = character()` contract at `docs/description-endpoint-spec.md:325-332` with a share-specific union description: character vector for non-share-only requests; structured object with prose/table/footer for requests containing shares.
  - Document the four table fields and qualified footer condition.
  - Update worked share examples if they still show the old prose.
  - Do not change `docs/api-contract.md` to expose internal model polymorphism; `/description` returns rendered Markdown/HTML.
- **Test Scenarios**: documentation references agree with builder field names and footer condition.
- **Tests**: documentation/source consistency review; package documentation checks where applicable.
- **Acceptance criteria**: No internal specification claims share interpretations are always sentence vectors; no irrelevant OpenAPI follow-up remains.

### 6. Run integrated verification
- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8, R9, R10
- **Files**: all touched source, test, and documentation files
- **Details**:
  - Run targeted builder and renderer tests first.
  - Run the package test suite.
  - Run `devtools::check()` and inspect new failures against the pre-change baseline.
  - Confirm no dependency changes and no unrelated description sections changed.
- **Test Scenarios**: full regression surface.
- **Tests**: targeted commands from Steps 1-4; `devtools::test()`; `devtools::check()`.
- **Acceptance criteria**: All targeted and package tests pass; check is clean or contains only documented pre-existing findings; diff stays within boundaries.

## Testing Strategy

- Builder tests are the primary semantic gate and use exact expected table values.
- Share computation tests establish the positive- and zero-denominator behavior behind the footer.
- Renderer tests focus on structure, ordering, and HTML escaping rather than duplicating formula tests.
- Existing exact-output tests protect non-share behavior.
- Full package tests and `devtools::check()` close the regression surface.

## Documentation Checklist

- [ ] Update `build_cell_definition()` roxygen return documentation.
- [ ] Update top-level description-model documentation if it describes `measure_interpretation` as always character.
- [ ] Update `docs/description-endpoint-spec.md` shape and worked share examples.
- [ ] Do not add an OpenAPI/internal-polymorphism follow-up.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Description wording misstates filtered denominators | Reuse `base_pop`/`full_pop` and assert filtered examples against `compute_shares.R` formulas |
| Footer overstates zero-weight behavior | Qualify it for positive weighted populations and test zero-weight cells |
| Markdown and HTML behavior drift | Implement parallel targeted branches and mirrored tests |
| HTML injection through dynamic labels | Reuse `.html_escape()` via `.render_table_html()` and test hostile labels |
| Internal documentation becomes stale | Update `docs/description-endpoint-spec.md` in the same change |
| Markdown labels containing delimiters corrupt the table | Accepted deferred risk; capture Markdown cell escaping as separate follow-up work |
| Duplicate requested measures are deduplicated by partitioning | Accepted risk: duplicate measures are not a supported meaningful request case |

## Out of Scope

- Structured cell-definition tables for non-share families.
- Continuous/non-boolean analysis-variable grammar.
- OpenAPI polymorphism changes.
- Suppression wording changes.
- Markdown escaping for pipes, newlines, and backslashes.
- Duplicate-measure preservation.

## Completion Contract

### Outcome

Share measures render as a structured four-column table in Markdown and HTML. The identity footer states it applies to cells with positive weighted population; non-share output remains unchanged.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | Correct structured payload for shares-only and mixed requests | `testthat::test_file("tests/testthat/test-cell-definition.R")` | yes |
| V2 | Correct filtered/unfiltered and grouped/ungrouped definitions | `tests/testthat/test-cell-definition.R` | yes |
| V3 | Qualified footer present only for 2+ shares; zero-weight behavior tested | Builder and `compute_shares` tests | yes |
| V4 | Markdown and HTML render tables; mixed prose precedes table | Renderer/API tests | yes |
| V5 | HTML dynamic values are escaped through `.html_escape()` | HTML renderer tests | yes |
| V6 | Non-share descriptions remain unchanged | Existing regression tests | yes |
| V7 | Internal model specification reflects the new shape | `docs/description-endpoint-spec.md` | yes |
| V8 | Package checks pass | `devtools::check()` | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | No new dependencies | `DESCRIPTION` diff |
| C2 | Only share interpretation shape changes | Source diff and regression tests |
| C3 | Footer is additional, not a replacement for measure rows | Builder and renderer tests |
| C4 | HTML retains inline styles and escaping | HTML source/tests |
| C5 | Suppression wording remains separate | Generated output assertions |

### Boundaries

- Allowed: builder share branch, renderer list handling, relevant tests, roxygen documentation, `docs/description-endpoint-spec.md`.
- Out of scope: generalized tables, continuous-variable grammar, OpenAPI polymorphism, suppression wording, Markdown delimiter escaping, duplicate-measure support.
- Accepted risk: duplicate measures are not a supported meaningful request case.
- Deferred: Markdown delimiter escaping remains follow-up work.

### Iteration Policy

1. Complete and test structured model semantics before renderer changes.
2. Update Markdown and HTML renderers together with mirrored coverage.
3. Retain the footer only with its positive-weight qualification and passing zero-weight tests.
4. Fix non-share regressions rather than changing established expectations.

### Blocked-Stop Conditions

- Actual computation semantics conflict with planned denominator wording.
- Existing HTML escaping cannot safely cover the new table values.
- Additional internal-model consumers require a different stable schema.
