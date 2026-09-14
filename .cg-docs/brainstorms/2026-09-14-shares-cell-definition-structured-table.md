---
date: 2026-09-14
title: "Structured Cell Definition table for share measures"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Approach A - Structured payload for shares, split from prose"
tags: [description-endpoint, cell-definition, shares, table-maker, api, ux, accuracy]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Structured Cell Definition Table for Share Measures

## Context

The `/description` endpoint (`R/description_builder.R` + `R/description_renderer.R` + `R/description_renderer_html.R`) generates a prose "Cell Definition" section that includes one sentence per requested measure. For most measure families (poverty, inequality, summary statistics) those sentences read well. For the three **share measures** — `pop_share` (Population share), `target_within_group_share` (Target share within cell), `target_survey_share` (Target share in sample base) — the sentences are near-indistinguishable because all three reference the same base population and grouping, and each sentence fuses denominator + numerator + grouping into one long clause.

Concrete failure case reported by the user (table disaggregated by Gender x Welfare quintile, filtered to Gender = female, target var "Improved water source"):

> The share of the total weighted survey population represented by survey-weighted individuals for whom Gender is among [female], within each Gender x Welfare quintile group. Within survey-weighted individuals for whom Gender is among [female], within each Gender x Welfare quintile group, the share for which Improved water source is true. The share of the total survey-weighted population represented by survey-weighted individuals for whom Gender is among [female], within each Gender x Welfare quintile group where Improved water source is true.

The three sentences look almost identical. The failure is not aesthetic - a user cannot reliably tell which measure computes which quantity, which denominator each uses, or what question each answers.

This is a direct follow-up to `2026-09-08-description-html-renderer-and-wording.md`, which fixed prose wording within the existing model shape but explicitly deferred any structural change to `build_description_model()`. The current brainstorm authorizes a scoped structural change - to the shares branch of `build_cell_definition()` only.

Prior related work:
- `2026-06-01-cell-share-measures.md` - defined the three share measures.
- `2026-09-08-description-html-renderer-and-wording.md` - added `description_renderer_html.R`, both renderers already have a `cell_definition` special-case branch and a "shares" group label, giving us the exact hooks to extend.

## Requirements

- **Purpose**: Make the meaning of the three share measures scannable and unambiguous. This is a correctness fix (users currently form wrong mental models), not a cosmetic one. Success = a reader can, in one glance, identify each measure's denominator, numerator, and the plain-language question it answers.
- **Users**: PIP platform UI (primary consumer, not yet integrated - free to define the response shape). Indirectly, end users of the PIP platform viewing table descriptions.
- **Inputs**: Same inputs as today (`description_metadata` fast path or `params` fallback via `table_maker()`).
- **Outputs**: For descriptions whose measure set includes any share measures, the `cell_definition` section's `measure_interpretation` field returns a **list** with three slots:
  - `prose` - character vector of sentences for non-share measures (unchanged path).
  - `shares_table` - `data.table` with columns `measure`, `denominator`, `numerator`, `plain_meaning` (one row per requested share measure).
  - `shares_footer` - character scalar with the identity note, present only when `>=2` share measures are requested; `NULL` otherwise.
  When no share measures are requested, `measure_interpretation` remains the character vector it is today (backward-compatible for that case).
- **Constraints**:
  - No new package dependencies (consistent with lean-dependency posture).
  - Do NOT change the 9-section shape of `build_description_model()`.
  - Do NOT touch the prose path for non-share measures.
  - Both renderers must stay in sync (Markdown and HTML) and both must handle: shares-only, mixed, and prose-only descriptions.
  - HTML renderer continues to use inline styles only and MUST HTML-escape all dynamic content in the table cells (values come from survey/covariate metadata).
  - Denominator/Numerator wording MUST accurately describe what `R/compute_shares.R` computes - the accuracy priority is non-negotiable.
- **Edge Cases**:
  - `by = NULL` (no cross-tabulation): render all three rows unchanged. Two of them (`target_within_group_share`, `target_survey_share`) collapse to the same numeric value; the table honestly reports this via identical Denominator strings. Do not special-case; flag as a wording-review item at implementation time.
  - Filters present: `pop_share` denominator is "All survey-weighted individuals" (unfiltered survey - that is the definition of population share); numerator reflects the filtered+grouped cell. `target_within_group_share` uses the filtered+grouped cell as denominator. `target_survey_share` uses unfiltered survey as denominator, filtered+grouped+target-true as numerator.
  - Suppression (`pop_share_threshold`): NOT mentioned in the shares table. Suppression is documented in its own section; the table describes measure definitions, not runtime filtering.
  - Analysis-var wording: the current templates use `"<analysis_var_label> is true"`. This reads fine for boolean/target variables (the only case share measures currently support) but is awkward for continuous variables. Documented as a known limitation - captured as a roadmap idea (see Next Steps), not fixed here.
- **Scope** (explicitly out of scope for this iteration):
  - No changes to `build_description_model()` orchestration or to any of the other 8 sections.
  - No changes to the prose path for non-share measures.
  - No general-purpose "structured cell definition for all measure families" - deferred to avoid scope creep.
  - No grammar engine for continuous vs. boolean analysis vars - captured as separate roadmap idea.
  - No OpenAPI spec update in this iteration - flagged as follow-up in Next Steps if the API contract has been formalized elsewhere.

## Approaches Considered

### Approach A: Structured payload for shares, split from prose (CHOSEN)

`build_cell_definition()` partitions requested measures into non-share and share groups. Non-share measures continue through the existing `switch(stat_group, ...)` prose path unchanged. Share measures skip the prose branch entirely and populate a structured `data.table` with columns `measure`, `denominator`, `numerator`, `plain_meaning`, one row per share measure. When any shares are present, `content$measure_interpretation` becomes a list `(prose, shares_table, shares_footer)`; otherwise it remains a character vector as today.

Both renderers (`description_renderer.R`, `description_renderer_html.R`) get one new branch in their existing `cell_definition` special-case block: if `measure_interpretation` is a list, emit the prose lines first, then the table (Markdown pipe-table / HTML `<table>` with inline styles), then the footer if present. If `measure_interpretation` is a character vector, behave as today.

Plain-meaning column templates (hand-authored in `build_cell_definition()`, matching the existing per-family `sprintf` idiom):

- `pop_share`
  - Denominator: `"All survey-weighted individuals"`
  - Numerator: `sprintf("Individuals in this cell (%s)", <cell_scope>)` where `<cell_scope>` is the composed filter + group qualifier
  - Plain meaning: `"What share of the total survey population does this cell represent?"`
- `target_within_group_share`
  - Denominator: `sprintf("Individuals in this cell (%s)", <cell_scope>)`
  - Numerator: `sprintf("Cell members for whom %s is true", analysis_var_label)`
  - Plain meaning: `sprintf("Within this cell, what share have %s?", analysis_var_label)` (with fallback wording for `by = NULL`)
- `target_survey_share`
  - Denominator: `"All survey-weighted individuals"`
  - Numerator: `sprintf("Cell members for whom %s is true", analysis_var_label)`
  - Plain meaning: `sprintf("What share of the total survey population is in this cell AND has %s?", analysis_var_label)`

Identity footer (rendered only when `>=2` share measures are requested):

> *Note: Target share in sample base = Population share x Target share within cell.*

Identity verified algebraically: `(cell_pop / total_pop) x (cell_targets / cell_pop) = cell_targets / total_pop`. Holds because `pop_share`'s and `target_within_group_share`'s denominators both use the same `full_pop` (base filter + `by` grouping) in the current engine implementation. Implementation MUST cross-check `R/compute_shares.R` to confirm this invariant before merge.

- **Pros**: Minimal blast radius (one function's return shape changes only for the shares branch; two renderer functions get one branch each). Zero new deps. Matches the existing per-family `switch` idiom in `build_cell_definition()` (lines 640-702). Non-share descriptions and downstream tests are untouched. UI receives clean structured JSON for the four columns for free.
- **Cons**: `content$measure_interpretation` becomes shape-polymorphic (character vector or list). Downstream code that assumed a character vector must be updated - today the only such code is the two renderers. Adds one new test dimension (prose-only / shares-only / mixed).
- **Effort**: Small (~2 person-days: ~0.5d builder + templates, ~0.5d per renderer, ~0.5d tests).

### Approach B: Always-list payload (uniform shape)

Same structured shape as A, but `measure_interpretation` is *always* the list, with `prose` always present (possibly empty) and `shares_table` always present (possibly NULL), for every description.

- **Pros**: One consistent schema, easier to formalize in an OpenAPI spec.
- **Cons**: Requires touching renderer paths and updating tests for descriptions that don't include any share measures - ripple with no user-visible benefit today. Contradicts "just fix shares" scoping.
- **Effort**: Small-medium.

### Approach C: Sidecar field

Keep `measure_interpretation` as the character vector for all measures (retaining today's confusing shares prose), and add a sibling field `measure_interpretation_shares_table` alongside it when shares are present. Renderers detect the sidecar and, when present, emit the table instead of the shares prose lines within `measure_interpretation`.

- **Pros**: Strictly backward-compatible.
- **Cons**: The confusing prose is still in the payload as a footgun. Renderers must skip specific lines from `measure_interpretation` and substitute the sidecar - more complex logic than A. Two ways to represent the same information indefinitely. UI has not integrated yet, so backward-compat has no consumer to protect.
- **Effort**: Small-medium.

## Decision

**Approach A - Structured payload for shares, split from prose.**

Rationale:
- Least code disruption: non-share path untouched, one new switch branch in the builder, one new detection branch in each renderer.
- Directly addresses the reported failure mode (users cannot distinguish share measures) with a UX pattern (four-column table with a "plain meaning" column) that separates the axes the prose fused together.
- Zero new dependencies; consistent with the project's lean-dependency and simplest-viable-option posture (established in `2026-09-08-description-html-renderer-and-wording.md`).
- The polymorphism cost (`measure_interpretation` is now character-vector-or-list) is contained: only the two renderers consume this field today, and both are updated in this change.
- The identity footer is additive - it does NOT replace measure rows; it sits below the table when `>=2` share measures are requested.

Devil's Advocate checks: problem validation (pre-validated by concrete reproduction), simplicity (no cheaper solution overlooked - the confusability is structural to prose), effort-value (~2 person-days for a correctness fix to a routinely-used section), charter alignment (deterministic; interoperability caveat captured; methodological accuracy locked in via a `compute_shares.R` cross-check requirement).

## Next Steps

For handoff to `/cg-plan`:

1. **Verify measure identity against implementation.** Before writing the footer note into the payload, read `R/compute_shares.R` and confirm that `target_survey_share = pop_share x target_within_group_share` holds under the current engine (specifically: both `pop_share` and `target_within_group_share` share the same `full_pop` denominator). If the invariant does not hold, either the footer text or the measure definitions need adjustment - the plan should treat this as a hard gate.
2. **Extend `build_cell_definition()`** in `R/description_builder.R`:
   - Partition `measures` into non-share and share subsets using the `measures_dt$stat_group == "shares"` predicate already used at line 665.
   - Keep the existing `sprintf` prose path for non-shares (produces `prose` character vector, possibly length 0).
   - Add a shares branch that builds a `data.table` with columns `measure`, `denominator`, `numerator`, `plain_meaning`, using the hand-authored templates specified in Approach A. Compose the cell-scope string from the existing `base_pop` + `group_qualifier` variables (lines 540-610) so filter + grouping wording stays consistent with the rest of the section.
   - Return `list(prose = <chr>, shares_table = <DT-or-NULL>, shares_footer = <chr-or-NULL>)` when shares are present; otherwise return the character vector as today.
3. **Update `render_description_markdown()`** in `R/description_renderer.R`:
   - In the existing `cell_definition` special-case block (line 335), detect list vs. character shape.
   - Emit prose lines as today, then a Markdown pipe-table for `shares_table` (four columns), then the italicized footer note if present.
4. **Update `render_description_html()`** in `R/description_renderer_html.R`:
   - Mirror the Markdown renderer's branch in the `cell_definition` block (line 549).
   - Emit a `<table>` with inline styles (matching the inline-styles-only convention from `2026-09-08`). HTML-escape every cell value.
5. **Cross-check `2026-08-31-api-contract-openapi.md`.** If the `/description` OpenAPI spec has been formalized, update it to reflect that `cell_definition.measure_interpretation` may be a character vector or an object `{prose, shares_table, shares_footer}`. If the spec has not been formalized, note the polymorphism as a spec-time follow-up.
6. **Tests to add / update:**
   - `test-description-model-interactive.R`: add cases for shares-only, mixed measures, and prose-only. Assert `shares_table` column names, one row per requested share measure, and correct denominator/numerator/plain-meaning strings for each of the three shares (both `by = NULL` and `by != NULL`). Assert `shares_footer` present iff `>=2` shares present.
   - `test-api-description.R` (and any Markdown-rendering test): assert Markdown output contains the pipe-table header and the footer sentence; HTML output contains a `<table>` and correctly-escaped dynamic values.
   - Add a regression test asserting that non-share-only descriptions (e.g. `measures = c("mean", "headcount")`) still produce a character-vector `measure_interpretation` unchanged from today.
7. **Capture as roadmap idea (deferred, not implemented here):** Analysis-var grammar for continuous vs. boolean vars - the `"<analysis_var_label> is true"` wording is safe today because share measures only support boolean/target vars, but this limitation will surface elsewhere in the description (and in any future non-boolean share measure). Track separately.
