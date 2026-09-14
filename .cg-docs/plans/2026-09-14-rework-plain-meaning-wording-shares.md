---
date: 2026-09-14
title: "Rework plain-meaning wording for share measures"
status: completed
completed-date: 2026-09-14
scope: "Lightweight"
brainstorm: null
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
artifact-schema-version: 1
tags: [description-endpoint, cell-definition, shares, wording, ux]
---

# Plan: Rework Plain-Meaning Wording for Share Measures

## Objective

Replace the abstract "this cell" / "total weighted survey population" plain-meaning templates for the three share measures (`pop_share`, `target_within_group_share`, `target_survey_share`) in `build_cell_definition()` with concrete "Among X, what fraction..." phrasing built from the actual filter clause, group label, and target-variable label already used elsewhere in the description, while keeping an honest, simple statement for the `pop_share` degenerate case (`by = NULL`).

## Context

This follows `.cg-docs/plans/2026-09-14-shares-cell-definition-structured-table-revised.md` (completed), which introduced the structured `shares_table` with a `plain_meaning` column. The user found the current plain-meaning text unclear and abstract, and requested concrete wording using actual variable labels, following a fixed pattern across three scenarios (filter+group, no-filter+group, filter+no-group).

**Accuracy correction agreed with the user during planning**: the user's original example wording for `pop_share` under `by = NULL` ("Among the total survey population, what fraction are individuals aged 0–14?") describes a quantity the system does not compute in that case. Filtering happens upstream of `compute_shares()` (confirmed in the prior plan's source-of-truth check), so when `by = NULL`, `pop_share`'s denominator and numerator are both the same filtered population and the ratio is always exactly 1 — it can never measure "the filter's share of the unfiltered survey." The user agreed to a simple, honest statement instead: **"This value is always 1 when there is no grouping."**

Building blocks already computed in `build_cell_definition()` and reused here (no new computation needed):
- `filter_text` — the composed filter clause (e.g. `"Age group is among [0 to 14]"`), only set when `filter_base` is non-empty.
- `format_covariate_description(by, resolved_labels$covariates)` — the group label (e.g. `"Area"`, `"Gender × Area"`), with a separate `pov_status`-aware branch already used for `population_scope`.
- `analysis_var_label` — the target variable's UI label (e.g. `"Improved water source"`).

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `pop_share` plain meaning, grouping present: `"Among {filter_phrase}, what fraction fall in {group_phrase}?"` | User pattern |
| R2 | `target_within_group_share` plain meaning, grouping present: `"Among {filter_phrase} in {group_phrase}, what fraction have {target label}?"` | User pattern |
| R3 | `target_survey_share` plain meaning, grouping present: `"Among {filter_phrase}, what fraction fall in {group_phrase} and have {target label}?"` | User pattern |
| R4 | `filter_phrase` = `"all individuals for whom {filter_text}"` when a filter is applied, else `"the total survey population"` | User pattern + accuracy |
| R5 | `group_phrase` = `"this {group label} group"`, using the same pov_status-aware group-label resolution as `population_scope` (no drift between the two) | Accuracy/consistency |
| R6 | `target_within_group_share` and `target_survey_share` plain meaning, `by = NULL`: both become `"Among {filter_phrase}, what fraction have {target label}?"` (identical text, since the values are numerically identical in this case) | User pattern + accuracy |
| R7 | `pop_share` plain meaning, `by = NULL`: `"This value is always 1 when there is no grouping."` | User decision (accuracy correction) |
| R8 | `denominator`, `numerator`, `measure`, and `shares_footer` logic remain unchanged | User scope constraint |
| R9 | Doc example (`docs/description-endpoint-spec.md`, Example 3) reflects the new plain-meaning wording | Consistency |

## Implementation Steps

### 1. Rework plain-meaning templates and update builder tests
- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8
- **Files**: `R/description_builder.R`, `tests/testthat/test-cell-definition.R`
- **Details**:
  - In `build_cell_definition()`, within the shares block (where `share_row()` is defined), compute:
    - `has_filter <- !is.null(filter_base) && length(filter_base) > 0`
    - `filter_phrase <- if (has_filter) sprintf("all individuals for whom %s", filter_text) else "the total survey population"`
    - A group-label resolver reused from the same logic already used for `population_scope`'s `pov_status`-aware branch (extract into a small internal helper, e.g. `.resolve_group_label(by, resolved_labels)`, called from both places to guarantee the group label never drifts between `population_scope` and the shares table's `plain_meaning`). When `by` is NULL/empty, this resolves to `NULL`.
    - `group_phrase <- if (!is.null(group_label)) sprintf("this %s group", group_label) else NULL`
  - Replace the three `plain_meaning` values in `share_row()`:
    - `pop_share`: if `no_grouping`, use `"This value is always 1 when there is no grouping."`; else `sprintf("Among %s, what fraction fall in %s?", filter_phrase, group_phrase)`.
    - `target_within_group_share`: if `no_grouping`, use `sprintf("Among %s, what fraction have %s?", filter_phrase, analysis_var_label)`; else `sprintf("Among %s in %s, what fraction have %s?", filter_phrase, group_phrase, analysis_var_label)`. This replaces the existing no-grouping special case text ("What share of the total weighted survey population has...").
    - `target_survey_share`: if `no_grouping`, use the same text as `target_within_group_share` in that case (R6); else `sprintf("Among %s, what fraction fall in %s and have %s?", filter_phrase, group_phrase, analysis_var_label)`.
  - Do not change `denominator`, `numerator`, `measure`, or `shares_footer` construction.
  - Update roxygen comments describing the plain-meaning wording rationale if present.
  - Update `tests/testthat/test-cell-definition.R`: revise expected `plain_meaning` strings in the Example 3 test, the `by=NULL` negative test, and the "all three shares, no filter, with grouping" test; add a new test covering filtered + no-grouping (to exercise R7 and the `target_within_group_share`/`target_survey_share` no-grouping unification from R6); add a case using `by = "pov_status"` to confirm the group label matches `population_scope`'s wording (R5/V2).
- **Test Scenarios**: filtered + grouped; unfiltered + grouped; filtered + ungrouped (pop_share degenerate text; target_within_group_share/target_survey_share identical text); `pov_status` grouping label consistency.
- **Tests**: `testthat::test_file("tests/testthat/test-cell-definition.R")`.
- **Acceptance criteria**: All new/updated assertions pass with exact expected text; existing denominator/numerator/footer assertions in the same file remain unchanged and pass; full test file run is green.

### 2. Update the internal spec's worked example
- **Requirements**: R9
- **Files**: `docs/description-endpoint-spec.md`
- **Details**: Update Example 3's plain-meaning column (added in the prior plan's Step 5) to show the new wording for `target_within_group_share` and `target_survey_share` under that example's filtered + grouped scenario.
- **Test Scenarios**: doc text matches the actual builder output for the same inputs.
- **Tests**: manual consistency check against Step 1's test expectations for the same scenario.
- **Acceptance criteria**: no stale plain-meaning wording remains in the doc.

## Testing Strategy

- Builder tests in `test-cell-definition.R` are the sole correctness gate — exact string assertions per scenario, mirroring the existing test file's convention.
- No renderer changes are needed since renderers already display whatever `plain_meaning` text the builder returns; existing renderer tests remain unaffected and serve as a regression check.
- Full package test suite run at the end to confirm no unrelated regressions.

## Documentation Checklist

- [ ] Update Example 3's plain-meaning column in `docs/description-endpoint-spec.md`.
- [ ] No roxygen `@return` shape change needed (only text content changes, not structure).

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Group label used in plain-meaning drifts from `population_scope`'s group label over time | Extract a single shared helper (`.resolve_group_label()`) used by both, rather than duplicating the pov_status branch logic |
| `target_within_group_share`/`target_survey_share` identical no-grouping text reads as a bug to a future maintainer | Add a code comment explaining the values are genuinely identical in this case (mirrors an existing comment pattern from the prior plan's degenerate-case handling) |
| New wording accidentally changes for non-degenerate cases already covered by passing tests | Full test file re-run after changes; any unexpected diff is treated as a regression, not silently accepted |

## Out of Scope

- Denominator, numerator, `measure`, and `shares_footer` wording/logic.
- Non-share measure templates (poverty/inequality/summary statistics).
- Renderer code changes (Markdown/HTML) — they already display the builder's text unchanged.
- Continuous/non-boolean analysis-variable grammar (pre-existing known limitation, not addressed here).

## Completion Contract

### Outcome

The three share-measure `plain_meaning` templates in `build_cell_definition()` use natural, concrete "Among X, what fraction..." phrasing built from actual filter/group/target labels instead of abstract "this cell" language, with an honest, simple statement for the `pop_share` degenerate case (`by = NULL`), and matching test/doc updates.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | New templates produce exact expected text for: filtered+grouped, unfiltered+grouped, filtered+ungrouped, unfiltered+ungrouped | `testthat::test_file("tests/testthat/test-cell-definition.R")` | yes |
| V2 | Group label reused from the same pov_status-aware logic as `population_scope` (no drift between the two) | Same file: a `by = "pov_status"` case | yes |
| V3 | `target_within_group_share` and `target_survey_share` plain-meaning texts are identical when `by = NULL` (since the values are identical) | Same file | yes |
| V4 | Existing non-wording tests (denominator/numerator/footer) remain unaffected | Same file, full run | yes |
| V5 | `docs/description-endpoint-spec.md` Example 3 plain-meaning column matches the new wording | Manual doc read | yes |
| V6 | Full package test suite has no regressions | `devtools::test()` | yes |

### Constraints

| ID | Constraint | Check |
|----|------------|-------|
| C1 | Only `plain_meaning` text changes; `denominator`, `numerator`, `measure`, and `shares_footer` logic untouched | Diff review |
| C2 | No new dependencies | `DESCRIPTION` diff |
| C3 | No change to non-share prose path | Diff review + regression tests |

### Boundaries

- Allowed: `R/description_builder.R` (plain-meaning template logic inside `build_cell_definition()`/`share_row`, plus an optional small shared group-label helper), `tests/testthat/test-cell-definition.R`, `docs/description-endpoint-spec.md`.
- Out of scope: denominator/numerator wording, footer wording, non-share measure templates, HTML/Markdown renderer code (they just display whatever text the builder returns).

### Iteration Policy

1. Implement and test the builder wording changes first.
2. Update the doc example only after tests confirm the exact new strings.

### Blocked-Stop Conditions

- Any existing test asserting old plain-meaning wording that isn't in this plan's known list is found — pause and confirm before overwriting.
- The pov_status-aware group-label logic can't be safely reused without duplicating meaningfully different behavior.
