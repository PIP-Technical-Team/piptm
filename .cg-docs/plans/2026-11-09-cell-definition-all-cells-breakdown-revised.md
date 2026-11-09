---
date: 2026-11-09
title: "Adaptive cell enumeration and per-slot category breakdown in GET /cell-definition (revised)"
status: completed
completed-date: 2026-11-09
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-11-09-cell-definition-all-cells-breakdown.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
completed-phases: [1, 2]
execution-report: ".cg-docs/work-reports/2026-11-09-cell-definition-all-cells-breakdown-revised.md"
tags: [cell-definition, page-2, api, endpoint, ux, wording, cell-explanation]
---

# Plan: Adaptive Cell Enumeration and Per-Slot Category Breakdown in GET /cell-definition (Revised)

## Objective

Make `what_a_cell_is` in `GET /cell-definition` explain the whole table rather
than only the first cell, without UI bloat. When the configured table has ≤ 4
cells (and known registry/filter categories), enumerate every combined cell in
plain English; when it has > 4 cells, emit a compact one-line-per-active-slot
category breakdown (max 4 lines) plus a single concrete example cell. Expose a
uniform `{ title, structure_text, items, example_text, dimensions,
example_cell_label }` shape so ITS can render `title`, `structure_text`,
`items`, and `example_text` with zero conditional logic.

## Context

`build_cell_explanation()` (`R/cell_explanation.R`) currently builds Section 2
(`what_a_cell_is`) via `.build_what_a_cell_is()`, which uses `.first_slot_code()`
to pick only `categories[[1L]]` of each active layout slot and emits a single
`example_cell_label` / `example_text`. Every other category of every selected
covariate is left unexplained for the Page-2 user, and `.first_slot_code()`
ignores `filter_base`, so a slot variable that is also filtered can yield a
contradictory example cell.

This revised plan incorporates all findings from `/cg-plan-review`:
- **[P1.1] Registry-unavailable fallback guard**: When `reg` is `NULL` (or any
  active slot lacks known categories from the registry or `filter_base`), do not
  append `" — <N> cells in total:"` to `structure_text`, set `items = list()`,
  and preserve existing fallback `structure_text` and `example_text`.
- **[P2.1] Grammatical per-slot breakdown (`> 4` cells)**: Format each active
  slot bullet as `"<Role> (<slot_noun>): <category_list>."` where `<Role>` is
  capitalized (`"Row"`, `"Column"`, `"Row group"`, `"Column group"`) and
  `<category_list>` follows the `.build_who_is_in_table()` single-variable rule
  (join `ci$noun` with `"or"` when all categories have `ci$noun`; otherwise
  prepend `"people "` to `.join_var_phrases(phrases)`). Ensure `lstatus` and
  `lstatus_year` code `"3"` in `inst/extdata/tm_cell_prose.yaml` join cleanly.
- **[P2.2] Canonical `display_slots` order & deterministic Cartesian product**:
  Both `<= 4`-cell enumeration and `> 4`-cell per-slot breakdown follow
  `display_slots` order (`rows`, `columns`, `superRows`, `superColumns`), with
  earlier slots varying outer/slowest and later slots varying inner/fastest.
  Extract a shared `.compose_cell_group()` helper used for both the first
  example cell and every enumerated cell in `<= 4`-cell mode.
- **[P2.3] Singular `1 cell in total` & safe filter-registry intersection**:
  Unwrap registry codes via `vapply(reg_entry$categories, function(x)
  as.character(x$code)[[1L]], character(1))`, match `clean_fb[[varname]]`
  case-insensitively against both `code` and `label`, fall back to
  `clean_fb[[varname]]` if the intersection is empty, and emit singular
  `" — 1 cell in total:"` when `total_cells == 1L`.
- **[P3.1 & P3.2] Normative `example_text` & read-only `/description` test**:
  Keep `example_text` wording identical across `<= 4`-cell and `> 4`-cell modes
  (`"For example, the first cell is <cell_group>."`, plus
  `" \u2014 within the filtered group above."` when `has_filter &&
  (!is_short_filter || any_shares)`), and keep `tests/testthat/test-cell-definition.R`
  as a read-only regression check.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `what_a_cell_is` exposes `{ title, structure_text, items, example_text, dimensions, example_cell_label }` in all cases; `items` is a plain-English `string[]` (empty `[]` when 0 slots or when registry categories are unavailable), `example_text` is `null` when 0 slots | brainstorm Approach 1 + P1.1 |
| R2 | When active slots have known categories and produce `1 <= total_cells <= 4`, `items` enumerates every combined cell in plain English (`rows`, `columns`, `superRows`, `superColumns` outer-to-inner, registry category order) | brainstorm Q3 + P2.2 |
| R3 | When active slots have known categories and produce `total_cells > 4`, `items` has one plain-English line per active slot (`<= 4` lines, in `display_slots` order) formatted as `"<Role> (<slot_noun>): <category_list>."` using grammatical noun/people phrases | brainstorm Q1–Q3 + P2.1 |
| R4 | When a layout slot variable is also constrained by `filter_base`, that slot's effective categories (enumeration, breakdown, `total_cells`, and first example cell) are restricted to the `filter_base`-retained categories in registry order | brainstorm edge case + P2.3 |
| R5 | `items` never exceeds 4 entries in any configuration | brainstorm compactness rule |
| R6 | Plain-English only — no parenthetical UI/code labels inside `items` entries | brainstorm Q5.1 |
| R7 | When categories are known and `>= 1` slot is active, `structure_text` appends `" \u2014 1 cell in total:"` (`total_cells == 1L`) or `" \u2014 <total_cells> cells in total:"` (`total_cells >= 2L`) | brainstorm samples + P2.3 |
| R8 | `what_is_shown` continues to use the exact same first example cell (`cell_group`) produced by `what_a_cell_is`, and `example_text` preserves the existing `"For example, the first cell is ..."` wording | existing contract + P3.1 |
| R9 | Runtime fallback (`reg = NULL` or unknown categories) omits the cell-count suffix, returns `items = list()`, and preserves existing fallback `structure_text` and `example_text` | existing R5 fallback + P1.1 |
| R10 | `docs/api-contract.md` and `SWAGGER_GUIDE.md` document the new `what_a_cell_is` schema with `<= 4`-cell and `> 4`-cell examples | plan docs checklist |

## Phase 1: Core builder implementation

### 1. Resolve effective categories per active slot and shared cell-group composer
- **Requirements**: R4, R9
- **Files**: `R/cell_explanation.R`, `inst/extdata/tm_cell_prose.yaml`
- **Details**:
  - Add `.slot_effective_categories(varname, reg, clean_fb)` in `R/cell_explanation.R`:
    - Extract registry categories from `reg[[varname]]$categories` using
      `c_code <- as.character(cat_item$code)[[1L]]` and
      `c_lbl <- as.character(cat_item$label)[[1L]]`.
    - If `varname %in% names(clean_fb)`, filter the registry categories to those
      matching `clean_fb[[varname]]` case-insensitively on `c_code` or `c_lbl`
      (preserving registry order). If no registry categories match (or `reg` has
      no categories for `varname`), use `clean_fb[[varname]]` directly.
    - Return `list(codes = <character>, known = TRUE)` when categories come from
      `reg[[varname]]$categories` or `clean_fb[[varname]]`, and
      `list(codes = .first_slot_code(varname, reg), known = FALSE)` when neither
      is available (`reg = NULL` fallback).
  - Extract `.compose_cell_group(slot_cat_list, has_filter, is_short_filter, any_shares, clean_fb, active_varnames, filter_phrases_trailing)`
    implementing the existing head-noun precedence (`columns > rows > superColumns > superRows`),
    `pre_modifiers` (`adj`), `trailing` phrases, and short-filter trailing suffix
    so both the first example cell and every cell in `<= 4`-cell enumeration use
    the exact same composition rule.
  - Check `lstatus` and `lstatus_year` in `inst/extdata/tm_cell_prose.yaml`
    (code `"3"`: `"who are outside the labor force"` or handle `"outside the labor force"`
    in `.join_var_phrases()`) so joining all 3 labor-status categories behind
    `"people "` is grammatical.
- **Test Scenarios**: happy path (unfiltered slot → all registry codes with
  `known = TRUE`); edge case (slot var in `filter_base` → only retained codes in
  registry order; non-matching filter code → falls back to `clean_fb[[varname]]`);
  error/fallback path (`reg = NULL` → `codes = "1"`, `known = FALSE`).
- **Tests**: `tests/testthat/test-cell-explanation.R`, `tests/testthat/test-cell-prose-sync.R`
- **Acceptance criteria**: `.slot_effective_categories()` and `.compose_cell_group()`
  handle all three paths cleanly; `test-cell-prose-sync.R` passes.

### 2. Build adaptive `items`, `structure_text`, and `example_text` in `.build_what_a_cell_is()`
- **Requirements**: R1, R2, R3, R5, R6, R7, R8, R9
- **Files**: `R/cell_explanation.R`
- **Details**:
  - For **0 active slots**: return `items = list()`, `example_text = NULL`,
    `dimensions = list()`, `example_cell_label = table_pop_short`, and the
    existing single-cell `structure_text`.
  - For **≥ 1 active slots**:
    - Resolve each active slot in `display_slots` order (`rows`, `columns`,
      `superRows`, `superColumns`) using `.slot_effective_categories()` and look
      up `.lookup_cat_prose()` for every effective code.
    - Set the slot's `example_category` and `cat_info` from its **first**
      effective category (`cat_infos[[1L]]`), and compute `cell_group` via
      `.compose_cell_group()`.
    - Check `all_known <- all(vapply(slot_resolved, function(sr) sr$known, logical(1)))`.
    - If `!all_known` (`reg = NULL` fallback): keep `structure_text` without a
      cell-count suffix, set `items = list()`, and keep `example_text` unchanged.
    - If `all_known`:
      - Compute `total_cells <- prod(vapply(slot_resolved, function(sr) length(sr$cat_infos), integer(1)))`.
      - Replace the trailing period of `structure_text` with
        `sprintf(" \u2014 %d %s in total:", total_cells, if (total_cells == 1L) "cell" else "cells")`.
      - If `total_cells <= 4L`: generate the Cartesian index grid across
        `slot_resolved` with earlier slots varying **outer/slowest** and later
        slots varying **inner/fastest**. For each combination, compose the cell
        phrase via `.compose_cell_group(...)` and apply `.cap_first()`. Store as
        `items = as.list(cell_phrases)`.
      - If `total_cells > 4L`: for each `sr` in `slot_resolved`, build
        `<category_list>`: when all `sr$cat_infos` have non-empty `noun`, join
        `noun` values with `.join_natural(..., conj = "or")`; otherwise paste
        `"people"` and `.join_var_phrases(phrases)`. Format each bullet as
        `sprintf("%s (%s): %s.", .cap_first(sr$role_label), sr$slot_noun, category_list)`
        and store as `items = as.list(slot_bullets)`.
    - Keep `example_text` across all ≥ 1-slot cases as
      `sprintf("For example, the first cell is %s \u2014 within the filtered group above.", cell_group)`
      when `has_filter && (!is_short_filter || any_shares)` and
      `sprintf("For example, the first cell is %s.", cell_group)` otherwise.
  - Update roxygen `@return` on `build_cell_explanation()` if needed.
- **Test Scenarios**: happy path (`2x2` → 4 combined cell items in outer-to-inner
  order; `5x2` → 2 per-slot bullets); edge case (1 slot with 1 filtered category
  → `" — 1 cell in total:"` and 1 item; 1 slot with 3 categories → 3 items;
  1 slot with 5 categories → 1 breakdown bullet); fallback path (`reg = NULL` →
  `items = list()`, no cell-count suffix).
- **Tests**: `tests/testthat/test-cell-explanation.R`
- **Acceptance criteria**: all branches emit the exact contract; `length(items) <= 4L`
  in every case.

## Phase 2: Tests, route verification, and documentation

### 3. Unit tests for enumeration, breakdown, filter overlap, and fallback
- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8, R9
- **Files**: `tests/testthat/test-cell-explanation.R`
- **Details**:
  - Update existing `structure_text` assertions in `test-cell-explanation.R` for
    Examples 1–4 where `all_known` is `TRUE` (which now include `" — <N> cells in total:"`)
    and verify `res$what_a_cell_is$items` for each example:
    - Example 1 (`educat4` (5) × `gender` (2) × `area` (2) = 20 cells): 3
      per-slot breakdown items in `Row`, `Column`, `Column group` order.
    - Example 2 (`area` (2) × `gender` (2) = 4 cells, shares): 4 combined cell
      items in `(Urban·Male, Urban·Female, Rural·Male, Rural·Female)` order.
    - Example 3 (`area` (2) × `gender` (2) = 4 cells, poverty): 4 combined cell
      items.
    - Example 4 (`pov_status` (2) × `gender` (2) = 4 cells, filtered to rural):
      4 combined cell items.
    - Example 5 (0 slots): `items` is `list()`, `example_text` is `NULL`.
  - Add dedicated unit tests for:
    - 1-slot `<= 4` cells (`row = "lstatus"`, 3 cells → 3 enumerated items).
    - 1-slot `> 4` cells (`row = "educat4"`, 5 cells → 1 breakdown item).
    - Filter-slot overlap (`row = "area"`, `column = "gender"`,
      `filter_base = list(area = "0")` Rural only → `2 cells in total`, first
      cell is `"men living in rural areas"`, not urban; and 1-slot
      `column = "area"`, `filter_base = list(area = "1")` → `"1 cell in total"`,
      1 item).
    - Registry-unavailable fallback (`release = "MISSING_REL"` → `items = list()`,
      no `"cells in total"` suffix).
    - `expect_lte(length(res$what_a_cell_is$items), 4L)` across all scenarios.
- **Test Scenarios**: happy/edge/error as listed above.
- **Tests**: `testthat::test_file("tests/testthat/test-cell-explanation.R")`
- **Acceptance criteria**: V1–V4 and C5 pass with 0 failures.

### 4. API route tests and full test suite verification
- **Requirements**: R1, R8
- **Files**: `tests/testthat/test-api-cell-definition.R`
- **Details**:
  - Update `tests/testthat/test-api-cell-definition.R` to assert that
    `GET /cell-definition` returns `data$what_a_cell_is$items` as a JSON array
     across:
    - 0-slot request (`items` is `[]`, length 0),
    - 1-slot `> 4`-cell request (`row = "educat4"`, `items` is a 1-element JSON
      array `["Row (education level): ..."]`, not unboxed to a scalar string),
    - `2x2` `<= 4`-cell request (`row = "area"`, `column = "gender"`, `items` is
      a 4-element JSON array),
    - multi-slot `> 4`-cell request (`row = "educat4"`, `column = "gender"`,
      `items` is a 2-element JSON array).
  - Run read-only `tests/testthat/test-cell-definition.R` and full `devtools::test()`.
- **Test Scenarios**: happy path (all 4 cardinalities of `items` over HTTP JSON);
  edge case (1-element `items` remains a JSON array after `jsonlite::fromJSON(..., simplifyVector = FALSE)`).
- **Tests**: `testthat::test_file("tests/testthat/test-api-cell-definition.R")`,
  `testthat::test_file("tests/testthat/test-cell-definition.R")`, `devtools::test()`
- **Acceptance criteria**: V5 and V6 pass.

### 5. Update API documentation
- **Requirements**: R10
- **Files**: `docs/api-contract.md`, `SWAGGER_GUIDE.md`
- **Details**:
  - Update `docs/api-contract.md` (`GET /cell-definition` section) to document
    `what_a_cell_is.items` (`string[]`, max 4 items), the `<= 4`-cell combined
    enumeration vs `> 4`-cell per-slot breakdown rule, the filter-overlap
    behavior, and worked JSON examples for both modes.
  - Update `SWAGGER_GUIDE.md` (`GET /cell-definition` response overview and
    example payload) to match.
- **Test Scenarios**: n/a (documentation).
- **Tests**: verify JSON examples in docs match test assertions.
- **Acceptance criteria**: V7 complete.

## Testing Strategy

- Fast inner loop: `testthat::test_file("tests/testthat/test-cell-explanation.R")`
  and `testthat::test_file("tests/testthat/test-cell-prose-sync.R")`.
- Plumber serialization verification: `testthat::test_file("tests/testthat/test-api-cell-definition.R")`.
- Full regression suite: `devtools::test()`.

## Documentation Checklist

- [ ] `docs/api-contract.md` updated with `what_a_cell_is.items` schema and `<= 4` / `> 4` examples (R10)
- [ ] `SWAGGER_GUIDE.md` updated with `what_a_cell_is.items` in `GET /cell-definition` (R10)
- [ ] `R/cell_explanation.R` roxygen updated if `@return` details change

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Single-element `items` (e.g., 1-slot `educat4` breakdown or 1-cell filtered table) unboxed to a JSON string if stored as character vector | Breaks UI `items.map(...)` | Always store `items` as an R `list` (`as.list(...)`), which plumber's JSON serializer always serializes as a JSON array `["..."]` even at length 1; assert with `simplifyVector = FALSE` in `test-api-cell-definition.R` |
| Registry-unavailable fallback (`reg = NULL`) treated as a 1-cell table | False `"1 cell in total"` and bogus item in fallback mode | Track `known` flag in `.slot_effective_categories()` and emit `items = list()` + no cell-count suffix when `!all_known` (P1.1) |
| Ungrammatical per-slot breakdown after colon in `> 4`-cell mode | Reads as `"Column (gender): who are men or women."` | Use `ci$noun` when all categories have `noun`, otherwise prepend `"people "` to `.join_var_phrases(phrases)` (P2.1) |
| Cartesian product slot order mismatch | Row/column enumeration order doesn't match table grid | Iterate `display_slots` (`rows`, `columns`, `superRows`, `superColumns`) outer-to-inner (P2.2) |

## Out of Scope

- Modifying `tests/testthat/test-cell-definition.R` (read-only regression check).
- Changes to `who_is_in_table`, `what_is_shown`, or `notes` logic beyond
  receiving the filter-aware first `cell_group`.
- Changes to `/description` renderers (`description_renderer.R`,
  `description_renderer_html.R`).
- Changes to `/table` or `/table/download`.
- i18n or UI-side code.

## Completion Contract

### Outcome

`build_cell_explanation()` returns a `what_a_cell_is` section with a uniform
`{ title, structure_text, items, example_text, dimensions, example_cell_label }`
shape where `items` is a plain-English `string[]`: full combined-cell
enumeration when `1 <= total_cells <= 4` (with known categories), a compact
one-line-per-active-slot category breakdown (`<= 4` lines) when
`total_cells > 4`, and `items = []` when 0 slots are active or when the variable
registry is unavailable. Slot categories are intersected with `filter_base` in
registry order, and `GET /cell-definition`, `docs/api-contract.md`,
`SWAGGER_GUIDE.md`, and tests reflect the revised contract.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | Unit tests for 0-slot, `<= 4`-cell (1-slot + `2x2`), `> 4`-cell, filter-slot overlap (including `1 cell in total`), and registry-unavailable fallback (`items = list()`, no cell-count suffix) pass | `testthat::test_file("tests/testthat/test-cell-explanation.R")` | 2 | yes |
| V2 | `what_a_cell_is` always exposes `title`, `structure_text`, `items` (list/array), and `example_text` | Field presence/type assertions in `test-cell-explanation.R` | 2 | yes |
| V3 | `items` never exceeds 4 entries in any configuration | `expect_lte(length(res$what_a_cell_is$items), 4L)` across scenarios | 2 | yes |
| V4 | Filtered slot lists only retained categories in registry order; example cell and `total_cells` reflect the filtered categories | Overlap test in `test-cell-explanation.R` | 2 | yes |
| V5 | `GET /cell-definition` route returns new shape; 0-item, 1-item, and multi-item `items` serialize as JSON arrays (`[]`, `["..."]`, `["...", "..."]`) | `testthat::test_file("tests/testthat/test-api-cell-definition.R")` | 2 | yes |
| V6 | Full package test suite and prose sync tests pass | `devtools::test()` | 2 | yes |
| V7 | `docs/api-contract.md` and `SWAGGER_GUIDE.md` updated with new `what_a_cell_is` schema and `<= 4`-cell / `> 4`-cell examples | `docs/api-contract.md`, `SWAGGER_GUIDE.md` diffs | 2 | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | No `table_maker()` call, no Arrow/microdata access, no computed values/sample counts | `R/cell_explanation.R` diff review | 1 |
| C2 | No new package dependencies | `DESCRIPTION` unchanged | final |
| C3 | Deterministic output: slot order is `rows`, `columns`, `superRows`, `superColumns` (outer-to-inner for Cartesian product) and category order follows the registry filtered by `filter_base` | V1 / V4 | 1 |
| C4 | `what_is_shown` anchors on the exact same first example cell (`cell_group`) produced by `what_a_cell_is` | V1 assertions | 1 |
| C5 | Registry-unavailable fallback (`reg = NULL`) returns `items = list()` and preserves fallback `structure_text` / `example_text` without false cell counts | V1 fallback test | 1 |

### Boundaries

- Allowed: `R/cell_explanation.R`, `inst/extdata/tm_cell_prose.yaml`,
  `man/build_cell_explanation.Rd` (if roxygen updated), `docs/api-contract.md`,
  `SWAGGER_GUIDE.md`, `tests/testthat/test-cell-explanation.R`,
  `tests/testthat/test-api-cell-definition.R`.
- Out of scope: Modifying `tests/testthat/test-cell-definition.R` (read-only
  regression check); changes to `who_is_in_table`, `what_is_shown`, or `notes`
  logic beyond receiving the filter-aware first `cell_group`; `/description`
  renderers; `/table` or `/table/download`; UI code.

### Iteration Policy

1. Implement Phase 1 in `R/cell_explanation.R` (and `tm_cell_prose.yaml` if
   needed), then run `test-cell-explanation.R` and `test-cell-prose-sync.R`.
2. Fix any failures until V1–V4 and C1–C5 pass.
3. Run `test-api-cell-definition.R` (V5) and read-only `test-cell-definition.R`,
   then `devtools::test()` (V6).
4. Update `docs/api-contract.md` and `SWAGGER_GUIDE.md` (V7) last to match
   verified output.

### Blocked-Stop Conditions

- Registry category order produces a contradictory example cell irreconcilable
  with `filter_base` semantics.
- A required test fixture category is missing from
  `tests/testthat/fixtures/registry-cell-prose-sample.json`.
- `devtools::test()` surfaces a pre-existing unrelated failure blocking the test
  suite.
