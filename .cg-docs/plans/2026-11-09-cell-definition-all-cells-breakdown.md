---
date: 2026-11-09
title: "Adaptive cell enumeration and per-slot category breakdown in GET /cell-definition"
status: active
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-11-09-cell-definition-all-cells-breakdown.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
artifact-schema-version: 1
phases: 2
tags: [cell-definition, page-2, api, endpoint, ux, wording, cell-explanation]
---

# Plan: Adaptive Cell Enumeration and Per-Slot Category Breakdown in GET /cell-definition

## Objective

Make `what_a_cell_is` in `GET /cell-definition` explain the whole table rather
than only the first cell, without UI bloat. When the configured table has ≤ 4
cells, enumerate every combined cell in plain English; when it has > 4 cells,
emit a compact one-line-per-active-slot category breakdown (max 4 lines) plus a
single concrete example cell. Expose a uniform `{ title, structure_text, items,
example_text }` shape so ITS can render it with zero conditional logic.

## Context

`build_cell_explanation()` (`R/cell_explanation.R`) currently builds Section 2
(`what_a_cell_is`) via `.build_what_a_cell_is()`, which uses `.first_slot_code()`
to pick only `categories[[1L]]` of each active layout slot and emits a single
`example_cell_label` / `example_text`. Every other category of every selected
covariate is left unexplained for the Page-2 user, and `.first_slot_code()`
ignores `filter_base`, so a slot variable that is also filtered can yield a
contradictory example cell.

The brainstorm (`.cg-docs/brainstorms/2026-11-09-cell-definition-all-cells-breakdown.md`,
scope Standard) chose **Approach 1**: a uniform display structure with a flat
`items: string[]`, full enumeration at ≤ 4 cells, factorized per-slot breakdown
above 4 cells, filter-aware slot categories, and plain-English-only phrasing.

Key facts the implementation must honor:
- Registry category **order is authoritative**. In the fixture registry,
  `gender` is `Male(1)` then `Female(0)` and `area` is `Urban(1)` then
  `Rural(0)`; the "first cell" and enumeration order follow this real order
  (the brainstorm's "women/rural" examples were illustrative only).
- The engine (`R/load_data.R:578-583`) filters rows by `filter_base` **before**
  grouping, so a slot variable also in `filter_base` only produces its retained
  categories.
- `what_is_shown` (Section 3) must keep anchoring on the same first example cell
  that `what_a_cell_is` produces (`cell_group` is already passed through).
- `/description`'s `cell_definition` section delegates to the same builder, so
  its tests must not regress.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `what_a_cell_is` exposes a uniform `{ title, structure_text, items, example_text }` shape in all cases; `items` is a plain-English `string[]` (empty `[]` when 0 slots), `example_text` is `null` when 0 slots | brainstorm Approach 1 |
| R2 | When active slots produce ≤ 4 total cells (and ≥ 1 slot), `items` enumerates every combined cell in plain English, in registry category order | brainstorm Q3 (≤4 threshold) |
| R3 | When active slots produce > 4 total cells, `items` has one plain-English line per active slot (≤ 4 lines) listing that slot's categories, and `example_text` gives the first combined cell | brainstorm Q1–Q3 |
| R4 | When a layout slot variable is also constrained by `filter_base`, that slot's resolved categories (enumeration, breakdown, and example cell) are restricted to the `filter_base`-retained categories | brainstorm edge case / `load_data.R` semantics |
| R5 | `items` never exceeds 4 entries in any configuration | brainstorm "keep it compact" |
| R6 | Plain-English only — no parenthetical UI/code labels inside `items` entries; keep footprint minimal | brainstorm Q5.1 |
| R7 | `structure_text` states the total cell count (e.g. "… — 8 cells in total:") | brainstorm samples |
| R8 | `what_is_shown` continues to use the same first example cell produced by `what_a_cell_is` | existing contract |
| R9 | Runtime fallback (registry unavailable) still returns valid, non-erroring `what_a_cell_is` | existing R5 fallback behavior |
| R10 | `docs/api-contract.md` and `SWAGGER_GUIDE.md` document the new `what_a_cell_is` schema with ≤4-cell and >4-cell examples | plan docs checklist |

## Phase 1: Core builder implementation

### 1. Resolve all effective categories per active slot (filter-aware)
- **Requirements**: R4
- **Files**: `R/cell_explanation.R`
- **Details**: Add a helper (e.g. `.slot_effective_categories(varname, reg, clean_fb)`)
  that returns the ordered category codes for a slot variable: all registry
  categories in registry order, intersected with `clean_fb[[varname]]` when the
  variable is present in `filter_base` (preserving registry order). Replace the
  single `.first_slot_code()` usage path in `.build_what_a_cell_is()` so each
  active slot resolves its full effective category list, and the example/first
  cell uses the first effective category (so a filtered slot never yields a
  contradictory example). Keep `.first_slot_code()` for the fallback/no-registry
  path (first effective code = first available).
- **Test Scenarios**: happy path (unfiltered slot → all categories); edge case
  (slot var filtered to a subset → only retained categories, registry order);
  error path (variable missing from registry → fallback single code).
- **Tests**: `tests/testthat/test-cell-explanation.R`
- **Acceptance criteria**: helper returns correct ordered, filter-intersected
  codes; example cell for a filtered slot uses a retained category.

### 2. Build adaptive `items` + `structure_text` + `example_text`
- **Requirements**: R1, R2, R3, R5, R6, R7, R8
- **Files**: `R/cell_explanation.R`
- **Details**: In `.build_what_a_cell_is()`, compute `total_cells` as the product
  of each active slot's effective category count. Then:
  - **0 active slots**: `items = list()`, `example_text = NULL`, `structure_text`
    = existing single-cell sentence (unchanged).
  - **≥1 slot and `total_cells <= 4`**: build `items` as the Cartesian product of
    effective categories across active slots (registry order, slots in
    column/row/superColumn/superRow display order), each rendered with the
    existing head-noun + trailing-phrase composition to a capitalized
    plain-English cell phrase; `structure_text` ends with
    `" — <total_cells> cells in total:"`; `example_text` points to the first
    cell (e.g. "The section below uses the first cell (<cell_group>) as the
    example.").
  - **`total_cells > 4`**: build `items` with one entry per active slot:
    `"<Role> (<slot_noun>): <phrase1>, <phrase2>, … or <phraseN>."` using
    plain-English category phrases (reuse `.lookup_cat_prose()` phrases /
    `.join_var_phrases`-style joining); `structure_text` ends with
    `" — <total_cells> cells in total:"`; `example_text` = existing
    "For example, the first cell is <cell_group>…" line.
  - Always set `cell_group` (first example cell) and return it so Section 3 (R8)
    is unchanged. Keep returning `dimensions` and `example_cell_label` for
    backward reference; add `items` and ensure `example_text` is `NULL` only in
    the 0-slot case.
  - Guarantee R5: ≤4-cell mode has ≤ 4 items by construction; >4-cell mode has
    ≤ 4 items (≤ 4 slots).
- **Test Scenarios**: happy path (2×2 → 4 enumerated items); edge case
  (1 slot, 4 categories → 4 enumerated items; 1 slot, 5 categories → >4 mode,
  1 breakdown line); error path (0 slots → empty items, null example_text).
- **Tests**: `tests/testthat/test-cell-explanation.R`
- **Acceptance criteria**: all three branches produce the specified fields;
  `what_is_shown` example matches `what_a_cell_is` first cell.

## Phase 2: Tests, delegation check, and documentation

### 3. Unit tests for enumeration, breakdown, and filter overlap
- **Requirements**: R1, R2, R3, R4, R5, R9
- **Files**: `tests/testthat/test-cell-explanation.R`
- **Details**: Using `.with_fixture_registry()`, add/extend tests for:
  (a) 0 slots → `items` is `list()`/length 0, `example_text` NULL;
  (b) 1 slot `row = educat4` (5 cats) → >4 mode, single breakdown line;
  (c) 1 slot `row = lstatus` (3 cats) → ≤4 mode, 3 enumerated items;
  (d) 2×2 `row = area`, `column = gender` → 4 enumerated items in registry order
  (Urban·Male first); (e) `row = educat4` (5) × `column = gender` (2) = 10 cells
  → >4 mode, 2 breakdown lines, example = first cell; (f) filter overlap:
  `column = area` with `filter_base = list(area = "1")` → area contributes only
  Urban, example cell is Urban, total_cells reflects the filtered count;
  (g) registry-unavailable fallback still returns a valid `what_a_cell_is`
  (R9). Add an explicit assertion `expect_lte(length(res$what_a_cell_is$items), 4L)`
  across the multi-slot scenarios (R5).
- **Test Scenarios**: happy/edge/error as enumerated above.
- **Tests**: `testthat::test_file("tests/testthat/test-cell-explanation.R")`
- **Acceptance criteria**: V1–V4, V3 pass; no regressions in existing
  `test-cell-explanation.R` assertions (update expected strings where the shape
  intentionally changed).

### 4. API route test + /description delegation regression
- **Requirements**: R1, R8
- **Files**: `tests/testthat/test-api-cell-definition.R`, `tests/testthat/test-cell-definition.R`
- **Details**: Assert `GET /cell-definition` returns `data.what_a_cell_is` with
  `title`, `structure_text`, `items` (array), and `example_text` for a ≤4-cell
  and a >4-cell request. Confirm JSON serialization keeps `items` as an array
  (including the single-item case; guard with the existing `list(na="null")`
  serializer and `I()`/`as.list` if needed to avoid scalar unboxing). Run the
  `/description` cell_definition delegation test to ensure no regression.
- **Test Scenarios**: happy path (both modes over HTTP); edge case
  (single-item `items` serializes as a JSON array, not a bare string).
- **Tests**: `testthat::test_file("tests/testthat/test-api-cell-definition.R")`,
  `testthat::test_file("tests/testthat/test-cell-definition.R")`, then
  `devtools::test()`
- **Acceptance criteria**: V5, V6 pass; `items` is always a JSON array.

### 5. Update API documentation
- **Requirements**: R10
- **Files**: `docs/api-contract.md`, `SWAGGER_GUIDE.md`
- **Details**: Replace the `what_a_cell_is` schema description with the new
  `{ title, structure_text, items[], example_text }` shape. Add two worked
  examples (a ≤4-cell enumeration and a >4-cell per-slot breakdown) and note the
  filter-aware category behavior and the ≤4-items guarantee for UI consumers.
- **Test Scenarios**: n/a (docs); cross-check examples against actual test output.
- **Tests**: manual diff review; examples mirror test fixtures.
- **Acceptance criteria**: V7 — docs match emitted JSON for both modes.

## Testing Strategy

- Primary signal: `test-cell-explanation.R` (pure builder, fastest).
- Serialization/transport: `test-api-cell-definition.R` over plumber fixtures.
- Regression guard: `test-cell-definition.R` (/description delegation) and full
  `devtools::test()`.
- Determinism: all assertions use fixed fixture registry category order; no
  Arrow/microdata access.

## Documentation Checklist

- [ ] `docs/api-contract.md` `what_a_cell_is` schema + 2 examples (R10)
- [ ] `SWAGGER_GUIDE.md` `GET /cell-definition` response shape note (R10)
- [ ] roxygen `@return` for `build_cell_explanation()` updated to mention `items`

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| Single-element `items` unboxed to a bare JSON string by the serializer | UI expects array, breaks rendering | Explicit array coercion (`as.list`/`I()`) + a test asserting array type for the 1-item case |
| Existing `test-cell-explanation.R` expected strings break due to intended shape change | Red suite, confusion between regression vs intended | Update only the assertions whose change is intended; keep `who_is_in_table`/`what_is_shown` assertions untouched |
| Filter-overlap example cell contradicts `filter_base` if order logic misses the intersection | Misleading explanation | `.slot_effective_categories()` intersects before picking the first cell; dedicated overlap test (f) |
| Per-slot breakdown line runs long for 10-category industry variables | UI footprint grows | Breakdown is 1 line per slot (max 4 lines); plain phrases only; acceptable per brainstorm (never the Cartesian list) |
| `/description` delegation regresses | Secondary endpoint breaks | Run `test-cell-definition.R` in Phase 2 Step 4 before docs |

## Out of Scope

- Changes to `who_is_in_table`, `what_is_shown`, or `notes` logic beyond passing
  the filter-aware first cell.
- Changes to `/description` renderers (`description_renderer.R`,
  `description_renderer_html.R`) beyond verifying no regression.
- `/table`, `/table/download`, Approach C response format.
- i18n / non-English output.
- Any UI/ITS-side rendering code or collapse/expand behavior.

## Completion Contract

### Outcome

`build_cell_explanation()` returns a `what_a_cell_is` with a uniform
`{ title, structure_text, items, example_text }` shape where `items` is a
plain-English `string[]`: full combined-cell enumeration at ≤ 4 cells, or a
compact one-line-per-active-slot category breakdown (≤ 4 lines) plus one example
cell at > 4 cells, with slot categories intersected against `filter_base`.
`GET /cell-definition`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`, and the test
suites reflect the new shape.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | 0-slot, ≤4-cell (1-slot + 2×2), >4-cell, filter-overlap unit tests pass | `testthat::test_file("tests/testthat/test-cell-explanation.R")` | 2 | yes |
| V2 | `what_a_cell_is` always exposes `title`, `structure_text`, `items` (array), `example_text` | field presence/type assertions in `test-cell-explanation.R` | 2 | yes |
| V3 | `items` never exceeds 4 entries | `expect_lte(length(items), 4L)` across scenarios | 2 | yes |
| V4 | Filtered slot lists only retained categories; example cell consistent with filter | overlap test (f) | 2 | yes |
| V5 | API route returns new shape; single-item `items` is a JSON array | `testthat::test_file("tests/testthat/test-api-cell-definition.R")` | 2 | yes |
| V6 | Full suite passes (incl. /description delegation) | `devtools::test()` | 2 | yes |
| V7 | Docs updated with new schema + both-mode examples | `docs/api-contract.md`, `SWAGGER_GUIDE.md` diffs | 2 | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | No `table_maker()` call, no Arrow/microdata access, no computed values/counts | `cell_explanation.R` diff review | 1 |
| C2 | No new package dependencies | `DESCRIPTION` unchanged | final |
| C3 | Enumeration & example cell follow registry category order, filtered by `filter_base` | V1/V4 | 1 |
| C4 | `what_is_shown` anchors on the same first cell as `what_a_cell_is` | V1 cross-check | 1 |
| C5 | Registry-unavailable fallback returns valid output | R9 test | 2 |

### Boundaries

- Allowed: `R/cell_explanation.R`, `docs/api-contract.md`, `SWAGGER_GUIDE.md`,
  `tests/testthat/test-cell-explanation.R`, `tests/testthat/test-api-cell-definition.R`,
  `tests/testthat/test-cell-definition.R`; minimal phrase additions to
  `inst/extdata/tm_cell_prose.yaml` only if a category composes poorly in list form.
- Out of scope: `who_is_in_table`/`what_is_shown`/`notes` logic changes beyond
  the filter-aware first cell; `/description` renderers; `/table`,
  `/table/download`; i18n; UI code.

### Iteration Policy

1. Implement Phase 1 (slot category resolution + adaptive builder), then run
   `test-cell-explanation.R` first for fastest signal.
2. Fix `cell_explanation.R` until V1–V4 pass.
3. Run `test-api-cell-definition.R` (V5) and `test-cell-definition.R`, then
   `devtools::test()` (V6).
4. Update docs (V7) last, once behavior is locked; mirror examples from tests.
5. If a category label reads awkwardly in a per-slot list, add a minimal phrase
   override in `tm_cell_prose.yaml` rather than editing registry data.

### Blocked-Stop Conditions

- Registry category order produces a contradictory example cell irreconcilable
  with `filter_base` semantics (needs a product decision).
- A required fixture category is missing and cannot be derived from the existing
  registry snapshot.
- `devtools::test()` surfaces a pre-existing unrelated failure that blocks the
  suite.
