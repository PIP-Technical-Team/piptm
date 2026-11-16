---
date: 2026-11-09
title: "Adaptive cell enumeration and per-slot category breakdown in GET /cell-definition"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Approach 1 — Uniform { title, structure_text, items, example_text } with full cell enumeration up to 4 cells and per-slot plain-English category breakdown above 4 cells"
tags: [cell-definition, page-2, api, endpoint, ux, wording, cell-explanation]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Adaptive Cell Enumeration and Per-Slot Category Breakdown in GET /cell-definition

## Context

`GET /cell-definition` (`R/cell_explanation.R`) currently explains Section 2
(`what_a_cell_is`) by stating which dimensions are assigned to rows, columns,
super-rows, and super-columns, and then giving an example for **only the first
cell** (`"For example, the first cell is ..."`), using `.first_slot_code()` to
pick the first category of each active variable.

From a user perspective on Page 2, showing only the first cell leaves the
remaining cells and categories unexplained. At the same time, naively listing
every combined cell across all active covariates ($N_{\text{row}} \times
N_{\text{col}} \times N_{\text{superRow}} \times N_{\text{superCol}}$) creates
severe redundancy and visual bloat when covariates have many categories (up to
560 cells in a 4-slot table).

## Requirements

1. **Eliminate single-cell ambiguity without UI bloat**:
   - **Small tables ($\le 4$ total cells)**: Enumerate every combined cell in
     plain English (e.g., for `area × gender`: `"Women living in rural areas"`,
     `"Men living in rural areas"`, `"Women living in urban areas"`,
     `"Men living in urban areas"`).
   - **Larger tables ($> 4$ total cells)**: Factorize by active layout slot —
     emit one compact plain-English bullet per occupied slot (at most 4 bullets
     total: Rows, Columns, Row groups, Column groups) listing all categories of
     that slot, followed by one concrete combined example cell to anchor
     Section 3 (`what_is_shown`).
   - **Single-cell tables (0 active slots)**: Keep `items` empty (`[]`) and
     `example_text` as `null`, with `structure_text` explaining that the table
     has a single cell covering the table population.
2. **Filter-aware slot categories**:
   - When a variable is present in both `filter_base` and an active layout slot,
     restrict that slot's resolved categories (and the example cell) to the
     categories retained by `filter_base`, preventing contradictory example
     cells while keeping the trailing overlap note in `notes`.
3. **Plain English only, minimal UI footprint**:
   - Prioritize concise plain-English phrases without redundant parenthetical
     code/UI labels.
   - Keep Page 2 purely static (no expand/collapse toggles required in the UI).
   - Guarantee `items` never exceeds 4 bullet strings in any configuration
     ($\le 4$ combined cells in small mode, or $\le 4$ occupied slots in
     factorized mode).
4. **Effortless UI contract for ITS**:
   - `what_a_cell_is` exposes a uniform display structure (`title`,
     `structure_text`, `items`, `example_text`) so the UI renders the exact
     same template in all cases without branching logic.

## Approaches Considered

### Approach 1: Uniform `{ title, structure_text, items, example_text }` (CHOSEN)

Expose a flat `items: string[]` array inside `what_a_cell_is` alongside
`title`, `structure_text`, and `example_text` (while retaining `dimensions` and
`example_cell_label` if needed for structural reference). When total cells
$\le 4$ (and at least 1 slot is active), `items` contains the plain-English
description of each combined cell (2–4 items) and `example_text` notes that the
section below uses the first cell as the example. When total cells $> 4$,
`items` contains one plain-English summary line per active slot (1–4 items)
listing its categories, and `example_text` gives the first combined cell as a
concrete example.

- **Pros**:
  - Zero conditional rendering logic for ITS: the UI always renders `title`,
    `structure_text`, `items` (as a list when non-empty), and `example_text`
    (when non-null).
  - Bounded vertical footprint: `items` has at most 4 lines whether the table
    has 2 cells or 560 cells.
  - Every category of every selected covariate is explained in plain English.
- **Cons**:
  - Slightly updates the `what_a_cell_is` JSON contract in `docs/api-contract.md`
    (acceptable because ITS has not yet integrated the endpoint).
- **Effort**: Small.

### Approach 2: Single Pre-Joined Paragraph (`{ title, structure_text, example_text }`)

Inline the cell enumeration or per-slot breakdown directly into
`structure_text` as a single paragraph with no `items` list.

- **Pros**: Minimum possible vertical height and no new JSON keys.
- **Cons**: Harder to scan on Page 2 when 3 or 4 slots are active compared to
  short bullet items.
- **Effort**: Small.

### Approach 3: Mode-Tagged Object (`mode: "cells" | "dimensions"` with nested objects)

Return different nested object arrays depending on whether total cells $\le 4$
or $> 4$.

- **Pros**: Exposes granular slot/category sub-objects for custom UI widgets.
- **Cons**: Pushes branching and string-assembly logic onto the UI developer.
- **Effort**: Medium.

## Decision

**Approach 1** — Uniform `{ title, structure_text, items, example_text }` with:
1. Full combined cell enumeration in `items` when active slots produce $\le 4$
   cells (`1 <= active_slots` and `total_cells <= 4`).
2. Per-slot plain-English category breakdown in `items` (one bullet per active
   slot, max 4 bullets) plus a concrete combined first-cell example in
   `example_text` when `total_cells > 4`.
3. Filter-aware category resolution when a layout slot variable is also
   constrained in `filter_base`.

## Next Steps

1. Update `.build_what_a_cell_is()` in `R/cell_explanation.R` to resolve all
   effective categories per active slot (intersecting with `filter_base` when
   the slot variable is filtered), compute `total_cells`, and populate `items`,
   `structure_text`, and `example_text` for the 0-slot, $\le 4$-cell, and
   $> 4$-cell cases.
2. Verify all category phrases in `inst/extdata/tm_cell_prose.yaml` compose
   cleanly both as combined cells ($\le 4$ cells) and as per-slot category lists
   ($> 4$ cells).
3. Update `docs/api-contract.md` and `SWAGGER_GUIDE.md` with the updated
   `what_a_cell_is` response schema and examples.
4. Add/update unit and plumber route tests in `tests/testthat/test-cell-explanation.R`
   and `tests/testthat/test-api-cell-definition.R` covering 0-slot, $\le 4$-cell
   (1-slot and $2 \times 2$), $> 4$-cell, and filter-slot overlap cases.
