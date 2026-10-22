---
date: 2026-10-19
title: "/table response format: layout slot assignment for the UI"
status: decided
scope: "Deep"
artifact-schema-version: 1
chosen-approach: "Approach C - uniform flat cells[] with fixed slot keys plus a per-survey layout block"
tags: [api, table-endpoint, response-format, layout-slots, ui-contract, table-maker, breaking-change]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# `/table` Response Format: Layout Slot Assignment for the UI

## Context

The `/table` endpoint (`inst/plumber/plumber.R:142-221`) returns a flat,
long-format array of rows. Each row carries the covariate **codes** it was
grouped by (`gender: 0`, `area: 0`, `educat4: 1`) but says nothing about
**which table position each covariate occupies**. The PIP platform UI needs
that mapping to place each variable correctly in the rendered cross-tab -
columns, rows, super columns, or super rows (`docs/ui-description.md:155-173`).

Without it the UI cannot build the table. This is the blocking gap.

### The slot concept already exists, and it is dead code

`R/table_maker.R:291-333` builds a `covariates` table whose first field is
`slot`, read at `R/table_maker.R:311` as:

```r
slot = cov_entry$slot %||% "rows"
```

But `piptm_layout_covariates()` (`R/registry.R:582-596`) returns only
`varname`, `label`, and `n_categories` - **there is no `slot` field in the
registry**. The `%||%` fallback therefore fires on every covariate, and every
covariate has been silently resolving to `"rows"` since the code was written.
This is not a cosmetic bug. It means the one place in the codebase that
claims to carry slot information has never carried it. `R/table_maker.R:309-327`
must be rewritten and the registry-driven `slot` idea removed entirely - it
is the wrong model (see Decision below). Slot is a property of **position in
`by`**, not of the variable.

### Prior related work

- `.cg-docs/brainstorms/2026-08-31-api-contract-openapi.md` - API contract
  formalization; `docs/api-contract.md:156-195` documents the current
  `/table` shape that this brainstorm supersedes.
- `.cg-docs/brainstorms/2026-09-14-shares-cell-definition-structured-table.md`
  - precedent for restructuring an API payload for UI comprehension, and the
  source of the "UI is the sole consumer, free to define the shape" posture.
- `.cg-docs/brainstorms/2026-04-27-table-maker-input-parameters.md` - the
  `by` parameter's original definition.

## Requirements

### Purpose

Give the UI enough information to render the cross-tab without inferring
anything. Success = the UI receives a response it can walk once and render
directly, making no decisions about placement, grouping, or cell content.

### The slot assignment rule (confirmed)

Slot assignment is **positional in `by`**, not a property of the variable:

| `by` position | Slot |
|---|---|
| 1 | columns |
| 2 | rows |
| 3 | super columns |
| 4 | super rows |

So `by = (X, Y, Z, W)` gives `X -> columns, Y -> rows, Z -> super columns,
W -> super rows`, while `by = (Y, X, W, Z)` gives `Y -> columns, X -> rows,
W -> super columns, Z -> super rows`. The mapping follows position, nothing
else. This matches the numbered slot labels in the UI spec
(`docs/ui-description.md:107-116`: "1 Columns", "2 Rows", "3 Super Columns",
"4 Super Rows").

Fewer than four covariates fill the slots in order from the top; trailing
slots stay empty. `by = NULL` is legal and yields a single national-level
cell per measure (`docs/ui-description.md:125`).

**Gapped assignments are impossible by design.** A user cannot place a
covariate in super rows while leaving columns empty. The user selects
covariates *in order*, and slot assignment follows automatically from
position. This is an intentional product decision, and it has a UI
consequence recorded under Next Steps: Decision 3 must present an **ordered
selection**, not four independently fillable drop targets.

### Users

The PIP platform UI is the sole consumer of `/table`. It is not yet in
production against this endpoint, so the response shape may be changed
outright - no versioning, no deprecation window, no compatibility shim.

### Inputs

**The request signature does not change.** This was audited and confirmed:

- `by` order is already preserved end to end. `plumber.R:131` declares it
  repeatable `[character]`; `validate_table_input()`
  (`inst/plumber/helpers.R:365-382`) performs set-membership checks only and
  never sorts, deduplicates, or normalizes; `table_maker()` iterates
  `params$by` in order. Both `?by=gender&by=educat4` and a POST body
  `"by": ["gender", "educat4"]` preserve order.
- No new parameters are required. Labels, density, and layout are all
  response-side concerns.
- Named slot parameters (`columns=`, `rows=`, ...) were considered and
  **rejected**: they are unnecessary once gapped assignments are impossible
  by design.

Two request-side items still need tightening, neither a signature change:

1. **Order-dependence is implicit and untested.** `plumber.R:131` documents
   `by` as "Disaggregation dimensions (optional, repeatable)" with no
   indication that order is semantically load-bearing. Once order determines
   layout this becomes a contract: it needs a roxygen note, a line in
   `docs/api-contract.md`, and a regression test. Nothing today would catch a
   future refactor that sorts or deduplicates `by`.
2. **The 4-dimension cap is enforced in the wrong layer.** `validate_table_input()`
   does not check it; `R/measures.R:157` aborts, which the plumber layer
   converts to a **422** (`plumber.R:203`). Since 4 is now a structural
   property of the slot model, this should be a **400** raised by
   `validate_table_input()`.

### Outputs

Decisions taken during this session:

| Question | Decision |
|---|---|
| Compatibility | Free to break; UI is sole consumer |
| Response depth | Variable - unfilled slots are not materialized |
| Self-description | Explicit `layout` block per survey |
| Super columns | Represented as a first-class slot (not omitted, as in the original proposal) |
| Category identity | Fixed keys `{code, label}` - never dynamic variable-named keys |
| Cell payload | `measures` namespaced under one key; `population` kept separate |
| Envelope | Retained; the surveys array nests inside `data` |
| Completeness | Dense - every cell the UI must render is present |
| Empty / suppressed cells | Backend concern; no per-cell flag. The computation layer returns a meaningful value and the UI renders it verbatim |
| `/table/download` | Unchanged - stays flat long-format |

### Constraints

- Charter: *"Interoperability: outputs must conform to schemas expected by
  downstream consumers."* Supports reshaping for the UI.
- Charter: *"Deterministic outputs: identical inputs must always produce
  identical results."* A dense response makes **cell ordering part of the
  contract**. Today ordering falls out of `data.table` grouping. It must
  become explicit and registry-driven, or two identical requests can return
  the same cells in different order. This is new work regardless of approach.
- No new package dependencies.
- `/table` and `/table/download` must not diverge - they serialize the same
  computation and a shared flat basis is the cheapest guarantee.

### Edge cases

- **`by = NULL`** - `layout` has all four slots `null`; `cells` holds exactly
  one cell with all four slot keys `null`.
- **NA covariate categories** - the current response already emits
  `educat4: null` rows (the missing-education group). These are real cells
  that must render. They need an explicit label; `"Unknown"` is used in the
  mock below, but the canonical label must come from the registry.
- **Suppression - out of scope for this redesign.** `pop_share_threshold`
  (default `0.01`) currently **drops rows from the output**
  (`R/table_maker.R` roxygen, `table_maker.R:415-424`). This behaviour
  **will be changed**: cells will no longer be dropped. The exact mechanism
  is to be defined later at the computation layer and is **not resolved
  here**. For the purposes of this response format, the UI simply receives
  all cells - it does not need to know anything about suppression.
- **Partial-match surveys** - surveys missing a requested dimension are
  excluded before loading. Their absence from `data` is already handled by
  the per-survey structure.
- **Multiple measures** - up to 4 per cell (`docs/ui-description.md:23`),
  all appearing inside `measures`.

### Scope - explicitly out of this iteration

- No change to the request signature.
- No change to `/table/download`'s flat output.
- **`include_metadata` is treated as always `false`.** The metadata branch
  (`plumber.R:187-195`, `:212-214`) is not exercised and is not modified.
- No OpenAPI schema file (none exists; plumber auto-generates from roxygen).
- **Suppression semantics.** Changing `pop_share_threshold` so that cells are
  no longer dropped is a separate computation-layer change. Defining the
  value an empty or suppressed cell carries is deferred with it, and is
  explicitly not a blocker for this format decision.

## Approaches Considered

### Approach A - Nested tree (the UI developers' proposal, corrected)

`data` holds survey objects, each nesting
`superRow -> row -> superColumn -> column`, with depth equal to the number of
covariates supplied.

- **Pros**: Exactly what the UI developers sketched; rendering is a recursive
  walk.
- **Cons**: The *name of the top-level array changes with depth*
  (`superRows` with 4 covariates, `rows` with 2, `columns` with 1), so the UI
  still branches on depth - defeating the goal. The original sketch also had
  **no place for super columns at all**; it nested only three levels.
  `table_maker()` returns a flat `data.table`, so building a nested list and
  getting `jsonlite` to serialize it correctly (auto-unboxing, singleton
  arrays collapsing to scalars, `na = "null"`) is substantial and
  error-prone. Largest payload, largest new test surface, and maximum
  divergence risk against the flat `/table/download`.
- **Effort**: Large.

### Approach B - Flat rows plus a `layout` block

Keep today's long-format rows untouched; add a `layout` object declaring the
slot mapping. The UI pivots.

- **Pros**: Smallest change by a wide margin - no change to `table_maker()`'s
  return value, no serialization risk, existing tests stay green, one shared
  serialization with `/table/download`. Delivers the actual missing
  *information* (which variable is which slot) in roughly a day.
- **Cons**: Pushes grouping and gap-filling onto the UI, which violates the
  stated principle that the UI renders and never interprets.
- **Effort**: Small.

### Approach C - Uniform flat `cells[]` with fixed slot keys - **CHOSEN**

`data` holds survey objects. Each carries a `layout` block (ordered category
lists per slot) and a flat, dense `cells[]` array. Every cell carries all
four slot keys - `superRow`, `row`, `superColumn`, `column` - with `null` for
slots that are not in use. **The key names never change with depth.**

- **Pros**: Fixed schema at every depth means one rendering path and zero
  branching - it achieves the UI developers' actual goal better than their
  own sketch does. Ordered `layout.*.categories` give the UI the exact header
  structure to draw, reducing placement to an index lookup. Far cheaper in R:
  a column rename plus a label join on the existing flat `data.table`, not a
  tree build. Trivially validatable with a JSON Schema. Maps 1:1 onto the
  flat CSV export, so `/table` and `/table/download` cannot drift.
- **Cons**: Not literally the shape the developers sketched, so it needs
  their sign-off. Repeats labels per cell, making it slightly more verbose
  per cell than A - though smaller overall than A's dense tree at depth 4.
- **Effort**: Medium.

## Decision

**Approach C.** The nested tree was not a requirement - solving the slot
mapping was. Approach C solves it more robustly: it removes the variable
key-name problem entirely, gives the UI a single uniform rendering path, is
materially cheaper to produce in R, and keeps `/table` structurally aligned
with `/table/download`.

Approach B was rejected despite its low cost because it requires the UI to
interpret and gap-fill, which contradicts the governing principle that the
**UI renders whatever it receives, full stop**.

### Governing principle

The response must always be **complete and unambiguous**. Every cell the UI
needs to render is present, already carrying a meaningful value - whatever
that value turns out to be for empty, sparse, or suppressed cells. The UI
never decides how to interpret or fill a cell. As a direct consequence there
is **no `suppressed: true` flag** or equivalent per-cell metadata: the UI
does not need to know *why* a cell holds a value, only *what* the value is.

### Agreed response format

Shareable copies of both mock payloads, plus a rendering guide for the UI
developers, live in `.cg-docs/brainstorms/2026-10-19-table-response-mocks/`.
The example below is reproduced from `example-a-three-covariates.json`.

Request used for the mock, against the real `COL_2008_GEIH_INC_ALL` figures
currently returned by `/table`:

```
GET /table
  ?analysis_var=welfare
  &pip_id=COL_2008_GEIH_INC_ALL
  &measures=mean
  &by=gender          # position 1 -> columns
  &by=educat4         # position 2 -> rows
  &by=area            # position 3 -> super columns
                      # position 4 -> super rows: unused
```

Cell ordering is deterministic and **driven by `layout`**, not by the data:
cells are sorted by the index of each category within
`layout.superRows.categories`, then `layout.rows.categories`, then
`layout.superColumns.categories`, then `layout.columns.categories`. Slots
that are `null` are skipped in the sort.

```json
{
  "status": ["success"],
  "data": [
    {
      "pip_id": "COL_2008_GEIH_INC_ALL",
      "country_code": "COL",
      "surveyid_year": 2008,
      "welfare_type": "INC",
      "poverty_line": null,
      "analysis_var": "welfare",
      "measures": ["mean"],

      "layout": {
        "columns": {
          "varname": "gender",
          "label": "Gender",
          "categories": [
            { "code": 0, "label": "Female" },
            { "code": 1, "label": "Male" }
          ]
        },
        "rows": {
          "varname": "educat4",
          "label": "Education level",
          "categories": [
            { "code": 1,    "label": "None" },
            { "code": 2,    "label": "Primary" },
            { "code": 3,    "label": "Secondary" },
            { "code": 4,    "label": "Tertiary" },
            { "code": null, "label": "Unknown" }
          ]
        },
        "superColumns": {
          "varname": "area",
          "label": "Urban / Rural",
          "categories": [
            { "code": 0, "label": "Rural" },
            { "code": 1, "label": "Urban" }
          ]
        },
        "superRows": null
      },

      "cells": [
        { "superRow": null, "row": { "code": 1, "label": "None" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 4.4965 }, "population": 806371 },
        { "superRow": null, "row": { "code": 1, "label": "None" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 5.1455 }, "population": 897726 },
        { "superRow": null, "row": { "code": 1, "label": "None" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 9.3964 }, "population": 1380661 },
        { "superRow": null, "row": { "code": 1, "label": "None" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 9.5799 }, "population": 1329288 },

        { "superRow": null, "row": { "code": 2, "label": "Primary" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 5.4371 }, "population": 2228477 },
        { "superRow": null, "row": { "code": 2, "label": "Primary" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 6.0411 }, "population": 2577526 },
        { "superRow": null, "row": { "code": 2, "label": "Primary" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 10.6746 }, "population": 4609251 },
        { "superRow": null, "row": { "code": 2, "label": "Primary" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 10.4928 }, "population": 4239466 },

        { "superRow": null, "row": { "code": 3, "label": "Secondary" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 6.7405 }, "population": 1282671 },
        { "superRow": null, "row": { "code": 3, "label": "Secondary" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 7.4746 }, "population": 1232360 },
        { "superRow": null, "row": { "code": 3, "label": "Secondary" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 13.3256 }, "population": 6527877 },
        { "superRow": null, "row": { "code": 3, "label": "Secondary" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 13.6483 }, "population": 6049374 },

        { "superRow": null, "row": { "code": 4, "label": "Tertiary" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 19.7614 }, "population": 111060 },
        { "superRow": null, "row": { "code": 4, "label": "Tertiary" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 27.2911 }, "population": 90850 },
        { "superRow": null, "row": { "code": 4, "label": "Tertiary" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 32.7174 }, "population": 2890294 },
        { "superRow": null, "row": { "code": 4, "label": "Tertiary" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 35.9631 }, "population": 2568378 },

        { "superRow": null, "row": { "code": null, "label": "Unknown" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 4.7541 }, "population": 291583 },
        { "superRow": null, "row": { "code": null, "label": "Unknown" },
          "superColumn": { "code": 0, "label": "Rural" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 4.7366 }, "population": 306678 },
        { "superRow": null, "row": { "code": null, "label": "Unknown" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 0, "label": "Female" },
          "measures": { "mean": 10.5641 }, "population": 765898 },
        { "superRow": null, "row": { "code": null, "label": "Unknown" },
          "superColumn": { "code": 1, "label": "Urban" },
          "column": { "code": 1, "label": "Male" },
          "measures": { "mean": 10.8691 }, "population": 796627 }
      ]
    }
  ],
  "warnings": [],
  "errors": [],
  "meta": {
    "release": ["20260401_TEST"],
    "n_surveys": [1]
  }
}
```

#### Format notes for the UI developers

- **Slot keys are fixed.** Every cell always has `superRow`, `row`,
  `superColumn`, and `column`. Unused slots are `null` in both `layout` and
  every cell. Rendering code never branches on how many covariates were
  requested.
- **`layout` is the header definition.** Its ordered `categories` arrays give
  the exact header sequence to draw and the exact cell ordering to expect.
  The UI should build headers from `layout` and place cells by index, not by
  discovering categories from the data.
- **No dynamic keys.** The original proposal used variable-named keys
  (`"area": 0`), which forces the consumer to read `varname` before it can
  read the code. Here `varname` lives once in `layout`; cells carry only
  `{code, label}`.
- **Multiple measures** nest inside `measures`, for example
  `"measures": { "mean": 10.47, "median": 8.91, "gini": 0.513 }`. The
  requested set and its order are echoed in the survey-level `measures`
  array, so the UI can render the in-cell stacked list in the user's chosen
  order (`docs/ui-description.md:80`).
- **`population` is deliberately outside `measures`** - it is cell metadata,
  not a user-selected statistic.
- **Multi-survey requests** append further objects to `data`, one per
  `pip_id`, each with its own `layout`. This maps directly onto the
  per-survey tabs in Step 3 (`docs/ui-description.md:135`).
- **Known deviation to confirm**: per-cell objects omit `varname` (it is in
  `layout`) to avoid repeating it on every cell. If the UI would rather have
  it inline, it is a cheap addition.

## Next Steps

Concrete actions for handoff to `/cg-plan`:

1. **Circulate the mock payloads to the UI developers for sign-off** before
   implementation starts. Shareable files live in
   `.cg-docs/brainstorms/2026-10-19-table-response-mocks/`:
   - `example-a-three-covariates.json` - `by=gender&by=educat4&by=area`;
     uses the exact values `/table` returns today, so only the structure
     differs from the current response.
   - `example-b-one-covariate.json` - `by=gender`; three slots `null`,
     demonstrating that the schema is invariant in the number of covariates.
     Values derived from example A by population-weighted aggregation
     (verified to reproduce exactly at 4 dp).
   - `README.md` - rendering guide, slot rule, provenance, and four specific
     questions for the developers.

   Confirm specifically: the fixed four-key cell schema, `layout`-driven
   ordering, the omitted per-cell `varname`, and the ordered-selection model
   for Decision 3.
2. **Replace the dead registry-driven `slot` lookup** at
   `R/table_maker.R:309-327` with positional derivation from `params$by`,
   as needed for the `/table` response.
3. **Define deterministic category ordering** sourced from the registry, to
   satisfy the charter's determinism constraint. This is a prerequisite for
   both `layout.*.categories` and `cells[]` ordering.
4. **Add the label join** so cells and layout categories carry `label`
   alongside `code`, including a canonical label for NA categories.
5. **Build the serializer** - rename and join on the existing flat
   `data.table`, then split per `pip_id`. Verify `jsonlite` auto-unboxing
   behaviour for singleton `categories` and `measures`. Implement against
   `include_metadata = false` only.
6. **Harden the request contract**: document `by` as order-significant in
   `plumber.R:131` roxygen and `docs/api-contract.md`, add a regression test
   asserting order preservation, and move the 4-dimension cap into
   `validate_table_input()` as a 400.
7. **Update tests**: `tests/testthat/test-api-endpoints.R` (envelope block at
   `:1135-1165` stays valid; `/table` round-trip at `:749-796` needs
   rewriting), plus new structural tests for `layout`, slot `null` handling,
   `by = NULL`, and ordering determinism.
8. **Confirm `/table/download` is untouched** -
   `tests/testthat/test-api-table-download.R` should remain green unmodified.
9. **Update `docs/api-contract.md:156-195`** to the new shape.

### Deferred - not part of this implementation

- **Suppression behaviour change.** `pop_share_threshold` will stop dropping
  cells from the output. Mechanism and the resulting cell value are defined
  later at the computation layer. The response format above is already
  compatible with whatever value is chosen.

### Follow-up items captured separately

- **UI consequence**: Decision 3 (`docs/ui-description.md:98-127`) must be
  redesigned from four independently fillable drop targets to an **ordered
  covariate selection**, so that gapped assignments are impossible by
  construction. This is a UI-side change implied by the positional rule and
  should be communicated alongside the mock.
