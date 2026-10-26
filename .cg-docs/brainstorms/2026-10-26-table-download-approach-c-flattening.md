---
date: 2026-10-26
title: "/table/download input & output shape under Approach C"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Approach 1 - Stateless server-side flattening of Approach C into one-row-per-cell (Shape A)"
tags: [api, plumber, table-download, approach-c, csv, rds, serialization, breaking-change]
---

# `/table/download` Input & Output Shape Under Approach C

## Context

`/table/download` (`inst/plumber/plumber.R:247-343`) is a stateless `POST`
endpoint that converts an already-computed `/table` result into a downloadable
CSV or RDS file without touching Arrow or recomputing via `table_maker()`.
Previously, `/table` returned a flat long-format array of row objects, and
`/table/download` simply ran `data.table::rbindlist(body$data, fill = TRUE)`
and wrote the result out.

With `.cg-docs/plans/2026-10-26-table-layout-cells-response-format.md`
completed, `/table` (with `include_metadata = false`) now returns **Approach
C**: `data[]` is an array of per-survey objects, each containing survey header
fields, a `layout` block (`columns`, `rows`, `superColumns`, `superRows`), and
an observed `cells[]` array where each cell has fixed slot keys (`superRow`,
`row`, `superColumn`, `column`, each `{code, label}` or `null`) and a
`measures` object keyed by measure name.

The client no longer holds flat rows to echo back. Forwarding the new `/table`
`data` array directly into `/table/download` currently fails with a pinned
HTTP 500 characterization test (`tests/testthat/test-api-table-download.R`).
This brainstorm resolves roadmap feature
`table-download-has-no-flat-rows-under-cells-response-format` (milestone
`phase-4-api-service`).

## Requirements

From the Q&A with the user:

1. **Stateless pass-through of `/table`'s `data`**: The client sends
   `{"format": "csv" | "rds", "data": <the data[] array from /table>}` with no
   client-side reshaping and no server-side recomputation.
2. **Row granularity — Shape A (one row per cell, measures as columns)**:
   Each element of `cells[]` becomes one row in the exported table. Each
   requested measure becomes its own column (in the survey's `measures[]`
   order). Values are preserved verbatim as formatted display strings (e.g.
   `"4.50"`, `"NR"`, or `NA`/`""` when JSON `null`) — never coerced via
   `as.numeric()`, preserving the disclosure-control `"NR"` sentinel and
   already-scaled percentage strings.
3. **Survey header columns (all 6, always present, leading)**:
   `pip_id`, `country_code`, `surveyid_year`, `welfare_type`, `analysis_var`,
   `poverty_line` (numeric; `NA_real_` when `null`).
4. **Dimension columns — variable names + category labels only**:
   For each active (non-`null`) slot in `layout`, in visual-hierarchy order
   (`superRows` → `rows` → `superColumns` → `columns`, matching the cell sort
   order), emit one column named after `layout[[slot]]$varname` (e.g.
   `educat4`, `area`, `gender`) populated with the cell's category `label`
   (e.g. `"None"`, `"Rural"`, `"Female"`, `"Unknown"`). Unused slots (`null` in
   `layout`) are omitted. When no slots are active (national table), no
   dimension columns are emitted.
5. **Multi-survey stacking**: Multiple survey objects in `data[]` stack into a
   single rectangular table in `data[]` order, preserving each survey's own
   `cells[]` order.
6. **Contract — Approach C only (clean break)**: `/table/download` accepts
   only the Approach C `data[]` structure. Legacy flat-row payloads and
   malformed payloads return HTTP `400` with a clear validation message in the
   standard JSON error envelope (replacing the current unhandled 500).
7. **Same rectangular table for both formats**: `format = "csv"` writes the
   flattened `data.table` via `data.table::fwrite()`; `format = "rds"` writes
   `as.data.frame(tbl)` via `saveRDS()`.

### Concrete example of the target table (Shape A)

For `column=gender`, `row=educat4`, `superColumn=area`,
`measures=["mean", "headcount"]`:

| pip_id | country_code | surveyid_year | welfare_type | analysis_var | poverty_line | educat4 | area | gender | mean | headcount |
|---|---|---|---|---|---|---|---|---|---|---|
| COL_2008_GEIH_INC_ALL | COL | 2008 | INC | welfare | 2.15 | None | Rural | Female | 4.50 | 32.10 |
| COL_2008_GEIH_INC_ALL | COL | 2008 | INC | welfare | 2.15 | None | Rural | Male | 5.15 | 28.40 |
| COL_2008_GEIH_INC_ALL | COL | 2008 | INC | welfare | 2.15 | None | Urban | Female | 9.40 | 14.05 |
| COL_2008_GEIH_INC_ALL | COL | 2008 | INC | welfare | 2.15 | Unknown | Urban | Male | 10.87 | NR |

## Approaches Considered

### Approach 1: Stateless server-side flattening into Shape A — **CHOSEN**

`/table/download` accepts `{format, data}` where `data` is the Approach C
per-survey array from `/table`, validates its structure, and flattens it into
the Shape A `data.table` via a pure helper (`flatten_table_response()`) before
writing CSV or RDS.

- **Pros**: Zero recomputation or Arrow access; downloaded values are
  guaranteed identical to on-screen values because they are literally the same
  strings; no bloat added to `/table`; both CSV and RDS return a clean,
  immediately usable tabular dataset; `table_maker()` and the compute layer
  remain untouched.
- **Cons**: Requires rewriting existing `/table/download` unit tests from
  legacy flat-row inputs to Approach C inputs.
- **Effort**: Small (~1 day).

### Approach 2: Dual response on `/table` (return both `cells[]` and flat rows)

`/table` returns both the Approach C structure and a pre-flattened row array so
`/table/download` can stay a dumb `rbindlist()` pass-through.

- **Pros**: Minimal change to `/table/download`.
- **Cons**: Doubles every `/table` response payload even when the user never
  clicks Download, undoing the payload cleanliness of Approach C.
- **Effort**: Small–Medium.

### Approach 3: Stateful / recomputing `/table/download`

`/table/download` takes the original `/table` query parameters and calls
`table_maker()` + `format_table_output()` again.

- **Pros**: Can reuse the flat table before `serialize_table_response()` runs.
- **Cons**: Breaks the stateless-download architecture; doubles Arrow/compute
  load; introduces drift risk between on-screen and downloaded numbers.
- **Effort**: Medium.

### Approach 4: Dump the raw nested Approach C list as-is (no flattening)

Write `body$data` directly without reshaping.

- **Pros**: Almost no code.
- **Cons**: Not viable for CSV (`data.table::fwrite()` cannot write nested
  lists/objects — this is the exact 500 error currently pinned), and produces a
  deeply nested list in RDS when R analysts expect a `data.frame`/`data.table`.
- **Effort**: Trivial, but broken for CSV.

## Decision

**Approach 1 — Stateless server-side flattening of Approach C into Shape A.**

It is the only option that simultaneously preserves the stateless pass-through
design of `/table/download`, keeps `/table` lean, leaves `table_maker()` and
the compute layer untouched, guarantees 1:1 value fidelity with what the user
sees on screen (including `"NR"` and scaled percentages), and produces a clean
rectangular table for both CSV and RDS.

## Next Steps

Hand off to `/cg-plan` to plan the implementation:

1. Implement a pure helper (e.g. `flatten_table_response(data)` in
   `R/table_response.R`) that validates an Approach C `data[]` payload and
   returns the Shape A `data.table` (`pip_id`, `country_code`, `surveyid_year`,
   `welfare_type`, `analysis_var`, `poverty_line`, active slot varname columns
   in `superRows` → `rows` → `superColumns` → `columns` order with category
   labels, then measure columns in `measures[]` order).
2. Wire `flatten_table_response()` into `POST /table/download` in
   `inst/plumber/plumber.R`, returning HTTP `400` with a clear error message
   when `data` is missing, empty, in the legacy flat-row shape, or structurally
   invalid. Remove the dead `population` numeric-coercion loop.
3. Update `tests/testthat/test-api-table-download.R`: replace the temporary
   500 characterization test with a 200 round-trip test (`/table` →
   `/table/download` CSV & RDS), update existing download tests to pass
   Approach C payloads, and add a 400 test for legacy flat-row rejection.
4. Update `docs/api-contract.md` (`/table/download` request/response schema,
   remove the temporary incompatibility note) and `NEWS.md`.
