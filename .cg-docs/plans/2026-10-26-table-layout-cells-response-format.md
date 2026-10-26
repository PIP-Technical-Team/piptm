---
date: 2026-10-26
title: "/table response format: layout + observed cells[] (Approach C)"
status: completed
completed-date: 2026-10-26
scope: "Deep"
brainstorm: ".cg-docs/brainstorms/2026-10-19-table-endpoint-layout-slot-response-format.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
execution-report: .cg-docs/work-reports/2026-10-26-table-layout-cells-response-format.md
artifact-schema-version: 1
completed-phases: [1, 2]
phases: 2
tags: [api, plumber, table-endpoint, response-format, layout-slots, cells, serializer, determinism, ui-contract, breaking-change]
---

# Plan: `/table` Response Format — `layout` + Observed `cells[]` (Approach C)

## Objective

Reshape the `/table` response (with `include_metadata` off) from flat
long-format rows into **Approach C**: `data[]` holds one object per survey,
each carrying header fields, a `layout` block (fixed keys `columns`, `rows`,
`superColumns`, `superRows`; `null` for unused slots) and a `cells[]` array of
the **observed** (computed/masked) cells. Every cell carries all four fixed
slot keys (`superRow`, `row`, `superColumn`, `column`, each `{code, label}` or
`null`) and a `measures` object keyed by measure name. Category order in
`layout` and cell order in `cells[]` are deterministic and registry-sourced.

`table_maker()` and every file under `R/` that computes or shapes the table
stay unchanged, **except one additive change**: canonical `pov_status`
categories are added to the registry (it currently has `n_categories = 2` but
no `{code,label}`). The reshape itself is a plumber-layer serialization step
consuming the existing flat `format_table_output()` table plus the `$slots`
list and normalized `analysis_var`/`poverty_line` from the validated request.

## Context

Approach C was decided in
`.cg-docs/brainstorms/2026-10-19-table-endpoint-layout-slot-response-format.md`
(status: decided). Agreed mock payloads and rendering guide live in
`.cg-docs/brainstorms/2026-10-19-table-response-mocks/`. **UI sign-off is
complete** (brainstorm Next Step 1 resolved).

This plan was revised after `/cg-plan-review` (8 findings: 5 P1, 3 P2). The
corrections are woven into the steps and noted inline below.

**Reconciliation with shipped work:**

- **Request contract already changed.**
  `.cg-docs/plans/2026-10-22-table-explicit-slot-parameters-revised.md`
  (completed 2026-10-23) replaced repeatable `by` with four explicit slot
  params. `validate_table_input()` (`inst/plumber/helpers.R:290-668`) already
  returns a fixed four-key `$slots` list (keys `column`, `row`, `superColumn`,
  `superRow`; `NULL` for unfilled) plus assembled `$by`. The handler
  (`plumber.R:150-234`) has `check$slots` available. The serializer consumes
  `$slots` directly — positional `by` re-derivation (brainstorm Next Step 2)
  and `by` order-hardening (Next Step 6) are obsolete.
- **The dead `slot` lookup at `R/table_maker.R:309-327` is out of scope** —
  it is in the `include_metadata` metadata-assembly path, not the data output.
- **Suppression already resolved.**
  `.cg-docs/plans/2026-10-20-pop-share-nr-masking.md` (completed 2026-10-22)
  replaced row-dropping with cell-level NR masking (`value = NA_real_`,
  `flag = "NR"`, no rows dropped). This plan does not touch suppression.
- **`population` is no longer produced** (`table_maker.R:898`
  `result[, population := NULL]`). Cells omit `population`.

**Finding-driven facts verified against the code:**

- **(P1.1) Header inputs are not in the flat table.** The flat output columns
  are `pip_id, country_code, surveyid_year, welfare_type` + `dim_cols` +
  `poverty_line, measure, value, flag` (`table_maker.R:899-901`). There is no
  `analysis_var` column, and `poverty_line` is row-specific: `NA_real_` for
  non-poverty rows, valued only for poverty rows (`compute_measures.R:149`).
  → The serializer must receive normalized `analysis_var` and the
  request-level `poverty_line` **explicitly** from the validated request
  (`check$poverty_line`, request `analysis_var`); it must not infer them from
  flat rows. Mixed poverty/non-poverty requests must be tested.
- **(P1.2) `pov_status` has no registry categories.**
  `.build_registry_entry()` sets `categories = NULL` for special-case
  varnames (`registry.R:95-113`); fixtures reproduce this
  (`tests/testthat/helper-fixtures.R:226-234`). → Add canonical `pov_status`
  `{code,label}` categories to the registry (codes `0`/`1` with approved
  labels, e.g. "Non-poor"/"Poor" — confirm wording against existing
  methodological usage before writing) so all consumers share one source.
  This is the single allowed additive `R/` change.
- **(P1.3) Output is observed combinations only, not Cartesian-dense.**
  `compute_measures()` groups via `collapse::GRP(dt, by = c("pip_id", by))`
  (`compute_measures.R:102-103`) — only combinations present in microdata.
  Retaining suppressed rows (NR masking) does not add absent combinations.
  → `cells[]` means **all observed/computed cells** (including NR-masked), not
  a synthesized Cartesian product. `layout.*.categories` still lists every
  registry category (so the UI can draw full headers), but a
  category-combination absent from the data has **no cell**. This is the
  UI-approved semantics; documented in the contract.
- **(P1.4) `meta.n_surveys` breaks after reshape.** The handler uses
  `data.table::uniqueN(out$result$data, by = "pip_id")`
  (`plumber.R:218-223`); once `data` is a list of survey objects this yields
  `NA_integer_`. → Compute the count before reshaping (on the flat table) or
  as `length()` of the per-survey list; test 1 and N surveys.
- **(P1.5) Flat-shape test assumptions exist beyond Block 6.** Confirmed at
  `test-api-endpoints.R:1180-1198, 1256-1297, 1468-1482` and the baseline
  comparison at `1484-1530` (reads `nrow(body$data)`, flat baseline fixtures
  in `tests/testthat/fixtures/table-slot-baseline/`), plus
  `test-phase4-table-and-endpoint.R:129,144,173,192` (`body$data$measure`,
  `out$measure`). → Inventory **every** `/table` response assertion across the
  suite; rewrite structural checks for the nested shape while preserving
  value-level regression coverage.
- **(P2.1) Category code typing + missing code.** Registry coerces every
  category code to character (`registry.R:71-72, 122-124`); flat `dim_cols`
  and the agreed mocks use numeric codes; the mock missing code is JSON
  `null`. → Normalize type for the join (compare as character), but emit codes
  with the intended semantic JSON type consistent with the mock, and emit the
  missing category as JSON `null` (not `"NA"`/`""`). Verify how the release
  registry represents the missing category before relying on a mapping name.
- **(P2.2) Mock values are raw; contract is formatted strings.** The mock has
  `"mean": 4.4965`; `format_table_output()` renders `"4.50"`
  (`format_table_output.R:101-114`). → Treat the existing mock as *structural*
  approval only; create a revised **post-format authoritative fixture**
  (string values, no `population`, explicit-slot request params) and test
  acceptance against that.
- **(P2.3) `/table/download` is knowingly incompatible.** After this plan,
  `/table` returns nested per-survey `{layout, cells[]}` objects;
  `/table/download` (`plumber.R:250-332`) `rbindlist()`s an array of flat row
  objects (`plumber.R:293`) and cannot consume the nested payload. → Do **not**
  claim alignment. Document the accepted temporary incompatibility and add a
  characterization test asserting the nested payload is not consumable by
  `/table/download` until the dependent feature
  `table-download-has-no-flat-rows-under-cells-response-format` lands.

**Serializer placement.** Implement as a new **exported pure function** in
`R/` (no Arrow/microdata access, no recomputation). It runs on the formatted
flat table + `check$slots` + normalized `analysis_var` + request-level
`poverty_line` + requested `measures` + `release`, producing the per-survey
`data[]`. Wire it in at `plumber.R:208-233`, replacing the flat
`result_data` passed to `api_response()` and fixing `meta.n_surveys`.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | `/table` (`include_metadata` off) returns `data[]` = one object per survey with `pip_id`, `country_code`, `surveyid_year`, `welfare_type`, `poverty_line`, `analysis_var`, `measures[]`, `layout`, `cells[]` | Brainstorm |
| R2 | `analysis_var` and survey-level `poverty_line` are passed explicitly from the validated request into the serializer, not inferred from flat rows | P1.1 |
| R3 | Mixed poverty/non-poverty measure requests produce correct, unambiguous header `poverty_line`/`analysis_var` | P1.1 |
| R4 | `layout` has fixed keys `columns`, `rows`, `superColumns`, `superRows`; unused slots `null`; each filled slot has `varname`, `label`, ordered `categories[{code,label}]` | Brainstorm |
| R5 | Slot→variable mapping taken from `$slots` (`column→columns`, `row→rows`, `superColumn→superColumns`, `superRow→superRows`) | Shipped slot contract |
| R6 | Canonical `pov_status` `{code,label}` categories added to the registry so `pov_status` layout/labels resolve | P1.2 |
| R7 | Every cell carries all four slot keys `superRow`, `row`, `superColumn`, `column`; each `{code,label}` or `null`; plus a `measures` object keyed by measure in requested order; cell value is the formatted display value (string/`"NR"`/`null`); no `population` | Brainstorm + P2.2 |
| R8 | `cells[]` = observed/computed cells only (incl. NR-masked); absent category combinations appear in `layout` but have no cell | P1.3 |
| R9 | `layout.*.categories` order is registry-sourced and deterministic | Charter determinism |
| R10 | `cells[]` order is deterministic: sorted by category index within `layout.superRows`, then `rows`, then `superColumns`, then `columns` (null slots skipped) | Brainstorm |
| R11 | Category codes normalized for the join (char-safe); emitted with mock-consistent JSON type; missing category emitted as JSON `null` | P2.1 |
| R12 | No-slots request (`by = NULL`): all four `layout` slots `null`; `cells[]` has exactly one cell with all four slot keys `null` | Brainstorm |
| R13 | Multi-survey requests append one `data[]` object per `pip_id`, each with its own `layout`; `meta.n_surveys` is correct for 1 and N surveys | Brainstorm + P1.4 |
| R14 | `jsonlite` serialization yields correct shapes (singleton `categories`/`measures`/`cells` are arrays; `{code,label}` objects auto-unbox scalars; `na = "null"` preserved) | Brainstorm Next Step 5 |
| R15 | `table_maker()` and all `R/` compute/output logic unchanged except the additive `pov_status` registry categories | Charter + user scope |
| R16 | All `/table` response assertions across the test suite inventoried and rewritten for the nested shape; value-level regression coverage preserved | P1.5 |
| R17 | A revised post-format authoritative fixture (string values, no `population`, explicit slots) is the acceptance artifact | P2.2 |
| R18 | `/table/download` unmodified; accepted temporary incompatibility documented; a characterization test asserts the nested payload is not consumable until the dependent feature lands | P2.3 |
| R19 | `docs/api-contract.md` `/table` section updated to Approach C + download incompatibility note | Brainstorm Next Step 9 |
| R20 | `include_metadata = true` branch not reshaped or exercised | Brainstorm out-of-scope |

## Implementation Steps

## Phase 1: Registry, serializer, and endpoint wire-in

### 1. Add canonical `pov_status` categories to the registry
- **Requirements**: R6, R15
- **Files**: `R/registry.R` (special-case branch at `:95-113`); corresponding
  test fixture `tests/testthat/helper-fixtures.R:226-234`
- **Details**: In `.build_registry_entry()`, give `pov_status` a canonical
  ordered `categories` list of `{code, label}` (codes `0`/`1`; confirm
  approved labels against existing methodological usage before writing).
  Keep `n_categories = 2` consistent. This is the only permitted additive
  compute-layer change. Update the fixture so the test registry carries the
  same categories.
- **Test Scenarios**: happy (`pov_status` entry now has two categories);
  edge (label casing harmonised like other categories); error (n_categories
  stays 2; no other special-case varname gains categories).
- **Tests**: `tests/testthat/test-registry*.R` (extend), `test-table-response.R`.
- **Acceptance criteria**: `piptm_variable_registry()$pov_status$categories`
  returns the two approved `{code,label}` entries.

### 2. Registry-driven layout + category resolution (incl. normalization)
- **Requirements**: R4, R5, R9, R11
- **Files**: `R/table_response.R` (new)
- **Details**: Internal helper that, given a slot varname + `release`, returns
  the ordered `categories` (`{code,label}`) from
  `piptm_variable_registry(release)`, preserving registry order and including
  the canonical missing-category entry (code → JSON `null`) when the registry
  defines one. Build `layout` by mapping `$slots` keys to fixed output keys,
  emitting `null` for unfilled slots. Define a **code normalization** rule:
  compare registry codes (character) and flat `dim_cols` codes (often numeric)
  as character for the join, but record the intended emit type so output codes
  match the mock's semantic type; missing category emits JSON `null`.
- **Test Scenarios**: happy (three filled slots → populated keys, one null;
  `pov_status` slot resolves via Step 1); edge (missing category → code null,
  labelled; numeric-vs-character code join matches); error (varname absent
  from registry → surfaced, not silent).
- **Tests**: `tests/testthat/test-table-response.R`.
- **Acceptance criteria**: `layout` matches R4/R9/R11 for the agreed
  three-covariate request; code join is char-safe.

### 3. Flat-to-cells serializer + per-survey assembly + endpoint wire-in
- **Requirements**: R1, R2, R3, R5, R7, R8, R10, R12, R13, R14
- **Files**: `R/table_response.R` (exported pure function); wire-in at
  `inst/plumber/plumber.R:208-233`
- **Details**: Exported serializer takes the formatted flat table, `$slots`,
  normalized `analysis_var`, request-level `poverty_line` (from
  `check$poverty_line`), requested `measures`, and `release`. For each
  `pip_id`: build header fields (R1; `analysis_var`/`poverty_line` from the
  request, R2/R3), `layout` (Step 2), and `cells[]` from the **observed** flat
  rows only (R8) — join `dim_cols` codes to registry `{code,label}`, set unused
  slot keys `null`, collapse per-cell measure rows into one `measures` object
  in requested order, carry formatted `value` (no `population`, R7). Sort cells
  per R10. Handle `by = NULL` (single all-null cell, R12) and multi-survey
  (R13). In the handler: compute `meta.n_surveys` before/independently of the
  reshape (flat `uniqueN` by `pip_id` or `length()` of the survey list, R13),
  then pass the assembled `data[]` to `api_response()`. Verify `jsonlite`
  shapes (R14); keep endpoint serializer `na = "null"`. Serializer failures
  route through the existing `capture_with_warnings()` → 422 envelope.
- **Test Scenarios**: happy (three-covariate → observed cells, fixed keys,
  measures object); edge (`by = NULL` single cell; single covariate three null
  slots; missing-category cell; mixed poverty/non-poverty measures; two
  surveys → two `data[]` + correct `n_surveys`); error (flat table missing an
  expected `dim_col` for a filled slot → 422).
- **Tests**: `tests/testthat/test-table-response.R`,
  `tests/testthat/test-api-endpoints.R` (round-trip via `.ep_router`).
- **Acceptance criteria**: endpoint response for the mock request is
  semantically equal to the revised post-format fixture (Step 5); fixed
  four-key cell schema holds at every depth; `meta.n_surveys` correct.

## Phase 2: Tests, determinism, fixtures, docs

### 4. Full `/table` test inventory + structural/determinism rewrite
- **Requirements**: R10, R13, R16, R20
- **Files**: `tests/testthat/test-api-endpoints.R` (Block 6 and the
  flat-shape assertions at `:1180-1198, 1256-1297, 1468-1482, 1484-1530`),
  `tests/testthat/test-phase4-table-and-endpoint.R` (`:129,144,173,192`),
  `tests/testthat/test-table-response.R`, baseline fixtures under
  `tests/testthat/fixtures/table-slot-baseline/`
- **Details**: Inventory every `/table` response assertion in the suite (list
  the affected files/lines/fixtures in the work report). Rewrite structural
  assertions (`nrow(body$data)`, `body$data$pip_id`, `body$data$measure`,
  flat baseline comparisons) for the nested `data[]`/`layout`/`cells[]` shape,
  **preserving value-level regression** (same computed numbers as before). Add:
  fixed-key presence on every cell; `layout` key invariance across covariate
  counts; `by = NULL` single-null-cell; **determinism** (two identical
  requests → byte-identical `layout` order and `cells[]` order); multi-survey
  per-survey layout + `n_surveys`; missing-category labelling. Confirm
  `include_metadata = true` tests are untouched and green (R20).
- **Test Scenarios**: happy, edge (null slots, missing category, single cell),
  determinism (repeat-call equality).
- **Tests**: `devtools::test(filter = "api-endpoints")`,
  `devtools::test(filter = "phase4-table")`,
  `devtools::test(filter = "table-response")`.
- **Acceptance criteria**: inventory documented; rewritten tests pass;
  determinism green; no stray flat-shape assertion remains.

### 5. Revised authoritative fixture + `/table/download` characterization + docs
- **Requirements**: R11, R14, R17, R18, R19
- **Files**: new post-format fixture under
  `tests/testthat/fixtures/` (e.g. `table-response-approach-c/`);
  `tests/testthat/` download characterization test;
  `docs/api-contract.md`; `NEWS.md`
- **Details**: Build the revised authoritative fixture from the agreed mock:
  string-formatted values (e.g. `"4.50"`), no `population`, explicit-slot
  request params, Approach C structure, correct code JSON types + `null`
  missing category. Point the acceptance test (Step 3) at it. Add a
  characterization test documenting that the nested `/table` payload is **not**
  consumable by `/table/download` (expected failure/rejection) until the
  dependent feature lands — `/table/download` itself unmodified. Update
  `docs/api-contract.md` `/table` to Approach C (layout keys, observed cells,
  fixed slot keys, no `population`, determinism, code types) and add the
  `/table/download` incompatibility note. Add a breaking-change `NEWS.md`
  entry.
- **Test Scenarios**: happy (fixture acceptance); edge (download
  characterization rejection); n/a (docs).
- **Tests**: full suite `devtools::test()`.
- **Acceptance criteria**: full suite green; fixture is the acceptance
  artifact; download incompatibility characterized and documented; contract
  doc reflects Approach C.

### 6. Final verification: compute-layer diff + docs regen
- **Requirements**: R15, R19, R20
- **Files**: (verify) `R/table_maker.R`, `R/compute_measures.R`,
  `tests/testthat/test-api-table-download.R`; `man/`
- **Details**: `git diff` review confirming no compute/output change except
  the additive `pov_status` registry categories and the new serializer;
  `/table/download` endpoint and its unit tests unmodified and green;
  `include_metadata = true` path untouched. Run `devtools::document()`;
  verify no stale `man/` Rd and the new serializer has roxygen.
- **Test Scenarios**: n/a (verification).
- **Tests**: full suite `devtools::test()`; `devtools::document()`.
- **Acceptance criteria**: diff scope matches C1; suite green; docs current.

## Testing Strategy

- Pure serializer unit tests (`test-table-response.R`) on hand-built flat
  tables + slot lists + explicit header values — no Arrow.
- Endpoint round-trip tests via `.ep_router` for real jsonlite shape (R14).
- Mixed-measure header test (R3); missing-category + code-type test (R11);
  observed-only density test (R8); `by = NULL` + `n_surveys` tests (R12/R13).
- Determinism: repeat identical request, assert identical serialized order.
- Acceptance against the revised post-format fixture (R17).
- `/table/download` characterization (R18) + regression (unchanged tests green).
- Follow `cg-skill-r-testing` (testthat 3+) and `data.table-collapse` conventions.

## Documentation Checklist

- [ ] roxygen for the new exported serializer (params incl. `analysis_var`/`poverty_line`, return shape, `@family api`, example).
- [ ] `docs/api-contract.md` `/table` rewritten to Approach C (observed cells, code types, no population).
- [ ] `/table/download` incompatibility note added to the contract.
- [ ] `NEWS.md` breaking-change entry for the `/table` response shape.
- [ ] `devtools::document()`; verify no stale Rd.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| `jsonlite` collapses singleton `categories`/`cells`/`measures` or coerces code types | Explicit list wrapping + `auto_unbox` discipline; R11/R14 shape+type tests at the endpoint layer |
| `pov_status` label wording not methodologically approved | Confirm labels against existing usage before writing; additive-only registry change (R6) |
| Treating "observed" as Cartesian-dense reintroduces absent cells | R8 density-semantics test; serializer never synthesizes combinations |
| Header `poverty_line`/`analysis_var` wrong for mixed-measure requests | Pass from validated request, not flat rows (R2/R3); mixed-measure test |
| `meta.n_surveys` returns NA on nested data | Compute before/independently of reshape (R13); 1- and N-survey tests |
| Incomplete test inventory lets a flat-shape assertion fail late | Full-suite inventory documented in the work report (R16) |
| `/table/download` silently breaks for real clients | Documented accepted incompatibility + characterization test (R18); C4 states it honestly |
| Hidden coupling forces a broader `table_maker()` change | Serializer is pure/additive; C1 enforced by `git diff`; blocked-stop if violated |

## Out of Scope

- `/table/download` redesign (dependent feature
  `table-download-has-no-flat-rows-under-cells-response-format`).
- `include_metadata = true` branch and `description_metadata` assembly.
- Suppression / `pop_share_threshold` semantics (already implemented).
- Request-contract changes (explicit slot params already shipped).
- Per-cell `population` restoration.
- Full Cartesian cell materialization for absent combinations.
- The dead registry `slot` lookup at `R/table_maker.R:309-327` (metadata path).
- Any OpenAPI schema file.

## Completion Contract

### Outcome
`GET`/`POST /table` (with `include_metadata` off) returns, per survey in
`data[]`, header fields (`pip_id`, `country_code`, `surveyid_year`,
`welfare_type`, `analysis_var`, `poverty_line`, `measures[]`) plus a `layout`
block (fixed keys `columns/rows/superColumns/superRows`, `null` for unused) and
a `cells[]` array of the observed (computed/masked) cells only. Every cell
carries all four fixed slot keys (`{code,label}` or `null`) and a `measures`
object (no `population`). Category order in `layout` and cell order in `cells[]`
are deterministic and registry-sourced, with codes compared via normalized type
and emitted with mock-consistent JSON types (`null` for the missing category).
`table_maker()` compute logic is unchanged except an additive registry
definition of `pov_status` categories.

### Verification Surface

| ID | Evidence Required | Command/Artifact | Phase | Required |
|----|-------------------|------------------|-------|----------|
| V1 | Serializer builds Approach C from formatted flat table + `$slots` + explicit `analysis_var`/`poverty_line`; three-covariate case semantically matches the revised post-format fixture | `tests/testthat/test-table-response.R` | 1 | yes |
| V2 | `layout` fixed keys; unused `null`; filled slot has `varname`, `label`, ordered `categories[{code,label}]` incl. `pov_status` from registry | `test-table-response.R` | 1 | yes |
| V3 | Every cell carries all four slot keys (`null` when unused) + `measures` object; no `population` | `test-table-response.R` / `test-api-endpoints.R` | 1 | yes |
| V4 | `analysis_var` + survey-level `poverty_line` correct for mixed poverty/non-poverty measure requests | mixed-measure test | 1 | yes |
| V5 | `cells[]` = observed cells only; a category combination absent from microdata appears in `layout` but not `cells[]` | density-semantics test | 1 | yes |
| V6 | Category code join normalized (char-vs-numeric safe); JSON emits mock-consistent code types and `null` for missing category | code-type test | 2 | yes |
| V7 | Category + cell ordering deterministic/registry-sourced (identical request → identical serialized order) | determinism repeat-call test | 2 | yes |
| V8 | No-slots request → all `layout` slots `null`, single all-null cell; `meta.n_surveys` correct for 1 and N surveys | edge + n_surveys tests | 2 | yes |
| V9 | Full inventory of flat-response assertions rewritten; value-level regression preserved; full suite green | `devtools::test()` summary | final | yes |
| V10 | `docs/api-contract.md` `/table` updated to Approach C; `/table/download` incompatibility documented | `docs/api-contract.md` diff | 2 | yes |
| V11 | Characterization test: current nested `/table` payload not consumable by `/table/download` until the follow-up feature | download characterization test | 2 | yes |
| V12 | Compute-layer diff limited to additive `pov_status` registry categories + new serializer; `man/` current | `git diff` + `devtools::document()` | final | yes |

### Constraints

| ID | Constraint | Check | Phase |
|----|------------|-------|-------|
| C1 | `table_maker()` and `R/*.R` compute/output logic unchanged except the additive `pov_status` registry categories and the new serializer file | `git diff` review | final |
| C2 | Deterministic outputs: identical inputs → identical category/cell order and code types | V7 | 2 |
| C3 | No new package dependencies | `DESCRIPTION` Imports unchanged | final |
| C4 | `/table/download` integration with the new payload is a known, accepted, temporary incompatibility pending the dependent feature — not claimed as aligned | V11 + documented note | 2 |
| C5 | Only `include_metadata = false` path reshaped; metadata branch untouched | existing metadata tests green | final |

### Boundaries
- Allowed: new exported pure serializer in `R/`; additive `pov_status`
  registry categories; plumber wire-in at `plumber.R:208-233` incl. passing
  `analysis_var`/`poverty_line` and fixing `meta.n_surveys`; full `/table`
  test/fixture inventory + rewrite; revised post-format authoritative fixture;
  `docs/api-contract.md` update; `NEWS.md` entry; a `/table/download`
  characterization test.
- Out of scope: `/table/download` redesign; `include_metadata = true`;
  suppression semantics; request-contract changes; per-cell `population`; full
  Cartesian cell materialization; the dead metadata-path `slot` lookup;
  OpenAPI schema file.

### Iteration Policy
1. On a failing structural/determinism test, fix the serializer/ordering/
   type-normalization, not the test's expected contract.
2. If registry-sourced order/labels and the agreed mock conflict, stop and
   surface it.
3. Registry category order + normalized code type are the single source of
   truth for `layout` and `cells[]`.
4. `cells[]` reflects only computed/masked cells; never synthesize absent
   combinations.

### Blocked-Stop Conditions
- A requested covariate (incl. `pov_status` after the additive registry change,
  and the missing category) still lacks resolvable `{code,label}` from the
  registry.
- Approach C would require changing `table_maker()`'s computed values or
  modifying `/table/download`, or a new dependency.
- `jsonlite` cannot emit the agreed code/measures/cells shapes and types
  without a `table_maker()` change or new dependency.

### Deviation policy
`ask` (default).
