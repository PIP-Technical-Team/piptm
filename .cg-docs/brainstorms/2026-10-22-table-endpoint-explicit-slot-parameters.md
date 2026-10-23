---
date: 2026-10-22
title: "/table request contract: explicit slot parameters replacing ordered by"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Approach A - four scalar slot query parameters (column, row, superColumn, superRow)"
tags: [api, table-endpoint, request-contract, layout-slots, ui-contract, breaking-change, validation]
supersedes: ".cg-docs/brainstorms/2026-10-19-table-endpoint-layout-slot-response-format.md (request-side decisions only)"
---
<!-- Valid status values: decided, in-progress, abandoned -->

# `/table` Request Contract: Explicit Slot Parameters Replacing Ordered `by`

## Context

This brainstorm **revises the request-side decisions** recorded in
`.cg-docs/brainstorms/2026-10-19-table-endpoint-layout-slot-response-format.md`
(status: decided, 2026-10-19). That document settled the `/table` **response**
format (Approach C: flat `cells[]` with fixed slot keys plus a per-survey
`layout` block) and concluded that **the request signature does not change** -
slot assignment would be derived **positionally** from the order of a single
repeatable `by` parameter:

| `by` position | Slot |
|---|---|
| 1 | columns |
| 2 | rows |
| 3 | super columns |
| 4 | super rows |

That document also explicitly **considered and rejected** named slot
parameters, on the grounds that they were "unnecessary once gapped assignments
are impossible by design."

**That rejection is now reversed.** The `/table` endpoint will accept four
explicit, named slot parameters instead of a single ordered `by`. The UI
presents four labelled boxes (Columns, Rows, Super Columns, Super Rows); the
user places one covariate in each; the API receives them by name and passes
them to the relevant slots.

### What is being revised

| 2026-10-19 decision | Status after this brainstorm |
|---|---|
| "The request signature does not change" | **Reversed** - four named params replace `by` |
| "Named slot parameters... were considered and **rejected**" | **Reversed** - adopted as Approach A |
| UI follow-up: "Decision 3 must be redesigned from four independently fillable drop targets to an **ordered covariate selection**" | **Reversed** - four boxes return, but with enforced fill-order (see gap rule below) |
| Response format (Approach C: `cells[]` + `layout`) | **Unchanged - still stands** |
| `/table/download` flat long-format | **Unchanged by this revision** (see Known Open Issue) |
| Suppression semantics deferred | **Still deferred** |
| `include_metadata` treated as always `false` | **Still true** |

### Why reverse it

The motivation is **explicitness and self-documentation of the request**, not
new capability. There is **no functional difference**: identical computations,
identical response shape, identical UI capability. The return is legibility -
plus one durable structural gain discussed under Decision below.

### Current implementation state (audited)

- `inst/plumber/plumber.R:131` declares `@param by:[character] Disaggregation
  dimensions (optional, repeatable)` with no indication that order is
  semantically load-bearing.
- `inst/plumber/plumber.R:145-147` - handler signature takes `by = NULL`.
- `inst/plumber/helpers.R:365-382` - `validate_table_input()` performs
  set-membership checks on `by` only; never sorts, deduplicates, or normalizes.
- `inst/plumber/helpers.R:385-391` - cross-validation requiring `poverty_line`
  when `pov_status` appears in `by`.
- `R/measures.R:180` - `length(by) > 4L` cap, raised via `cli_abort()`, which
  the plumber layer converts to a **422**.
- `R/table_maker.R:293-295` - iterates `params$by` to build the covariates
  table; contains the dead registry-driven `slot` lookup documented in the
  2026-10-19 brainstorm.
- `inst/plumber/plumber.R:226-240` - `/table/download` is a **stateless POST**
  accepting `{"format": "csv"|"rds", "data": [...rows...]}`. It takes **no
  `by` parameter at all** and requires no request-side change.

## Requirements

### Purpose

Make the `/table` request contract explicit and self-documenting. Replace an
implicit positional convention (`by` order determines slot) with named
parameters whose slot assignment is visible in the URL, in the plumber
auto-generated OpenAPI spec, and in logs.

Confirmed during this session: the goal is **purely explicitness**. Supporting
gapped slot assignments is *not* a motivation, and gaps are explicitly
rejected (see Edge Cases).

### Users

The PIP platform UI is the **sole consumer** of `/table`. It is not yet in
production against this endpoint, so the request shape may be changed outright
- no versioning, no deprecation window, no compatibility shim.

No other automated system or internal script depends on the `by` parameter of
the HTTP endpoint.

### Inputs - the new request signature

Four scalar parameters replace `by`:

| Parameter | Slot | Cardinality |
|---|---|---|
| `column` | Columns | exactly one variable name, optional |
| `row` | Rows | exactly one variable name, optional |
| `superColumn` | Super Columns | exactly one variable name, optional |
| `superRow` | Super Rows | exactly one variable name, optional |

Example:

```
GET /table
  ?analysis_var=welfare
  &pip_id=COL_2008_GEIH_INC_ALL
  &measures=mean
  &column=gender
  &row=educat4
  &superColumn=area
```

**Naming decision**: parameters use the **singular** form (`column`, `row`,
`superColumn`, `superRow`), matching the per-cell keys in the response's
`cells[]` array, because each parameter carries exactly one variable. Note the
response's `layout` block uses the **plural** form (`columns`, `rows`,
`superColumns`, `superRows`) - this asymmetry is pre-existing and intentional:
`layout` describes a slot's full category list, cells describe one category.

**All four are optional.** Omitting all four yields the national-level
single-cell table per measure, exactly as `by = NULL` does today.

### Internal boundary - where slots become `by`

**Decision: thin API layer (Option (a)).** `table_maker()` keeps its existing
`by =` argument unchanged. The four slot parameters are assembled into an
ordered `by` vector at the API boundary.

Rationale:

1. **Separation of concerns.** `by` is the internal currency of the whole
   computation engine: it flows `table_maker(by=)` -> `compute_measures(by=)`
   -> `compute_shares()` / `compute_inequality()` / `compute_poverty()`, where
   it becomes a `collapse::GRP(c("pip_id", by))` grouping vector. A grouping
   engine has no concept of "super column" - it has a concept of "group by
   these N columns." Whether a dimension renders as a column or a super row is
   a **presentation** fact with zero bearing on the computation. Pushing slot
   identity down means the engine carries data it never reads.
2. **The reconstruction is trivial and lossless**, so the supposed advantage of
   carrying slots end-to-end evaporates. Both artifacts derive from one
   structure and therefore cannot drift:
   ```r
   slots <- list(column = column, row = row,
                 superColumn = superColumn, superRow = superRow)
   slots <- Filter(Negate(is.null), slots)
   by    <- unlist(slots, use.names = FALSE)   # -> computation engine
   # names(slots)                              # -> response `layout` block
   ```
3. **Blast radius.** Option (b) (slots first-class in the engine) would break
   `table_maker()`'s exported signature and touch ~100+ test call sites that
   pass `by =`, immediately before the Phase 3 "CRAN-ready,
   `devtools::check()` clean" hardening pass. Option (a) leaves all existing
   tests green and the public R API unbroken.

**Assembly location**: inside `validate_table_input()`
(`inst/plumber/helpers.R`). It already returns coerced values that the handler
destructures (`check$poverty_line`, `check$ppp` at `plumber.R:159-161`), so it
extends naturally to also return:

- `check$by` - the ordered character vector for `table_maker()`
- `check$slots` - the named list for deriving the response `layout` block

The existing dimension-membership check (`helpers.R:366-382`) moves to operate
on the assembled vector, and the `pov_status` / `poverty_line`
cross-validation (`helpers.R:385-391`) continues to work unchanged against
`check$by`.

**Important consequence**: because the slot ordering is now a *named* construct
(`slots`) rather than a bare positional vector, the ordering convention lives
in one short, fully-controlled hop inside the API layer - not across an HTTP
boundary where a client could reorder it.

### Outputs

**The response format is unchanged** from the 2026-10-19 decision (Approach C).
The `layout` block is now derived from `names(slots)` instead of from `by`
position, but its shape and the `cells[]` shape are identical:

```json
"layout": {
  "columns":      { "varname": "gender",  "label": "Gender", "categories": [...] },
  "rows":         { "varname": "educat4", "label": "Education level", "categories": [...] },
  "superColumns": { "varname": "area",    "label": "Urban / Rural", "categories": [...] },
  "superRows":    null
}
```

Every cell still carries all four fixed keys (`superRow`, `row`,
`superColumn`, `column`), `null` where unused. Rendering code never branches on
how many covariates were requested.

### Constraints

- Charter: *"Interoperability: outputs must conform to schemas expected by
  downstream consumers."* Satisfied - the response schema is unchanged and the
  request becomes a better-specified contract.
- Charter: *"Deterministic outputs: identical inputs must always produce
  identical results."* **Improved.** Under `by`, determinism rested on an
  untested ordering convention; the 2026-10-19 brainstorm itself noted that
  "nothing today would catch a future refactor that sorts or deduplicates
  `by`." Named slots make the input-to-slot mapping structurally deterministic.
- No new package dependencies.
- `table_maker()`'s exported signature must not change.
- Existing `table_maker()` / `compute_*()` tests must remain green unmodified.

### Edge cases - all four resolved

1. **Same variable in two slots** (`?column=gender&row=gender`) -> **reject,
   400.** Previously unexpressible with `by`; now expressible and invalid.

2. **Gaps - non-contiguous slot filling** (`?column=gender&superRow=area`,
   leaving `row` and `superColumn` empty) -> **reject, 400.** Slots must fill
   in order: `column`, then `row`, then `superColumn`, then `superRow`.

   Rationale: the response schema *would* tolerate gaps (fixed four keys with
   `null`), and the computation *would* work (internally just a 2-dimension
   grouping). The problem is the rendered result: a super-row band wrapping a
   single unlabelled row - a degenerate table whose rendering behaviour has
   never been specified. Since gap support is not a motivation for this change,
   rejecting gaps keeps the revision purely cosmetic at the contract level and
   avoids opening an unscoped UI design question.

   **UI consequence**: the four boxes must enforce fill-order (box *n+1*
   disabled until box *n* is filled). This preserves the no-gaps invariant by
   construction, exactly as the ordered-selection model did - the difference is
   that the slots are now *named* in the request rather than *inferred* from
   position.

3. **Repeated parameter for one slot** (`?column=gender&column=area`) ->
   **reject, 400.** Plumber delivers repeated query params as a length-2
   character vector, which makes the slot unassignable: `layout.columns` has
   room for exactly one `varname`, so accepting it would require silently
   dropping one value or silently promoting it into another slot. Both are
   silent, data-dependent behaviours - precisely what this change exists to
   eliminate. A length guard in `validate_table_input()` costs two lines:
   ```r
   if (length(column) > 1L) errors <- c(errors, "`column` accepts a single variable.")
   ```

4. **The 4-dimension cap** (`R/measures.R:180`) -> **leave in place.** It
   becomes structurally unreachable from the API (4 params x 1 value each), but
   remains a valid defensive guard for direct `table_maker()` callers. No
   change needed; the 422-vs-400 concern raised in the 2026-10-19 brainstorm
   dissolves, since the API can no longer produce more than four dimensions.

5. **`pov_status` in a slot** - unchanged behaviour. The existing
   cross-validation requiring `poverty_line` when `pov_status` is used as a
   disaggregation dimension operates on the assembled `check$by`.

6. **NA covariate categories** - unchanged from the 2026-10-19 decision; still
   require a canonical registry-sourced label.

### Scope - explicitly out of this iteration

- **`table_maker()`'s signature** - stays `by =`. Decision (a) above.
- **`/table/download`** - takes no `by` parameter (it is a stateless
  echo-back endpoint), so there is nothing to change request-side.
- **The response `cells[]` structure** - Approach C from 2026-10-19 stands
  unmodified.
- **Suppression semantics** - still deferred to a separate computation-layer
  change, as in the original brainstorm.
- **`include_metadata`** - still treated as always `false`; the metadata
  branch (`plumber.R:188-196`, `:215-217`) is not exercised or modified.
- **The dead registry-driven `slot` lookup** at `R/table_maker.R:293-327` -
  still needs replacing, but that is carried in the 2026-10-19 brainstorm's
  Next Steps, not re-decided here.

### Known open issue - carried forward, not resolved here

**`/table/download` has no flat rows to receive.** The endpoint is stateless:
the client POSTs back the `data` rows it received from `/table`
(`plumber.R:228-233`). Once `/table` returns Approach C's `cells[]` +
`layout` structure instead of flat long-format rows, the client no longer
holds flat rows to send. The 2026-10-19 brainstorm stated `/table/download`
"stays flat long-format," which implicitly assumed flat rows remained
available client-side.

This is **pre-existing** - introduced by the response-format decision, not by
this request-contract revision - and is **not resolved here**. Registered
separately on the roadmap.

## Approaches Considered

### Approach A - Four scalar query params - **CHOSEN**

`column`, `row`, `superColumn`, `superRow`; each accepts exactly one variable
name; all optional; gaps and repeats rejected with 400.

- **Pros**: Self-documenting in the URL and in plumber's auto-generated
  OpenAPI - each slot becomes a named, individually-documented parameter.
  Works identically for GET query strings and POST bodies. Request params
  mirror the response's `cells[]` keys 1:1, so the UI's four boxes map to four
  params with zero translation. Minimal diff to `plumber.R` and
  `validate_table_input()`.
- **Cons**: Four correlated params requiring joint validation (gaps,
  duplicates) rather than independent per-param checks. A fifth slot would
  require a fifth param - though the UI spec fixes the count at four.
- **Effort**: Small.

### Approach B - Single JSON `layout` object param

One param carrying `{"column":"gender","row":"educat4"}`, parsed like the
existing `filter_base` (`plumber.R:134`).

- **Pros**: Groups correlated params into one unit, making joint validation
  natural. Precedent exists in the codebase. Extensible without signature
  changes.
- **Cons**: **Actively works against the stated goal.** A URL-encoded JSON blob
  is *less* self-documenting than four named params - opaque in the OpenAPI
  spec (just "a string"), opaque in logs, awkward to construct by hand or test
  with curl. Trades positional opacity for encoding opacity. Adds a JSON parse
  failure mode per request.
- **Effort**: Small, but negative value against the goal.

### Approach C - Four params with `by` retained as a deprecated alias

Accept both; map `by` positionally as today with a deprecation warning.

- **Pros**: Non-breaking; existing callers keep working through a transition.
- **Cons**: **There is nothing to transition.** The UI team is the sole
  consumer and the endpoint is not in production against this contract.
  Dual-path input doubles the validation surface and the test matrix, and
  creates a conflict case (`by` *and* `column` both supplied) needing its own
  rule. A deprecation shim for zero users is pure cost.
- **Effort**: Medium.

## Decision

**Approach A.** It is the only option that advances the stated goal. B
optimizes for a validation convenience that is not needed - joint validation is
a dozen lines either way - at the direct expense of explicitness, the entire
reason for the change. C solves a compatibility problem that does not exist.

### The durable gain beyond legibility

The user-visible benefit of this change is **zero**: same computations, same
response, same UI capability. It is worth doing anyway because it **moves a
correctness constraint from convention into the type system**.

Under ordered `by`, "order is semantically load-bearing" lives in
documentation and a regression test. The 2026-10-19 brainstorm acknowledged the
fragility directly: *"Nothing today would catch a future refactor that sorts or
deduplicates `by`."* A test catches reordering only where the test exercises
it. Under named parameters, that entire class of bug becomes
**unrepresentable** - a refactor cannot accidentally swap `column` and
`superRow`.

### Acknowledged 80/20 fallback

If schedule pressure appears, a legitimate cheaper path exists and was
declined knowingly: add `@param by` roxygen documenting order-significance plus
the regression test the 2026-10-19 brainstorm already specified (its Next Step
6). Roughly an hour versus a day or two, addressing the fragility while leaving
the ordering convention implicit but tested. Approach A is preferred because
the type-level fix holds everywhere and permanently, but the fallback is
recorded as a conscious trade-off, not an oversight.

### Risk flagged during devil's advocate

**This is the second reversal of the same decision in three days.** The
2026-10-19 brainstorm explicitly considered and rejected named slot params;
nothing in the codebase changed between then and now - only the reasoning did.
That is legitimate, but **the UI team has not yet signed off on the response
mocks** (Next Step 1 of the 2026-10-19 brainstorm, still outstanding). There is
a real risk their feedback triggers a third revision.

**Mitigation: obtain UI sign-off on the response shape and this request shape
together, in a single round, before implementation begins.**

### Documentation risk to address

The gap-rejection rule means `?column=gender&superRow=area` returns 400 while
the **computationally identical** `?column=gender&row=area` succeeds. Two
requests producing the same grouping, one rejected. This is defensible as a
deliberate product constraint, but it must be documented in
`docs/api-contract.md` as an explicit layout rule - otherwise it reads as a
validation quirk or a bug to anyone reaching the API without UI context.

## Next Steps

Concrete actions for handoff to `/cg-plan`:

1. **Obtain UI team sign-off on the combined contract** - the four named
   request params *and* the 2026-10-19 response mocks
   (`.cg-docs/brainstorms/2026-10-19-table-response-mocks/`) in one round,
   before implementation starts. Confirm specifically: singular param naming,
   the enforced fill-order in the four UI boxes, and the fixed four-key cell
   schema.
2. **Replace the `by` parameter in `plumber.R`** - swap `by = NULL` for
   `column = NULL, row = NULL, superColumn = NULL, superRow = NULL` in the
   `/table` handler signature (`plumber.R:145-147`), and replace the
   `@param by` roxygen (`plumber.R:131`) with four documented slot params.
3. **Extend `validate_table_input()`** (`inst/plumber/helpers.R`) to:
   - accept the four slot params,
   - reject length > 1 per slot (400),
   - reject duplicate variables across slots (400),
   - reject gapped assignments (400),
   - validate dimension membership against `pip_valid_dimensions()` on the
     assembled vector (relocating the check at `helpers.R:366-382`),
   - return `check$by` (ordered vector) and `check$slots` (named list).
4. **Derive the response `layout` block from `check$slots`** rather than from
   `by` position.
5. **Update `docs/api-contract.md`** - document the four params, the
   fill-order rule, and explicitly explain why a gapped request is rejected
   despite being computationally valid.
6. **Add tests**: one per rejection rule (repeat, duplicate, gap), all-four-
   empty producing the national-level table, each slot in isolation, and a
   round-trip asserting that `check$slots` and `check$by` stay consistent.
7. **Confirm `table_maker()` and all `compute_*()` tests remain green
   unmodified** - this is the acceptance criterion for decision (a).
8. **Confirm `/table/download` tests remain green unmodified**
   (`tests/testthat/test-api-table-download.R`) - it takes no slot params.

### Deferred - not part of this implementation

- **`/table/download` flat-rows gap** - see Known Open Issue above. Registered
  on the roadmap separately.
- **Suppression behaviour change** - carried unchanged from the 2026-10-19
  brainstorm.
- **The dead registry `slot` lookup** at `R/table_maker.R:293-327` - carried in
  the 2026-10-19 brainstorm's Next Steps.
