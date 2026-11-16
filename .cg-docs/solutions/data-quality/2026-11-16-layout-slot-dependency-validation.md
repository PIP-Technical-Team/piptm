---
date: 2026-11-16
title: "Replace strict ordered-slot validation with parent-child dependency checks"
category: "data-quality"
language: "R"
tags: [api-validation, plumber, layout-slots, error-messages, ux, table-endpoint]
root-cause: "Validation enforced a contiguous fill-order (column -> row -> superColumn -> superRow) when the actual structural requirement was only two independent parent-child dependencies"
severity: "P2"
---

# Replace strict ordered-slot validation with parent-child dependency checks

## Problem

The `/table`, `/table/download`, and `/metadata` endpoints in {piptm} exposed
four layout slots (`column`, `row`, `superColumn`, `superRow`) used to
disaggregate PIP survey tables. Validation required the slots to be filled in
a strict contiguous prefix with no gaps: `column` -> `row` -> `superColumn` ->
`superRow`. A request with `row` alone, or `column` + `superColumn` without
`row`, was rejected with HTTP 400 even though the request was semantically
well-formed and computable. This was stricter than the data model required
and blocked legitimate user requests such as a row-only breakdown.

## Root Cause

The real structural constraint is not a total order across all four slots —
it is two independent parent-child relationships:
- `superColumn` requires `column` (a "super" grouping needs a base axis to
  group).
- `superRow` requires `row`.
- `column` and `row` are otherwise independent of each other and of request
  argument order.

The original validator (`.validate_table_shape()` in
`inst/plumber/helpers.R`, and its mirror `.validate_and_normalize_table_params()`
in `R/table_params.R` for direct R callers) checked
`identical(filled_names, expected_prefix)` against the full 4-slot ordering,
which conflated "no gaps in the full sequence" with the actual dependency
graph. This caused `row` alone, or `column + superColumn` without `row`, to
be misclassified as invalid.

## Solution

1. **Verify downstream code has no hidden order assumption before touching
   validation.** Traced `build_table_layout()`, `serialize_table_response()`,
   `flatten_table_response()`, `.build_layout_content()`, and
   `.build_what_a_cell_is()` — all treat the four slots as independent named
   keys (`column`, `row`, `superColumn`, `superRow`), not a contiguous
   prefix. This confirmed the fix was validation-only.

2. **Replace the prefix/gap check with two independent dependency checks**,
   in both `inst/plumber/helpers.R` and `R/table_params.R`:

   ```r
   if ("superColumn" %in% filled_names &&
       !("column" %in% filled_names) &&
       !("column" %in% invalid_names)) {
     errors <- c(errors, .PIPTM_SUPERCOLUMN_DEPENDENCY_USER_ERROR)
   }
   if ("superRow" %in% filled_names &&
       !("row" %in% filled_names) &&
       !("row" %in% invalid_names)) {
     errors <- c(errors, .PIPTM_SUPERROW_DEPENDENCY_USER_ERROR)
   }
   ```

   The `!("column" %in% invalid_names)` guard is important: it prevents a
   slot that already failed its own type/arity check from *also* triggering
   a misleading cascading dependency error on its child slot.

3. **Centralize user-facing error strings** as named constants in
   `R/errors.R` (`.PIPTM_SUPERCOLUMN_DEPENDENCY_USER_ERROR`,
   `.PIPTM_SUPERROW_DEPENDENCY_USER_ERROR`) rather than duplicating the
   literal string in both the Plumber layer and the R package layer. The PIP
   UI displays these strings verbatim, so wording must be a single source of
   truth and must read as a complete, non-technical sentence (e.g.
   *"`superColumn` requires `column` to be filled. Please assign a variable
   to Columns before using Super Columns."*) rather than a terse
   parameter-only fragment.

4. **For direct R callers that pass slots without a `by` vector** (e.g.
   `build_description_model(column = "gender")`), derive `by` from the
   filled slot values when `by` is `NULL`, so dimension validation and the
   `pov_status` + `poverty_line` requirement are not silently bypassed:

   ```r
   if (is.null(by) && length(slot_dims) > 0L) {
     by <- slot_dims
   }
   ```

   This was a real gap found during review (`P1.1`): without it, direct
   callers could skip "unknown dimension" and poverty-line validation
   entirely.

5. Updated all Plumber/OpenAPI doc strings, `SWAGGER_GUIDE.md`, and
   `docs/api-contract.md` to describe the dependency rule instead of the old
   ordering rule, and updated tests across 7 files to cover the full set of
   valid/invalid slot combinations (including argument-order invariance and
   cascading-error-suppression cases).

## Prevention

- When an API exposes several optional, loosely related parameters, check
  whether "must be filled in strict order" is really the constraint, or
  whether it is a smaller set of pairwise/parent-child dependencies. A
  contiguous-prefix check is a much stronger (and often wrong) constraint
  than the data actually requires.
- Before changing a validation rule, trace every downstream consumer of the
  validated shape (serializer, flattener, description/metadata builder) to
  confirm none of them silently assume the old ordering. This turns a
  "might be a big refactor" unknown into a scoped, validation-only change.
- When a validation failure is isolated to one slot (wrong type, wrong
  arity, duplicate value), exclude that slot from triggering a *second*,
  misleading error on any slot that depends on it. Report one clear root
  cause per request, not a cascade.
- Centralize any error string that is displayed verbatim in a UI as a named
  constant. Duplicating the literal string across the Plumber layer and the
  R package layer guarantees drift the next time the wording changes.
- For direct (non-HTTP) callers of a shared validation helper, never assume
  they always pass a pre-built `by`/aggregate vector — derive it from
  whatever slot-level arguments they did pass, or validation meant to apply
  to the HTTP boundary (unknown dimensions, required poverty line, etc.)
  silently does not apply to R-level callers.

## Related

- Plan: `.cg-docs/plans/2026-11-13-layout-slot-dependency-rule.md`
- Review: `.cg-docs/reviews/2026-11-13-layout-slot-dependency-rule-review.md`
- Verify review: `.cg-docs/reviews/2026-11-13-layout-slot-dependency-rule-verify-review.md`
- Supersedes the "fill order is column, row, superColumn, superRow; gaps are
  HTTP 400" decision recorded in
  `.cg-docs/plans/2026-10-22-table-explicit-slot-parameters-revised.md`.
- `.cg-docs/solutions/bugs/2026-11-13-metadata-direct-caller-validation-parity.md`
  — the prior solution that first centralized slot validation into
  `.validate_and_normalize_table_params()` with the now-superseded contiguous
  fill-order check; this solution replaces that check with the parent-child
  rule and closes the `by`-derivation gap it left for direct callers.
- `.cg-docs/solutions/bugs/2026-10-26-data-table-by-named-vector-silent-bug.md`
  — related slot-handling pitfall: filtering `c("column","row","superColumn",
  "superRow")` as a named vector before use can silently keep stale names;
  relevant when touching this same slot-name set.
