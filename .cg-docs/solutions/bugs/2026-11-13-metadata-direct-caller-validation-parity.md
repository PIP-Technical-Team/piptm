---
date: 2026-11-13
title: "Metadata direct-caller validation parity via shared table-params guardrails"
category: "bugs"
language: "R"
tags: [metadata, validation, build_description_model, table_params, slot-shape, filter_base, registry, api]
root-cause: "Validation rules for table-shaping lived partly at route-level and partly in compute-time code, so direct metadata builders could bypass slot/poverty/filter edge checks and drift from table_maker() behavior."
severity: "P2"
---

# Metadata direct-caller validation parity via shared table-params guardrails

## Problem

Metadata generation and compute generation accepted the same conceptual inputs
(`pip_id`, slots, measures, `filter_base`, `poverty_line`) but did not always
enforce the same rules on the same boundary.

Symptoms observed in review and triage:

- direct callers of `build_description_model()` could hit edge cases where
  slot-shape checks were weaker than route-level checks;
- `filter_base = list()` behaved as a malformed filter in one path instead of
  as an intentional "no filters" request;
- repeated registry scans inside metadata section builders added avoidable
  overhead and made behavior harder to reason about.

This created a drift risk: identical user intent could produce different
validation outcomes depending on whether the call entered via API routes,
metadata builders, or compute paths.

## Root Cause

Validation logic was split across layers:

1. route/endpoint validators for query/body shape;
2. late compute-time checks in `table_maker()`;
3. metadata-builder assumptions in `build_description_model()`.

Because direct metadata calls do not traverse all route and compute branches,
some guardrails (slot arity/order/duplication, poverty-line coherence for
`pov_status`, and empty filter semantics) were not uniformly enforced at the
shared parameter boundary.

## Solution

Centralize table-shaping guardrails in
`R/table_params.R:.validate_and_normalize_table_params()` and route both
compute and metadata paths through it.

### 1) Shared validation now covers direct-caller slot semantics

- Added optional slot arguments (`column`, `row`, `superColumn`, `superRow`) to
  shared validation.
- Enforced:
  - scalar non-empty character values per slot,
  - left-to-right contiguous fill order,
  - no duplicate variables across slots,
  - no duplicate variables in `by`.

```r
if (length(filled) > 0L) {
  expected_prefix <- slot_order[seq_along(filled)]
  if (!identical(filled, expected_prefix)) {
    piptm_abort(...)
  }
  if (anyDuplicated(slot_dims) > 0L) {
    piptm_abort(...)
  }
}
```

### 2) Poverty-line coherence is enforced once, upstream

- `by` containing `pov_status` now requires a single positive finite
  `poverty_line` at shared-validation time.
- Removed redundant late check from compute path and documented that
  `table_maker()` now depends on upstream guardrails for this invariant.

### 3) Empty filter list normalized as no-filter intent

- `filter_base = list()` now normalizes to `NULL` instead of aborting.
- This aligns direct calls with API semantics where empty/omitted filter input
  means "do not apply sample-base filtering".

### 4) Metadata builder avoids repeated full-registry scans

- `build_description_model()` now derives role-specific registry slices once
  (`filter`, `covariate`, `measure_spec`) and passes them into section builders
  instead of repeatedly resolving release-backed accessors.

### 5) Regression coverage extended at direct-function boundary

Added/expanded tests in:

- `tests/testthat/test-table-params.R`
- `tests/testthat/test-description-builder.R`

Coverage now includes slot-gap/duplicate rejection, multi-valued slot rejection,
`filter_base = list()` normalization, partial `pip_id` match tolerance, and
defensive behavior when category metadata is absent.

## Prevention

- Treat `table_params` shared validators as the **single source of truth** for
  table-shaping rules; route-level validators should reject transport errors,
  not duplicate domain logic.
- Keep compute and metadata paths on the same normalized contract object.
- For every new defensive guard, add tests that call the helper directly and
  tests that call at least one public consumer path.
- Normalize "empty means disabled" semantics explicitly (`NULL` vs empty list
  vs empty string) and pin behavior in tests.
- Prefer one-time registry slicing per request over repeated role scans in
  section builders.

## Related

- `.cg-docs/brainstorms/2026-11-12-metadata-rework-and-table-maker-decoupling.md`
  — metadata decoupling architecture and contract goals.
- `.cg-docs/solutions/bugs/2026-11-04-advisory-deferral-request-builder-architecture.md`
  — advisory recommendation to isolate slot-shape checks as pure helpers.
- `.cg-docs/solutions/bugs/2026-10-26-table-download-approach-c-hardening.md`
  — boundary-hardening pattern: validate shape at the boundary and keep
  transport-independent transforms isolated.
- `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md`
  — defensive schema validation and malformed-input test strategy.
