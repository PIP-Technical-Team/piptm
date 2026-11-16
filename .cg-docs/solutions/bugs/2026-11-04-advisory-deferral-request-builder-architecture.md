---
date: 2026-11-04
title: "Advisory-item deferral: delegating request-body builder architecture to platform layer"
category: "bugs"
language: "R"
tags: [api, architecture, platform, delegation, async-body-building, query-delegation, cell-definition, advisory]
root-cause: "Query-parameter request builders (filtering, composition) are better managed at platform/UI layer than within the validator; deferring simplifies the validator and decouples transport concerns"
severity: "P3"
---

# Advisory-Item Deferral: Delegating Request-Body Builder Architecture to Platform Layer

## Problem

During GET-only migration of `/cell-definition`, the standard review flagged [P3.7], an advisory finding:

> `validate_cell_definition_query()` delegates validation via an indirect `body_repr` list, rather than validating directly on the parsed query parameters. This design carries forward from the POST route's structure (where the `body` dict was parsed externally).

**The finding**: This is not incorrect, but it indicates an architectural opportunity — the validator could be simplified by removing the intermediate `body_repr` abstraction and validating raw query params directly, or by moving request-composition logic (building complex filter structures) to the platform layer.

## Context

### Current Design
The `/cell-definition` endpoint (both POST and now GET) uses this flow:

1. Plumber binds query parameters to function args: `analysis_var`, `measures`, `filter_base` (JSON string), etc.
2. `validate_cell_definition_query()` receives these scalar/vector args
3. Internally, it could validate each param directly, but instead constructs an intermediate `body_repr` list grouping related fields (e.g., `body_repr$analysis_var`, `body_repr$slots`, `body_repr$filter_base`)
4. Calls `.validate_table_shape(body_repr)` and applies post-shape checks on `body_repr` fields
5. Returns a named list with coerced values

### Why It Exists
The intermediate `body_repr` exists because:
- It mirrors the original POST route's JSON body structure
- It allows code reuse with `.validate_table_shape()`, which was designed for body-parsing validators
- It provides a mental model: "treat query params as if they were a JSON body"

### Why It's Architectural Concern
Over query parameters, the intermediate `body_repr` adds indirection:
- Readers must trace `query_param → body_repr field → shape validation → coerced value`
- The abstraction is not strictly necessary; validators can work directly on scalars/vectors
- Complex request composition (e.g., building a multi-dimensional filter object from repeated query params) belongs at the **platform/UI layer**, not inside the validator

## Root Cause

The validator was designed to accept a pre-built body structure (JSON parsed) and validate it. When migrating to query parameters, the natural fit was to construct a temporary "pretend body" representation and feed it to the same validator. This works, but it obscures the data flow and locks architectural choices about request composition into the validator itself.

Better separation of concerns: **validators should validate the final intended values**, not intermediate representations.

## Solution

### Current (Deferred) Approach
For the GET-only migration, **P3.7 was intentionally skipped** because:

1. The current design works and passes all tests
2. Refactoring to remove `body_repr` is a non-blocking architectural improvement
3. The validator correctly separates query-param binding (plumber) from validation (handler) from response building (builder)

**Decision**: Defer the architectural simplification to a future ticket. Document the deferral explicitly.

### Future Refactoring (When to Apply)

This pattern should be addressed if any of these occur:
- A new GET endpoint is added that needs similar query-composition logic
- The platform team signals that request-building needs to move to the client side (less plumber, more OpenAPI spec)
- Performance profiling shows indirection overhead (unlikely, but measurable in high-volume scenarios)

When refactoring:

**Step 1: Move request composition to the platform layer**

Instead of the API validator building a complex filter object, the **platform (UI/SDK)** should:
- Accept user input (e.g., "filter by age_group 1–5 and gender Male")
- Construct the final `filter_base` JSON at the client
- Send the fully-formed JSON string in the query param

This puts request composition where it belongs: in the consumer's domain model.

**Step 2: Simplify the validator to direct param validation**

```r
# ❌ Current: intermediate representation
validate_cell_definition_query <- function(...) {
  body_repr <- list(analysis_var = analysis_var, measures = measures, ...)
  check <- .validate_table_shape(body_repr)
  # post-shape checks on body_repr...
}

# ✅ Future: direct validation
validate_cell_definition_query <- function(...) {
  # Validate scalars directly against known rules
  if (!analysis_var %in% c("welfare", "pov_status")) {
    errors <- c(errors, "invalid analysis_var")
  }
  # No intermediate list construction
  list(valid = length(errors) == 0L, errors = errors, ...)
}
```

**Step 3: Reuse only the shape logic, not the representation**

Extract the pure shape-validation rules (e.g., "slot gap check", "duplicate slot variable check") into small standalone helpers:

```r
.check_slot_gap <- function(slots) {
  # Pure function: given a vector of slot names, return error or NULL
}

.check_duplicate_slot_var <- function(slots, var_name) {
  # Pure function: given a slot list and var, return error or NULL
}
```

Call these directly without constructing intermediate lists.

## Prevention

| Rule | Anti-pattern to avoid |
|------|----------------------|
| Keep validators focused on final values | Validators that construct intermediate representations carry forward legacy transport concerns |
| Move request composition to the consumer | Validators that build complex objects from raw params couple the validator to client design |
| Use pure shape-checking helpers | Validators that reuse body-parsing logic via intermediate lists add indirection and test complexity |
| Document architectural deferral decisions | Skip an advisory finding without explaining when/why to revisit |

## Related

- `.cg-docs/reviews/2026-11-04-cell-definition-get-only-review.md` — P3.7 advisory finding (deferred)
- `.cg-docs/plans/2026-11-04-cell-definition-get-only.md` — implementation plan; no refactoring requested by user
- `inst/plumber/helpers.R:validate_cell_definition_query()` — the deferred simplification target
- `.cg-docs/solutions/testing-patterns/2026-11-04-query-parameter-endpoint-migration-testing.md` — companion solution on testing and validator hardening
- `.cg-docs/solutions/bugs/2026-11-13-metadata-direct-caller-validation-parity.md` — follow-on implementation of shared slot-shape and parity guardrails in the metadata/table parameter path
