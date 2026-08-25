---
date: 2026-08-24
title: "Step 3 Description Metadata Contract Redesign"
status: decided
scope: "Deep"
artifact-schema-version: 1
chosen-approach: "Semantic Redesign (Balanced)"
tags: [architecture, metadata, api, step-3, execution-truth, description]
---

# Step 3 Description Metadata Contract Redesign

## Context

This brainstorm audits the current Step 3 description implementation (completed Aug 21, 2026) and redesigns the metadata contract to achieve execution truth. The original implementation (from brainstorms `2026-08-13-step3-description-file.md` and `2026-08-20-step3-description-architecture.md`) introduced a `with_meta = TRUE` sidecar for `table_maker()` that captures specification, execution, provenance, and warnings alongside the computed data.

**Problem:** After implementing and reviewing the feature, 10 major semantic defects were identified in the metadata contract:

1. **Survey status ambiguity**: `included_surveys` is populated before dimension/filter exclusions (line 424), so it reports surveys that were later excluded
2. **Exclusion tracking incomplete**: Filter-base exclusions not harvested into `excluded_surveys`
3. **Release resolution unclear**: Multiple release values (requested, current, resolved) with no single authority
4. **PPP metadata missing**: Requested PPP year captured, but not the resolved PPP or physical column actually used
5. **Measures vs families confusion**: `measures_computed` stores family names ("summary_stats", "inequality") not actual measure names ("mean", "gini")
6. **Filters ambiguity**: `filters_applied` field initialized but never populated
7. **Layout role inference**: Roles (rows/columns) guessed from dimension order in renderer, not explicit in metadata
8. **Description source unclear**: Is the description generated from a cached result or a fresh recomputation?
9. **Specification vs execution blurred**: Current schema doesn't clearly separate "what was requested" from "what actually happened"
10. **Edge case coverage**: Description renderer assumes success, doesn't handle empty surveys, all-excluded, or zero dimensions clearly

The description must be a **factual execution record** that researchers, policy analysts, and users can trust for citation and reproducibility. Current metadata does not meet this standard.

## Requirements

### Functional Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Description accurately reflects what `table_maker()` actually executed | User requirement |
| R2 | Clear distinction between requested parameters and execution results | Audit findings 1, 5, 9 |
| R3 | Survey lifecycle tracked: requested → matched → loaded → excluded (with reasons and stages) | Audit findings 1, 2 |
| R4 | Release and PPP resolution tracked: requested value → resolved value → physical column used | Audit findings 3, 4 |
| R5 | Measures reported as actual measure names dispatched, not family names | Audit finding 5 |
| R6 | Filters reported as normalized request (not per-survey category availability) | Audit finding 6, load_surveys behavior |
| R7 | Layout roles explicit or clearly documented | Audit finding 7 |
| R8 | Cell definition generated from execution truth, not specification | Audit finding 10 |
| R9 | Warnings remain as strings (human-readable, not structured objects) | Simplicity decision |
| R10 | `table_maker()` default path (`with_meta = FALSE`) remains unchanged | Backward compatibility |
| R11 | `/table` endpoint contract unchanged | Backward compatibility |
| R12 | Work phased into independently testable deliverables | User requirement |

### Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NF1 | Metadata overhead < 10% of `table_maker(with_meta = TRUE)` runtime |
| NF2 | Field names match semantic meaning (no misleading names) |
| NF3 | Schema extensible for future audit needs without breaking changes |

### Out of Scope

- Per-survey PPP differences (PPP is uniform across batch)
- Structured warning objects (keeping strings for simplicity)
- Per-survey filter category availability tracking (would require new infrastructure)
- Reproducibility code snippets (deferred)
- Timestamp metadata (optional, low priority)

## Current Implementation Behavior

From code inspection and review findings:

### `table_maker()` Edge Case Behavior

| Scenario | Behavior | Lines |
|----------|----------|-------|
| Missing surveys from manifest | `cli_warn()` → continue with matched | 435 |
| Filter-base exclusions | `cli_warn()` → continue with surveys that have all filter vars | 466 |
| Dimension exclusions | `cli_warn()` → continue with surveys that have all dimensions | 539 |
| All surveys excluded | `cli_abort()` → stop execution, no result | 494, 561 |
| Zero rows after loading | `cli_abort()` → stop execution | 600 |
| Suppression triggered | `cli_warn()` → continue, drop non-share measures | 738 |

**Pattern**: Partial exclusions emit warnings and continue; total failures abort.

### Current Metadata Defects

From `R/table_maker.R` and `R/description.R`:

1. **Line 424**: `included_surveys` populated from `entries` after manifest join **before** dimension pre-filter (line 539) and filter-base pre-filter (line 466)
2. **Line 474-487**: Filter-base exclusions warned but not harvested into `.meta_state[["excluded_surveys"]]`
3. **Line 336**: `measures_computed` populated with `names(families)` (e.g., "summary_stats") not measure names (e.g., "mean")
4. **Line 296-302**: `.meta_state` fields `filters_applied`, `suppression`, `ppp_used` initialized but never populated
5. **Line 308-316**: Warning handler defined but never registered via `withCallingHandlers()` (P0.1 finding)
6. **Line 98-180**: `.build_specification()` doesn't accept `release` parameter, uses default for all registry calls
7. **Line 596**: `ppp_used` populated with requested `ppp`, not resolved `ppp` or physical column name
8. **R/description.R:256-264**: Layout roles inferred from dimension order in renderer, not stored in specification

### `load_surveys()` Filter Behavior

From `R/load_data.R` lines 578-593:

- `filter_base` applied as Arrow filter uniformly to all surveys before collection
- No per-survey tracking of which categories were present
- Filter is "request-level" (what was asked) not "execution-level" (what each survey had)
- This is correct behavior — description should report the request, not infer per-survey execution

## Approaches Considered

### Approach 1: Surgical Fix (Conservative)

**Summary**: Keep current field structure, fix only harvesting logic and semantics.

**Strategy**:
- Preserve existing field names (`included_surveys`, `excluded_surveys`, `measures_computed`)
- Fix where and when each field is populated
- Clarify semantic meaning through documentation
- Minimal breaking changes

**Phases**:
1. Metadata Harvesting (3-4 days): Fix population points, register warning handler, thread `release`
2. Description Model (2-3 days): Update model to use corrected execution fields
3. Renderer (1-2 days): Fix cell definition and edge cases

**Pros**:
- Least disruptive — field names unchanged
- Fast to implement — surgery on existing code
- Low regression risk

**Cons**:
- Semantic ambiguity remains (`included_surveys` still sounds like "requested")
- Doesn't fully address "requested vs executed" distinction
- May confuse future maintainers about timing

**Effort**: Medium (6-9 days)

**Recommended?** No — doesn't achieve execution truth goal. Field names mislead.

---

### Approach 2: Semantic Redesign (Balanced) ✅ CHOSEN

**Summary**: Redesign metadata schema to explicitly separate requested vs. executed, keep implementation structure.

**Strategy**:
- Split `specification` and `execution` semantics clearly
- Rename ambiguous fields to reflect execution truth
- Add explicit concepts: `requested_pip_id`, `loaded_surveys`, `resolved_release`, `resolved_ppp`
- Keep implementation flow (same harvesting points, clearer semantics)

**New Schema**:

```r
# Specification: What was requested (with labels from registry)
specification <- list(
  pip_id              = requested pip_id vector (before exclusions),
  analysis_var        = list(name = "welfare", label = "Welfare"),
  measures            = list(
                          list(name = "mean", label = "Mean welfare", family = "summary_stats"),
                          list(name = "gini", label = "Gini index", family = "inequality")
                        ),
  poverty_line        = as requested (or NULL),
  ppp                 = as requested (or 2021L default),
  by                  = list(
                          list(name = "gender", label = "Gender", role = "rows", categories = ...),
                          list(name = "area", label = "Area", role = "columns", categories = ...)
                        ),
  filter_base         = list(
                          list(varname = "age_group", label = "Age group", kept = list("15-24", "25-64", "65+"))
                        ),
  pop_share_threshold = 0.01
)

# Execution: What actually happened
execution <- list(
  # Survey lifecycle
  requested_pip_id    = original pip_id vector from function call,
  loaded_surveys      = data.table(pip_id, country_code, year, welfare_type),  # contributed data
  excluded_surveys    = data.table(pip_id, reason, stage),  # stage: "manifest" | "filter_pre" | "dimension_pre"
  
  # Resolution
  resolved_release    = single authority release ID used throughout,
  resolved_ppp        = PPP year actually used (after resolution),
  ppp_column_used     = physical column name (e.g., "welfare_ppp_2021"),
  
  # Execution state
  filters_applied     = normalized_filter_base (the request),
  measures_computed   = c("mean", "gini"),  # actual measure names dispatched
  suppression         = list(threshold = 0.01, n_suppressed_cells = 2L)
)

# Provenance: Package and data version
provenance <- list(
  package_version = "0.4.2",
  release         = "20260206",
  generated_at    = NULL  # optional, deferred
)

# Warnings: Captured messages (strings)
warnings <- c(
  "Excluding 2 surveys that lack all requested dimensions: ...",
  "Suppressing non-share measures for 2 cells with pop_share < 0.01: ..."
)
```

**Implementation Changes**:

1. **Rename & reposition `included_surveys`**:
   - Rename to `loaded_surveys`
   - Populate **after** all exclusions (after line 569, before load_surveys call)
   - Semantic: surveys that contributed data to final result

2. **Add `requested_pip_id`**:
   - Capture original `pip_id` argument at function entry
   - Semantic: what user asked for (before manifest resolution)

3. **Add `excluded_surveys.stage`**:
   - Track which exclusion point: "manifest", "filter_pre", "dimension_pre"
   - Harvest at lines 433-441 (manifest), 474-487 (filter_pre), 547-557 (dimension_pre)

4. **Add resolution tracking**:
   - `resolved_release`: Use consistent release throughout (resolve once at line 409)
   - `resolved_ppp`: Track effective PPP year after resolution
   - `ppp_column_used`: Physical column from `load_surveys()` (e.g., "welfare_ppp_2021")

5. **Fix `measures_computed`**:
   - Store measure names: `measures` (e.g., c("mean", "gini"))
   - Not family names: `names(families)` (e.g., c("summary_stats", "inequality"))

6. **Populate `filters_applied`**:
   - Already computed as `normalized_filter_base` at line 380-400
   - Store in `.meta_state[["filters_applied"]]` at line 404

7. **Fix warning handler**:
   - Register via `withCallingHandlers()` wrapping compute body (P0.1 fix)
   - Correct append target: `.meta_state[["warnings"]][[length(...)]]` not `.meta_state[[length(...)]]`

8. **Thread release through `.build_specification()`**:
   - Add `release` parameter
   - Pass to all registry calls: `piptm_analysis_variables(release)`, `piptm_stat_groups(release)`, etc.

9. **Layout roles**:
   - Phase 1: Keep position-based inference, document contract clearly
   - Phase 2: Update description model to use inference
   - Phase 3 (deferred): Make roles explicit in registry if evidence shows inference is problematic

**Phases**:

**Phase 1: Execution Truth Schema (4-5 days)**
- Redesign `execution` and `specification` schemas as above
- Implement all harvesting fixes (1-9 above)
- Register warning handler correctly
- Thread release consistently
- Rewrite `test-table-maker-with-meta.R` for new schema
- Acceptance: All execution fields accurately populated, 128 tests pass

**Phase 2: Description Model Update (2-3 days)**
- Update `build_description_model()` to consume new schema
- Use `loaded_surveys` (not `included_surveys`)
- Display `requested_pip_id` vs `loaded_surveys` count
- Show exclusion stages in markdown
- Document layout role inference contract
- Rewrite `test-description-model.R`
- Acceptance: Model correctly interprets new schema

**Phase 3: Renderer & Edge Cases (2 days)**
- Fix cell definition generation using execution truth
- Handle all conditional sections (0-4 dimensions, exclusions, suppression)
- Test edge cases: empty exclusions, 4-variable layouts, singular grammar
- Update `test-description-renderer.R` and `test-api-description-endpoint.R`
- Acceptance: Markdown accurate for all scenarios

**Pros**:
- ✅ Clear execution truth — field names match semantics
- ✅ Explicit requested vs. executed separation
- ✅ Future-proof — maintainers understand timing
- ✅ Achieves all 11 audit goals
- ✅ Independently testable phases

**Cons**:
- Breaking change to `with_meta = TRUE` schema (but no external users yet)
- More test rewrites than Approach 1
- Slightly more complex schema

**Effort**: Medium-Large (8-10 days total)

**Recommended?** YES

---

### Approach 3: Full Contract Redesign (Aggressive)

**Summary**: Redesign from first principles with maximum granularity.

**Strategy**:
- Full schema redesign with per-survey execution tracking
- Structured warning objects (`{code, level, data, message}`)
- Per-survey filter category availability tracking
- Full audit trail with timeline

**Phases**:
1. Schema & Harvesting (6-7 days): Per-survey granularity, structured warnings
2. Model & Renderer (3-4 days): Advanced templating
3. Testing (3 days): Property-based testing, full adversarial suite

**Pros**:
- Maximum detail and audit capability
- Structured data for programmatic queries
- Future-proof for all use cases

**Cons**:
- ❌ Massive effort (12-14 days)
- ❌ Over-engineering for current needs
- ❌ Per-survey tracking requires new infrastructure
- ❌ Complexity burden for maintainers

**Effort**: Large (12-14 days)

**Recommended?** No — overkill. Users need execution truth, not per-survey audit logs.

## Decision

**Chosen Approach:** Approach 2: Semantic Redesign (Balanced)

**Rationale**:
1. **Achieves execution truth goal**: Field names match semantics, metadata accurately reflects what happened
2. **Clear contract**: Explicit separation of requested vs. executed eliminates ambiguity
3. **Manageable effort**: 8-10 days with 3 independently testable phases
4. **Addresses all 11 audit questions**: Survey status, release resolution, PPP metadata, measures vs families, filters, layout roles, description source, specification vs execution separation, model schema, edge cases, renderer correctness
5. **Future-proof**: Schema is extensible without breaking changes
6. **No external users yet**: Breaking `with_meta = TRUE` schema has zero migration cost

**Safeguards** (from devil's advocate):
- Timebox Phase 1 to 5 days maximum — reassess if longer
- Measure metadata overhead before/after — abort if >10% performance impact
- Ship Phase 1 for internal validation before committing to Phases 2-3
- Defer layout role registry changes to Phase 3 if scope creep emerges

**Decision on out-of-scope items**:
- ✅ PPP: One global value (not per-survey)
- ✅ Warnings: Keep as strings (not structured objects)
- ✅ Filters: Report requested `filter_base` globally (not per-survey availability)
- ✅ Reproducibility code: Deferred
- ✅ Timestamp: Optional (`generated_at` in provenance, low priority)

## Next Steps

### For `/cg-plan`

1. **Create corrective implementation plan**:
   - Title: "Step 3 Description Metadata Contract — Corrective Implementation"
   - Mark as corrective plan superseding `2026-08-20-step3-description-implementation.md`
   - Define final `table_result` contract (specification, execution, provenance, warnings)
   - Detail 3 phases with acceptance criteria per phase
   - Include test strategy: rewrite `with_meta = TRUE` tests, preserve default path tests

2. **Phase 1 deliverables**:
   - New `execution` schema implemented
   - New `specification` schema implemented
   - All 9 harvesting fixes applied
   - Warning handler registered
   - Release threaded consistently
   - 128 tests pass (default path unchanged)
   - New `with_meta = TRUE` tests pass

3. **Phase 2 deliverables**:
   - `build_description_model()` updated for new schema
   - Model tests rewritten and passing

4. **Phase 3 deliverables**:
   - Renderer updated
   - Edge cases handled
   - All description tests passing
   - API endpoint tests passing

### For Manual Work

- Run `devtools::test()` baseline before Phase 1
- Benchmark `table_maker(with_meta = TRUE)` memory/runtime before Phase 1
- Internal validation after Phase 1 before proceeding

### For Future Enhancements (Out of Scope)

- Structured warning objects (if UI needs programmatic filtering)
- Per-survey execution tracking (if audit requirements emerge)
- Server-side PDF generation
- Reproducibility code snippets
- Multiple languages
