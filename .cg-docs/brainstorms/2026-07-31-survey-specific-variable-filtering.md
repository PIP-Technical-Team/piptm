---
date: 2026-07-31
title: "Survey-Specific Variable Filtering for Step 2"
status: decided
scope: "Lightweight"
chosen-approach: "Approach A — In-Handler Intersection Logic"
tags: [api, endpoints, ui-integration, filtering]
---

# Survey-Specific Variable Filtering for Step 2

## Context

The ITS UI development team requested a way to persist survey selections from Step 1 (survey grid) so that if Step 2 (variable selection) crashes or refreshes, users don't lose their work.

Initial assumption was that this required server-side session storage. After clarification, the actual requirement is simpler: the UI will store `pip_id` values client-side (localStorage/sessionStorage), and the API needs to filter `/categories` and `/covariates` endpoints to only show variables available in those selected surveys.

This prevents users from selecting filter variables or layout covariates that don't exist in their survey selection, which would otherwise cause surveys to be dropped from results with warnings.

## Requirements

1. **Survey-specific filtering**: `/categories` and `/covariates` should accept an optional `pip_id[]` parameter and return only variables present in ALL selected surveys (intersection).
2. **Backwards compatibility**: When `pip_id` parameter is omitted, endpoints return the full catalog (existing behavior preserved).
3. **Simple UX**: Show users only variables that work for all their surveys — no warnings, no dropped surveys, no complexity.
4. **Empty intersection handling**: If selected surveys share no common variables, return an empty array (UI shows appropriate message).
5. **No server-side sessions**: UI handles storage of `pip_id` values; API is stateless.

## Out of Scope

- Server-side session storage (`POST /session/surveys`, `GET /session/:id/surveys`) — deferred until/unless UI team explicitly requests it
- Coverage metadata (showing "available in 8/10 surveys") — deemed too complex for v1
- Two-tier UI grouping (safe vs. partial-coverage variables) — too complex for v1
- Union-based filtering (showing variables present in ANY survey) — rejected in favor of simpler intersection approach

## Approaches Considered

### Approach A: In-Handler Intersection Logic (Chosen)

Add `pip_id[]` parameter handling directly in the `/categories` and `/covariates` endpoint handlers. Compute intersection inline using `Reduce(intersect, entries$dimensions)`.

**Implementation pattern:**
```r
#* @param pip_id:[character] Survey identifiers (optional)
#* @get /categories
function(pip_id = NULL, release = NULL, res) {
  if (is.null(pip_id)) {
    # Return full catalog (backwards compatible)
    data <- piptm::piptm_filter_categories(rel)
  } else {
    # Get manifest entries for selected surveys
    mf <- piptm::piptm_manifest(rel)
    entries <- mf[pip_id %chin% pip_id]
    
    # Compute intersection of dimensions
    dims_intersection <- Reduce(intersect, entries$dimensions)
    
    # Filter catalog to intersection
    all_cats <- piptm::piptm_filter_categories(rel)
    data <- Filter(function(cat) cat$varname %in% dims_intersection, all_cats)
  }
  # ... response building
}
```

**Pros**:
- Simple — ~30 lines per endpoint
- No new files or helpers needed
- Backwards compatible (empty `pip_id` → full catalog)
- Self-contained logic
- Fast to implement (half day)

**Cons**:
- Duplicated intersection logic between two endpoints (acceptable for 5 lines)
- Slightly longer handlers

**Effort**: Small (half day)

**Recommended?** Yes

### Approach B: Shared Helper Function

Extract intersection logic into `get_dimension_intersection(pip_id, release)` helper in `helpers.R`. Both endpoints call this helper.

**Pros**:
- DRY — reusable helper
- Easier to test intersection logic in isolation
- Cleaner endpoint handlers

**Cons**:
- Slightly more indirection
- One extra function to maintain

**Effort**: Small (half day)

**Recommended?** Good alternative if we expect more endpoints to need this logic

### Approach C: Package-Level Exported Function

Add `piptm::piptm_dimension_intersection(pip_id, release)` as an exported package function in `R/manifest_utils.R`.

**Pros**:
- Fully testable via testthat
- Reusable beyond API (other R scripts, notebooks)
- Documents the intersection contract

**Cons**:
- Heavier — adds to package API surface
- More files to navigate
- Overkill for 2 endpoints

**Effort**: Small-to-medium (1 day)

**Recommended?** No — over-engineering for this use case

## Decision

**Approach A** — In-Handler Intersection Logic.

**Rationale**:
- Fastest to implement
- Logic is simple enough (5 lines) that duplication between two endpoints isn't harmful
- Keeps everything in `plumber.R` where endpoint logic belongs
- Easy to refactor to Approach B later if we add more endpoints needing this logic
- Intersection approach (vs. union) is simpler and prevents user confusion — no warnings about dropped surveys

## Edge Cases Handled

1. **Empty `pip_id` parameter**: Return full catalog (backwards compatible)
2. **No matching surveys in manifest**: Return empty array with appropriate metadata
3. **Empty intersection**: Return empty array (UI can show "No common variables across selected surveys")
4. **Invalid `pip_id` values**: Filtered out by manifest join; warnings captured and returned in response envelope

## API Changes

### Modified Endpoints

#### `GET /categories`
**Before**: Always returned full catalog from `piptm_filter_categories()`

**After**: 
- No `pip_id` param → full catalog (unchanged)
- With `pip_id` param → filtered to intersection

**New parameters**:
- `pip_id`: Character vector (optional, repeatable) — survey identifiers

**Response changes**:
- Added `meta.filtered`: Boolean indicating whether filtering was applied
- Added `meta.n_surveys`: Count of surveys used for filtering (when applicable)

#### `GET /covariates`
**Before**: Always returned full catalog from `piptm_layout_covariates()`

**After**: Same pattern as `/categories`

**New parameters**:
- `pip_id`: Character vector (optional, repeatable) — survey identifiers

**Response changes**: Same as `/categories`

## Next Steps

1. Implement modified `/categories` endpoint in `inst/plumber/plumber.R`
2. Implement modified `/covariates` endpoint in `inst/plumber/plumber.R`
3. Test with Insomnia:
   - `/categories` with no params → full catalog
   - `/categories?pip_id=COL_2010_GEIH_INC_ALL` → filtered catalog
   - `/categories?pip_id=X&pip_id=Y` → intersection
   - Same tests for `/covariates`
4. Document new parameters in endpoint roxygen comments
5. Update API documentation (if exists) or OpenAPI spec (auto-generated by plumber)
6. Communicate to ITS team:
   - Both endpoints now accept `pip_id[]` parameter
   - Returns intersection only (variables present in ALL selected surveys)
   - Empty result = no common variables → UI should guide user to adjust selection

## Future Considerations

If UI team later requests server-side session storage:
- Add `POST /session/surveys` (store pip_ids, return session_id)
- Add `GET /session/:id/surveys` (retrieve pip_ids by session_id)
- Modify `/categories` and `/covariates` to also accept `session_id` parameter (alternative to `pip_id[]`)
- Use in-memory environment for v1, migrate to Redis if multi-worker scaling is needed

This decision keeps the door open for session storage while delivering immediate value with simpler stateless filtering.
