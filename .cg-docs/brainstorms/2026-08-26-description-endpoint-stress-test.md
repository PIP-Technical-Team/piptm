---
date: 2026-08-26
title: "/description Endpoint Design Stress Test"
status: decided
scope: "Deep"
artifact-schema-version: 1
chosen-approach: "Inline Metadata Assembly (Approach A)"
tags: [description-endpoint, table_maker, api, cell-definition, metadata]
---

# /description Endpoint Design Stress Test

## Context

The user attached a comprehensive technical specification for a new `/description` endpoint that generates factual, dynamically constructed natural-language descriptions of tabular results produced by `table_maker()`. The goal was to stress-test the design, identify gaps, and confirm all requirements are properly set up before planning begins.

The spec defines a 3-tier decoupled system: Backend Enhancement Layer → Description Data Model Layer → Rendering Layer. The core challenge is the cell definition algorithm, which must generate mathematically accurate descriptions that adapt to all permutations of filters, analysis variables, measures, and covariates.

## Requirements (Confirmed During Q&A)

1. **Consumer profile**: All consumers are researchers, economists, data scientists — technical users who understand the terminology. No glossary or interpretation hints needed.
2. **No recomputation**: The `/description` endpoint must NOT re-run `table_maker()`. It consumes metadata already produced by `/table?include_metadata=true`. Two modes supported: fast path (metadata POST) and fallback (param-based recomputation for edge cases like browser refresh).
3. **Return type consistency**: `table_maker()` should always return a list. When `include_metadata = FALSE` (default): `list(data = <data.table>)`. When `TRUE`: `list(data = <data.table>, description_metadata = <list>)`. This is a breaking change to existing callers but keeps the API contract clean.
4. **English only**: No internationalization for v1.
5. **All four worked examples are v1 requirements**: Simple aggregate mean, filtered poverty with covariates, binary analysis variable with target shares, poverty status as covariate.
6. **Strictly factual**: No interpretation, no policy recommendations, no statistical methodology beyond naming measures.

## Approaches Considered

### Approach A: Inline Metadata Assembly (Chosen)
`table_maker()` assembles `description_metadata` internally when `include_metadata = TRUE`. New files `description_builder.R` and `description_renderer.R` transform metadata → structured model → Markdown.

**Pros**: All variables in scope at computation point; no data duplication; follows the spec exactly; lowest risk given the detailed spec.
**Cons**: `table_maker()` grows by ~100-150 lines; metadata logic coupled to computation pipeline.

### Approach B: Post-Hoc Metadata Extraction
Separate `extract_description_metadata(result, params)` function post-processes the result.

**Pros**: Clean separation; independently testable.
**Cons**: Some metadata (suppression events, warnings, excluded surveys) is lost after `table_maker()` returns; duplicates resolution logic.

### Approach C: Hybrid — Metadata Bubble with Lazy Rendering
`table_maker()` captures minimal raw metadata; builder does all label resolution.

**Pros**: Minimal code change to `table_maker()`; builder is independently testable.
**Cons**: Two-phase design adds complexity; slightly more work in builder.

## Decision

**Approach A (Inline Metadata Assembly)** chosen for the following reasons:
- The spec is detailed enough that inline assembly is the safest path
- ~100-150 lines of metadata code doesn't justify a separate extraction layer
- Having all variables in scope avoids the data-loss problem that kills Approach B
- The cell definition algorithm needs access to intermediate computation state (suppression events, excluded surveys, warnings) that is only available inside `table_maker()`

**Key architectural decisions:**
1. `/table?include_metadata=true` returns metadata inline (fast path)
2. `/description` accepts metadata via POST body (render-only, ~50ms)
3. `/description` also accepts params as a fallback (recomputes, slower — rare recovery path)
4. `table_maker()` always returns a list — consistent return type regardless of `include_metadata`
5. Internal assertions validate return structure at the source

## Next Steps

1. **Phase 1 (3-5 days)**: Backend enhancements to `table_maker.R`
   - Add `include_metadata` parameter (default `FALSE`)
   - Instrument execution log capture (loaded/excluded surveys, filters, suppression, warnings)
   - Build `.build_description_metadata()` helper
   - Modify return logic to always return a list
   - Update all existing `table_maker()` call sites to unwrap `result$data`
   - Regression tests: `include_metadata = FALSE` returns identical `data.table` as before

2. **Phase 2 (4-6 days)**: Description Data Model Builder (`R/description_builder.R`)
   - `build_description_model(metadata, params)` — structured model with conditional visibility
   - `build_cell_definition()` — implements Section IV algorithm for all measure families
   - Helper functions: `resolve_filter_labels()`, `resolve_measure_labels()`, `format_covariate_description()`
   - Unit tests for each helper + integration tests for all 4 worked examples

3. **Phase 3 (2-3 days)**: Markdown Renderer (`R/description_renderer.R`)
   - `render_description_markdown(model)` — iterates sections, renders content
   - Golden file tests against expected Markdown output

4. **Phase 4 (1-2 days)**: API Endpoint (`inst/plumber/plumber.R`)
   - Add `/description` route (POST, accepts metadata JSON)
   - Update `/table` endpoint to support `include_metadata` param

5. **Phase 5 (2 days)**: Documentation & examples

**Roadmap item added**: "Description endpoint cache key generation" (idea status) under Phase 4 — for future caching optimization.
