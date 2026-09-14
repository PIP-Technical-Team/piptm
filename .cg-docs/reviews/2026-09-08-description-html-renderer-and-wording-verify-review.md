---
date: 2026-09-14
depth: light
parent-review: .cg-docs/reviews/2026-09-08-description-html-renderer-and-wording-review.md
type: verification
findings:
  P1.1: fixed
  P2.1: fixed
  P2.2: fixed
  P2.3: fixed
  P2.4: fixed
  P3.1: fixed
---

## Review Report

**Review mode**: verify (light depth, forced per `mode:verify`)
**Files reviewed**: 7 uncommitted working-tree changes (`R/description_builder.R`, `R/description_renderer.R`, `R/description_renderer_html.R`, `inst/plumber/plumber.R`, `tests/testthat/test-cell-definition.R`, `tests/testthat/test-description-builder.R`, `tests/testthat/test-description-renderer.R`)
**Findings**: 6 (P0: 0, P1: 1, P2: 4, P3: 1)
**Parent review**: `.cg-docs/reviews/2026-09-08-description-html-renderer-and-wording-review.md`
**Verified as resolved**: `P1.1` (vector `poverty_line` crash), `P1.2` (untested JSON-list coercion), `P1.3` (missing endpoint doc), `P1.4` (Markdown/HTML label divergence — partially; see new P1.1 below), `P2.2` (missing Content-Type comment), `P2.4` (missing cross-reference comment), `P2.13` (unnecessary `rep()`), `P3.16` (duplicated format-dispatch logic)
**Brain findings applied**: `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md` — used to verify both the multi-threshold `poverty_line` fix and the list/empty-list coercion test coverage against the documented defensive-coding pattern for this endpoint.

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-code-quality] `R/description_renderer_html.R:184-190` vs `R/description_renderer.R:391` — HTML `stat_group` label map uses `welfare = "Welfare"` where Markdown correctly uses `shares = "Shares"`
  **Why**: The canonical `stat_group` values produced by the codebase are `summary_statistics`, `poverty`, `inequality`, and `shares` — there is no `welfare` stat_group. HTML's map has a dead, never-matched `welfare` entry and is missing the real `shares` entry; it currently produces a plausible-looking result for `shares` only by falling through to the generic title-case fallback (single word, no underscore to reformat), which is coincidental, not correct. This is a genuine new cross-file divergence between the two renderers, introduced independently of this fix-triage session (pre-existing in `description_renderer_html.R`), but it directly undermines the P1.4 fix's goal of making Markdown and HTML content consistent — per the verify suppression policy, cross-file breakage is always reported regardless of fix scope.
  **Fix**: Replace `welfare = "Welfare"` with `shares = "Shares"` in `R/description_renderer_html.R`'s `stat_group_map` to match `R/description_renderer.R:391` exactly.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-code-quality] `R/description_renderer.R` (`.display_label_markdown`) vs `R/description_renderer_html.R` (`.display_label_html`) — the two label-mapping functions disagree on the label for `analysis_var_type` in `statistics_selected` ("Analysis variable type" vs "Variable type")
  **Why**: Direct content divergence between the two renderers for the exact same field, in the very functions added to eliminate this class of divergence (P1.4).
  **Fix**: Pick one label and use it in both `.display_label_markdown()` and `.display_label_html()`.

- **[P2.2]** [cg-code-quality] `R/description_renderer.R` (`.display_label_markdown`) — missing `filters_applied`/`layout_configuration`/`provenance` section-specific branches that `.display_label_html()` has, and a weaker generic fallback (returns the raw key with no formatting, vs HTML's `gsub("_", " ", key)` + capitalize)
  **Why**: For these three sections, Markdown output still surfaces raw internal keys (e.g. `filters`, `layout`) while HTML shows friendly labels — the same divergence class P1.4 was meant to resolve, just in sections not covered by the original P1.4 fix.
  **Fix**: Add matching `filters_applied`/`layout_configuration`/`provenance` branches to `.display_label_markdown()`, and align its generic fallback with HTML's `gsub("_"," ", key)` + capitalize behavior.

- **[P2.3]** [cg-code-quality] `R/description_builder.R:524-527` — dead variable `poverty_line_text` in `build_cell_definition()`
  **Why**: `.format_poverty_line_display(poverty_line_values, include_currency = TRUE)` is assigned to `poverty_line_text` but never referenced; the function instead hand-rolls an equivalent `/day`-suffixed string a few lines later as `poverty_line_daily_text`, duplicating logic the helper already encapsulates.
  **Fix**: Remove the unused `poverty_line_text` assignment, or extend `.format_poverty_line_display()` with a `suffix` parameter and call it once instead of maintaining two parallel formatting paths.

- **[P2.4]** [cg-testing] `tests/testthat/test-cell-definition.R` — the JSON-list-coercion defensive fix was duplicated into `build_cell_definition()` (via `.coerce_description_scalar()`) but only `build_description_model()` (in `test-description-builder.R`) has regression tests for it
  **Why**: All `.make_resolved_labels(analysis_var_info = list(...))` calls in `test-cell-definition.R` use plain scalar strings, never a list-wrapped or empty-list `ui_label`/`tm_type`. The same JSON round-trip that motivated the original P1.2 fix flows through `build_cell_definition()` via the `/description` endpoint's metadata path, so a future regression in this call site would go uncaught.
  **Fix**: Add two tests to `test-cell-definition.R`: (a) `ui_label = list("Welfare")`/`tm_type = list("continuous")` unwraps to scalar text in `measure_interpretation`; (b) `ui_label = list()`/`tm_type = list()` falls back to the raw `analysis_var` value and `"unknown"` respectively.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/description_builder.R:602-605` — inconsistent continuation-line indentation in the `pov_status` `sprintf()` call (28 spaces vs the project's 2-space-multiple style used in neighboring `sprintf()` calls)
  **Fix**: Re-indent to match standard style:
  ```r
  group_qualifier <- sprintf(
    ", within each %s group (%s)",
    pov_label,
    poverty_group_text
  )
  ```

### ✅ Passed

- **cg-testing**: `devtools::test(filter = "description-builder|cell-definition|description-renderer")` → **`FAIL 0 | WARN 0 | SKIP 0 | PASS 202`**. Confirmed both `pov_status`-grouping and plain "poverty" stat_group code paths are exercised for multi-threshold `poverty_line`; JSON-round-trip coercion tests cover both list-unwrap and empty-list-fallback cases exactly as the prior review's fix description required; Markdown-renderer test updates only relabel expected strings to match the newly aligned labels — no structural assertions were weakened, and the byte-identical regression test still passes.
- **cg-testing**: No cross-file breakage detected between `plumber.R`'s `render_description()` dispatch helper and the two renderers.
- **cg-code-quality**: New helper functions (`.coerce_description_scalar()`, `.normalize_poverty_line_values()`, `.format_poverty_line_display()`, `.display_label_markdown()`, `.display_table_header_markdown()`, `.display_table_value_markdown()`) are well-formed, non-duplicative internally, and correctly documented with `@keywords internal`.
