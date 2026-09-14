---
date: 2026-09-10
depth: standard
type: standard
plan: .cg-docs/plans/2026-09-08-description-html-renderer-and-wording.md
findings:
  P1.1: fixed
  P1.2: fixed
  P1.3: fixed
  P1.4: fixed
  P2.1: open
  P2.2: fixed
  P2.3: open
  P2.4: fixed
  P2.5: open
  P2.6: open
  P2.7: open
  P2.8: open
  P2.9: open
  P2.10: open
  P2.11: open
  P2.12: open
  P2.13: fixed
  P3.1: open
  P3.2: open
  P3.3: open
  P3.4: open
  P3.5: open
  P3.6: open
  P3.7: open
  P3.8: open
  P3.9: open
  P3.10: open
  P3.11: open
  P3.12: open
  P3.13: open
  P3.14: open
  P3.15: open
  P3.16: fixed
---

## Review Report

**Review mode**: standard (explicit user request; diff also matches the `architecture-risk` auto-routing trigger — new public API-contract field on `/description` — kept at standard per explicit-mode precedence, noted here as focus emphasis for `@cg-architecture`)
**Files reviewed**: 9 (`R/description_builder.R`, `R/description_renderer_html.R`, `R/description_renderer.R`, `inst/plumber/helpers.R`, `inst/plumber/plumber.R`, `NAMESPACE`, `tests/testthat/test-api-description.R`, `tests/testthat/test-cell-definition.R`, `tests/testthat/test-description-builder.R`, `tests/testthat/test-description-renderer-html.R`)
**Scope**: `git diff 391502e^..HEAD` — the 4 commits implementing `.cg-docs/plans/2026-09-08-description-html-renderer-and-wording.md`
**Findings**: 32 (P0: 0, P1: 4, P2: 13, P3: 15)
**Brain findings applied**: `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md` — this exact endpoint previously had P0 JSON round-trip type bugs; used to focus `@cg-data-quality`/`@cg-testing` review on the new `format` field and `analysis_var_label`/`type` coercion paths.

### P1 — CRITICAL (must fix before merge)

- **[P1.1]** [cg-data-quality, confirmed live via interactive `table_maker()` execution] `R/description_builder.R:343` (`has_poverty <- !is.null(pl) && length(pl) > 0L && !is.na(pl)`) and `R/description_builder.R:520-524,574-579,632-634` (`sprintf("...%.2f...", poverty_line, ...)`) — **`build_description_model()` crashes whenever `poverty_line` has length ≥ 2**
  **Why**: `&&` requires a scalar logical; `!is.na(pl)` returns a length-2+ vector for a multi-element `poverty_line`, raising `Error: 'length = 2' in coercion to logical(1)`. Reproduced live: `table_maker(..., poverty_line = c(3.65, 6.85), include_metadata = TRUE)` → `build_description_model()` crashes for both `pov_status` and non-`pov_status` analyses. `table_maker()` itself explicitly documents/supports vector `poverty_line` for multi-threshold FGT measures, so this is a reachable regression in a documented use case, not user error. Even after fixing the `&&` guard, the `sprintf()` calls at the three other lines are not vectorized-safe either and will independently break the `character(1)` `vapply` contract in `build_cell_definition()`.
  **Fix**: Vectorize the guard (`!anyNA(pl)` instead of `!is.na(pl)`), and decide a canonical multi-threshold rendering (e.g. `paste(sprintf("$%.2f", poverty_line), collapse = " / ")`) applied consistently everywhere `poverty_line` is interpolated into text. Add a test with `poverty_line = c(3.65, 6.85)` for both `pov_status` and non-`pov_status` scenarios.

- **[P1.2]** [cg-testing] `R/description_builder.R:318-334` — New JSON-round-trip defensive coercion for `analysis_var_label`/`analysis_var_type` (list→scalar unwrapping, empty-length fallback) is completely untested
  **Why**: This is exactly the bug class flagged in `.cg-docs/solutions/data-quality/2026-08-27-json-roundtrip-defensive-coding.md` for this same endpoint — `jsonlite::fromJSON()` round-trip can deliver `ui_label`/`tm_type` as a length-1 list, an empty list, or a plain scalar, and none of these three branches has a test.
  **Fix**: Add three tests to `test-description-builder.R`: (1) length-1-list unwraps to scalar, (2) empty-list `ui_label` falls back to `as.character(params$analysis_var)`, (3) empty `tm_type` falls back to `"unknown"`.

- **[P1.3]** [cg-documentation] `inst/plumber/plumber.R:225-241` (pre-fix) — `/description` endpoint's Swagger/OpenAPI comment block did not document the new `format` parameter
  **Why**: The plan's Documentation Checklist explicitly required this. Swagger/OpenAPI consumers had no way to discover the option.
  **Status**: **Fixed in this review** — see Autofix Applied below (added `format` documentation to the `#*` comment block).

- **[P1.4]** [cg-code-quality + cg-architecture] `R/description_renderer_html.R` vs `R/description_renderer.R` — the two renderers already diverge in **content**, not just markup, beyond the plan's accepted "future drift" risk
  **Why**: The plan's Risks table frames the Markdown/HTML duplication as a *future* desync risk if the model schema changes. In practice, `description_renderer_html.R` ships day-one with `.display_label_html()`/`.display_table_header_html()`/`.display_table_value_html()` that translate raw internal keys (`pip_id` → "Survey identifier", stat-group codes → "Poverty"/"Inequality"/etc.) into human labels, while `render_description_markdown()` emits the same raw keys verbatim. The two output formats are already materially different for the same model — not called out anywhere in the plan's requirements or approved mock description.
  **Fix**: Either explicitly scope this presentation-label divergence as approved (update the plan/brainstorm), or move the label-mapping into a shared, dialect-neutral helper consumed by both renderers so Markdown benefits from the same readability and the two formats stay in lockstep for non-styling content.

### P2 — IMPORTANT (should fix)

- **[P2.1]** [cg-architecture, cg-code-quality] `inst/plumber/plumber.R` (pre-fix) / `inst/plumber/helpers.R:538-548` — `format` normalization was duplicated between `validate_description_input()` and the route handler
  **Why**: `validate_description_input()` parses/normalizes/validates `format` but discarded the normalized value; the handler independently re-derived it. Verified this does **not** currently cause a behavioral bug (the `check$valid` gate correctly rejects invalid values with 400 before the handler's re-derivation runs), but it is duplication-driven drift risk if either side's normalization rules change independently.
  **Fix**: Have `validate_description_input()` return the normalized `format` (e.g. `check$format`) and have the handler consume it directly instead of re-deriving from `body$format`. (Not auto-applied — changes a function's `@return` contract; needs a human check of all call sites.)

- **[P2.2]** [cg-documentation, cg-reproducibility, cg-architecture] `inst/plumber/plumber.R` — missing required Step 4/R7 code comment documenting the verified `Content-Type` override behavior
  **Status**: **Fixed in this review** — see Autofix Applied below.

- **[P2.3]** [cg-documentation] `NEWS.md` — no changelog entry for the new `format` option / `render_description_html()` export
  **Fix**: Add a "New features" entry: "`POST /description` gains a `format` field (`markdown`/`html`); new exported `render_description_html()`."

- **[P2.4]** [cg-documentation] `R/description_renderer_html.R` / `R/description_renderer.R` — missing cross-reference comment for the parallel-file tradeoff
  **Status**: **Fixed in this review** — see Autofix Applied below.

- **[P2.5]** [cg-performance] `R/description_renderer_html.R:142-153` — `.render_table_html()` re-extracts the full column vector on every row iteration (`dt[[cols[[j]]]][[i]]` inside the row loop), giving `nrow * ncol` column lookups instead of `ncol`
  **Fix**: Extract each column once before the row loop (`col_values <- lapply(cols, function(cn) dt[[cn]])`), then index `col_values[[j]][[i]]` inside the loop. (Deferred — behavior-preserving but touches renderer hot-path logic; recommend applying with a focused before/after test run rather than as a blind autofix.)

- **[P2.6]** [cg-performance] `R/description_renderer_html.R` — `.html_escape()` called per-item inside `vapply` loops (section titles, list items) instead of once on the whole vector, even though it's already vectorized internally
  **Fix**: Escape vectors up front (e.g. in `.render_char_vector_html()`), then index into pre-escaped vectors inside the loop. (Deferred, same reasoning as P2.5.)

- **[P2.7]** [cg-code-quality] `R/description_renderer_html.R:203-380` — `.render_named_list_html()` is a ~180-line function mixing generic list traversal with section-specific special-casing (`identical(section_name, "statistics_selected")` x3, `identical(section_name, "overview")` x1) inline
  **Fix**: Extract the `poverty_line` special case into its own helper, and the per-section hidden-key list into a lookup table checked once at loop start.

- **[P2.8]** [cg-testing] `inst/plumber/helpers.R` / `inst/plumber/plumber.R` — no test for malformed `format` shapes (JSON array/length ≥2, non-character/numeric, `NA`) at unit or router level
  **Why**: Validation logic was manually verified in this review to already reject these shapes correctly, but the coverage gap matters given this endpoint's documented history of exactly this bug class.
  **Fix**: Add unit tests for `validate_description_input(list(format = c("html","markdown"), ...))`, `format = 123`, `format = NA`; add a router-level test for the same shapes asserting HTTP 400.

- **[P2.9]** [cg-testing] `R/description_renderer_html.R` — no test for `render_description_html()`'s own documented edge cases: NULL-content sections skipped, non-list `model` errors via `stopifnot`, fully-invisible model returns `""`
  **Fix**: Add `expect_error(render_description_html("x"))`, an all-`visible = FALSE` model returning `""`, and a visible section with `content = NULL` confirmed absent from output.

- **[P2.10]** [cg-testing] `tests/testthat/test-description-renderer-html.R` — HTML escaping is tested on only one scalar field, not on table cells or nested list items, even though those code paths call `.html_escape()` independently
  **Why**: Manually confirmed (via code inspection, `@cg-data-quality`) that escaping coverage in the renderer code itself is complete — this is a test-gap finding, not a live vulnerability.
  **Fix**: Add a test with a filter/covariate label containing `&`, `<`, `"` and assert the escaped form appears inside the rendered `<td>`/`<li>`.

- **[P2.11]** [cg-code-quality] `R/description_builder.R:333-338` (pre-fix location) — `measures_dt` carried a fully repeated `analysis_var_label` column purely to satisfy the renderer's per-row display needs, coupling the builder's data shape to a rendering-only concern
  **Status**: Partially addressed by the `rep()` removal in this review's autofix (P2.13); the underlying coupling (storing a section-level constant redundantly per-row) remains and is a design question for the plan owner, not purely mechanical — left open.

- **[P2.12]** [cg-version-control] `check_out.txt`, `check_out2.txt` (~670KB/~630KB) and `benchmarks/data-cache.rds` (~1.1MB) — tracked generated/binary artifacts
  **Why**: Pre-date this plan's 4 commits (introduced in `b2b11cd`/earlier); flagged as pre-existing hygiene debt encountered during this review, not introduced by this plan.
  **Fix**: Out of this plan's scope — `git rm --cached` + `.gitignore` update in a separate `chore` commit.

### P3 — MINOR (nice to have)

- **[P3.1]** [cg-code-quality] `R/description_builder.R:318-334` — duplicated list/unlist coercion pattern for `analysis_var_label`/`analysis_var_type`; extract a shared `.coerce_scalar_label()` helper.
- **[P3.2]** [cg-code-quality] Repeated wording fragments (e.g. "survey-weighted individuals") typed independently in 4+ places; consider shared string constants to avoid future wording drift.
- **[P3.3]** [cg-code-quality] `R/description_renderer_html.R:38-56` — imperative `sections[[length(sections)+1]] <-` growth loop is inconsistent with the `vapply` idiom used a few lines later in the same file.
- **[P3.4]** [cg-documentation] `render_description_html()` is tagged both `@export` and `@keywords internal` — contradictory; confirm against `render_description_markdown()`'s tagging and align.
- **[P3.5]** [cg-documentation] `@param model` says "9 sections" without naming them; add a `@seealso [build_description_model()]` cross-reference or list section names explicitly.
- **[P3.6]** [cg-documentation] `format` validation error message doesn't mention that omitting `format` defaults to `"markdown"`.
- **[P3.7]** [cg-testing] Fallback-path `format: "html"` lacks an explicit `Content-Type`-asserting test (only the fast path asserts it); add a skip-gated fallback-path test per the plan's own allowance.
- **[P3.8]** [cg-testing] `.make_render_model_html()` full-model fixture is reused for single-purpose tests (escaping, border style), increasing unrelated coupling; use minimal inline models for single-purpose tests.
- **[P3.9]** [cg-testing] The 400-invalid-format test asserts only a substring match on the error message, not the response envelope's structural shape.
- **[P3.10]** [cg-testing] `test-cell-definition.R` has several brittle full-sentence `expect_equal()` assertions; consider `expect_match()` on key substrings for some cases.
- **[P3.11]** [cg-data-quality] `.display_table_value_html()`'s stat-group label map hardcodes the same 4 groups that may already exist in `piptm_stat_groups()`'s registry — drift risk if the registry changes.
- **[P3.12]** [cg-reproducibility] `DESCRIPTION` pins `plumber (>= 1.1.0)` with no upper bound, despite the Content-Type-override behavior being documented as version-dependent, internal plumber behavior (not a stable public API guarantee).
- **[P3.13]** [cg-architecture] `.scalar_text_html()` added a `length(value) == 0` guard that `.scalar_text()` (Markdown) lacks — a latent, unfixed robustness gap in the pre-existing Markdown path that was fixed only in the new file.
- **[P3.14]** [cg-version-control] Commit `391502e` message has a stray trailing quote; `91c4583` has a typo ("mew" → "new"). Cosmetic; no history rewrite needed on a shared branch.
- **[P3.15]** [cg-performance] Minor `c()`-growth-in-loop patterns (`blocks`, `list_items`, `poverty_items` in `.render_named_list_html()`) — negligible at current section/key counts, but inconsistent with the `vapply` idiom used elsewhere in the file.

### ✅ Passed

- **cg-data-quality**: `.html_escape()` coverage confirmed complete for every dynamic-content insertion point in `R/description_renderer_html.R` (section titles, scalar text, table headers/cells, list labels, char-vector items and names) — all 5 required characters (`&`, `<`, `>`, `"`, `'`) covered.
- **cg-reproducibility**: No new package dependencies introduced (Constraint C1 holds); no hardcoded absolute paths; no new non-determinism sources in the renderer itself (pure function of `model`).
- **cg-version-control**: No secrets, credentials, or unexpected large binaries introduced by this plan's 4 commits specifically; working tree clean.

## Autofix Applied (this review)

Applied 3 safe, purely additive/mechanical, behavior-preserving fixes and re-ran the targeted test suite (`devtools::test(filter = "description|api-description")` → **`FAIL 0 | WARN 3 | SKIP 1 | PASS 228`**, no regressions):

1. **P2.4 / P1.4 (partial)** — Added `@note` cross-reference comments to both `render_description_markdown()` (`R/description_renderer.R`) and `render_description_html()` (`R/description_renderer_html.R`) documenting the parallel-structure tradeoff, per the plan's own Risk-table mitigation.
2. **P2.2** — Added the required Step 4/R7 comment near the `/description` handler (`inst/plumber/plumber.R`) documenting the verified `Content-Type` override behavior, and added `format` parameter documentation to the endpoint's Swagger comment block (addresses **P1.3**).
3. **P2.13** — Removed the unnecessary `rep(analysis_var_label, nrow(measures_dt))` in `R/description_builder.R`; `data.table()` already recycles length-1 vectors.
4. **P3.16** *(code-quality, safe_auto)* — Extracted the duplicated `if (identical(format, "html")) render_description_html(model) else render_description_markdown(model)` dispatch (present at both the fast-path and fallback-path call sites) into a single local `render_description()` helper in `inst/plumber/plumber.R`.

All other findings, including the P1.1 crash bug (confirmed via live execution, not merely static analysis) and all `[manual]`-tagged items, are left **open** for `/cg-fix-triage` — none were auto-applied because they touch statistical/data-shape logic, public function contracts, or renderer hot-path behavior that warrants explicit approval.
