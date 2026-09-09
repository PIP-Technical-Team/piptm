---
date: 2026-09-08
title: "HTML Description Renderer + Wording Quality Improvements"
status: completed
completed-date: 2026-09-09
failing-steps: []
scope: "Standard"
brainstorm: ".cg-docs/brainstorms/2026-09-08-description-html-renderer-and-wording.md"
language: "R"
estimated-effort: "medium"
deviation-policy: "ask"
execution-report: ".cg-docs/work-reports/2026-09-09-description-html-renderer-and-wording.md"
artifact-schema-version: 1
tags: [description-endpoint, html-renderer, api, table-maker, wording, ux]
---

# Plan: HTML Description Renderer + Wording Quality Improvements

## Objective

Add an HTML rendering option to the `/description` endpoint (inline styles
only, all dynamic content escaped), selectable via a `format` field in the
JSON request body that defaults to the current Markdown behavior; and fix
awkward auto-generated sentence wording in the description model's
templates before building the HTML renderer, so neither output format
carries forward known poor phrasing.

## Context

The `/description` endpoint (`inst/plumber/plumber.R`) currently returns
only Markdown, produced by `build_description_model()`
(`R/description_builder.R`) and `render_description_markdown()`
(`R/description_renderer.R`). The PIP platform UI team would benefit from an
HTML alternative for easier DOM consumption and custom styling (bold text,
colors). Separately, some auto-generated sentences (assembled via `sprintf()`
template strings with interpolated values) read as poor English in certain
cases. Full rationale, alternatives considered, and decision log are in the
linked brainstorm.

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | Collect 2-3 real `table_maker()` examples of awkward generated sentences to ground the wording audit | Brainstorm Next Steps |
| R2 | Audit and rewrite sentence templates in `description_builder.R` (and formatting helpers) so interpolated sentences read naturally for common cases; no grammar-engine, no structural/schema changes | Brainstorm Decision |
| R3 | Update existing tests whose assertions depend on exact wording strings | Brainstorm Next Steps |
| R4 | Create `R/description_renderer_html.R` with `render_description_html()` mirroring `render_description_markdown()`'s dispatch structure (table / named list / char vector / scalar), emitting HTML with inline styles only | Brainstorm Decision (Approach 1) |
| R5 | HTML-escape all dynamic content before insertion (labels, notes, survey names, filter values, etc.) | Brainstorm Constraints |
| R6 | Add a `format` field to `POST /description`'s JSON body (`markdown` default, `html` new option; body-only, no query-string source); set `Content-Type` accordingly; no new endpoint, no breaking change to current callers | Brainstorm Decision; Plan Review P1.1 |
| R7 | Verify that `res$setHeader("Content-Type", ...)` set inside the `/description` handler correctly overrides the `@serializer text` default in the installed plumber version, before implementing the format branching | Plan Review P2.1 |
| R8 | Export `render_description_html()` and update `NAMESPACE`/roxygen docs | Codebase convention (existing exports for `build_description_model`, `render_description_markdown`) |
| R9 | Add test coverage for the HTML renderer (all section/content types, escaping) and for the new `format` parameter on `/description` | Brainstorm Next Steps |
| R10 | Create a static HTML mock demonstrating the final inline-style visual design, using real two-covariate/two-measure example content, and get plan-owner approval before implementing the renderer | Plan Revision (user request) |

## Implementation Steps

### 1. Collect wording audit examples
- **Requirements**: R1
- **Files**: none (research step; notes captured inline in this plan's execution, not a new file)
- **Details**: Run or inspect existing fixtures/tests (`test-description-model-interactive.R`, `test-api-description.R`) and, if feasible, a real `table_maker()` call, to gather 2-3 concrete examples of awkward sentences (e.g. singular/plural mismatches in `.build_overview_content()`'s `structure_desc`, comma-joining edge cases in `format_covariate_description()`, filter/measure sentence edge cases in `build_cell_definition()`). Document the specific problem in each (e.g. "reads awkwardly when `n_measures == 1` and `by` has 3+ values").
- **Test Scenarios**: n/a (research/documentation step)
- **Tests**: n/a
- **Acceptance criteria**: At least 2 concrete before/after examples documented to guide Step 2's edits.

### 2. Wording audit and template rewrites
- **Requirements**: R2
- **Files**: `R/description_builder.R`
- **Details**: Using the examples from Step 1, rewrite the affected `sprintf()`/`paste0()` template strings in `.build_overview_content()`, `.build_filters_content()`, `.build_execution_content()`, `build_cell_definition()`, and `format_covariate_description()`/`format_welfare_type()`/`format_slot_label()` as needed. Fix at the root cause: if a formatting helper (e.g. `format_covariate_description()`) is the source of awkwardness (not the surrounding template), fix the helper once rather than patching every call site. Do not change the model's section structure, field names, or `visible`/`title`/`content` shape — only the text content values.
- **Test Scenarios**: happy path (single measure/survey/covariate), edge case (plural counts, multiple covariates, `pov_status` special case, poverty-line-applicable vs not), error path (n/a — no new error conditions introduced)
- **Tests**: `tests/testthat/test-description-model-interactive.R` (manual/interactive verification), any unit tests in `tests/testthat/` covering `description_builder.R` content functions
- **Acceptance criteria**: The Step 1 examples now read naturally; `devtools::document()` not required (no roxygen changes expected here) but re-run relevant tests to confirm no unintended structural breakage.

### 3. Update tests affected by wording changes
- **Requirements**: R3
- **Files**: `tests/testthat/test-api-description.R`, `tests/testthat/test-description-model-interactive.R`, `tests/testthat/test-description-helpers.R`, any other test file asserting exact string content from `description_builder.R` output
- **Details**: Search test files for `expect_true(grepl(...))`, `expect_identical(...)`, `expect_equal(...)`, or `expect_match(...)` assertions against sentence text produced by Step 2's changed functions (e.g. the existing `expect_true(grepl("The Mean of Welfare", body, fixed = TRUE))` in `test-api-description.R`, and helper-level assertions like `expect_equal(format_welfare_type("INC"), "Income")` in `test-description-helpers.R`). Before finalizing Step 2, run a repo-wide search (e.g. `rg "expect_equal|expect_match|grepl" tests/testthat | xargs rg "<changed helper name>"`) for every helper touched, to catch assertion sites not already listed here. Update expected strings to match the new wording. Do not touch assertions about model *shape* (section names, `visible`, `title` fields) — those should be unaffected.
- **Test Scenarios**: happy path (updated string matches), regression (old string assertions would now correctly fail, confirming the audit trail)
- **Tests**: `devtools::test(filter = "description")`
- **Acceptance criteria**: All description-related tests pass with updated wording expectations, including `test-description-helpers.R`; no shape/structural assertions were touched.

### 4. Verify Content-Type override behavior under `@serializer text`
- **Requirements**: R7
- **Files**: none (spike/verification step; findings documented inline for Step 7)
- **Details**: Before implementing the format branching in the endpoint, run a small isolated spike: add a temporary plumber route (or extend a throwaway test) that calls `res$setHeader("Content-Type", "text/html; charset=utf-8")` inside a handler decorated with `@serializer text`, and confirm via the response headers that the manual override wins over the serializer's default `text/plain`. Check this against the plumber version currently installed/declared in `DESCRIPTION`. Document the verified behavior (and any plumber-version caveat) as a comment near the `/description` handler in Step 7.
- **Test Scenarios**: happy path (header override confirmed to survive serialization), failure path (if the override does NOT survive, document the actual behavior and adjust Step 7's approach — e.g. switching the endpoint's serializer, or returning a wrapped envelope with an explicit content type — before proceeding)
- **Tests**: ad hoc spike (not part of the permanent test suite); outcome recorded as a code comment in Step 7
- **Acceptance criteria**: Verified, documented answer to "does `res$setHeader('Content-Type', ...)` override `@serializer text`'s default in this plumber version?" before Step 7 is implemented.

### 5. Create a static HTML mock of the description output
- **Requirements**: R10
- **Files**: `.cg-docs/plans/assets/2026-09-08-description-html-mock.html` (new; design artifact, not part of the package build — not under `R/`, `inst/`, or `tests/`)
- **Details**: Using the actual Markdown output from a two-covariate, two-measure `table_maker()` example (e.g. adapt `.make_desc_metadata()` from `test-api-description.R`, extended to two covariates and two measures, and run it through the existing `build_description_model()` + `render_description_markdown()` to get real section content) as the source content, hand-write a single static HTML file with inline styles applied directly to every element type that will appear in the renderer: section wrapper, `<h2>` heading, key-value `<li>` items, bold block labels, `<table>`, `<th>`, `<td>`, `<ul>`, `<ol>`, and nested/indented lists. The file is a design artifact only — it does not need to compile, be sourced by any R code, or ship with the package.
- **Test Scenarios**: n/a (design/visual artifact, not code)
- **Tests**: n/a
- **Acceptance criteria**: The mock file opens correctly in a browser, and its visual output (colors, weights, spacing, nesting) is explicitly approved by **the plan owner** (not the UI team — their HTML injection model remains unconfirmed and is not a blocker for this approval) before Step 6 begins. When Step 6 (the renderer) is implemented, its output must visually match this mock; the inline style strings shown in the mock are copied directly into the renderer's tag-emitting call sites (see Constraint C8 — no additional abstraction).

### 6. Build the HTML renderer
- **Requirements**: R4, R5, R10
- **Files**: `R/description_renderer_html.R` (new)
- **Details**: Mirror `R/description_renderer.R`'s structure exactly, renaming functions with an `_html` suffix or equivalent internal naming (e.g. `.render_section_content_html()`, `.render_table_html()`, `.render_named_list_html()`, `.render_char_vector_html()`, `.scalar_text_html()` reusing `.scalar_text()` where formatting logic is identical). Style strings for each element type must match the mock approved in Step 5, copied directly into the tag-emitting call sites (no style dictionary, no templating layer — see Constraint C8). Emit:
  - Sections as `<section>` or `<div>` blocks with a `<h2 style="...">` heading per visible section (mirroring the `## title` Markdown heading).
  - Data.frame/data.table content as an HTML `<table style="...">` with `<thead>`/`<tbody>`, escaping every cell value.
  - Named lists as `<ul>`/`<li>` bullets (mirroring `- key: value`), with a lead `description` value rendered as a `<p>`. Nested lists/tables recurse exactly as in the Markdown renderer.
  - Character vectors as an `<ol>` numbered list, with named entries rendering the name as `<b style="...">label:</b>` prefix (mirroring `**label:**`).
  - All values inserted via a small internal `.html_escape()` helper (escape `&`, `<`, `>`, `"`, and `'` → `&#39;`) applied to every dynamic string before insertion into any tag content or attribute.
  - Return a single character scalar of HTML, consistent with `render_description_markdown()`'s return contract (empty string when there are no visible sections with content).
- **Test Scenarios**: happy path (typical 9-section model renders valid HTML with expected tags), edge case (content with `&`, `<`, `>`, `"`, or `'` characters is escaped; empty/NULL sections are skipped exactly as in the Markdown renderer; nested lists/tables render correctly), error path (non-list model input triggers the same `stopifnot(is.list(model))` guard as the Markdown renderer)
- **Tests**: new `tests/testthat/test-description-renderer-html.R`
- **Acceptance criteria**: For the same model input, `render_description_html()` produces matching section titles and matching leaf-level content values as `render_description_markdown()` (ignoring markup/escaping differences between the two formats — see Out of Scope note on the pre-existing Markdown `|`-escaping gap); output visually matches the Step 5 approved mock; all dynamic content is escaped (including `'`); function is exported.

### 7. Add `format` field to `/description` endpoint's JSON body
- **Requirements**: R6
- **Files**: `inst/plumber/plumber.R`, `inst/plumber/helpers.R`
- **Details**: In the `POST /description` handler, read `format` exclusively from the parsed JSON `body$format` (no query-string source — locked in per Plan Review P1.1, since the endpoint's `@parser json` + `body`-argument signature does not reliably expose query params as bound function args across clients). Default to `"markdown"` when `format` is absent or `NULL`; reject any non-`"markdown"`/`"html"` value explicitly (see error path below) rather than silently falling back. Branch to call `piptm::render_description_markdown(model)` or `piptm::render_description_html(model)` accordingly. When `format == "html"`, set the response `Content-Type` header per the verified behavior from Step 4 (e.g. `res$setHeader("Content-Type", "text/html; charset=utf-8")` if confirmed to override `@serializer text`, or the alternative approach documented in Step 4 if not); leave `Content-Type` as-is (`text/plain`, per the current `@serializer text` default) for `format == "markdown"` to preserve existing behavior. Reject unrecognized `format` values (anything other than `"markdown"`/`"html"`/absent) with the existing 400 error pattern (via `error_json()`), matching the endpoint's existing validation style in `validate_description_input()`.
- **Test Scenarios**: happy path (`format: "markdown"` and omitted `format` both return current Markdown behavior with unchanged `Content-Type`), edge case (`format: "html"` returns HTML with `text/html` content type, applies to both the metadata fast path and the params fallback path), error path (unrecognized `format` value, e.g. `"pdf"`, returns 400 with a clear error message)
- **Tests**: `tests/testthat/test-api-description.R` (extend)
- **Acceptance criteria**: `POST /description` with `{"format": "html", ...}` in the JSON body returns HTML for both fast and fallback paths; default/`format: "markdown"`/omitted-`format` behavior is byte-for-byte unchanged from current output for the same input.

### 8. Extend API tests for HTML format
- **Requirements**: R9
- **Files**: `tests/testthat/test-api-description.R`
- **Details**: Add test cases mirroring the existing Block 1 fast-path test but with `format: "html"` in the request body, asserting on HTML markers (e.g. `<h2`, `<table`) instead of `## Table Overview`. Add a test confirming an unrecognized `format` value returns 400. Add a test confirming default/omitted `format` is unaffected (reuse or extend the existing Block 1 test).
- **Test Scenarios**: happy path (HTML fast path), edge case (fallback path with `format: "html"`, if Arrow fixtures are available — otherwise `skip()` consistent with the existing fallback test's pattern), error path (unrecognized `format` value)
- **Tests**: `devtools::test(filter = "api-description")`
- **Acceptance criteria**: New tests pass; existing tests in this file continue to pass unmodified except where Step 3 updated wording expectations.

### 9. Documentation and exports
- **Requirements**: R8
- **Files**: `R/description_renderer_html.R` (roxygen), `NAMESPACE`, `man/` (generated)
- **Details**: Add complete roxygen documentation to `render_description_html()` (and any exported helpers) following the existing pattern in `description_renderer.R` (`@param`, `@return`, `@examples`, `@export`/`@keywords internal` as appropriate). Run `devtools::document()` to regenerate `NAMESPACE` and `man/` pages.
- **Test Scenarios**: n/a (documentation step)
- **Tests**: n/a
- **Acceptance criteria**: `devtools::document()` runs cleanly; `NAMESPACE` includes `export(render_description_html)`; no roxygen warnings.

### 10. Full regression pass
- **Requirements**: R2, R3, R4, R5, R6, R7, R8, R9, R10
- **Files**: none (verification step)
- **Details**: Run the full test suite and, if available, `devtools::check()` to confirm no regressions across the package from the wording and rendering changes.
- **Test Scenarios**: full suite (happy/edge/error paths already covered by Steps 2-3, 6-8's individual tests)
- **Tests**: `devtools::test()`; `devtools::check()` (optional, per V10)
- **Acceptance criteria**: `devtools::test()` reports zero failures; any new `devtools::check()` NOTES/WARNINGs are reviewed and either fixed or explicitly accepted as pre-existing.

## Testing Strategy

- Reuse the existing `plumber::plumb()`-based router-testing pattern from
  `test-api-description.R` (`.desc_router$call(make_api_req(...))`) for all
  new endpoint-level tests — no network socket, consistent with current
  conventions.
- Unit-test `render_description_html()` directly against synthetic model
  fixtures (can adapt `.make_desc_metadata()` + `build_description_model()`
  from the existing test file) rather than only through the API layer, to
  isolate renderer bugs from endpoint plumbing.
- Wording changes are verified both by updated `grepl()`/`expect_match()`
  assertions (Step 3) and, where feasible, the interactive script
  (`test-description-model-interactive.R`) for manual eyeballing.
- Escaping behavior gets dedicated test cases with deliberately
  crafted input containing `&`, `<`, `>`, `"`, and `'` in a label/note field.
- The `Content-Type` header-override spike (Step 4) is run once, ad hoc,
  before the endpoint branching (Step 7) is written, to avoid discovering
  a plumber-version incompatibility mid-implementation.
- The Step 5 mock is a visual/design check, not an automated test; its
  approval is a manual gate before Step 6 starts, verified by eye, not by
  `testthat`.

## Documentation Checklist

- [ ] `render_description_html()` has complete roxygen (`@param`, `@return`,
      `@examples`, `@export`)
- [ ] `NAMESPACE` regenerated via `devtools::document()`
- [ ] `inst/plumber/plumber.R`'s `/description` endpoint comment block
      updated to document the new `format` parameter and its accepted values
- [ ] `R/api.R`'s `run_api()` roxygen endpoint table (or equivalent
      user-facing endpoint docs) updated if it enumerates per-parameter
      details for `/description`

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Wording rewrites break existing test assertions in unexpected/hidden ways (tests elsewhere in the suite may also grep for old sentence text) | Run full `devtools::test()` after Step 2, not just the description-specific files, to catch any missed assertions before proceeding to Step 6; Step 3 now explicitly includes `test-description-helpers.R` in its scan |
| Plumber's fixed `@serializer text` may not allow per-request `Content-Type` switching as expected | Dedicated Step 4 spike verifies `res$setHeader()` behavior against the installed plumber version *before* Step 7 writes the branching logic; if unreliable, Step 4 documents a fallback approach instead of discovering it mid-implementation |
| HTML escaping is incomplete or inconsistent across the renderer's recursive dispatch paths (nested lists/tables), allowing a stray unescaped value to break markup, including in attribute contexts (e.g. a missed `'` inside a quoted attribute) | Centralize escaping in a single `.html_escape()` helper (covering `&`, `<`, `>`, `"`, and `'`) called at every leaf-value insertion point (never ad hoc per call site); dedicated escaping test cases in Step 6/8 with special characters in nested structures and in attribute-adjacent content |
| Duplication between `description_renderer.R` and the new `description_renderer_html.R` causes future model changes to update only one renderer, silently desyncing Markdown and HTML output | Documented as an accepted trade-off in the brainstorm (Approach 1 chosen over the shared-traversal Approach 2); add a code comment cross-referencing both files so future editors are aware of the parallel structure |
| UI team's HTML injection model (raw injection, iframe, or styled fragment) remains unconfirmed, so inline-only styling could still turn out insufficient later | Out of this plan's control; flagged as an external dependency in the brainstorm, not a blocker for implementation since inline styles work under any injection model |
| The Step 5 static mock could visually diverge from what Step 6's renderer actually produces if style strings are copied inattentively, or the mock could become a forgotten one-off with no lasting reference | Step 6's acceptance criteria explicitly require visual matching against the Step 5 mock; the mock file is kept alongside the plan (`.cg-docs/plans/assets/`) as a lasting reference artifact, not deleted after approval |

## Out of Scope

- Changes to `build_description_model()`'s section structure, field names,
  or the 9-section schema.
- A general-purpose grammar engine (plural/singular handling beyond
  existing `if (n == 1) "x" else "y"` patterns, dynamic article agreement).
- CSS classes, design tokens, or stylesheet hooks for the HTML output
  (inline styles only, per brainstorm decision).
- Resolving or depending on the UI team's confirmation of their HTML
  injection model.
- A JSON output format for `/description` (mentioned as a future
  possibility in `description_builder.R`'s roxygen, not part of this plan).
- Performance optimization of the renderers (no indication either renderer
  is a bottleneck).
- Fixing the pre-existing Markdown-side escaping gap: `.render_table()` in
  `description_renderer.R` inserts cell values via `as.character()` with no
  escaping of `|` or newlines, so any cell value containing a pipe character
  already mis-renders in the current Markdown output. This is a known,
  separate issue (Plan Review P2.3), tracked here but not fixed by this
  plan; it does not block the HTML renderer's equivalence acceptance
  criterion (Step 6), which is scoped to leaf-value content, not markup.

## Completion Contract

### Outcome
The `/description` endpoint supports an HTML output format (inline styles
only, all dynamic content escaped, matching a plan-owner-approved static
mock) selectable via a `format` field in the JSON request body, defaulting
to the current Markdown behavior; and the description model's sentence
templates in `description_builder.R` read naturally across common
interpolation cases, verified against real `table_maker()` examples.

### Verification Surface
| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | Wording audit examples collected and template fixes applied | This plan's Step 1/2 notes + diff of `description_builder.R` | yes |
| V2 | Existing description model tests pass after wording changes, including `test-description-helpers.R` | `devtools::test(filter = "description")` | yes |
| V3 | `Content-Type` header-override behavior verified against installed plumber version before endpoint branching is implemented | Step 4 spike notes / code comment near `/description` handler | yes |
| V4 | Static HTML mock created and approved by the plan owner before renderer implementation begins | `.cg-docs/plans/assets/2026-09-08-description-html-mock.html` + plan owner's explicit approval recorded in session notes | yes |
| V5 | New HTML renderer produces valid HTML for all 9 section types, with correct structure, matching leaf-level values, and escaping covering `&`, `<`, `>`, `"`, and `'` | `testthat::test_file("tests/testthat/test-description-renderer-html.R")` | yes |
| V6 | Renderer's visual output (colors, weights, spacing, nesting) matches the Step 5 approved mock | Manual visual comparison of renderer output against `.cg-docs/plans/assets/2026-09-08-description-html-mock.html`, confirmed by the plan owner — not verifiable by `testthat` | yes |
| V7 | `POST /description` with `format: "html"` in the JSON body returns HTML with correct `Content-Type`; `format: "markdown"`/omitted preserves current behavior; unrecognized `format` returns 400 | `testthat::test_file("tests/testthat/test-api-description.R")` | yes |
| V8 | Full test suite still passes (no regressions) | `devtools::test()` | yes |
| V9 | `devtools::document()` run, `NAMESPACE`/Rd updated for new exported function | `git diff NAMESPACE man/` | yes |
| V10 | `devtools::check()` clean (or no new NOTES/WARNINGs introduced) | `devtools::check()` | no |

### Constraints
| ID | Constraint | Check |
|----|------------|-------|
| C1 | No new package dependencies introduced | `git diff DESCRIPTION` shows no new `Imports`/`Suggests` |
| C2 | `build_description_model()` structure/schema unchanged (9 sections, same field names) | Diff review of `description_builder.R`; existing shape-assertion tests pass |
| C3 | Markdown rendering path (`description_renderer.R`, `render_description_markdown()`) untouched except where wording-template changes flow through shared content | Diff review confirms no logic changes to `.render_*` dispatch functions |
| C4 | All dynamic content in HTML output is escaped (`&`, `<`, `>`, `"`, `'`) before insertion | Dedicated escaping unit tests in V5 |
| C5 | Styling is inline-only — no CSS classes added to HTML output | Code review of `description_renderer_html.R` |
| C6 | Default `/description` behavior (no `format` field, or `format: "markdown"`) remains Markdown, for backward compatibility | V7 test case with omitted `format` field |
| C7 | `format` is read exclusively from the JSON request body (`body$format`); no query-string source | Code review of Step 7's handler; V7 tests exercise body-only delivery |
| C8 | Style strings in `description_renderer_html.R` are written directly into tag-emitting call sites; no style dictionary, templating layer, or additional abstraction is introduced | Code review of `description_renderer_html.R`; visually confirmed via V6 |

### Boundaries
- **Allowed**: Editing `R/description_builder.R` (sentence templates only,
  no structural changes), creating `R/description_renderer_html.R`, creating
  the static mock at `.cg-docs/plans/assets/2026-09-08-description-html-mock.html`,
  editing `inst/plumber/plumber.R` and `inst/plumber/helpers.R` for the
  format parameter, adding/updating tests, updating `NAMESPACE`/roxygen docs.
- **Out of scope**: Changes to `build_description_model()`'s section
  structure or field names; a general-purpose grammar engine (plural/article
  agreement); CSS classes or stylesheet hooks; a style dictionary,
  templating layer, or other styling abstraction beyond copied inline
  strings (C8); resolving the UI team's HTML injection model (tracked as an
  external dependency, not blocking); JSON output format.

### Iteration Policy
1. Complete Phase 1 (wording: Steps 1-3) fully — including test updates for
   changed string assertions across all affected test files, including
   `test-description-helpers.R` — before starting Phase 2 (HTML renderer:
   Steps 4-10).
2. If a wording fix reveals that awkwardness stems from a formatting helper
   (e.g. `format_covariate_description()`) rather than the sentence
   template, fix the helper directly rather than working around it in
   multiple templates.
3. If Phase 1 stakeholder/tone review is not readily resolvable within this
   session, proceed to Phase 2 against current wording and flag Phase 1 as
   needing a follow-up pass, per the brainstorm's noted risk — do not block
   indefinitely.
4. Run the Step 4 `Content-Type` spike before writing Step 7's branching
   logic; do not assume `res$setHeader()` overrides `@serializer text`
   without verifying it first.
5. Do not begin Step 6 (the renderer) until the Step 5 mock has been
   explicitly approved by the plan owner. This approval gate belongs to the
   plan owner, not the UI platform team — their HTML injection model is a
   separate, non-blocking open question tracked in the brainstorm.
6. Prefer extending existing test files (`test-description-model-interactive.R`,
   `test-api-description.R`, `test-description-helpers.R`) over creating
   parallel ones, except for the new HTML-renderer-specific test file.

### Blocked-Stop Conditions
- Any required change to `build_description_model()`'s section structure or
  field names (would violate C2/out-of-scope).
- A new package dependency appears necessary to implement escaping or HTML
  generation (would violate C1) — stop and flag for a scope/plan revision
  instead of adding it silently.
- Existing Markdown-path tests fail after wording changes in a way not
  resolvable by updating the expected string (may indicate an unintended
  structural change) — stop and report.
- Step 6 (renderer implementation) is started before the Step 5 mock has
  been approved by the plan owner — stop and obtain approval first.
</content>
