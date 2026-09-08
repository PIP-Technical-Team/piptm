---
date: 2026-09-08
title: "HTML Description Renderer + Wording Quality Improvements"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Wording-first (targeted template rewrites), then parallel HTML renderer (inline styles only)"
tags: [description-endpoint, html-renderer, api, table-maker, wording, ux]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# HTML Description Renderer + Wording Quality Improvements

## Context

Follow-up to the `/description` endpoint (implemented per
`.cg-docs/plans/2026-08-26-description-endpoint-implementation.md`). The
current `/description` endpoint returns Markdown, produced by
`build_description_model()` (`R/description_builder.R`) and
`render_description_markdown()` (`R/description_renderer.R`).

The user wants to (1) add an HTML output option for the description, so the
PIP platform UI can consume it more easily and apply custom styling
(bold text, colors), and (2) separately noticed that dynamically interpolated
sentences in the description model sometimes read as poor English, and wants
that fixed as part of the same effort, before the HTML work, so the HTML
output isn't shipped against already-known-awkward text.

## Requirements

- **Purpose**: Easier UI consumption of description content; ability to
  customize appearance (bold, color) beyond what Markdown offers; and
  correct, natural-reading generated sentences regardless of which
  dynamic values are interpolated.
- **Users**: PIP platform UI developers (consumers of the endpoint);
  indirectly, end users of the PIP platform viewing table descriptions.
- **Inputs/Outputs**: Same inputs as today (`description_metadata` fast path,
  or `params` fallback path via `table_maker()`). Output adds an HTML
  rendering alongside the existing Markdown rendering, selected via a
  format parameter on the existing `/description` endpoint (Markdown remains
  the default for backward compatibility).
- **Constraints**:
  - No new package dependencies (ruled out an HTML-templating library like
    `htmltools` in favor of hand-rolled string rendering, matching the
    project's lean-dependency posture).
  - `build_description_model()` and the description model's 9-section
    structure are NOT to be changed in this iteration — only the rendering
    layer (Phase 2) and the sentence templates that produce section content
    (Phase 1).
  - Styling approach: **inline styles only** (e.g.
    `<b style="color:blue">Colombia</b>`) — no CSS classes. Chosen for
    simplicity and because the UI team's HTML-injection model (raw
    injection vs. iframe vs. styled fragment with design tokens) is not
    yet confirmed; inline styles work regardless of injection method.
  - All dynamic content must be HTML-escaped before insertion (values come
    from survey/covariate metadata, not fixed templates, so escaping is
    mandatory even though the source is "internal/trusted" data).
- **Edge Cases**:
  - HTML injection model (raw `v-html`/`dangerouslySetInnerHTML` vs.
    iframe vs. styled fragment with design tokens) is unconfirmed with the
    UI team — flagged as an open dependency; inline-styles choice mitigates
    this by working under any injection model.
  - Some observed "poor English" may originate in formatting helpers (e.g.
    `format_covariate_description()`'s comma-joining logic) rather than the
    surrounding sentence templates — the Phase 1 audit should check both.
- **Scope** (explicitly out of scope for this iteration):
  - No changes to `build_description_model()` / model structure or schema.
  - No general-purpose grammar engine (e.g. singular/plural handling,
    dynamic article agreement) — targeted template rewrites only, for
    commonly observed interpolation cases.
  - No CSS classes / stylesheet hooks for the UI team to theme against —
    can be revisited later if inline-only proves limiting.
  - No decision yet on the UI's HTML injection model — tracked as an open
    dependency, not resolved here.

## Approaches Considered

### Approach 1: Parallel renderer file (mirror existing structure) — CHOSEN for HTML phase
Create `description_renderer_html.R` with `render_description_html()` and
HTML-specific dispatch helpers mirroring `description_renderer.R`'s existing
per-type logic (table / named list / char vector / scalar), emitting HTML
tags with inline styles instead of Markdown syntax.
- **Pros**: Lowest risk — Markdown path untouched; matches existing codebase
  convention (one file per concern); no new dependencies.
- **Cons**: Duplicates traversal/dispatch logic between the two renderers;
  future model shape changes require updating both.
- **Effort**: Small.

### Approach 2: Shared traversal + format-specific formatters
Extract the section-walking/dispatch logic into one internal traversal
function parameterized by a formatter object (table/list/vector/scalar
functions), with Markdown and HTML as two formatter instances.
- **Pros**: Single source of truth for model traversal; less duplication if
  a third format appears later.
- **Cons**: Requires refactoring the existing, tested Markdown renderer;
  more upfront design risk for no immediate payoff.
- **Effort**: Medium.

### Approach 3: HTML-templating library (e.g. `htmltools`)
Build HTML nodes via a templating package's tag-builder API instead of
manual string concatenation.
- **Pros**: Library-handled escaping and tag correctness; more idiomatic
  for richer future HTML.
- **Cons**: New dependency not justified by the narrow escaping need here;
  conflicts with the project's lean-dependency posture.
- **Effort**: Small–Medium.

## Decision

**Wording first, then HTML renderer, in one combined plan, two sequential
phases:**

1. **Phase 1 — Wording quality** (`R/description_builder.R`): Audit the
   sentence templates across the 9 description sections against real
   `table_maker()` output examples; apply targeted rewrites so interpolated
   sentences read naturally for common cases. Check whether some awkwardness
   stems from formatting helpers (e.g. `format_covariate_description()`)
   rather than the sentence templates themselves. Update any tests that
   assert on exact string content; model *shape* assertions should be
   unaffected since no structural changes are made.

2. **Phase 2 — HTML renderer** (Approach 1 — parallel renderer file): Add
   `description_renderer_html.R` mirroring the Markdown renderer's dispatch
   structure, using inline styles only and mandatory HTML-escaping of all
   dynamic content. Expose via a format parameter on the existing
   `/description` endpoint (Markdown remains the default).

Chosen over Approaches 2 and 3 because: no new dependencies are introduced,
the well-tested Markdown rendering path stays untouched, and it matches the
project's preference for the simplest viable option at this stage. The
sequential wording-first ordering ensures the HTML output isn't built and
reviewed against text already known to be awkward — both renderers consume
the same model content, so wording fixes benefit Markdown output too.

**Flagged risk**: if Phase 1's wording review extends significantly (e.g.
non-technical stakeholder sign-off on tone), consider allowing Phase 2 to
start in parallel against current text and re-running the HTML renderer
against Phase 1's finalized wording before merge, rather than being strictly
blocked end-to-end.

## Next Steps

For handoff to `/cg-plan`:
- Collect 2-3 concrete "poor English" examples from real `table_maker()`
  outputs to ground the Phase 1 template audit.
- Confirm with the UI team, in parallel (non-blocking), their HTML injection
  model (raw injection / iframe / styled fragment) — informs whether inline
  styles alone remain sufficient going forward, though not required to start
  implementation since inline styles work under any injection model.
- Plan test updates: existing `test-description-model-interactive.R` and
  `test-api-description.R` assertions on exact string content will need
  updates after Phase 1; new tests needed for the HTML renderer (Phase 2)
  and for the new format parameter on `/description`, including escaping
  behavior for dynamic content.
</content>
