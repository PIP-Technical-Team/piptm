---
date: 2026-11-17
title: "Table Maker Quarto Presentation for Wider PIP Team"
status: decided
scope: "Lightweight"
artifact-schema-version: 1
chosen-approach: "Single revealjs deck with custom WB SCSS theme + live registry-sourced data"
tags: [presentation, quarto, table-maker, communication, revealjs, registry]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Table Maker Quarto Presentation for Wider PIP Team

## Context

The user wants a Quarto presentation introducing Table Maker to the wider PIP
team. The goal is to communicate (a) the 3-step UX of the Table Maker UI, and
(b) the breadth/value of its inputs (surveys, variables, roles) and features
(fast computation, partitioned microdata, harmonized variables), in a way that
makes the tool's value clearly visible to a largely non-technical audience.
The user explicitly wants to rely on the QA UI build for screenshots and on
`dev/registry_overview.R` / the variable registry for input numbers, and wants
"few beautiful slides" rather than an exhaustive deck.

No existing brainstorm, plan, or presentation artifact covers this topic.
No relevant Project Brain entries were found (brain topics are scoped to
`table_maker()` computation internals, the service/API layer, and
`description_builder.R` — not communication/presentation content).

## Requirements

- **Audience**: Wider PIP team, mostly non-technical (country economists,
  data users) — emphasize user-facing value over implementation internals.
- **UX demonstration**: Show the three Table Maker UI steps (survey
  selection grid, measure/slice decisions, results) via annotated
  screenshots from the QA build. The user will supply the screenshots
  (image placeholders go into the deck skeleton; brainstorming/skill
  permissions do not allow this agent to capture them).
- **Inputs slide(s)**: Pull real current numbers from the registry (via
  logic similar to `dev/registry_overview.R`) — e.g. survey/country/year
  coverage — but go beyond raw counts: explicitly cover *what analysis
  variables are available* and *what statistics can be computed on them*
  (continuous vs binary vs poverty-status gating, as documented in
  `docs/ui-description.md`).
- **Features slide(s)**: Convey fast computation, partitioned microdata,
  harmonized variables (e.g. age, gender, area) as concrete value — what
  Table Maker lets a user compute, not just internal architecture.
- **Full feature coverage (analysis side + metadata side)**: The deck must
  clearly communicate the *complete* feature set of Table Maker, not only
  the headline items. This spans both:
  - **Analysis side**: analysis variables and the statistics computable on
    them (continuous vs binary vs poverty-status gating per
    `docs/ui-description.md`), sample-base filtering, the four layout
    slots (columns/rows/super columns/super rows), multi-survey comparison,
    poverty line handling, export actions (CSV/XLSX/copy).
  - **Metadata side**: survey/country/year/welfare-type coverage, variable
    registry roles and availability filtering, harmonized variables across
    surveys, release/manifest-based reproducibility, partitioned
    Arrow/Parquet microdata access.
  No feature category should be silently omitted from the narrative — each
  should get at least a mention or grouped callout, even in a concise deck.
- **Optional backup slide — technical roadmap**: Include one short,
  clearly-marked **optional/backup slide** (e.g. an appendix slide, to be
  skipped or shown at the presenter's discretion) giving a broad, high-level
  technical pipeline view for anyone in the room who wants it: inputs →
  computations → API (i.e. harmonized survey data / Arrow partitions →
  `table_maker()` computation engine → Plumber API service layer → PIP
  platform UI). This stays high-level (architecture-notes level, not
  code-level) and is separate from the main non-technical narrative.
- **Format**: Quarto `revealjs` (HTML), ~10 core slides total, plus the one
  optional/backup technical-roadmap slide noted above (kept outside the
  core count since it may not be shown).
- **Style**: World Bank / PIP visual branding (custom SCSS theme); no
  existing branding/theme assets found in the repo, so the theme will be
  built from scratch using WB color conventions.
- **Reproducibility**: Registry-derived numbers should be sourced live via
  executed R code at render time (not hardcoded), consistent with the
  project's reproducibility stance, but guarded so a missing/misconfigured
  `PIPTM_REGISTRY_DIR`/`PIPTM_MANIFEST_DIR` at render time produces a clear
  fallback/error rather than a hard crash.

## Approaches Considered

### Approach 1: Single revealjs deck with custom WB SCSS theme + registry data pulled at render time (chosen)
One `.qmd` revealjs presentation with a custom `theme.scss` (WB blue/gold
palette), ~10 slides, R code chunks that query the variable registry inline
(cached, guarded against missing env vars) to embed live counts and feature
callouts, with screenshot placeholders for the 3 UX slides.
- Pros: Fully reproducible (numbers stay current on re-render); one
  self-contained artifact; delivers real theming control for "beautiful."
- Cons: Slightly more setup (first SCSS/revealjs config in this repo);
  needs defensive guards around registry access.

### Approach 2: Static content deck (hardcoded one-time registry snapshot)
Same slide structure and theme, but numbers/feature lists are written as
plain text captured once manually, with no R chunks executed at render.
- Pros: Fastest to build; no runtime dependency on registry env vars.
- Cons: Numbers go stale silently on future re-renders/reuse; contradicts
  the user's stated intent to "rely on the registry overview."

### Approach 3 (not chosen, flagged as follow-up): Extract a reusable `registry_summary()` helper from `dev/registry_overview.R`
Would give the deck a clean, testable, reusable data source instead of
inline logic duplicated from `dev/registry_overview.R`.
- Pros: Cleaner separation; reusable for future decks/dashboards.
- Cons: Expands scope beyond a Lightweight presentation task into
  package/dev-tooling refactor; out of scope for this brainstorm and for
  this skill's file permissions (cannot modify `dev/` or `R/`).

## Decision

Approach 1 chosen: build a single `revealjs` Quarto deck (~10 core slides)
with a custom WB-branded SCSS theme. Content sourced live from the variable
registry (guarded against render-time failures), structured around:
(1) the problem/value of Table Maker, (2) the 3-step UX walkthrough with
user-supplied screenshots, (3) what inputs are available (surveys,
variables, roles — not just counts but computable statistics per variable
type), (4) core architectural features framed as user value (speed,
partitioning, harmonization), (5) closing/call-to-action slide — plus one
optional/backup slide covering the broad technical roadmap (inputs →
computations → API) for presenter discretion.

The core narrative must give complete coverage of Table Maker's feature
set across both the analysis side (variables, statistics, sample-base
filtering, layout slots, multi-survey comparison, exports) and the
metadata side (survey/registry coverage, harmonized variables,
manifest-based reproducibility, partitioned microdata), so no major
capability is left uncommunicated even within a concise deck.

Devil's advocate pass (Lightweight scope — effort-value + charter
alignment only): effort is proportional to value (a few hours for a
clear, reusable team-facing artifact); no conflict with
`compound-gpid.md` constraints, since reproducibility/determinism apply
to the computation engine and this deck's live registry sourcing actually
reinforces that principle rather than conflicting with it. One residual
risk flagged and accepted: render-time dependency on registry env vars,
to be mitigated with a defensive fallback rather than treated as a
blocker.

## Next Steps

For handoff to `/cg-plan`:

- Define the exact `.qmd` file location (e.g. `docs/presentations/` or
  similar — to be confirmed in planning, since this brainstorm has no
  file-write permission outside `.cg-docs/brainstorms/`).
- Draft the ~10-core-slide outline/content per section above, cross-checked
  against `docs/ui-description.md` and the computation-engine features in
  `compound-gpid.md` to ensure every analysis-side and metadata-side
  feature category gets at least a callout (use a checklist during drafting
  so nothing is silently dropped).
- Draft the one optional/backup technical-roadmap slide (inputs →
  computations → API), clearly marked as skippable/appendix in the deck.
- Build the custom WB SCSS theme (colors/fonts).
- Implement guarded R logic to pull registry-derived numbers and feature
  descriptions (what analysis variables + statistics are available per
  variable type), reusing the approach in `dev/registry_overview.R`
  without modifying it.
- Add labeled image placeholders for the 3 UX screenshots (Step 1 survey
  grid, Step 2 measure/slice decisions, Step 3 results), to be filled in
  by the user from the QA build.
- Render and visually review the deck for polish before sharing with the
  wider PIP team.
- Optional/separate follow-up (not in this plan's scope): consider
  extracting a reusable `registry_summary()` helper from
  `dev/registry_overview.R` if more decks/dashboards will need the same
  data later (Approach 3, flagged but deferred).
