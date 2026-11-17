---
date: 2026-11-17
title: "Table Maker Quarto Presentation for Wider PIP Team"
status: completed
completed-date: 2026-11-17
scope: "Lightweight"
brainstorm: ".cg-docs/brainstorms/2026-11-17-table-maker-quarto-presentation.md"
language: "R"
estimated-effort: "small"
deviation-policy: "ask"
artifact-schema-version: 1
tags: [presentation, quarto, table-maker, communication, revealjs, registry]
phases: 2  # convenience hint -- may be stale; always recount from ## Phase headers
execution-report: ".cg-docs/work-reports/2026-11-17-table-maker-quarto-presentation.md"
completed-phases: [1, 2]
---

# Plan: Table Maker Quarto Presentation for Wider PIP Team

## Objective

Build a single, beautiful, WB-branded Quarto `revealjs` presentation that
introduces Table Maker to the wider PIP team. The deck must clearly
communicate (a) the 3-step UX of the Table Maker UI via annotated
screenshots, (b) the complete feature set on both the analysis side
(variables, statistics, sample-base filtering, layout slots, multi-survey
comparison, exports) and the metadata side (survey/registry coverage,
harmonized variables, manifest reproducibility, partitioned microdata), and
(c) an optional high-level technical-roadmap backup slide (inputs →
computations → API) for presenter discretion — all in ~10 core slides,
targeted at a mostly non-technical audience.

## Context

This plan implements the decision captured in
`.cg-docs/brainstorms/2026-11-17-table-maker-quarto-presentation.md`
(scope: Lightweight, chosen approach: single revealjs deck with custom WB
SCSS theme + live registry-sourced data).

The package already exposes the exact functions needed to source registry
content without duplicating internal logic, as demonstrated by
`dev/registry_overview.R` (a dev-only HTML catalogue generator, read-only
reference — not to be modified):

- `piptm_analysis_variables(release)` — analysis variables, their type
  (continuous/binary/categorical/welfare/poverty), and applicable stat
  groups.
- `piptm_filter_categories(release)` — sample-base filter variables and
  their subcategories (Step 2 Decision 1 in the UI).
- `piptm_layout_covariates(release)` — layout-slot covariates and category
  counts (Step 2 Decision 3 in the UI).
- `piptm_stat_groups(release)` — statistic groups/measures (Shares and
  Counts, Summary Statistics, Inequality, etc.).
- `piptm_current_release()` — current release ID, for defaulting when no
  release argument is supplied.

`docs/ui-description.md` is the authoritative source for the 3-step UX
narrative (survey grid, measure/slice decisions, results) and must be used
verbatim for structuring the UX slides' talking points (screenshots
themselves are user-supplied, out of scope here).

`compound-gpid.md` documents the project's value proposition (fast,
reproducible poverty/inequality/welfare computation on harmonized,
partitioned survey data) and is the source for the "why this matters"
framing and the optional technical-roadmap slide's pipeline description.

`docs/` is already listed in `.Rbuildignore`, so a new
`docs/presentations/` subfolder needs no additional ignore-file change.

Environment note: the `quarto` CLI is not available on this execution
machine's PATH. Rendering verification (V1, V5) may need to happen on the
user's local machine (RStudio/VS Code Quarto extension) or after the CLI is
installed. R chunk logic should still be validated independently via
`Rscript` where practical (see Iteration Policy).

## Requirements

| ID | Requirement | Source |
|----|-------------|--------|
| R1 | New Quarto `revealjs` file at `docs/presentations/table-maker-overview.qmd`, ~10 core slides | Brainstorm Requirements/Format |
| R2 | Custom WB-branded SCSS theme file (`docs/presentations/theme.scss`) referenced by the `.qmd` | Brainstorm Requirements/Style |
| R3 | 3-step UX slides (Step 1 survey grid, Step 2 decisions, Step 3 results) with labeled image placeholders, narrated per `docs/ui-description.md` | Brainstorm Requirements/UX demonstration |
| R4 | Inputs slide(s): live registry-sourced survey/variable coverage via `piptm_*` functions, going beyond counts to cover which analysis variables exist and which statistics apply to each (by variable type/gating) | Brainstorm Requirements/Inputs slide(s) |
| R5 | Full analysis-side feature coverage: analysis variables + statistics, sample-base filtering, 4 layout slots, multi-survey comparison, poverty line handling, exports (CSV/XLSX/copy) | Brainstorm Requirements/Full feature coverage |
| R6 | Full metadata-side feature coverage: survey/country/year/welfare-type coverage, variable registry roles, harmonized variables, manifest-based reproducibility, partitioned Arrow/Parquet access | Brainstorm Requirements/Full feature coverage |
| R7 | One optional/backup technical-roadmap slide (inputs → computations → API), clearly marked as skippable/appendix, kept outside the core ~10-slide count | Brainstorm Requirements/Optional backup slide |
| R8 | Registry R chunks guarded: missing/misconfigured `PIPTM_REGISTRY_DIR`/`PIPTM_MANIFEST_DIR` produces a clear fallback message, not a hard render crash | Brainstorm Requirements/Reproducibility |
| R9 | No modification to `dev/registry_overview.R`, any `R/` source file, or `DESCRIPTION` | Brainstorm Approach 1 cons + file-permission boundary |

## Implementation Steps

## Phase 1: Scaffold, theme, and narrative slides

### 1. Scaffold the presentation file and theme
- **Requirements**: R1, R2
- **Files**: `docs/presentations/table-maker-overview.qmd` (new),
  `docs/presentations/theme.scss` (new), `docs/presentations/images/`
  (new, empty placeholder subfolder)
- **Details**: Create the `.qmd` with `revealjs` YAML header
  (`format: revealjs`, `theme: [default, theme.scss]`, title, author,
  date). Create `theme.scss` with WB-style SCSS variables (primary blue,
  gold/accent, neutral background, heading font stack) following standard
  Quarto revealjs SCSS variable/rule conventions (`/*-- scss:defaults --*/`
  then `/*-- scss:rules --*/`). Add a title slide and a closing/
  call-to-action slide as the first and last of the ~10 core slides.
- **Test Scenarios**: happy path (file parses as valid YAML + Quarto
  front matter); edge case (SCSS file with no matching selectors still
  loads without error); error path (malformed YAML front matter is
  caught before moving to content steps).
- **Tests**: Manual YAML front-matter lint (no automated test framework
  applies to a presentation artifact); `Rscript -e "rmarkdown::yaml_front_matter('docs/presentations/table-maker-overview.qmd')"` to confirm parseable front matter.
- **Acceptance criteria**: File exists with valid Quarto front matter
  referencing `theme.scss`; `theme.scss` exists with WB color variables
  defined.

### 2. Write the value/context slides (why Table Maker matters)
- **Requirements**: R1, R6
- **Files**: `docs/presentations/table-maker-overview.qmd`
- **Details**: Add 1-2 slides framing the problem Table Maker solves
  (fast, reproducible poverty/inequality/welfare statistics from
  harmonized PIP survey data) and its core value props: fast computation,
  partitioned microdata (Arrow/Parquet), harmonized variables (age,
  gender, area, etc.), manifest-based reproducibility — drawn from
  `compound-gpid.md` Objective/Architecture Notes, written in
  non-technical language (no code, no internal function names).
- **Test Scenarios**: happy path (slide content references each of the
  four value props: speed, partitioning, harmonization, reproducibility);
  edge case (claims stay qualitative/non-technical, no internal jargon
  like "manifest-first lazy validation" leaking into audience-facing
  text); error path (none — content-only step).
- **Tests**: Manual content checklist against R6 feature list.
- **Acceptance criteria**: All four value props present and phrased for
  a non-technical audience.

### 3. Write the 3-step UX slides with image placeholders
- **Requirements**: R3
- **Files**: `docs/presentations/table-maker-overview.qmd`,
  `docs/presentations/images/` (placeholder filenames only)
- **Details**: Add 3 slides (one per UI step), each with a clearly
  labeled placeholder image reference (e.g.
  `![Step 1: survey selection grid](images/step1-survey-grid.png)`
  — file not yet present, to be supplied by the user) and 2-4 bullet
  talking points per slide summarizing the step's purpose and key
  interactions, sourced from `docs/ui-description.md` (grid/toolbar for
  Step 1; sample-base filter, statistics, layout slots for Step 2;
  summary rail, tabs, exports for Step 3).
- **Test Scenarios**: happy path (each step slide references the correct
  placeholder filename and matches the step's actual UI behavior per
  `docs/ui-description.md`); edge case (placeholder image paths do not
  break the Quarto render even though the files don't exist yet — confirm
  Quarto renders with a broken-image icon rather than failing the whole
  document); error path (none beyond the above).
- **Tests**: Manual cross-check of each slide's bullets against
  `docs/ui-description.md` sections "Step 1", "Step 2", "Step 3".
- **Acceptance criteria**: 3 slides present, each naming its placeholder
  image file explicitly and accurately summarizing that step.

## Phase 2: Data-driven content, backup slide, and final render

### 4. Implement guarded registry-sourced inputs/features slides (R chunks)
- **Requirements**: R4, R5, R8, R9
- **Files**: `docs/presentations/table-maker-overview.qmd`
- **Details**: Add a setup R chunk (`#| include: false`) that:
  1. Wraps `piptm::piptm_analysis_variables()`,
     `piptm::piptm_filter_categories()`,
     `piptm::piptm_layout_covariates()`, `piptm::piptm_stat_groups()`,
     and `piptm::piptm_current_release()` calls in `tryCatch()`.
  2. On error (e.g. missing `PIPTM_REGISTRY_DIR`/`PIPTM_MANIFEST_DIR`),
     falls back to a clearly labeled placeholder data structure (e.g. a
     short named list with an explanatory `"(data unavailable in this
     render environment)"` string) instead of halting the render.
  3. Does not call internal (non-exported, `.`-prefixed) functions and
     does not modify `dev/registry_overview.R` — only sources the same
     exported functions it already uses, inline in the `.qmd`.
  Then add 2-3 content slides built from this data:
  - An "Inputs" slide: survey/country/year/welfare-type coverage summary
    (counts via `length()` on the returned structures, framed
    qualitatively, e.g. "13 countries, 2008-2023, income and consumption
    surveys").
  - An "Analysis variables & statistics" slide: for each analysis
    variable type (continuous/binary/poverty), name which statistics
    apply (Mean/Median/Gini/etc. for continuous; Share for binary;
    poverty rate/gap/severity for poverty status) — this is the "what
    Table Maker lets you compute" slide, not just a count.
  - A "Slice and filter" slide: the 4 layout slots (Columns, Rows, Super
    Columns, Super Rows), sample-base filtering, multi-survey comparison,
    and export options (CSV/XLSX/copy), completing R5's analysis-side
    coverage.
- **Test Scenarios**: happy path (registry functions succeed; slides show
  real current data from the configured registry); edge case (registry
  env vars unset or registry unreachable — fallback text renders instead
  of a crash); error path (a `piptm_*` function throws an unexpected
  error type not anticipated by the `tryCatch` — confirm the catch is
  generic enough to still produce the fallback rather than propagate).
- **Tests**: `Rscript -e "devtools::load_all('.'); source('<extracted setup chunk logic>')"` run twice: once with
  `PIPTM_REGISTRY_DIR`/`PIPTM_MANIFEST_DIR` set correctly, once with them
  unset, confirming no unhandled error in either case.
- **Acceptance criteria**: With env vars set, slides show real
  registry-derived content; with env vars unset, slides show the labeled
  fallback instead of failing the whole render; R5 and R6 feature
  checklist items are each mentioned somewhere across slides 2-4 of this
  step plus Step 2's value slide.

### 5. Write the optional/backup technical-roadmap slide
- **Requirements**: R7
- **Files**: `docs/presentations/table-maker-overview.qmd`
- **Details**: Add exactly one slide, placed after the core closing slide
  (e.g. using a Quarto `## Appendix` section or a slide clearly titled
  "Backup: Technical Pipeline (optional)"), showing a simple high-level
  flow: harmonized survey data / Arrow partitions → `table_maker()`
  computation engine → Plumber API service layer → PIP platform UI. Keep
  it architecture-notes level (no code snippets, no internal function
  names beyond `table_maker()` itself), and add a one-line note on the
  slide itself stating it is optional/backup and may be skipped.
- **Test Scenarios**: happy path (slide is visually/structurally
  distinguishable from the core 10, e.g. via a `---` section break or
  explicit "Backup" label); edge case (slide does not get silently counted
  as slide 11 of the "core" narrative in the title slide's stated slide
  count, if one is shown); error path (none — content-only step).
- **Tests**: Manual check that the slide is clearly marked and positioned
  after the core deck's closing slide.
- **Acceptance criteria**: Exactly one optional/backup slide exists,
  clearly labeled, positioned outside the core narrative flow.

### 6. Full-deck render and content-completeness check
- **Requirements**: R1, R2, R3, R4, R5, R6, R7, R8, R9
- **Files**: `docs/presentations/table-maker-overview.qmd`,
  `docs/presentations/theme.scss`
- **Details**: Attempt `quarto render docs/presentations/table-maker-overview.qmd`
  (or `rmarkdown::render()` as a fallback check for R-chunk execution if
  the `quarto` CLI is unavailable in this environment — see Iteration
  Policy). Visually confirm the custom SCSS theme is applied (not
  default revealjs styling). Run the R5/R6 feature checklist against the
  rendered or source content to confirm no feature category was silently
  dropped. Confirm the slide count: ~10 core slides + exactly 1 labeled
  optional/backup slide.
- **Test Scenarios**: happy path (render succeeds, theme visible, all
  feature-checklist items present); edge case (quarto CLI unavailable —
  fall back to validating R chunk logic standalone per Iteration Policy
  and flag full HTML-render verification as deferred to the user); error
  path (render fails for a reason other than missing CLI — report the
  exact error and do not mark this step done).
- **Tests**: `quarto render docs/presentations/table-maker-overview.qmd`
  (primary); `Rscript -e "rmarkdown::render('docs/presentations/table-maker-overview.qmd', output_format = 'html_document')"` as a degraded fallback purely to exercise the R chunks if `quarto` is absent (output format mismatch is acceptable for this fallback check only — it is not a substitute for a true revealjs render).
- **Acceptance criteria**: Either (a) a rendered HTML file exists showing
  the custom theme and all content, or (b) if `quarto` is unavailable,
  all R chunk logic has been validated standalone with no errors under
  both env-var-set and env-var-unset conditions, and this limitation is
  explicitly reported to the user.

## Testing Strategy

This is a content/presentation artifact, not package code, so there is no
`testthat` suite to extend. Verification relies on:
- Quarto/R render execution (primary, `quarto render`; fallback
  `rmarkdown::render` for R-chunk-only validation if the CLI is absent).
- Manual content checklists cross-referencing `docs/ui-description.md`
  (Step 3) and the R5/R6 feature lists (Step 4, Step 6) to ensure no
  feature category is silently omitted.
- Manual visual inspection of the rendered theme (Step 6).
- Explicit dual-condition check (env vars set / unset) of the guarded
  registry R chunk (Step 4, Step 6).

## Documentation Checklist

- [ ] No `R/` source files touched (presentation only).
- [ ] No `DESCRIPTION` changes (no new dependencies introduced).
- [ ] `docs/presentations/table-maker-overview.qmd` self-describes its
      purpose and audience in a short header comment or title-slide
      subtitle.
- [ ] Image placeholder filenames are descriptive enough for the user to
      know exactly which QA screenshot to supply for each.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| `quarto` CLI unavailable in the implementation environment, blocking full HTML render verification | Validate R chunk logic independently via `Rscript`/`rmarkdown::render` fallback; explicitly report the render-verification gap to the user rather than silently skipping it (see Iteration Policy) |
| Registry env vars (`PIPTM_REGISTRY_DIR`, `PIPTM_MANIFEST_DIR`) misconfigured or unset at render time | Guarded `tryCatch` fallback content per R8/Step 4, tested under both conditions |
| Feature coverage (R5/R6) silently drops a category given the "few beautiful slides" brevity goal | Explicit checklist step (Step 6) cross-referencing the brainstorm's full analysis-side/metadata-side feature lists before declaring done |

## Out of Scope

- Capturing actual QA-build screenshots (user-supplied later; this plan
  only creates labeled placeholders).
- Modifying `dev/registry_overview.R`, any `R/` source file, or
  `DESCRIPTION`.
- Extracting a reusable `registry_summary()` helper (tracked separately
  on the roadmap as an `idea`-status feature in Phase 3; not part of this
  plan).
- `.Rbuildignore` changes (already covers `docs/`, so no edit needed).
- PowerPoint/pptx export or any non-`revealjs` output format.

## Completion Contract

### Outcome
A self-contained Quarto revealjs presentation
(`docs/presentations/table-maker-overview.qmd` + a custom WB-branded
`theme.scss`) exists, renders to HTML locally, and communicates Table
Maker's 3-step UX, full analysis-side and metadata-side feature set, and
registry-sourced inputs to a non-technical PIP audience in ~10 core slides
plus one optional/backup technical-roadmap slide.

### Verification Surface
| ID | Evidence Required | Command/Artifact | Required |
|----|-------------------|------------------|----------|
| V1 | `.qmd` renders to HTML without error | `quarto render docs/presentations/table-maker-overview.qmd` (or RStudio/VS Code Quarto Render) | yes |
| V2 | Registry-derived R chunks execute and produce non-empty output when env vars are set | Manual run of the chunks / rendered HTML inspection | yes |
| V3 | Missing/misconfigured `PIPTM_REGISTRY_DIR`/`PIPTM_MANIFEST_DIR` produces a clear fallback message, not a hard render crash | Render (or `Rscript` fallback) with env vars unset; inspect output | yes |
| V4 | All required slide topics present (3-step UX placeholders, full analysis-side + metadata-side feature list, optional backup technical-roadmap slide marked as such) | Manual content checklist against brainstorm requirements | yes |
| V5 | Custom SCSS theme applied (not default revealjs theme) | Visual inspection of rendered HTML | yes |

### Constraints
| ID | Constraint | Check |
|----|------------|-------|
| C1 | No modification to `dev/registry_overview.R` or any `R/` source file | `git diff` shows no changes outside `docs/presentations/` |
| C2 | No new package dependencies added to `DESCRIPTION` | `git diff DESCRIPTION` is empty |
| C3 | Registry access must reuse exported `piptm_*` functions (`piptm_analysis_variables`, `piptm_filter_categories`, `piptm_layout_covariates`, `piptm_stat_groups`, `piptm_current_release`), not duplicate internal logic | Code review of `.qmd` R chunks |

### Boundaries
- Allowed: new `.qmd` file, new `theme.scss`, new `images/` subfolder with
  placeholder filenames, all under `docs/presentations/`.
- Out of scope: capturing actual QA screenshots (user-supplied later),
  modifying `dev/registry_overview.R`, extracting a reusable
  `registry_summary()` helper (tracked separately on the roadmap as an
  idea), `.Rbuildignore` changes (already covers `docs/`).

### Iteration Policy
1. If `quarto` CLI is unavailable in the execution environment, render
   verification may be deferred to the user's local machine/RStudio;
   the implementer should still validate R chunk logic standalone (e.g.
   via `Rscript`) before declaring done.
2. If registry env vars are unset in the implementation environment,
   fallback-path behavior (V3) should be verified via a mocked/forced-
   missing-env-var test rather than blocking on infrastructure access.

### Blocked-Stop Conditions
- `quarto` is not installed anywhere accessible and R chunk logic cannot
  be independently validated via `Rscript` either.
- The variable registry functions (`piptm_analysis_variables`, etc.)
  throw on a basic call even with env vars correctly set, indicating a
  deeper package issue outside this plan's scope.
