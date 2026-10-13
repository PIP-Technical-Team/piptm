---
date: 2026-10-13
title: "Description model UX improvements — static vs interactive"
status: in-progress
scope: "Standard"
artifact-schema-version: 1
chosen-approach: ""
tags: [description-endpoint, table-maker, api, ux, cell-definition]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# Description Model UX Improvements — Static vs Interactive

## Context

The `/description` endpoint is implemented and serving rendered HTML (default)
or Markdown from a 9-section model built off `description_metadata`. The PIP
platform UI is **not yet integrated** and is waiting on API guidance for how
to consume the endpoint.

The author is not convinced the current description model is the best shape
for end users. This brainstorm started a series of questions on consumption
model and possible improvements. It is paused with open points; no approach
has been chosen.

Branch: `feat/description-model-improvements` (from `new_description_model`).

Related prior work (do not reopen unless needed):
- `.cg-docs/brainstorms/2026-09-08-description-html-renderer-and-wording.md`
- `.cg-docs/brainstorms/2026-09-14-shares-cell-definition-structured-table.md`

## Requirements

Captured so far (incomplete; Q&A stopped before constraints / edge cases / scope):

- **Purpose**: Improve how table results are explained to PIP platform users;
  give UI developers a clear consumption contract.
- **Users**: PIP platform UI (primary, not yet integrated); end users viewing
  table descriptions.
- **Inputs/Outputs (current)**: Fast path = `description_metadata` from
  `/table?include_metadata=true`; fallback = recompute via table params.
  Output is a rendered HTML or Markdown **document fragment**, not JSON
  sections, not cell-scoped payloads, and not numbers from the result table.
- **Open product question**: Keep a static full-document description, move
  toward hover/pin cell-aware copy, or a hybrid (static now, structured
  later).

## Discussion so far

### How the UI can use the endpoint today

Success responses are raw HTML (default) or Markdown — **not** the usual JSON
envelope. Errors remain JSON (`400`/`422`).

Practical sequence: `/table` with `include_metadata=true`, then
`POST /description` with that metadata. Fallback recomputation should not be
the UI default.

Display options if they use the current contract:

1. Inject HTML as-is (`v-html` / `innerHTML`) — matches the default; inline
   styles baked in; weak theming; HTML injection.
2. Sandbox in an iframe — safer isolation; awkward height/a11y.
3. Request Markdown and render client-side — UI owns styling; table rendering
   will drift.
4. Structured 9-section JSON is **not** available — they cannot map sections
   to cards/tooltips without scraping.

Option 1 does **not** create a link. The API returns a full multi-section
card (`max-width: 980px`, variable height). Placement (always visible vs
button/drawer/modal) is entirely a UI choice.

### Interactive (hover/pin) vs current model

The mock is a **single-cell caption with numbers**. Current `/description` is
a **table-wide document with no numbers**.

Hard gaps for hover/pin:

1. No cell identity in metadata (variables/slots yes; Male × Urban ×
   Argentina 2014 no).
2. No values (`description_metadata` does not include the result table).

Household counts in the mock are also unavailable (cell `population` is
weighted people, not households).

Feasible splits:

- **A. UI interpolates locally** — expose a structured JSON model
  (glossary, filter sentence, layout-slot recipe, templates); UI fills the
  hovered cell from `/table` rows. Best split; medium API work.
- **B. Per-hover `/description` call** — rejected (latency, still need
  values + cell keys).
- **C. Pre-render every cell** — rejected (payload size, staleness).

Interactive copy must still handle suppression, multi-survey tables, and
share measures. The current document is stronger on provenance, exclusions,
suppression, and PPP than the mock.

### Static alignment toward the mock (no hover)

Keeping static HTML and rewriting copy is days-not-weeks: templates, section
order, exact-string tests. No new endpoint. UI still injects HTML.

Can copy without going interactive: spec line, “who is counted” as
exclusion language, short measure glossary, generic stepwise recipe
(sample base → layout slots → statistic).

Cannot copy while static: a real cell name, real values, text that changes
on hover. Static analogue of “pinned cell, in words” is a **recipe**, not a
caption of one number.

Charter-safe if provenance, exclusions, suppression, and PPP are kept
(possibly collapsed). Shares four-column table should stay.

## Approaches Considered

### Approach 1: Keep static HTML, rewrite toward the mock
Lead with a generic cell sentence and/or stepwise recipe; glossary;
preserve provenance/suppression lower on the page. Smallest change for UI.

### Approach 2: Structured JSON model for UI composition
Expose the 9-section model (and later cell templates) as JSON; HTML/Markdown
remain optional renderers. Unlocks hover later without a second rewrite.

### Approach 3: Cell-aware interactive description
New contract: cell keys + values in the description path, or UI-side
interpolation (Approach A above). Explicitly deferred until static vs
structured is decided.

## Decision

None yet. Status: `in-progress`.

## Open points (to be discussed)

1. **UI consumption recommendation** — Tell the UI team to inject static
   HTML now, wait for a rewritten static document, or wait for a JSON
   model?
2. **Static lead shape** — (1) generic cell sentence then glossary + recipe;
   (2) stepwise recipe only; (3) keep nine sections and only rewrite Cell
   Definition.
3. **Interactive later?** — Is hover/pin in scope for a later iteration, or
   out of scope for this product? If later, should we emit structured JSON
   now so the UI is not stuck on HTML injection?
4. **Numbers in the description** — Stay definition-only, or eventually
   interpolate `/table` values (static baked-in vs UI-side)?
5. **What to keep from the current document** that the mock drops:
   provenance, excluded surveys, suppression, PPP, shares table.
6. **Roadmap** — New milestone/feature vs an idea under Phase 4 API service.
   Not registered yet.

## Next Steps

- Resume this brainstorm and close the open points.
- Then either `/cg-plan` (if an approach is chosen) or `@cg-roadmap` to
  register an idea.
- Do not tell the UI team the contract is final until (1) and (3) are
  decided.
