---
date: 2026-11-08
title: "User-Facing API Error Messages"
status: decided
scope: "Deep"
artifact-schema-version: 1
chosen-approach: "Custom condition class with embedded user_message (incremental rollout, highest-traffic sites first)"
tags: [api, error-handling, plumber, ui-contract, cli_abort]
---
<!-- Valid status values: decided, in-progress, abandoned -->

# User-Facing API Error Messages

## Context

The `{piptm}` API (`inst/plumber/plumber.R`) raises domain errors internally
via `cli::cli_abort()` across ~10+ R source files (`load_data.R`,
`table_maker.R`, `measures.R`, `compute_measures.R`, `compute_shares.R`,
`compute_inequality.R`, `manifest.R`, `format_table_output.R`). These messages
are written for R developers debugging the package: they use cli/glue markup
(`{.val {x}}`, `{?s}`), internal vocabulary (`pip_id`, `manifest`, `triplet`,
`release`), and assume familiarity with the computation engine's internals.

`inst/plumber/helpers.R::capture_with_warnings()` is the only point where
these errors are caught before reaching the client: it does
`tryCatch(..., error = function(e) conditionMessage(e))`, which only strips
ANSI codes — it performs no actual translation. Every endpoint handler
forwards this raw string into the `errors[]` array of the JSON envelope at
HTTP 422 (see `README.md` error conventions). UI developers currently have no
way to get a message that is safe and appropriate to show directly to an end
user (e.g. "the countries you requested are not available" phrased for a
non-technical audience) — they only receive the internal debug string
verbatim.

The user's concrete example: calling `/table` with countries whose survey
isn't available in the current release returns a technically-accurate but
developer-oriented error message (e.g. referencing "survey triplet" /
"manifest" matching failures from `table_maker.R`), which is unsuitable to
render directly in the PIP platform UI.

## Requirements

- Scope is strictly the **translation** of existing errors into user-safe
  text — not a change to when/whether errors are raised, nor to
  all-or-nothing vs. partial-success behavior for mixed valid/invalid
  requests (that is existing domain logic and stays untouched).
- UI need is a single readable, user-safe sentence per error — no
  machine-readable error codes or branching-by-error-type required today.
- English only; no i18n/translation-key requirement at this time.
- Every error reaching the API client must be safe to show a non-technical
  end user: no raw internal vocabulary, no cli/glue markup artifacts.
- Errors without an authored user-facing message must never leak the raw
  internal text — they fall back to a fixed generic sentence (e.g.
  "Something went wrong processing this request.") and the gap should be
  flagged/logged for follow-up, not silently ignored.
- The developer-facing message (current `cli_abort()` text) must remain
  available for logs/debugging — nothing is lost for maintainers.

## Approaches Considered

### Approach 1: Custom condition class with embedded `user_message` (chosen)

Introduce an internal helper (e.g. `piptm_abort(user_message, dev_message,
class = "piptm_error", ...)`) that raises a condition carrying both the
existing developer-facing message and a new `user_message` field, authored at
each call site by whoever understands the real failure context. Update
`capture_with_warnings()` to prefer `user_message` when present, falling back
to a generic safe string otherwise.

- **Pros**: message authored closest to the logic that knows the true cause;
  no drift between a separate catalog and the code; developer message stays
  intact for logs; incremental migration is possible per call site; fits the
  project's existing per-site `cli_abort()` pattern.
- **Cons**: requires touching ~40+ call sites across ~10 files; risk of
  inconsistent tone without a shared style guideline; new call sites written
  later could forget to set `user_message` (mitigated by the generic
  fallback + lint/review check).

### Approach 2: Central error-catalog/translation table at the API boundary (rejected)

Leave `cli_abort()` calls untouched; match known error signatures by class or
regex against `conditionMessage()` inside `capture_with_warnings()`, mapping
to pre-written user sentences.

- **Pros**: no changes to existing computation code; all user copy
  centralized in one file.
- **Cons**: regex matching against free text is fragile — any future wording
  tweak to an internal `cli_abort()` call silently breaks the mapping and
  re-leaks the raw technical message with no warning; matching by condition
  class still requires adding classes to most call sites anyway, eroding the
  supposed effort savings; catalog maintainer is disconnected from the code
  change that introduced/altered the error, so coverage gaps are likely and
  hard to detect.

### Approach 3: Hybrid — condition classes only + central catalog keyed by class (considered, deferred)

Give each domain error a distinct condition class but keep the user-facing
text in a central catalog keyed by class, decoupling "what happened" from
"what to tell the user."

- **Pros**: would support future stable error codes and i18n without
  redoing call-site work.
- **Cons**: over-engineered relative to today's confirmed requirements (no
  codes, no i18n needed now); adds a second artifact (the catalog) to keep in
  sync for no near-term benefit.
- Noted as a future evolution path: Approach 1's `user_message` field could
  later be promoted into this shape (adding a stable `class`/code) without
  redoing the call-site authoring work, if codes or i18n become a real
  requirement.

## Decision

**Approach 1, rolled out incrementally against the highest-traffic error
sites first** (survey/country not found, PPP not available, invalid
disaggregation dimensions, unmatched survey triplets), rather than requiring
100% call-site coverage before shipping. Rationale from the devil's-advocate
pass:

- *Effort-value*: covering every one of the ~40+ `cli_abort()` sites in v1 is
  disproportionate to value — most user traffic will hit a small, identifiable
  set of validation/availability errors. Shipping those first, backed by the
  generic safe fallback for anything unmapped, gets UI developers a working,
  non-leaking contract quickly; remaining sites can be migrated against real
  usage/logs.
- *Charter alignment*: `compound-gpid.md`'s declared constraints
  (reproducibility, methodological consistency, deterministic outputs,
  scalability, interoperability) are about the computation engine and do not
  address API/UI messaging — no conflict. Note for charter maintainers: the
  charter lists "API service layer" and "UI" as separate Phase 2+
  deliverables; this error-UX work sits at that API/UI boundary and is not
  yet a named roadmap item (see Next Steps / Handoff).
- *Problem validation*: pre-validated by the user's own concrete example
  (the `/table` country-not-available case) — no further validation needed.
- *Simplicity check*: no existing configuration/convention already solves
  this; the current `conditionMessage()` passthrough is confirmed to be the
  entire "translation" layer today.

## Next Steps

For handoff to `/cg-plan`:

1. Design `piptm_abort()` (or equivalent) as a thin wrapper around
   `cli::cli_abort()` that accepts a `user_message` argument and stores it on
   the condition object (alongside the existing developer message), plus a
   `class` argument for forward compatibility with Approach 3 if ever needed.
2. Update `inst/plumber/helpers.R::capture_with_warnings()` to read
   `user_message` off the caught condition when present; otherwise return the
   fixed generic fallback string (never raw `conditionMessage(e)`).
3. Identify and migrate the highest-traffic error sites first (candidates
   identified during this brainstorm: `table_maker.R:104` unmatched survey
   triplets, `load_data.R:312` PPP not available, `load_data.R:503` no
   surveys remain after PPP filtering, `measures.R:195` dimension not
   available, `table_maker.R:525` empty `pip_id` after resolution).
4. Add a lightweight coverage check (lint rule, test, or code-review
   checklist item) flagging any `cli_abort()` call in `R/` that lacks a
   paired `user_message` and isn't an explicitly acknowledged "rare/internal"
   exception — to catch regressions on new call sites without requiring 100%
   coverage up front.
5. Update `docs/api-contract.md` and `README.md`'s error-conventions section
   to document the new guarantee: `errors[]` entries are always safe to
   display verbatim to end users.
6. Decide and document the fallback copy text (e.g. "Something went wrong
   processing this request.") in one place for consistency.
