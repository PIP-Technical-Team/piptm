---
date: 2026-10-22
title: "/table/download silently corrupts data when a sibling endpoint's output format changes"
category: "bugs"
language: "R"
tags: [plumber, api, format-contract, type-coercion, data-corruption, masking, suppression, download, regression-tests]
root-cause: "A downstream endpoint (/table/download) blindly coerced an upstream endpoint's (/table) `value` column to numeric, assuming it was always raw numeric; when /table's output contract changed to emit formatted display strings (scaled percentages, an \"NR\" masking sentinel), the coercion silently destroyed both signals instead of erroring"
severity: "P0"
---

# /table/download silently corrupts data when a sibling endpoint's output format changes

## Problem

While implementing cell-level `NR` masking and API value formatting for
`table_maker()`/`format_table_output()` (plan:
`.cg-docs/plans/2026-10-20-pop-share-nr-masking.md`), a `data-risk` review
surfaced a P0 finding in a completely different, unmodified-by-this-plan
endpoint: `POST /table/download`.

`/table/download` is designed to accept rows "typically copied from the
`/table` response" (per `docs/api-contract.md`) and convert them to CSV/RDS.
Before this session's work, `/table`'s `value` field was always a raw
numeric. After this session's work, `/table`'s `value` field became a
**formatted character string**: scaled-and-rounded percentages (e.g.
`"45.67"`), whole-number counts, or the literal sentinel `"NR"` for masked
(disclosure-controlled) cells.

`/table/download`'s existing code did:

```r
for (col in intersect(c("value", "population", "poverty_line"), names(tbl))) {
  data.table::set(tbl, j = col, value = suppressWarnings(as.numeric(tbl[[col]])))
}
```

`as.numeric("NR")` silently returns `NA` — indistinguishable from genuine
missing data, destroying the disclosure-control signal entirely. For
non-masked percentage rows, the re-materialized numeric value was the
already-×100-scaled percentage (e.g. `45.67`), not the raw proportion
(`0.4567`) that `table_maker()` actually produces — a silent unit/scale
corruption for any consumer re-ingesting the public `/table` JSON contract.

No test caught this because every existing `/table/download` test fed it
hand-built numeric fixtures, never a payload shaped like the *current*
`/table` response.

## Root Cause

**Implicit, untested format-contract coupling between two independently
versioned endpoints.** `/table/download`'s input handling encoded an
assumption ("`value` is always raw numeric") that was true only by
historical accident, not by any enforced contract. When the producing
endpoint (`/table`) changed its output representation as part of an
unrelated feature (disclosure-control masking + display formatting), the
consuming endpoint's silent, best-effort coercion (`suppressWarnings(as.numeric(...))`)
converted a format mismatch into silent data corruption instead of a visible
error — the worst possible failure mode for a disclosure-control sentinel.

This is a specific instance of a general anti-pattern: **any code that
re-ingests another endpoint's output and performs a "defensive" type
coercion on it must be re-audited whenever that endpoint's output contract
changes.** `grep`/dependency tooling does not catch this kind of coupling,
because there is no import or function call linking the two endpoints — only
a documented, informal "payload shape" contract in `docs/api-contract.md`.

## Solution

Exclude `value` from the blind numeric-coercion loop entirely, since by the
time this endpoint runs, `value` is either already numeric (hand-built
payloads) or is the current display-formatted string (payloads copied from
`/table`) — in both cases, the right behavior is to pass it through
unchanged rather than guess at its type:

```r
# NOTE: `value` is intentionally excluded from numeric coercion below.
# Since the /table formatting change, `value` may already be a formatted
# display string (e.g. "45.67" for a scaled percentage, or the "NR"
# suppression sentinel for masked cells). Coercing it with as.numeric()
# here would silently turn "NR" into NA (destroying the disclosure-control
# signal) and would not "undo" the x100 percentage scaling already applied
# by format_table_output(). Payloads that already carry a raw numeric
# `value` (e.g. hand-built row objects, not copied from /table) are left
# untouched as-is, since JSON numeric values round-trip as numeric without
# needing explicit coercion.
for (col in intersect(c("population", "poverty_line"), names(tbl))) {
  data.table::set(tbl, j = col, value = suppressWarnings(as.numeric(tbl[[col]])))
}
```

Added a regression test asserting both sentinel values survive a CSV
round-trip verbatim:

```r
test_that("/table/download preserves 'value' verbatim when it arrives as a
           formatted /table string (NR sentinel, scaled percentage)", {
  formatted_rows <- list(
    list(pip_id = "AAA_2020", measure = "gini", value = "NR", poverty_line = NULL),
    list(pip_id = "AAA_2020", measure = "headcount", value = "45.67", poverty_line = NULL)
  )
  res <- .dl_router$call(.dl_req(.dl_json("csv", data = formatted_rows)))
  df <- data.table::fread(text = rawToChar(res$body), colClasses = "character")
  expect_equal(df$value[df$measure == "gini"], "NR")
  expect_equal(df$value[df$measure == "headcount"], "45.67")
})
```

## Prevention

- **When an endpoint's output format changes, grep every other endpoint for
  code that re-ingests "the same shape" of payload**, not just code that
  calls the changed function directly. Format contracts between endpoints
  are rarely enforced by the type system and are easy to miss in a
  function-level dependency search.
- **Never apply a blanket `suppressWarnings(as.numeric(x))` to a field
  whose type/format is a documented, evolvable API contract rather than an
  internal implementation detail.** If a field might legitimately be a
  string sentinel (like `"NR"`), coercing it silently is strictly worse than
  leaving it untouched or raising an explicit error for truly malformed
  input.
- **Test consuming endpoints with fixtures shaped like the actual current
  producing-endpoint output**, not just historically-accurate hand-built
  payloads. A test that only ever constructs its own input data cannot catch
  drift between two endpoints' contracts.
- This project's `pop_share_threshold` suppression design has gone through
  three documented iterations (see Related) — the two most recent are not
  only *not* contradictory with respect to `/table/download`'s coercion bug,
  they are the direct *cause* of this bug surfacing, since the newest
  iteration is what introduced the `"NR"` sentinel and formatted-string
  `value` type into the `/table` contract in the first place. Any future
  change to a producing endpoint's response *shape* (not just its values)
  should trigger an explicit audit of every endpoint documented as accepting
  "a payload shaped like \<other endpoint\>'s response."

## Related

- `.cg-docs/solutions/bugs/2026-10-16-table-download-parser-and-size-guard.md` — an earlier, independent hardening pass on the same `/table/download` endpoint (parser/size-guard reliability); this entry documents a third, distinct bug class (format-contract leakage) found later in the same endpoint.
- `.cg-docs/solutions/bugs/2026-10-16-table-download-serializer-and-swagger-body.md` — a second independent bug class (serializer/header/Swagger) on the same endpoint, superseding part of the first entry's serializer choice. Three unrelated bug classes in one endpoint across two sessions suggests `/table/download` specifically would benefit from a dedicated live-API + contract-drift smoke test, beyond unit-level `pr$call()` coverage (see that entry's "Prevention" section on live-server verification, which is a complementary concern to this entry's format-contract issue).
- **⚠️ Stale brainstorm requiring manual correction** — `.cg-docs/brainstorms/2026-07-02-pop-share-threshold-always-active.md` is still marked `status: decided` and documents "ALL shares-family measures (`pop_share`, `target_within_group_share`, `target_survey_share`) are retained for below-threshold cells" as a deliberate, final design decision. This is now **superseded** by the 2026-10-20/22 work (plan: `.cg-docs/plans/2026-10-20-pop-share-nr-masking.md`; see `NEWS.md` "Breaking changes"), which masks share-family measures uniformly along with everything else — the retention exemption was intentionally removed. I do not have write permission to `.cg-docs/brainstorms/` to add a supersession marker; **a human should update that brainstorm's frontmatter** (e.g. add `superseded-by: .cg-docs/plans/2026-10-20-pop-share-nr-masking.md`) so future brain queries and readers don't rely on the stale retention behavior.
- `.cg-docs/plans/2026-10-20-pop-share-nr-masking.md` — the plan whose implementation changed `/table`'s output contract and indirectly surfaced this bug.
- `.cg-docs/reviews/2026-10-20-pop-share-nr-masking-review.md` — the `data-risk` review (finding P0.1) that caught this issue; see also its two verify-review follow-ups confirming the fix and subsequent refactors introduced no regressions.
