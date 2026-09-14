---
date: 2026-09-14
title: "ANSI escape codes leak into /description endpoint warning text"
category: "bugs"
type: "bug"
language: "R"
tags: [cli, ansi, warnings, description-endpoint, table_maker, api]
root-cause: "cli_warn() emits ANSI-styled messages that are captured verbatim by conditionMessage() in table_maker()'s withCallingHandlers, without stripping escape codes before surfacing them via execution$warnings"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
red-phase-confirmed: "yes"
expected-behavior-source: "package-convention"
test-gap: "missing-test"
---

# ANSI escape codes leak into /description endpoint warning text

## Symptom

When a `table_maker()` call triggers cell suppression (`pop_share_threshold`),
the resulting warning surfaced in the `/description` endpoint's metadata
(`execution$warnings`) contained raw ANSI escape codes instead of clean,
readable text. For example:

```
Warnings: [1m[22mSuppressing non-share measures for 1 cell with pop_share < 0.01: [36mℹ[39m pip_id=BEL_2009_EU-SILC_INC_ALL, pov_status=1 (pop_share=0.0028)
```

## Expected Behavior Source

Package-convention — warnings surfaced through the `/description` endpoint's
metadata are consumed by non-terminal clients (JSON/text API responses, the
PIP platform UI), not a terminal. The convention is that such payload text
must be plain, human-readable strings with no ANSI/terminal escape
sequences. Specifically: the captured warning text must contain no ANSI CSI
sequences (`ESC [ ... <letter>`), regardless of the color-support settings
of the R session that produced it.

## Root Cause

`cli_warn()` (called at `R/table_maker.R` inside the suppression-handling
block) styles its message with ANSI escape codes — bold spans, colored `i`
info-bullet glyphs — whenever the ambient session is detected as
color-capable (via `cli.num_colors`/color-support detection). The
`withCallingHandlers()` block that captures these warnings
(`R/table_maker.R`, `warning = function(w) { captured_warnings <<- c(...,
conditionMessage(w)) }`) stored `conditionMessage(w)` verbatim, including the
embedded ANSI codes. This captured text flowed unmodified through
`captured_warnings` → `execution$warnings` → `.build_description_metadata()`
→ the `/description` endpoint's rendered warning text, so any plumber server
process where cli detected color support (e.g. driven by terminal
detection or `NO_COLOR` not being set) leaked raw escape codes into the API
response instead of clean text.

## Reproduction Test

Added to `tests/testthat/test-table-maker-suppression.R`:

```r
test_that("captured suppression warnings surfaced via include_metadata are plain text (no ANSI escapes)", {
  fx <- make_sup_fixture()
  activate_sup_fixture(fx)
  withr::defer(reset_piptm_env_sup())

  withr::local_options(cli.num_colors = 256L)

  out <- piptm::table_maker(
    pip_id              = "TST_2020_ECH_INC_ALL",
    analysis_var        = "welfare",
    measures            = "mean",
    by                  = "gender",
    ppp                 = 2021L,
    pop_share_threshold = 0.01,
    include_metadata    = TRUE
  )

  warnings_out <- out$description_metadata$execution$warnings
  expect_true(length(warnings_out) > 0L)

  ansi_pattern <- "\u001b\\[[0-9;]*[a-zA-Z]"
  expect_false(any(grepl(ansi_pattern, warnings_out)))
})
```

This test forces `cli.num_colors = 256L` so ANSI styling is emitted
deterministically regardless of the test runner's own terminal
capabilities, reproducing the same conditions as a plumber server process
where cli detects color support.

## Test Gap

**missing-test** — no existing test asserted that captured warnings
surfaced through `include_metadata`/description output are free of ANSI
escape sequences. The only prior related coverage
(`capture_with_warnings() collects cli warnings` in
`tests/testthat/test-api-helpers.R`) only checked that the plain-text
substring was present via `grepl()`, which still passes even when ANSI
codes surround the substring, since `cli_warn()` styles specific spans
(bullets, bold segments) rather than the entire message. No test existed
that specifically decoded or asserted the *absence* of ANSI control
sequences in captured warning text.

## Fix

Strip ANSI styling from the warning message at the point of capture in
`R/table_maker.R`, using `cli::ansi_strip()` (cli is already a package
dependency):

```r
warning = function(w) {
  # cli_warn() styles its message with ANSI escape codes (bold spans,
  # "i" bullet glyphs, colors) whenever the ambient session is detected
  # as color-capable. These captured warnings are surfaced verbatim in
  # the /description endpoint's text/JSON payload, which is consumed by
  # non-terminal clients, so strip ANSI styling before capturing.
  captured_warnings <<- c(captured_warnings, cli::ansi_strip(conditionMessage(w)))
  tryCatch(invokeRestart("muffleWarning"), error = function(e) NULL)
}
```

## Lessons Learned

Any code path that captures `conditionMessage()` from a `cli_warn()`/
`cli_abort()`/`cli_inform()` condition for reuse outside of a terminal
(API responses, logs consumed by non-terminal systems, persisted text)
must explicitly strip ANSI styling with `cli::ansi_strip()` at the capture
boundary — cli conditions are not guaranteed to be plain text, since their
formatting depends on ambient color-support detection which can vary
between interactive sessions and server processes. This is a
`missing-test` gap: tests that assert on captured warning/message text
should include an explicit check for the *absence* of ANSI control
sequences (not just substring presence via `grepl`), especially for any
warning/message capture boundary that feeds into an API or file output.
Consider auditing other warning/message capture points (e.g.
`capture_with_warnings()` in `inst/plumber/helpers.R`) for the same latent
risk, since they share the same `conditionMessage()`-without-stripping
pattern.

## Related

None.
