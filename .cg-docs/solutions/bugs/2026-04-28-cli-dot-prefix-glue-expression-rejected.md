---
date: 2026-04-28
title: "cli >= 3.4.0 rejects glue expressions starting with a dot"
category: "bugs"
type: "bug"
language: "R"
tags: [cli, glue, dot-prefix, cli_abort, cli_warn, NSE, interpolation]
root-cause: "cli >= 3.4.0 reserves {.xxx} syntax for cli styles; bare variable names starting with a dot (e.g. {.ids}) are now rejected with 'Invalid cli literal'"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# `cli` ≥ 3.4.0 Rejects Glue Expressions Starting with a Dot

## Symptom

A `cli_abort()` call threw an error instead of the intended message:

```
Error in `"fun(..., .envir = .envir)"`:
! Invalid cli literal: `{.ids}` starts with a dot.
ℹ Interpreted literals must not start with a dot in cli >= 3.4.0.
ℹ `{}` expressions starting with a dot are now only used for cli styles.
ℹ To avoid this error, put a space character after the starting `{`
  or use parentheses: `{(.ids)}`
```

The error was triggered when `table_maker()` tried to format its error message
listing the unmatched `pip_id` values stored in the local variable `.ids`.

## Root Cause

cli uses `{.xxx}` as its own **inline markup syntax** (e.g. `{.val}`,
`{.arg}`, `{.fn}`, `{.path}`). Starting with cli 3.4.0, any glue expression
whose content begins with a dot is treated as a cli style directive, not a
variable lookup. Because `.ids` starts with a dot, cli attempted to interpret
it as a style tag, failed, and threw an error.

This behaviour change was intentional: it prevents accidental collisions
between user variable names and cli's markup namespace.

```r
# WRONG — .ids looks like a cli style to cli >= 3.4.0
cli_abort("Requested pip_ids: {.val {.ids}}")

# CORRECT — wrap in parentheses to force evaluation as expression
cli_abort("Requested pip_ids: {.val {(.ids)}}")
```

## Fix

Wrap the dot-prefixed variable in parentheses inside the glue expression:

```r
cli_abort(
  c(
    "No matching surveys found in manifest.",
    "i" = "Requested pip_id{?s}: {.val {(.ids)}}"
  )
)
```

The parentheses tell cli's glue engine that this is a regular R expression to
evaluate, not a style directive.

## Prevention

**Rule**: Never use a local variable whose name starts with a dot directly
inside a cli glue expression. Either rename the variable (`ids` instead of
`.ids`) or wrap it in parentheses (`{(.ids)}`).

**Recommended patterns**:

```r
# Option 1 — rename to avoid the dot entirely (cleanest)
ids <- pip_id
cli_abort("Requested: {.val {ids}}")

# Option 2 — parentheses (preserves dot-naming convention elsewhere)
cli_abort("Requested: {.val {(.ids)}}")

# Option 3 — space after brace (also valid per cli docs)
cli_abort("Requested: {.val { .ids}}")
```

Note that dot-prefixed names are sometimes necessary in data.table contexts
(see the column-shadowing solution) — Option 2 is the best bridge when the
dot-prefix is intentional.

## Related

- `.cg-docs/solutions/bugs/2026-04-14-datatable-column-shadows-function-argument.md`
  — explains *why* dot-prefixed local variables are sometimes necessary in
  data.table code, creating a tension with this cli restriction
- cli changelog: https://cli.r-lib.org/news/index.html (v3.4.0 section)
- cli inline markup reference: https://cli.r-lib.org/reference/inline-markup.html
