---
date: 2026-04-28
title: "fsetdiff attributed to collapse namespace — function does not exist there"
category: "bugs"
type: "bug"
language: "R"
tags: [data.table, collapse, namespace, fsetdiff, importFrom, NAMESPACE, roxygen2]
root-cause: "@importFrom collapse fsetdiff was written in roxygen2 docs but fsetdiff lives in data.table, not collapse; devtools::document() emitted a warning and the NAMESPACE entry was silently excluded, causing a runtime error"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# `fsetdiff` Attributed to Wrong Namespace (`collapse` vs `data.table`)

## Symptom

At runtime, calling `pip_lookup()` with an unmatched triplet threw:

```
Error: 'fsetdiff' is not an exported object from 'namespace:collapse'
```

During `devtools::document()`, the warning was visible but easy to miss:

```
@importFrom Excluding unknown export from collapse: `fsetdiff`
```

Because `devtools::document()` silently skips unresolvable imports, the
`NAMESPACE` file did not include the `fsetdiff` import, and the function
fell back to a bare `fsetdiff` call that resolved to `collapse::fsetdiff` —
which does not exist.

## Root Cause

`fsetdiff()` is a **data.table** function, not a collapse function.
The two packages have overlapping vocabularies (`fsetdiff`, `funique`,
`forder`, etc.) and it is easy to mis-attribute a function to the wrong
package when writing `@importFrom` directives.

```r
# WRONG
#' @importFrom collapse fsetdiff

# CORRECT
#' @importFrom data.table fsetdiff
```

The roxygen2/devtools toolchain does **not** error on unknown imports — it
emits a warning and excludes the line, leaving the function unimported.
The package loads fine; the bug only surfaces at runtime when the code path
is exercised.

## Fix

Change the `@importFrom` directive to the correct package:

```r
#' @importFrom data.table is.data.table data.table setcolorder rbindlist set fsetdiff
```

And ensure all call sites use `data.table::fsetdiff(...)` (or rely on the
import):

```r
unmatched <- data.table::fsetdiff(
  query,
  matched[, .(country_code, year, welfare_type)]
)
```

## Prevention

**Rule**: When in doubt about which package owns a function, use
`getAnywhere("fsetdiff")` or `packageName(environment(fsetdiff))` in a live R
session before writing `@importFrom`.

**Quick reference** for commonly confused data.table vs collapse functions:

| Function | Package |
|---|---|
| `fsetdiff` | **data.table** |
| `funique` | collapse (also `data.table::uniqueN`) |
| `forder` | data.table |
| `fmean`, `fsum`, `fvar` | **collapse** |
| `rbindlist` | **data.table** |
| `rowbind` | **collapse** |

**After every `devtools::document()` run**, scan stderr for lines containing
`Excluding unknown export` — these are silent failures that will become runtime
errors.

## Related

- `.cg-docs/solutions/bugs/2026-04-14-datatable-column-shadows-function-argument.md`
  — related data.table scoping/namespace confusion pattern
- data.table reference manual: `?data.table::fsetdiff`
- collapse reference manual: `?collapse::funique`
