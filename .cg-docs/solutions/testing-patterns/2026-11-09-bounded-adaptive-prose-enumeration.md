---
date: 2026-11-09
title: "Bounded adaptive prose enumeration: factorizing multi-dimensional grid explanations into bounded UI contracts"
category: "testing-patterns"
language: "R"
tags: [api, endpoint, cell-definition, prose, json-serialization, plumber, testthat, unboxing, cartesian-product, bounded-contracts]
root-cause: "Cartesian product of layout covariates causes exponential text bloat if fully enumerated; naive one-cell examples leave all other categories opaque; single-element array unboxing in Plumber breaks client-side array contracts"
severity: "P2"
---

# Bounded Adaptive Prose Enumeration: Factorizing Multi-Dimensional Grid Explanations into Bounded UI Contracts

## Problem

When generating natural-language explanations of user-configured multi-dimensional tables (e.g. `GET /cell-definition` for Page 2 layout slots `row`, `column`, `superRow`, `superColumn`):

1. **The single-cell example failure**: Providing plain-English wording for only the first combined cell (e.g., `"For example, the first cell is men living in urban areas"`) leaves all remaining categories (categories 2..N across all active dimensions) completely unexplained to the user.
2. **The Cartesian explosion problem**: Fully enumerating every combined cell ($N_1 \times N_2 \times N_3 \times N_4$) quickly explodes. A table with 4 dimensions can produce up to 560 cells, turning a preview into hundreds of lines of repetitive boilerplate where 90% of the words are repeated.
3. **Transport/JSON unboxing vulnerability**: When exposing a list of strings (`items`) over an HTTP API via Plumber, a single-element character vector (`c("...")`) is automatically unboxed by default into a bare JSON string (`"items": "..."`), breaking frontend clients that expect a uniform array (`"items": ["..."]`).
4. **Registry fallback count hazard**: If the variable registry is unavailable at runtime, naive code that falls back to dummy category codes (e.g., code `"1"`) can falsely compute $1 \times 1 \times 1 = 1$ and claim a 3-dimensional table has `"1 cell in total"`.

## Root Cause

- Cross-multiplying dimensions ($N_1 \times N_2$) produces exponential redundancy. In contrast, explaining dimensions additively ($N_1 + N_2 + \dots$) scales linearly and covers 100% of the active categories.
- At small cardinalities ($\le 4$ total cells, e.g. $2 \times 2$ or 1 slot with 3 categories), listing every combined cell is concise and intuitive. Above 4 cells, full enumeration obscures the table's grid structure.
- In R Plumber serializers (`list(na = "null")` with `jsonlite`), a character vector of length 1 or an unboxed list can serialize as a scalar string unless wrapped explicitly as an R list (`as.list()`) or `I()`.

## Solution

### 1. Bounded Adaptive Split (`items` array, guaranteed $\le 4$ entries)

Implement an adaptive builder that guarantees `items` is always a JSON array of plain-English strings with at most 4 entries:

```r
# Case A: 0 active slots -> single-cell table
if (length(active_display) == 0L) {
  return(list(
    structure_text = sprintf("No row or column variables are selected, so the table has a single cell covering %s.", table_pop_short),
    items = list(),
    example_text = NULL
  ))
}

# Case B: Known categories and total_cells <= 4L -> full Cartesian enumeration
if (all_known && total_cells <= 4L) {
  # Vary earlier slots outer/slowest and later slots inner/fastest
  grid <- do.call(expand.grid, c(lapply(n_each, seq_len), list(KEEP.OUT.ATTRS = FALSE)))
  grid <- grid[do.call(order, as.list(grid)), , drop = FALSE]
  cell_phrases <- vapply(seq_len(nrow(grid)), function(r) {
    picks <- lapply(seq_along(slot_resolved), function(j) {
      list(slot = slot_resolved[[j]]$slot, cat_info = slot_resolved[[j]]$cat_infos[[grid[r, j]]])
    })
    .cap_first(.compose_cell_group(picks, trailing_extra = trailing_extra))
  }, character(1))
  items <- as.list(cell_phrases) # as.list prevents JSON scalar unboxing
}

# Case C: Known categories and total_cells > 4L -> 1 line per active slot (<= 4 lines)
if (all_known && total_cells > 4L) {
  slot_bullets <- vapply(slot_resolved, function(sr) {
    sprintf("%s (%s): %s.", .cap_first(sr$role_label), sr$slot_noun, .slot_category_list(sr$cat_infos))
  }, character(1))
  items <- as.list(slot_bullets)
}

# Case D: Unresolved registry -> graceful count-free fallback
if (!all_known) {
  items <- list()
  # structure_text omits "— N cells in total:"
}
```

### 2. Filter-Aware Category Intersections

When a layout slot variable is also constrained by `filter_base`, intersect the slot's effective categories with the filter's allowed codes/labels in registry order:

```r
.slot_effective_categories <- function(varname, reg, clean_fb) {
  # ... extract reg_codes and reg_labels ...
  if (varname %in% names(clean_fb)) {
    fb_lower <- tolower(trimws(as.character(clean_fb[[varname]])))
    keep <- tolower(reg_codes) %in% fb_lower | tolower(reg_labels) %in% fb_lower
    retained <- reg_codes[keep]
    if (length(retained) > 0L) return(list(codes = retained, known = TRUE))
  }
  return(list(codes = reg_codes, known = TRUE))
}
```

### 3. Verification & Plumber Unbox Guard Tests

In Plumber integration tests (`test-api-cell-definition.R`), explicitly verify that single-element lists serialize as arrays:

```r
body <- if (is.raw(res$body)) rawToChar(res$body) else res$body
expect_match(body, "\"items\"\\s*:\\s*\\[", perl = TRUE)
expect_false(grepl("\"items\"\\s*:\\s*\"", body, perl = TRUE))
```

And in `testthat` 3rd edition, use `label = nm` rather than `info = nm` when asserting bounded size in loops:

```r
for (nm in names(scenarios)) {
  expect_lte(length(scenarios[[nm]]$what_a_cell_is$items), 4L, label = nm)
}
```

## Prevention

- **Never multiply dimensions for UI text**: When explaining multi-slot tables, never emit raw Cartesian products unless the total count is strictly bounded ($\le 4$). Above that threshold, factorize by slot.
- **Always store array-contract API fields as `as.list()` in R**: Even when a field contains a single string, wrap it as an R list so Plumber's JSON serializer does not unbox it to a bare string.
- **Guard fallback branches against false cardinality**: Fallback code paths with missing metadata should drop numeric counts rather than reporting false counts derived from dummy fallback records.

## Related

- `.cg-docs/solutions/testing-patterns/2026-11-04-query-parameter-endpoint-migration-testing.md`
- `.cg-docs/solutions/bugs/2026-11-04-advisory-deferral-request-builder-architecture.md`
- `.cg-docs/plans/2026-11-09-cell-definition-all-cells-breakdown-revised.md`
