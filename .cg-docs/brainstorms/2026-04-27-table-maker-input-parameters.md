---
date: 2026-04-27
title: "table_maker() Input Parameter Structure"
status: open
scope: "Lightweight"
tags: [table-maker, api, parameters, step-7]
---

# table_maker() Input Parameter Structure

## Problem

The Step 7 plan defines `table_maker(country_code, year, welfare_type, ...)`
with three separate vectors. Filtering the manifest via
`country_code %in% .cc & year %in% .yr & welfare_type == .wt` produces a
**Cartesian product** — requesting COL-2010 and BOL-2000 would also match
COL-2000 and BOL-2010 if they exist. Users specify **exact tuples**, not
independent filters.

Additionally, `welfare_type` is a scalar in the plan but users may request
mixed welfare types across surveys (e.g., COL-2010-INC + BOL-2000-CON).

## Requirements

1. Represent exact survey tuples only — no Cartesian products
2. Easy to parse from a plumber URL query string
3. Easy to iterate in R
4. Scale to many (50+) survey requests
5. Mixed welfare types across surveys

## Approaches

### Approach A: `pip_id` vector (recommended)

```r
table_maker(
  pip_id        = c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
  measures      = c("headcount", "gini", "mean"),
  poverty_lines = c(2.15, 3.65),
  by            = c("gender", "area"),
  release       = NULL
)
```

**Manifest filter**: `mf[pip_id %chin% requested_ids]` — exact match, no
Cartesian product, no ambiguity.

**URL encoding**: `?pip_id=COL_2010_GEIH_INC_ALL&pip_id=BOL_2000_ECH_INC_ALL`
(plumber natively parses repeated query params as a vector).

**Pros**:
- Simplest R signature — one character vector
- Exact manifest lookup (pip_id is the primary key)
- No Cartesian product possible
- URL encoding is natural (repeated param)
- Already used internally everywhere (`dt$pip_id`, manifest keyed by pip_id)
- Scales trivially

**Cons**:
- Caller must know the full pip_id string (includes survey acronym + version)
- pip_id is an internal identifier — may be unfriendly for ad-hoc R usage

**Mitigation**: Provide a helper `pip_lookup(country_code, year,
welfare_type)` that resolves human-friendly triplets to pip_ids via the
manifest. The API layer calls `pip_lookup()` before `table_maker()` if
needed. `table_maker()` itself stays clean.

---

### Approach B: data.table / data.frame of tuples

```r
table_maker(
  surveys = data.table(
    country_code = c("COL", "BOL"),
    year         = c(2010L, 2000L),
    welfare_type = c("INC", "CON")
  ),
  measures      = c("headcount", "gini", "mean"),
  poverty_lines = c(2.15, 3.65),
  by            = c("gender", "area")
)
```

**Manifest filter**: `mf[surveys, on = .(country_code, year, welfare_type)]`
— exact join, no Cartesian product.

**Pros**:
- Explicit tuples, no ambiguity
- Natural for R users (pass a data.frame)
- Extensible (add columns like `version` later)

**Cons**:
- Harder to parse from URL query string (need positional alignment of
  parallel arrays: `?country_code=COL&country_code=BOL&year=2010&year=2000`)
- Plumber doesn't natively parse parallel arrays into a data.frame
- More ceremony for single-survey calls

---

### Approach C: Separate vectors (current plan — Cartesian)

```r
table_maker(
  country_code = c("COL", "BOL"),
  year         = c(2010L, 2000L),
  welfare_type = c("INC", "CON"),
  ...
)
```

**Manifest filter**: Cartesian — matches all combinations. Would need
post-hoc filtering or user must manually ensure no unwanted cross-matches.

**Pros**: Familiar parameter style.
**Cons**: **Fundamentally broken** for the tuple use case. Not fixable
without adding complexity that recreates Approach A or B.

---

### Approach D: List of lists

```r
table_maker(
  surveys = list(
    list(country_code = "COL", year = 2010L, welfare_type = "INC"),
    list(country_code = "BOL", year = 2000L, welfare_type = "CON")
  ),
  ...
)
```

**Pros**: Explicit tuples.
**Cons**: Verbose. Awkward to construct. Hard to parse from URL. No
data.table join possible without conversion. Worst ergonomics of all options.

## Recommendation

**Approach A (`pip_id` vector)** for `table_maker()` itself.

- `table_maker()` is an internal computation function, not a user-facing CLI.
  Using `pip_id` is natural — it's the primary key everywhere in the package.
- The API layer (plumber) receives pip_ids from the UI (which knows them).
- For ad-hoc R usage, provide `pip_lookup()` as a convenience.

**Fallback**: If the team prefers human-friendly parameters on `table_maker()`
itself, use **Approach B** (data.table of tuples) with a thin `pip_lookup()`
wrapper that converts triplets → pip_ids before the join.

## Iteration pseudocode (Approach A)

```r
table_maker <- function(pip_id, measures, poverty_lines = NULL,
                        by = NULL, release = NULL) {
  # 1. Validate
  families <- .classify_measures(measures)
  .validate_poverty_lines(poverty_lines, families)
  .validate_by(by)

  # 2. Manifest lookup
  mf <- piptm_manifest(release)
  entries <- mf[pip_id %chin% .pip_id]  # exact match, no Cartesian
  if (nrow(entries) == 0L) cli::cli_abort("No matching surveys in manifest.")

  # 3. Dimension pre-filter (partial match / NA fill logic)
  # ... as in plan Step 7.4 ...

  # 4. Load

  dt <- load_surveys(entries, release = release)

  # 5. Age binning
  if ("age" %in% by) { .bin_age(dt); by[by == "age"] <- "age_group" }

  # 6. Per-survey compute
  results <- lapply(unique(dt$pip_id), function(pid) {
    sdt <- dt[pip_id == pid]
    for (d in setdiff(by, names(sdt))) set(sdt, j = d, value = NA_character_)
    res <- compute_measures(sdt, measures, poverty_lines, by)
    res[, pip_id := pid]
    res
  })

  # 7. Combine + reorder
  rbindlist(results)
}
```
