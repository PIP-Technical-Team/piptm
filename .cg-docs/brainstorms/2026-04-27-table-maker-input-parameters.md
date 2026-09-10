---
date: 2026-04-27
title: "table_maker() Input Parameter Structure"
status: decided
chosen-approach: "Approach A with built-in triplet fallback"
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

**Approach A (`pip_id` vector) with built-in triplet fallback.**

`table_maker()` accepts `pip_id` as its primary input (the API hot path),
but also accepts `country_code` / `year` / `welfare_type` triplets for
ad-hoc R console usage. When triplets are provided instead of `pip_id`,
the function internally calls `pip_lookup()` to resolve them.

### Rationale

- **API path (primary)**: The UI already has pip_ids from the manifest it
  loaded to populate survey pickers. It sends them directly — no resolution
  needed, no ambiguity, no Cartesian product.
- **Ad-hoc R path (secondary)**: An R user working interactively knows
  "Colombia, 2010, income" but not `"COL_2010_GEIH_INC_ALL"`. Requiring
  a separate `pip_lookup()` call before every `table_maker()` call is
  friction. Accepting triplets directly eliminates that extra step.
- **Cost**: ~5 lines of dispatch logic at the top of `table_maker()` and
  a slightly wider signature. No impact on the computation path — by the
  time the function reaches the manifest lookup, it always has a `pip_id`
  vector.

### Dual-input signature

```r
table_maker <- function(pip_id = NULL,
                        country_code = NULL,
                        year = NULL,
                        welfare_type = NULL,
                        measures,
                        poverty_lines = NULL,
                        by = NULL,
                        release = NULL) {


  # --- Resolve survey identifiers ---
  # Either pip_id OR (country_code + year + welfare_type) must be provided.
  if (is.null(pip_id)) {
    if (is.null(country_code) || is.null(year) || is.null(welfare_type)) {
      cli::cli_abort(
        "Provide either {.arg pip_id} or all of {.arg country_code},
         {.arg year}, and {.arg welfare_type}."
      )
    }
    pip_id <- pip_lookup(country_code, year, welfare_type, release)
  }

  # From here on, only pip_id is used.
  # ... validation, manifest lookup, load, compute ...
}
```

### Calling patterns

```r
# Pattern 1 — API / programmatic (pip_id known)
table_maker(
  pip_id   = c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
  measures = c("headcount", "gini")
)

# Pattern 2 — Ad-hoc R console (human-friendly triplets)
table_maker(
  country_code = c("COL", "BOL"),
  year         = c(2010L, 2000L),
  welfare_type = c("INC", "INC"),
  measures     = c("headcount", "gini")
)
```

Both patterns converge to the same internal path after the first 5 lines.

### Validation rules for the dual input

| `pip_id` | Triplets | Outcome |
|----------|----------|---------|
| provided | NULL | Use pip_id directly |
| NULL | all three provided | Resolve via `pip_lookup()` |
| NULL | any missing | Error: provide pip_id or all three triplets |
| provided | also provided | Use pip_id, ignore triplets (with optional warning) |

### Why not Approach B (data.table of tuples)?

The dual-input pattern achieves the same user-friendliness without
requiring users to construct a `data.table()` for simple calls. It also
avoids the URL-parsing problem — the API path uses `pip_id` directly,
and triplet resolution only happens for R console callers.

## Iteration pseudocode (dual-input)

```r
table_maker <- function(pip_id = NULL,
                        country_code = NULL,
                        year = NULL,
                        welfare_type = NULL,
                        measures,
                        poverty_lines = NULL,
                        by = NULL,
                        release = NULL) {

  # 0. Resolve survey identifiers
  if (is.null(pip_id)) {
    if (is.null(country_code) || is.null(year) || is.null(welfare_type)) {
      cli::cli_abort(
        "Provide either {.arg pip_id} or all of {.arg country_code},
         {.arg year}, and {.arg welfare_type}."
      )
    }
    pip_id <- pip_lookup(country_code, year, welfare_type, release)
  }

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

---

## Full Pipeline Analysis: UI → URL → Plumber → table_maker()

### 1. URL Construction — Who Resolves pip_ids?

**Option 1a: UI sends pip_ids directly.**
The UI already knows the pip_id for each survey row the user selects (it
comes from the manifest served to the frontend). The URL contains the
exact identifiers:

```
GET /table?pip_id=COL_2010_GEIH_INC_ALL&pip_id=BOL_2000_ECH_INC_ALL
            &measures=headcount&measures=gini&poverty_lines=2.15
```

**Option 1b: UI sends triplets, API resolves.**
The UI sends human-friendly fields; the plumber endpoint calls
`pip_lookup()` before `table_maker()`:

```
GET /table?country_code=COL&year=2010&welfare_type=INC
           &country_code=BOL&year=2000&welfare_type=CON
           &measures=headcount&measures=gini
```

**Trade-offs:**

| Criterion | 1a (pip_id in URL) | 1b (triplets in URL) |
|---|---|---|
| Coupling | UI must have manifest | UI only needs cc/year/wt |
| Ambiguity | None — PK lookup | Parallel arrays must stay aligned |
| Plumber parsing | Trivial (single repeated param) | Fragile (3 parallel arrays → positional zip) |
| URL readability | Opaque IDs | Human-readable |
| Extra API call | None | `pip_lookup()` on every request |
| Cacheability | pip_id is stable within a release | Same |

**Verdict**: **Option 1a**. The PIP UI already loads manifest data to
populate dropdowns. It knows pip_ids. Sending them directly is simpler,
unambiguous, and avoids the parallel-array problem entirely. For ad-hoc
users who don't know pip_ids, expose a separate `/lookup` endpoint (see
§4 below).

---

### 2. URL Length & Scalability

A pip_id is ~25 characters. With URL encoding overhead:

| Surveys | Approx URL length (pip_id params only) |
|---------|----------------------------------------|
| 10 | ~350 chars |
| 50 | ~1,750 chars |
| 100 | ~3,500 chars |

HTTP/1.1 has no formal URL length limit, but browsers cap around 2,000–8,000
characters and some proxies at ~4,000. So:

- **≤50 surveys**: GET with repeated params is fine.
- **>50 surveys**: Switch to POST with JSON body.

**Recommendation**: Support both.

```
# GET — for typical interactive use (≤50 surveys)
GET /table?pip_id=X&pip_id=Y&measures=headcount&...

# POST — for batch/programmatic use
POST /table
Content-Type: application/json

{
  "pip_id": ["COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL", ...],
  "measures": ["headcount", "gini"],
  "poverty_lines": [2.15, 3.65],
  "by": ["gender", "area"]
}
```

Plumber supports both on the same endpoint via `@get` + `@post` or a
`@plumber` function. The POST body is parsed identically to query params.

A "request ID" or hash is unnecessary complexity at this stage — it
introduces statefulness (server must store the mapping). Defer unless
proven needed.

---

### 3. Plumber API Design

```r
#* Compute cross-tabulated measures for selected surveys
#* @get /table
#* @post /table
#* @param pip_id:character Survey identifiers (pip_id from manifest)
#* @param measures:character Measure names (e.g. headcount, gini, mean)
#* @param poverty_lines:numeric Poverty lines (required if poverty measures requested)
#* @param by:character Disaggregation dimensions (gender, area, educat4, age)
#* @param release:character Release identifier (defaults to current)
#* @serializer json list(na = "null")
function(pip_id, measures, poverty_lines = NULL, by = NULL, release = NULL) {

  # Plumber coercion: query params arrive as character vectors.
  # poverty_lines needs explicit numeric conversion.
  if (!is.null(poverty_lines)) {
    poverty_lines <- as.numeric(poverty_lines)
  }

  result <- table_maker(
    pip_id        = pip_id,
    measures      = measures,
    poverty_lines = poverty_lines,
    by            = by,
    release       = release
  )

  result
}
```

**Plumber parsing details:**

- Repeated query params (`?pip_id=X&pip_id=Y`) → character vector
  `c("X", "Y")` automatically.
- **Single value edge case**: `?pip_id=X` → character scalar `"X"`, not
  length-1 vector. This is fine — `%chin%` handles both.
- All query params arrive as **character**. `poverty_lines` must be
  coerced to numeric. `year` is not needed (it's embedded in pip_id).
- POST with JSON body: plumber parses JSON arrays directly into R vectors
  with correct types.

---

### 4. Transformation Layer — pip_lookup() for Triplet Resolution

For users who don't know pip_ids, expose a separate lookup endpoint:

```r
#' Resolve survey triplets to pip_ids
#'
#' @param country_code Character vector of country codes.
#' @param year Integer vector of years (same length as country_code).
#' @param welfare_type Character vector of welfare types (same length).
#' @param release Release identifier (default: current).
#' @return Character vector of matching pip_ids.
#' @export
pip_lookup <- function(country_code, year, welfare_type, release = NULL) {
  stopifnot(
    length(country_code) == length(year),
    length(country_code) == length(welfare_type)
  )

  mf <- piptm_manifest(release)
  query <- data.table(
    country_code = country_code,
    year         = as.integer(year),
    welfare_type = welfare_type
  )

  matched <- mf[query, on = .(country_code, year, welfare_type), nomatch = NULL]

  if (nrow(matched) < nrow(query)) {
    unmatched <- fsetdiff(query, matched[, .(country_code, year, welfare_type)])
    cli::cli_warn(c(
      "{nrow(unmatched)} survey{?s} not found in manifest:",
      "i" = "{paste(unmatched$country_code, unmatched$year,
              unmatched$welfare_type, sep = '-')}"
    ))
  }

  matched$pip_id
}
```

**Plumber endpoint:**

```r
#* Resolve country/year/welfare_type triplets to pip_ids
#* @get /lookup
#* @param country_code:character Country codes
#* @param year:integer Survey years
#* @param welfare_type:character Welfare types (INC or CON)
#* @param release:character Release identifier
#* @serializer json
function(country_code, year, welfare_type, release = NULL) {
  list(pip_id = pip_lookup(country_code, as.integer(year),
                           welfare_type, release))
}
```

**Usage from UI**: If the UI only has triplets, it calls `/lookup` first,
then `/table` with the returned pip_ids. Two requests, but the lookup is
cheap (in-memory manifest join) and cacheable.

---

### 5. End-to-End Example

**Step 1 — User selects surveys in the UI:**

The user picks:
- Colombia 2010, Income (GEIH survey)
- Bolivia 2000, Income (ECH survey)

They select measures: headcount, gini, mean.
Poverty line: 2.15.
Dimensions: gender, area.

**Step 2 — UI constructs the URL:**

The UI already loaded the manifest to populate the survey picker. It knows
the pip_ids. It builds:

```
GET /table?pip_id=COL_2010_GEIH_INC_ALL&pip_id=BOL_2000_ECH_INC_ALL
           &measures=headcount&measures=gini&measures=mean
           &poverty_lines=2.15
           &by=gender&by=area
```

**Step 3 — Plumber parses:**

```r
pip_id        = c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL")
measures      = c("headcount", "gini", "mean")
poverty_lines = "2.15"   # character — coerced to numeric(2.15) in handler
by            = c("gender", "area")
release       = NULL      # default
```

**Step 4 — Handler calls table_maker():**

```r
table_maker(
  pip_id        = c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
  measures      = c("headcount", "gini", "mean"),
  poverty_lines = 2.15,
  by            = c("gender", "area")
)
```

**Step 5 — Inside table_maker():**

1. `.classify_measures()` → `list(poverty = "headcount", inequality = "gini", welfare = "mean")`
2. `.validate_poverty_lines(2.15, ...)` → OK
3. `.validate_by(c("gender", "area"))` → OK
4. `mf[pip_id %chin% c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL")]` → 2 entries
5. Dimension pre-filter: both surveys have gender + area → full match, no warnings
6. `load_surveys(entries)` → flat data.table, ~50K rows per survey
7. Per-survey loop: `compute_measures()` for each pip_id
8. `rbindlist()` → final long-format result

**Step 6 — Response:**

```json
[
  {"pip_id":"COL_2010_GEIH_INC_ALL","gender":"male","area":"urban",
   "poverty_line":2.15,"measure":"headcount","value":0.042,"population":12345},
  {"pip_id":"COL_2010_GEIH_INC_ALL","gender":"male","area":"urban",
   "poverty_line":null,"measure":"gini","value":0.51,"population":12345},
  ...
]
```

---

### 6. Alternative Flow — Ad-hoc R User Without pip_ids

```r
# Step 1: Resolve triplets
ids <- pip_lookup(
  country_code = c("COL", "BOL"),
  year         = c(2010L, 2000L),
  welfare_type = c("INC", "INC")
)
# ids == c("COL_2010_GEIH_INC_ALL", "BOL_2000_ECH_INC_ALL")

# Step 2: Compute
result <- table_maker(
  pip_id   = ids,
  measures = c("headcount", "gini"),
  poverty_lines = 2.15,
  by       = "gender"
)
```

---

## Conclusion

Approach A with built-in triplet fallback propagates cleanly through the
full pipeline:

- **UI**: Already has pip_ids from manifest → embeds them directly in URL.
- **URL**: Simple repeated params; POST for batch. No parallel-array fragility.
- **Plumber**: Trivial parsing; only `poverty_lines` needs type coercion.
  The API endpoint only uses the `pip_id` parameter.
- **table_maker()**: Accepts both `pip_id` (API path) and
  `country_code`/`year`/`welfare_type` triplets (ad-hoc R path). Both
  converge to a PK lookup internally — no Cartesian product either way.
- **Ad-hoc users**: Call `table_maker()` directly with triplets — no
  separate `pip_lookup()` step needed (though `pip_lookup()` remains
  available as a standalone utility).

No design compromises identified. Dual-input Approach A is confirmed as
the recommended interface.
