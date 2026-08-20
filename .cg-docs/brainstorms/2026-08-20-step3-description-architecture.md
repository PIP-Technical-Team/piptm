---
date: 2026-08-20
title: "Step 3 Description File - Architecture Revision"
status: decided
scope: "Standard"
artifact-schema-version: 1
chosen-approach: "Sidecar with_meta + description_model + markdown renderer"
tags: [ui, api, documentation, step-3, metadata, architecture]
---

# Step 3 Description File - Architecture Revision

## Context

This brainstorm revises and extends the Aug 13 brainstorm (`2026-08-13-step3-description-file.md`), which chose "Structured Markdown with R-side Generation" (Approach 1). The original approach had `build_table_description()` accept the same parameters as `table_maker()` and re-derive everything from raw arguments.

**Problem with the original approach:** `build_table_description()` risks becoming a second implementation of `table_maker()` - re-resolving manifests, re-validating, re-loading surveys, just to reconstruct description metadata. This is wasteful and fragile: the description must report what *actually happened* (e.g., 5 surveys requested, 3 included, 2 excluded) which requires re-running the full validation/loading pipeline from raw arguments.

**New insight:** The description needs the *executed truth* - what `table_maker()` actually did during computation - not a re-derivation from raw inputs. This requires a richer return type from `table_maker()` that captures specification, execution metadata, and provenance alongside the computed data.

**Three key decisions to resolve:**
1. What `table_maker()` should return to feed the description template
2. What the description template contains (sections) and how it gets populated
3. How the `/description` endpoint returns the description and how the UI renders it

## Requirements

### What the description must convey

1. **What each cell is** - which statistic, which population subset, which filters
2. **How the table is structured** - which dimensions are in rows vs. columns
3. **What happened during computation** - which surveys were included/excluded and why
4. **What methodological choices were made** - poverty line, PPP year, suppression threshold

### Design constraints

- `table_maker()` must NOT break the existing `/table` endpoint (JSON contract unchanged)
- The description must be built from the *executed* result, not re-derived from arguments
- Markdown is the first renderer but NOT the permanent source of truth
- Only the most important info - no interpretation, no formula details, no survey links
- The template must be maintainable and scalable - adding a new statistic should not require editing conditional branches in the description function
- Backward compatibility: existing 414+ tests must not break

## Architecture Decision

### Decision 1: What `table_maker()` returns - Sidecar `with_meta = TRUE`

**Chosen:** A sidecar variant that leaves the default return untouched.

```r
# Default - unchanged, backward-compatible
table_maker(pip_id, analysis_var, measures, ...)
# Returns: data.table (existing long-format output)

# With metadata - new variant for description and future audit
table_maker(pip_id, analysis_var, measures, ..., with_meta = TRUE)
# Returns: list(
#   data          = <data.table>,
#   specification = list(...),
#   execution     = list(...),
#   provenance    = list(...),
#   warnings      = list(...)
# )
```

**Why this over S3 wrapper:** The S3 approach (returning a `piptm_table_result` class) would require changing the default return type of `table_maker()`, which breaks `is.data.table()`, `nrow()`, and other downstream code across 414+ existing tests. The sidecar variant is zero-risk to existing consumers - `with_meta = TRUE` is opt-in and the default path is identical to today.

**Fields in the `with_meta` return:**

| Field | Contents | Populated from |
|-------|----------|----------------|
| `data` | Existing long-format data.table | Current pipeline output |
| `specification` | Normalized, labeled inputs | `table_maker()` arguments + registry lookups |
| `execution` | What actually happened | Manifest join results, dimension pre-filter, load_surveys(), compute_measures(), suppression logic |
| `provenance` | Package version, release ID | Package environment |
| `warnings` | Structured warning records | Existing `cli_warn()` calls, refactored to structured records |

**`specification` contents:**
```r
list(
  pip_id           = c("COL_2019_GEIH_INC_ALL", ...),
  analysis_var     = list(name = "welfare", label = "Welfare"),
  measures         = list(
    list(name = "mean", label = "Mean welfare", family = "summary_stats"),
    list(name = "gini", label = "Gini index", family = "inequality")
  ),
  poverty_line     = NULL,
  ppp              = 2021L,
  by               = list(
    list(name = "gender", label = "Gender", role = "rows",
         categories = list(list(code = "0", label = "Female"), list(code = "1", label = "Male"))),
    list(name = "area", label = "Area", role = "columns",
         categories = list(list(code = "0", label = "Urban"), list(code = "1", label = "Rural")))
  ),
  filter_base      = NULL,
  pop_share_threshold = 0.01
)
```

**`execution` contents:**
```r
list(
  included_surveys = data.table(
    pip_id = c("COL_2019_GEIH_INC_ALL", ...),
    country_code = "COL", surveyid_year = 2019L, welfare_type = "INC"
  ),
  excluded_surveys = data.table(
    pip_id = character(0), reason = character(0)
  ),
  filters_applied  = NULL,
  measures_computed = c("mean", "gini"),
  suppression      = list(threshold = 0.01, n_suppressed_cells = 0L),
  ppp_used         = "welfare_ppp_2021"
)
```

**Implementation approach:** `table_maker()` already performs all the work (manifest lookup, dimension pre-filter, load, compute, suppression). The `with_meta` path captures this information along the way using lightweight state variables, then bundles them into the return list. This is NOT a second pass - it is the same computation with metadata harvested as a side effect.

---

### Decision 2: Description template content and population

**Chosen:** A `description_model` (structured list) with conditional sections, populated from the `table_result`, rendered to markdown by a renderer function.

**Key principle:** The description model is an intermediate representation - NOT markdown itself. Markdown is one renderer. The model is the reusable, testable artifact.

**Sections and their sources:**

| Section | Source in `table_result` | Condition |
|---------|-------------------------|-----------|
| `metadata` | `provenance` (release, package version, PPP year) | always |
| `surveys` | `execution$included_surveys` + `execution$excluded_surveys` | always; excluded shown only if non-empty |
| `sample` | `specification$filter_base` with labels, or "full survey sample" | always |
| `statistics` | `specification$analysis_var` + `specification$measures` with labels | always |
| `poverty_line` | `specification$poverty_line` | only if non-NULL |
| `layout` | `specification$by` mapped to roles | only if `by` non-NULL |
| `cell_definition` | generated from `by` + `analysis_var` + `measures` | always |
| `suppression` | `execution$suppression` | always (adjusts message) |
| `warnings` | `execution$warnings` | only if non-empty |

**How sections get populated:**

The `description_model` is built by `build_description_model(table_result)`, which reads *labeled* data from the registry rather than raw codes. The existing registry functions (`piptm_filter_categories()`, `piptm_layout_covariates()`, `piptm_stat_groups()`) provide label translations.

**Cell definition generation** - programmatic from the layout:
- 0 dimensions: "Statistics are computed for the full survey sample."
- 1+ dimensions: "Each cell represents one combination of [var1], [var2]..."
- With poverty line: "...using a poverty line of $X.XX per day (PPP year)."

**Template mechanism:** T1 (R functions with conditional logic) for the first iteration. The `description_model` is the structured, testable artifact; `render_description_markdown(model)` is a thin R function that walks the model and emits sections.

---

### Decision 3: Endpoint and UI rendering

**Chosen:** `GET /description` returns `{model, markdown}`. Stateless - accepts same params as `/table`, calls `table_maker(with_meta = TRUE)` internally.

**Endpoint contract:**

```
GET /description?pip_id=COL_2019_GEIH_INC_ALL&analysis_var=welfare&measures=mean&measures=gini&by=gender&by=area&ppp=2021

Response:
{
  "status": "success",
  "data": {
    "model": { ... },
    "markdown": "# Table Description\n\n..."
  },
  "warnings": [],
  "errors": [],
  "meta": {
    "release": "20260206",
    "n_surveys": 1
  }
}
```

**Why stateless recompute is acceptable:** Description requests are infrequent (user clicks "view description" once per table). The `table_maker()` call for 15 surveys runs in ~0.12s per the charter.

**UI rendering:**
- UI receives `{model, markdown}` from `/description`
- Renders `markdown` using a markdown viewer component
- "Download PDF" button uses browser print-to-PDF
- If the UI later needs structured access, the `model` field is already there

---

## Approach Comparison

### Approach A: `build_table_description()` from raw arguments (original Aug 13)

**Summary:** Takes same parameters as `table_maker()` and re-derives everything.

**Pros:** Simplest to implement - no changes to `table_maker()`.

**Cons:** Re-derives manifest lookup, validation, survey resolution - duplicated logic. Cannot report what actually happened. Fragile.

**Effort:** Small (2-3 days)

**Recommended?** No - duplication risk and inability to report execution reality.

---

### Approach B: S3 `table_result` wrapper (rejected)

**Summary:** Change `table_maker()` return type to an S3 class.

**Pros:** Clean separation, polymorphic rendering.

**Cons:** Breaks existing tests, `/table` contract, high regression risk.

**Effort:** Medium-large (5-7 days)

**Recommended?** No - disproportionate regression risk.

---

### Approach C: Sidecar `with_meta = TRUE` (CHOSEN)

**Summary:** Default return unchanged. `with_meta = TRUE` returns richer list.

**Pros:** Zero risk, same execution truth, incremental adoption, clean S3 migration path.

**Cons:** Two return types depending on flag.

**Effort:** Medium (3-5 days)

**Recommended?** Yes - delivers description capability with minimal risk.

---

## Mock Output

### Simple scenario

**Input:** Single survey, welfare, 2 dimensions, no filters.

```markdown
# Table Description

**Generated:** 2026-08-20 - **Release:** 20260206 - **PPP year:** 2021

## Surveys Analyzed

| Country | Year | Welfare type | Survey ID |
|---------|------|--------------|-----------|
| Colombia | 2019 | Income | COL_2019_GEIH_INC_ALL |

## Sample Base

The full survey sample was used (no filters applied).

## Statistics

- **Analysis variable:** Welfare
- **Measures:** Mean welfare, Gini index

## Table Structure

- **Rows:** Gender (2 categories: Female, Male)
- **Columns:** Area (2 categories: Urban, Rural)

Each cell represents one combination of gender and area. Statistics are calculated for the weighted population in that subgroup within the full survey sample.

## Cell Definition

Each cell shows mean welfare and Gini index for one gender-area combination, weighted by survey sampling weights.

## Suppression

Cells with a population share below 1% are suppressed (non-share measures removed). No cells were suppressed in this table.
```

### Complex scenario

**Input:** 3 surveys, poverty analysis, 4 layout slots, sample-base filters, 2 excluded.

```markdown
# Table Description

**Generated:** 2026-08-20 - **Release:** 20260206 - **PPP year:** 2021

## Surveys Analyzed

| Country | Year | Welfare type | Survey ID |
|---------|------|--------------|-----------|
| Colombia | 2019 | Income | COL_2019_GEIH_INC_ALL |
| Bolivia | 2018 | Income | BOL_2018_ECH_INC_ALL |
| Peru | 2019 | Income | PER_2019_ENAHO_INC_ALL |

### Excluded Surveys

| Country | Year | Survey ID | Reason |
|---------|------|-----------|--------|
| Brazil | 2019 | BRA_2019_PNADC_INC_ALL | Missing dimension: educat4 |
| Mexico | 2020 | MEX_2020_ENIGH_INC_ALL | Missing dimensions: educat4, gender |

2 of 5 requested surveys were excluded because they lack one or more requested disaggregation dimensions.

## Sample Base

Filtered to:
- **Age group:** 15 to 24, 25 to 64, 65 and above (children under 15 excluded)

## Statistics

- **Analysis variable:** Poverty status
- **Poverty line:** $6.85 per day (PPP 2021)
- **Measures:** Poverty headcount, Poverty gap index, Population share

## Table Structure

- **Super Rows:** Age group (3 categories: 15-24, 25-64, 65+)
- **Rows:** Education level (4 categories: None, Primary, Secondary, Tertiary)
- **Super Columns:** Area (2 categories: Urban, Rural)
- **Columns:** Gender (2 categories: Female, Male)

Each cell represents one unique combination of education level, gender, area, and age group. Statistics are calculated for the weighted population in that subgroup within the specified sample base.

## Cell Definition

Each cell shows poverty headcount, poverty gap index, and population share for one combination of education level, gender, area, and age group, using a poverty line of $6.85 per day (PPP 2021). Statistics are weighted by survey sampling weights. Cells with a population share below 1% are suppressed.

## Suppression

Cells with a population share below 1% are suppressed (non-share measures removed). No cells were suppressed in this table.
```

---

## Next Steps

### R Package (`piptm`)

1. **Implement `with_meta` flag in `table_maker()`** (`R/table_maker.R`)
   - Add `with_meta = FALSE` parameter
   - When TRUE: capture specification (with registry labels), execution metadata (included/excluded surveys, applied filters, suppression), and provenance during the existing pipeline
   - Return `list(data, specification, execution, provenance, warnings)` instead of bare data.table
   - Default path (`with_meta = FALSE`) returns data.table unchanged

2. **Create `build_description_model()` function** (`R/description.R`)
   - Accept `table_result` (the `with_meta = TRUE` return)
   - Read labeled data from registry functions
   - Build structured list with conditional sections
   - Return the `description_model`

3. **Create `render_description_markdown()` function** (`R/description.R`)
   - Accept `description_model`
   - Walk sections, emit markdown
   - Return character string

4. **Create `build_table_description()` convenience function** (`R/description.R`)
   - Accept same parameters as `table_maker()`
   - Internally calls `table_maker(with_meta = TRUE)`, then `build_description_model()`, then `render_description_markdown()`
   - Returns markdown string directly

5. **Write unit tests**

6. **Documentation**

### API Layer

7. **New endpoint: `GET /description`** (`inst/plumber/plumber.R`)
   - Query parameters: same as `/table` endpoint
   - Calls `piptm::table_maker(with_meta = TRUE)`
   - Returns `{status, data: {model, markdown}, warnings, errors, meta}`

8. **Update API documentation**

### Testing & Validation

9. **Integration testing**
10. **User acceptance**

### Future Enhancements (Out of Scope)

- S3 `table_result` class wrapping the sidecar list
- Structured warning codes
- Server-side PDF generation
- Reproducibility code snippets
- Multiple languages
- Survey documentation links
- Interpretation notes section

## Devil's Advocate Validation

**Problem validation:** Pre-validated. Users need to understand, share, and cite complex tables.

**Simplicity check:** Sidecar `with_meta = TRUE` is the simplest approach satisfying both execution-truth and backward-compatibility.

**Effort-value check:** ~1-2 days over Approach A eliminates duplication risk and unlocks execution-vs-requested distinction. Proportional to value.

**Charter alignment:**
- Reproducibility: description documents all inputs
- Deterministic outputs: timestamps omitted from reproducible description
- Interoperability: `/table` contract unchanged
- Scalability: description model is renderer-agnostic

**Decision reversibility:** Highly reversible. Sidecar list can become S3 wrapper fields. Dead `with_meta` code removable without affecting default path.
