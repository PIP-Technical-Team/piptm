---
date: 2026-03-23
title: "Pre-Arrow Cleaning & Standardization Specification"
status: active
applies_to: "{pipdata} arrow_generation.R (Step 3)"
language: R
tags: [phase-0, cleaning, standardization, pipdata, breakdown-dimensions]
---

# Pre-Arrow Cleaning & Standardization Specification

## Purpose

This document defines the rules for transforming a {pipdata} clean survey dataset (`.qs2` file) into a standardized data.table that is ready to be written as a Parquet file in the Master Arrow Repository.

The output of this step is the **direct input to `arrow::write_parquet()`**. The schema the Parquet file must conform to is defined separately in `inst/schema/arrow-schema.json`.

**This document defines *how* to produce correct values from the inputs. It does not define the schema itself.**

---

## Inputs

Each conversion in {pipdata} receives two objects:

- **`data`** a data.table of row-level microdata (welfare, weight, breakdown dimensions, etc.)
- **`metadata`** a named list of survey-level identifiers (`country_code`, `surveyid_year`, `survey_acronym`, `survey_id`, `welfare_type`, etc.)

These two objects are always available together. No schema column is derived from any other source.
---

## Data Flow

```
data (microdata) + metadata (survey identifiers)
         
1. Column selection and injection
        
2. Type casting
         
3. Breakdown dimension standardization
         
4. Pre-write validation
        
Standardized data.table -> write_parquet()
```

---

## Step 1: Column Selection and Injection

Only columns defined in `arrow-schema.json` may appear in the output. All other columns must be dropped.

Required columns come from two distinct sources:

### 1A: Columns sourced from `metadata`

| Output column    | Source                     | Action       | Notes |
|-----------------|---------------------------|--------------|-------|
| `country_code`  | `metadata$country_code`   | inject       | `toupper()` to ensure uppercase ISO3 |
| `surveyid_year` | `metadata$surveyid_year`  | inject       | `as.integer()` |
| `welfare_type`  | `metadata$welfare_type`   | inject+recode| Full string to code: `"income"` -> `"INC"`, `"consumption"` -> `"CON"` |
| `survey_id`     | `metadata$survey_id`      | inject       | Use as-is; `as.character()` |
| `survey_acronym`| `metadata$survey_acronym` | inject       | `toupper()` |

### 1B: Columns sourced from `data`

| Output column | Source column | Action | Notes |
|---|---|---|---|
| `welfare` | `welfare` | keep | Assumed PPP-adjusted |
| `weight`  | `weight`  | keep | Assumed clean and strictly positive |

### 1C: Optional breakdown dimension columns

Derived from data columns (see Step 3). Added only when the required source column(s) are present:

| Output column | Present if… |
|---|---|
| `gender`    | `gender` or `male` column is present |
| `area`      | `area` or `urban` column is present |
| `education` | `educat4`, `educat5`, `educat7`, or `educy` column is present |
| `age`       | `age` column is present |

**Rule**: if a breakdown column cannot be derived, omit it entirely. Do **not** include it as an all-`NA` column.

### 1.1 Injection code template

```r
wt_raw <- toupper(metadata$welfare_type)
dt[, `:=`(
  country_code   = toupper(metadata$country_code),
  surveyid_year  = as.integer(metadata$surveyid_year),
  survey_acronym = toupper(metadata$survey_acronym),
  welfare_type   = fcase(
    wt_raw == "INCOME",      "INC",
    wt_raw == "CONSUMPTION", "CON",
    default = wt_raw
  ),
  survey_id      = as.character(metadata$survey_id)
)]
```

---

## Step 2: Type Casting

Metadata-sourced columns are cast during injection (§1.1). Apply these casts to data-sourced columns:

| Column      | Target R type | Cast expression                           |
|-------------|--------------|-------------------------------------------|
| `welfare`   | double       | `as.double()`                             |
| `weight`    | double       | `as.double()`                             |
| `gender`    | factor       | see §3.1                                  |
| `area`      | factor       | see §3.2                                  |
| `education` | factor       | see §3.3                                  |
| `age`       | integer      | `as.integer()` — continuous, not a factor |

---

## Step 3: Breakdown Dimension Standardization

### 3.1 Gender

**Source columns** (in priority order):

| Column   | Type      | Expected values           | Use when… |
|----------|-----------|--------------------------|----------|
| `gender` | character | `"male"`, `"female"`     | preferred |
| `male`   | numeric   | `1` (male), `0` (female) | fallback  |

**Rules**:

1. If `gender` column is present: normalize to lowercase, restrict to `{"male", "female"}`.
2. Else if `male` column is present: recode `1 → "male"`, `0 → "female"`. Any other value → `NA`.
3. Else: omit the column.

```r
# From gender column
dt[, gender := factor(tolower(trimws(gender)), levels = c("male", "female"))]

# From male column (fallback)
dt[, gender := factor(
  fcase(male == 1L, "male", male == 0L, "female", default = NA_character_),
  levels = c("male", "female")
)]
```

**Factor levels**: `c("male", "female")` — fixed and ordered. No other values permitted.

**NA handling**: `NA` within the column is permitted. If all rows are `NA`, omit the column.

---

### 3.2 Area (Urban/Rural)

**Source columns** (in priority order):

| Column  | Type      | Expected values          | Use when… |
|---------|-----------|-------------------------|-----------|
| `area`  | character | `"urban"`, `"rural"`    | preferred |
| `urban` | numeric   | `1` (urban), `0` (rural)| fallback  |

**Rules**:

1. If `area` column is present: normalize to lowercase, restrict to `{"urban", "rural"}`.
2. Else if `urban` column is present: recode `1 → "urban"`, `0 → "rural"`. Any other value → `NA`.
3. Else: omit the column.

```r
# From area column
dt[, area := factor(tolower(trimws(area)), levels = c("urban", "rural"))]

# From urban column (fallback)
dt[, area := factor(
  fcase(urban == 1L, "urban", urban == 0L, "rural", default = NA_character_),
  levels = c("urban", "rural")
)]
```

**Factor levels**: `c("urban", "rural")` — fixed and ordered.

---

### 3.3 Education

The output is a 4-level ordered factor. Source columns are used in priority order:

| Priority | Source column | Type      | Use when… |
|----------|-------------|-----------|----------|
| 1st      | `educat4`   | character | preferred — already 4-category |
| 2nd      | `educat5`   | character | if `educat4` absent |
| 3rd      | `educat7`   | integer   | if `educat4` and `educat5` absent |
| 4th      | `educy`     | numeric   | last resort — years of education |

**Target levels** (ordered): `c("No education", "Primary", "Secondary", "Tertiary")`

#### From `educat4`

Expected string values: `"No education"`, `"Primary"`, `"Secondary"`, `"Tertiary"`.

```r
valid_levels <- c("No education", "Primary", "Secondary", "Tertiary")
dt[, education := factor(
  fifelse(educat4 %in% valid_levels, educat4, NA_character_),
  levels = valid_levels
)]
```

#### From `educat5` (fallback 1)

| `educat5` value | → `education` |
|---|---|
| `"No education"` | `"No education"` |
| `"Primary incomplete"` | `"Primary"` |
| `"Primary complete but secondary incomplete"` | `"Primary"` |
| `"Secondary complete"` | `"Secondary"` |
| `"Some tertiary/post-secondary"` | `"Tertiary"` |
| `NA` | `NA` |

```r
dt[, education := factor(fcase(
  educat5 == "No education",                                           "No education",
  educat5 %in% c("Primary incomplete",
                 "Primary complete but secondary incomplete"),         "Primary",
  educat5 == "Secondary complete",                                     "Secondary",
  educat5 == "Some tertiary/post-secondary",                          "Tertiary",
  default = NA_character_
), levels = c("No education", "Primary", "Secondary", "Tertiary"))]
```

#### From `educat7` (fallback 2)

| `educat7` value | → `education` |
|---|---|
| 1 | `"No education"` |
| 2, 3 | `"Primary"` |
| 4, 5 | `"Secondary"` |
| 6, 7 | `"Tertiary"` |
| `NA` | `NA` |

```r
dt[, education := factor(fcase(
  educat7 == 1L,           "No education",
  educat7 %in% c(2L, 3L), "Primary",
  educat7 %in% c(4L, 5L), "Secondary",
  educat7 %in% c(6L, 7L), "Tertiary",
  default = NA_character_
), levels = c("No education", "Primary", "Secondary", "Tertiary"))]
```

#### From `educy` (fallback 3 — last resort)

| Years of education | → `education` |
|---|---|
| 0 | `"No education"` |
| 1–6 | `"Primary"` |
| 7–12 | `"Secondary"` |
| ≥ 13 | `"Tertiary"` |
| `NA` | `NA` |

```r
dt[, education := factor(fcase(
  educy == 0,               "No education",
  educy >= 1 & educy <= 6,  "Primary",
  educy >= 7 & educy <= 12, "Secondary",
  educy >= 13,              "Tertiary",
  default = NA_character_
), levels = c("No education", "Primary", "Secondary", "Tertiary"))]
```

#### NA handling

`NA` in the education column is permitted — it commonly occurs for young children and individuals with missing records. Do not impute or drop these rows. If the resulting column is entirely `NA`, omit it from the output.

---

### 3.4 Age

**Source column**: `age` (numeric — continuous integer years).

**Rule**: Store as a continuous `integer` (`int32`). Age-group binning is performed at query time by {piptm}, not at ingestion.

```r
dt[, age := as.integer(age)]
```

`NA` is permitted. If the `age` column is absent, omit from output.

---

## Step 4: Pre-Write Validation

Run these checks before calling `write_parquet()`. Abort with a descriptive error on failure.

### 4.1 Required columns present

```
country_code, surveyid_year, welfare_type, survey_id,
survey_acronym, welfare, weight
```

### 4.2 Partition key consistency

All rows in a single file must share the same value for each partition key:

```r
stopifnot(
  dt[, uniqueN(country_code)]  == 1L,
  dt[, uniqueN(surveyid_year)] == 1L,
  dt[, uniqueN(welfare_type)]  == 1L
)
```

### 4.3 Welfare type values

```r
stopifnot(dt[, all(welfare_type %in% c("INC", "CON"))])
```

### 4.4 Welfare validity

Zero values are permitted (valid zero-income observations). Negative and non-finite values are not.

```r
n_zero <- dt[, sum(welfare == 0, na.rm = TRUE)]
if (n_zero > 0L) {
  rlang::warn(paste0(n_zero, " rows have welfare == 0 in survey: ", unique(dt$survey_id)))
}
stopifnot(
  dt[, all(is.finite(welfare))],
  dt[, all(welfare >= 0)]
)
```

### 4.5 Weight validity

```r
stopifnot(
  dt[, all(!is.na(weight))],
  dt[, all(is.finite(weight))],
  dt[, all(weight > 0)]
)
```

### 4.6 Country code format

```r
stopifnot(dt[, all(grepl("^[A-Z]{3}$", country_code))])
```

### 4.7 Factor level conformance

```r
valid_gender    <- c("male", "female")
valid_area      <- c("urban", "rural")
valid_education <- c("No education", "Primary", "Secondary", "Tertiary")

if ("gender" %in% names(dt)) {
  stopifnot(dt[, all(is.na(gender) | gender %in% valid_gender)])
}
if ("area" %in% names(dt)) {
  stopifnot(dt[, all(is.na(area) | area %in% valid_area)])
}
if ("education" %in% names(dt)) {
  stopifnot(dt[, all(is.na(education) | education %in% valid_education)])
}
if ("age" %in% names(dt)) {
  stopifnot(dt[, all(is.na(age) | (age >= 0L & age <= 130L))])
}
```

### 4.8 No extra columns

```r
allowed_cols <- c(
  "country_code", "surveyid_year", "welfare_type", "survey_id",
  "survey_acronym", "welfare", "weight",
  "gender", "area", "education", "age"
)
extra <- setdiff(names(dt), allowed_cols)
if (length(extra) > 0L) {
  cli::cli_abort("Unexpected columns in output: {.val {extra}}. Drop before writing.")
}
```

---

## Summary: Column Mapping

| Output column    | Source   | Source field/column              | Transformation |
|-----------------|----------|----------------------------------|----------------|
| `country_code`  | metadata | `$country_code`                  | `toupper()` |
| `surveyid_year` | metadata | `$surveyid_year`                 | `as.integer()` |
| `welfare_type`  | metadata | `$welfare_type`                  | recode to `"INC"` / `"CON"` |
| `survey_id`     | metadata | `$survey_id`                     | `as.character()` |
| `survey_acronym`| metadata | `$survey_acronym`                | `toupper()` |
| `welfare`       | data     | `welfare`                        | `as.double()` |
| `weight`        | data     | `weight`                         | `as.double()` |
| `gender`        | data     | `gender` or `male`               | see §3.1 |
| `area`          | data     | `area` or `urban`                | see §3.2 |
| `education`     | data     | `educat4` (or fallbacks)         | see §3.3 |
| `age`           | data     | `age`                            | `as.integer()` |

---

## Assumptions

| Assumption | Justification |
|---|---|
| `welfare` is already PPP-adjusted | {pipdata} ALL module applies PPP deflation before producing the `.qs2` file |
| `weight` is clean and strictly positive | {pipdata} harmonization validates sampling weights upstream |
| `metadata` is always available alongside `data` | {pipdata} loading functions return both objects together |
| `metadata$welfare_type` is a full string (`"income"`, `"consumption"`) | Observed convention in {pipdata} output; recode is applied unconditionally |

---

## Known Edge Cases

| Case | Rule |
|---|---|
| `welfare == 0` | Permitted. Warn but do not abort. |
| `NA` in education for young children or missing records | Permitted. Leave as `NA`. Do not impute. |
| Survey missing `gender`, `area`, or `age` column | Omit the column entirely; do not write an all-`NA` column. |
| Education source column present but all-`NA` | Treat as absent; omit `education` from output. |
| `welfare_type` in unexpected case or form | `toupper()` applied before recode; unknown values passed through with a warning. |
| `educat4` values with unexpected casing | Normalize with an explicit lookup before factoring. |

---

## Out of Scope

- PPP adjustment (applied upstream in {pipdata})
- Age-group binning (applied at query time in {piptm})
- Multi-survey stacking or aggregation
- Manifest generation
