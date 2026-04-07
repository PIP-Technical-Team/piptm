---
date: 2026-04-07
title: "Partition directory keys don't match data column names"
category: "bugs"
type: "bug"
language: "R"
tags: [arrow, parquet, hive-partitioning, partition-keys, pipdata, piptm]
root-cause: "Hive partition directory prefixes used 'country=' and 'year=' instead of the actual column names 'country_code=' and 'surveyid_year='"
severity: "P1"
test-written: "yes"
fix-confirmed: "yes"
---

# Partition Directory Keys Don't Match Data Column Names

## Symptom

Parquet files were written into directories named `country=<cc>/year=<yr>/...`
but the actual column names in the data are `country_code` and `surveyid_year`.
Arrow's Hive partitioning requires the directory prefix to match the column name
exactly. The mismatch would cause `open_dataset()` to either fail, produce wrong
results, or expose the Hive partition columns under the wrong names when reading
back the data.

## Root Cause

Four hardcoded string prefixes were wrong:

| Location | Old (wrong) | New (correct) |
|---|---|---|
| `pipdata/R/arrow_generation.R` — `.build_partition_dir()` | `"country="` / `"year="` | `"country_code="` / `"surveyid_year="` |
| `pipdata/R/arrow_generation.R` — `rel_path` in `write_survey_parquet()` | `"country="` / `"year="` | `"country_code="` / `"surveyid_year="` |
| `pipdata/R/manifest_generation.R` — `.derive_parquet_path()` | `"country="` / `"year="` | `"country_code="` / `"surveyid_year="` |
| `piptm/R/validate_parquet.R` — `.vp_parse_partition_path()` | `extract("country")` / `extract("year")` | `extract("country_code")` / `extract("surveyid_year")` |

The parser in `piptm` also looked for the wrong prefixes, so path-matching
validation checks would silently return `NA` instead of the true values,
meaning partition-path consistency checks never fired.

## Reproduction Test

Added to `pipdata/tests/testthat/test-arrow-generation.R`:

```r
test_that(".build_partition_dir returns a 4-level Hive path including version=", {
  result <- pipdata:::.build_partition_dir(
    arrow_repo_path = "/arrow",
    country_code    = "COL",
    surveyid_year   = 2010L,
    welfare_type    = "INC",
    version         = "v01_v02"
  )
  expect_true(grepl("country_code=COL",   result))
  expect_true(grepl("surveyid_year=2010", result))
  # Must NOT use old names
  expect_false(grepl("country=COL", result))
  expect_false(grepl("/year=2010",  result))
})

test_that(".build_partition_dir path components are in correct order", {
  result <- gsub("\\\\", "/", pipdata:::.build_partition_dir(...))
  parts  <- strsplit(result, "/")[[1L]]
  parts  <- parts[nchar(parts) > 0L]
  expect_match(parts[length(parts) - 3], "^country_code=")
  expect_match(parts[length(parts) - 2], "^surveyid_year=")
})
```

## Fix

Changed all partition-directory prefix strings and the path parser across
`{pipdata}` and `{piptm}`:

### `pipdata/R/arrow_generation.R` — `.build_partition_dir()`
```r
# Before
paste0("country=",  country_code),
paste0("year=",     surveyid_year),

# After
paste0("country_code=",  country_code),
paste0("surveyid_year=", surveyid_year),
```

Same change applied to the `rel_path` literal inside `write_survey_parquet()`.

### `pipdata/R/manifest_generation.R` — `.derive_parquet_path()`
```r
# Before
paste0("country=",  country_code),
paste0("year=",     surveyid_year),

# After
paste0("country_code=",  country_code),
paste0("surveyid_year=", surveyid_year),
```

### `piptm/R/validate_parquet.R` — `.vp_parse_partition_path()`
```r
# Before
country_raw <- extract("country")
year_raw    <- extract("year")

# After
country_raw <- extract("country_code")
year_raw    <- extract("surveyid_year")
```

All test fixture helpers (`write_valid_parquet`, `write_fixture_parquet`) and
hardcoded path strings in the test files were updated to match.

## Lessons Learned

**Arrow Hive partition key names must be identical to the corresponding data
column names.** Arrow uses the directory segment name (the part before `=`) as
the partition column name when materialising the dataset schema. If the
directory name and the column name differ, Arrow either creates a second
column or silently produces NULL for the partition column.

**Anti-pattern:** Using short or abbreviated directory names (`country`, `year`)
when the actual column is named more explicitly (`country_code`,
`surveyid_year`). Always derive partition directory prefixes directly from the
schema definition — never type them as free-form strings.

**Pattern to follow:** Define a single canonical list of `<column_name>=` prefix
strings (ideally derived programmatically from `pip_arrow_schema()`) and use
that list in *both* the writer and the parser. This makes it impossible for them
to drift.

## Related

None.
