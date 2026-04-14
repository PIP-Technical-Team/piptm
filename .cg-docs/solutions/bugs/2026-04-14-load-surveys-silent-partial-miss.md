---
date: 2026-04-14
title: "load_surveys() silently skips missing partitions when path helper is not shared"
category: "bugs"
type: "bug"
language: "R"
tags: [arrow, parquet, load_surveys, partial-miss, hive-partitioning, data-loading, piptm]
root-cause: "load_surveys() used its own inline list.files() loop while load_survey_microdata() used a shared helper; the inline loop returned an empty vector for missing partitions rather than erroring, so missing surveys were silently dropped"
severity: "P2"
test-written: "yes"
fix-confirmed: "yes"
---

# `load_surveys()` Silently Skips Missing Partitions

## Symptom

When a manifest entry referred to a partition directory that did not exist on
disk, `load_surveys()` returned a result that silently omitted the missing
survey — no error, no warning. The returned `data.table` contained fewer rows
than expected with no indication that any surveys had been dropped.

`load_survey_microdata()` (single-survey variant) correctly errored for the
same missing partition, because it used a dedicated helper that calls
`cli::cli_abort()` when `list.files()` returns an empty vector.

## Root Cause

The two loading functions used different backends for Parquet file discovery:

- `load_survey_microdata()` called `.build_parquet_paths()`, which errors on
  an empty result.
- `load_surveys()` had its own inline `lapply` + `list.files()` loop that was
  introduced in an earlier performance refactor. This loop silently returned
  `character(0)` for missing directories and `unlist()` collapsed those gaps
  away, so `open_dataset()` was called on a reduced file list with no
  indication that any surveys were missing.

The divergence was introduced because the performance refactor rewrote only
`load_surveys()` and did not extract a shared helper. The two functions then
drifted silently.

## Reproduction Test

```r
test_that("load_surveys() errors when a manifest entry has no Parquet files on disk", {
  tmp <- withr::local_tempdir()
  arrow_root <- file.path(tmp, "arrow")

  # Write fixture for COL/2010/INC (exists on disk)
  col_leaf <- file.path(
    arrow_root,
    "country_code=COL", "surveyid_year=2010", "welfare_type=INC", "version=v01_v01"
  )
  dir.create(col_leaf, recursive = TRUE)
  arrow::write_parquet(
    data.frame(welfare = 1, weight = 1, pip_id = "COL_2010_ENCV_INC_ALL"),
    file.path(col_leaf, "part-0.parquet")
  )

  # Manifest includes BOL/2015/CON — NO directory on disk
  mf <- data.table::data.table(
    country_code = c("COL",                    "BOL"),
    year         = c(2010L,                    2015L),
    welfare_type = c("INC",                    "CON"),
    version      = c("v01_v01",               "v01_v01"),
    pip_id       = c("COL_2010_ENCV_INC_ALL", "BOL_2015_EH_CON_ALL"),
    dimensions   = list(c("area"), c("area"))
  )

  withr::with_options(
    list(piptm.arrow_root = arrow_root, piptm.manifest_dir = NULL),
    {
      piptm::set_arrow_root(arrow_root)
      expect_error(piptm::load_surveys(mf), regexp = "No Parquet files found")
    }
  )
})
```

## Fix

Extracted a single `.build_parquet_paths()` internal helper used by **both**
`load_survey_microdata()` and `load_surveys()`. The helper:

1. Constructs the exact Hive leaf path from the four partition keys.
2. Calls `list.files(leaf, pattern = "\\.parquet$", full.names = TRUE, recursive = FALSE)`.
3. Errors immediately via `cli::cli_abort()` if the result is empty.

```r
.build_parquet_paths <- function(arrow_root, country_code, year,
                                  welfare_type, version) {
  leaf <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  files <- list.files(leaf, pattern = "\\.parquet$",
                      full.names = TRUE, recursive = FALSE)
  if (length(files) == 0L) {
    cli::cli_abort(c(
      "No Parquet files found for {.val {country_code}} / {year} / {.val {welfare_type}} / version {.val {version}}.",
      "i" = "Expected partition path: {.path {leaf}}",
      "i" = "Arrow root: {.path {arrow_root}}"
    ))
  }
  files
}
```

`load_surveys()` path collection now calls it per row:

```r
parquet_files <- unlist(lapply(seq_len(nrow(entries_dt)), function(i) {
  .build_parquet_paths(
    arrow_root   = arrow_root,
    country_code = entries_dt$country_code[[i]],
    year         = entries_dt$year[[i]],
    welfare_type = entries_dt$welfare_type[[i]],
    version      = entries_dt$version[[i]]
  )
}))
```

An additional integrity check was added after `collect()` to catch any
unexpected `pip_id` values that might have leaked from adjacent partitions:

```r
loaded_ids <- unique(dt$pip_id)
unexpected <- setdiff(loaded_ids, entries_dt$pip_id)
if (length(unexpected) > 0L) {
  cli::cli_abort(c(
    "Loaded unexpected survey(s): {.val {unexpected}}.",
    "i" = "This may indicate partition path contamination or a manifest mismatch."
  ))
}
```

## Prevention

**Rule**: When two functions share identical logic for resolving or discovering
file paths, extract a shared internal helper immediately. Never let two
functions drift with independent inline implementations of the same resolution
step.

**Anti-pattern**: Refactoring one function for performance while leaving the
other untouched. The two diverge silently and error-handling invariants are
lost.

**Pattern to follow**:
- Name shared helpers with a leading dot (`.build_parquet_paths`) to signal
  internal-only status.
- The helper is the single place where "no files found" is an error. Neither
  caller needs to repeat this check.
- Always write a partial-miss regression test (one entry present, one missing)
  for any batch loading function.

## Related

- `.cg-docs/solutions/bugs/2026-04-07-partition-key-prefix-mismatch.md` — related
  Arrow/Hive partition path bug (wrong key prefix names)
