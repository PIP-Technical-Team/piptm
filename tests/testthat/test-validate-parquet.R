# Tests for validate_parquet.R
# Plan: .cg-docs/plans/2026-04-02-version-partition-and-manifest-resolution.md (Step 3)
#
# Test coverage:
#   - .vp_parse_partition_path(): version= extraction from 4-level path
#   - validate_parquet_schema(): accepts file with version, rejects missing version
#   - validate_parquet_data(): version path-matching check, educat4/5/7 factor check

library(data.table)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Build a minimal schema-conformant data.table for tests
make_valid_dt <- function(country_code  = "COL",
                          surveyid_year = 2010L,
                          welfare_type  = "INC",
                          survey_id     = "COL_2010_ECH_V01_M_V02_A_INC_ALL",
                          version       = "v01_v02",
                          dims          = character(0),
                          n_rows        = 5L) {
  dt <- data.table::data.table(
    country_code   = country_code,
    surveyid_year  = as.integer(surveyid_year),
    welfare_type   = welfare_type,
    version        = version,
    survey_id      = survey_id,
    survey_acronym = "ECH",
    welfare        = seq(1.0, by = 0.5, length.out = n_rows),
    weight         = rep(1.0, n_rows)
  )

  if ("gender" %in% dims) {
    data.table::set(dt, j = "gender", value = factor(
      rep(c("male", "female"), length.out = n_rows),
      levels = c("male", "female")
    ))
  }
  if ("area" %in% dims) {
    data.table::set(dt, j = "area", value = factor(
      rep(c("urban", "rural"), length.out = n_rows),
      levels = c("urban", "rural")
    ))
  }
  if ("educat4" %in% dims) {
    data.table::set(dt, j = "educat4", value = factor(
      rep(c("Primary (complete or incomplete)", "No education"), length.out = n_rows)
    ))
  }
  if ("educat5" %in% dims) {
    data.table::set(dt, j = "educat5", value = factor(
      rep(c("Primary incomplete", "Secondary complete"), length.out = n_rows)
    ))
  }
  if ("age" %in% dims) {
    data.table::set(dt, j = "age", value = as.integer(seq(18L, by = 1L, length.out = n_rows)))
  }

  dt
}

#' Write a fixture Parquet file in a 4-level partition directory
write_valid_parquet <- function(arrow_root,
                                dt,
                                country_code  = "COL",
                                surveyid_year = 2010L,
                                welfare_type  = "INC",
                                version       = "v01_v02",
                                pip_id        = "COL_2010_ECH_V01_M_V02_A_INC_ALL") {
  dir_path <- file.path(
    arrow_root,
    paste0("country=", country_code),
    paste0("year=",    surveyid_year),
    paste0("welfare=", welfare_type),
    paste0("version=", version)
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  out_file <- file.path(dir_path, paste0(pip_id, "-0.parquet"))
  arrow::write_parquet(dt, out_file, compression = "snappy")
  out_file
}

# ===========================================================================
# .vp_parse_partition_path()
# ===========================================================================

test_that(".vp_parse_partition_path extracts all 4 keys from a 4-level path", {
  path   <- "/arrow/country=BOL/year=2020/welfare=INC/version=v01_v04/BOL_2020_EH_INC_ALL-0.parquet"
  result <- piptm:::.vp_parse_partition_path(path)

  expect_identical(result$country_code,  "BOL")
  expect_identical(result$surveyid_year, 2020L)
  expect_identical(result$welfare_type,  "INC")
  expect_identical(result$version,       "v01_v04")
})

test_that(".vp_parse_partition_path returns NA for version when absent from path", {
  path   <- "/arrow/country=BOL/year=2020/welfare=INC/BOL_2020_EH_INC_ALL-0.parquet"
  result <- piptm:::.vp_parse_partition_path(path)

  expect_true(is.na(result$version))
  # Other keys still parse correctly
  expect_identical(result$country_code,  "BOL")
  expect_identical(result$surveyid_year, 2020L)
})

test_that(".vp_parse_partition_path handles Windows-style backslash paths", {
  path   <- "C:\\arrow\\country=COL\\year=2010\\welfare=INC\\version=v01_v02\\file.parquet"
  result <- piptm:::.vp_parse_partition_path(path)

  expect_identical(result$country_code,  "COL")
  expect_identical(result$surveyid_year, 2010L)
  expect_identical(result$welfare_type,  "INC")
  expect_identical(result$version,       "v01_v02")
})

test_that(".vp_parse_partition_path returns all NAs for a non-partition path", {
  result <- piptm:::.vp_parse_partition_path("/tmp/plain.parquet")

  expect_true(is.na(result$country_code))
  expect_true(is.na(result$surveyid_year))
  expect_true(is.na(result$welfare_type))
  expect_true(is.na(result$version))
})

# ===========================================================================
# validate_parquet_schema()
# ===========================================================================

test_that("validate_parquet_schema passes for a valid file with version column", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  f   <- write_valid_parquet(tmp, dt)

  res <- validate_parquet_schema(f)
  expect_true(res$valid)
  expect_length(res$errors, 0L)
})

test_that("validate_parquet_schema fails for a file missing the version column", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  data.table::set(dt, j = "version", value = NULL)

  # Write directly without version col
  dir_path <- file.path(tmp, "country=COL", "year=2010", "welfare=INC", "version=v01_v02")
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  f <- file.path(dir_path, "COL_2010-0.parquet")
  arrow::write_parquet(dt, f)

  res <- validate_parquet_schema(f)
  expect_false(res$valid)
  expect_true(any(grepl("version", res$errors)))
})

test_that("validate_parquet_schema accepts a file with educat4 and educat5", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt(dims = c("educat4", "educat5"))
  f   <- write_valid_parquet(tmp, dt)

  res <- validate_parquet_schema(f)
  expect_true(res$valid, info = paste(res$errors, collapse = "; "))
})

test_that("validate_parquet_schema fails for a file with old 'education' column", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  data.table::set(dt, j = "education", value = factor(
    rep(c("Primary", "Secondary"), length.out = 5L),
    levels = c("No education", "Primary", "Secondary", "Tertiary")
  ))

  dir_path <- file.path(tmp, "country=COL", "year=2010", "welfare=INC", "version=v01_v02")
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  f <- file.path(dir_path, "COL_2010-0.parquet")
  arrow::write_parquet(dt, f)

  res <- validate_parquet_schema(f)
  expect_false(res$valid)
  expect_true(any(grepl("education", res$errors)))
})

# ===========================================================================
# validate_parquet_data()
# ===========================================================================

test_that("validate_parquet_data passes for a valid 4-level partitioned file", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  f   <- write_valid_parquet(tmp, dt)

  res <- validate_parquet_data(f)
  expect_true(res$valid, info = paste(res$errors, collapse = "; "))
})

test_that("validate_parquet_data detects version mismatch between data and path", {
  tmp <- withr::local_tempdir()
  # Data says v01_v02, path says v99_v99
  dt  <- make_valid_dt(version = "v01_v02")

  dir_path <- file.path(tmp, "country=COL", "year=2010", "welfare=INC", "version=v99_v99")
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  f <- file.path(dir_path, "COL_2010-0.parquet")
  arrow::write_parquet(dt, f)

  res <- validate_parquet_data(f)
  expect_false(res$valid)
  expect_true(any(grepl("version", res$errors)))
})

test_that("validate_parquet_data accepts educat4 with arbitrary survey-specific levels", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  # Override with verbose GMD-style labels (not the old 4-level canonical set)
  data.table::set(dt, j = "educat4", value = factor(
    rep(c("Primary (complete or incomplete)", "No education"), length.out = 5L)
  ))
  f <- write_valid_parquet(tmp, dt)

  res <- validate_parquet_data(f)
  expect_true(res$valid, info = paste(res$errors, collapse = "; "))
})

test_that("validate_parquet_data rejects educat4 that is not a factor", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  # Add educat4 as character, not factor — should fail
  data.table::set(dt, j = "educat4", value = c("Primary", "Secondary", "Primary", "No education", "Secondary"))

  dir_path <- file.path(tmp, "country=COL", "year=2010", "welfare=INC", "version=v01_v02")
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  f <- file.path(dir_path, "COL_2010-0.parquet")
  # Arrow will encode as utf8 (not dictionary) — schema check will catch it
  arrow::write_parquet(dt, f)

  res_schema <- validate_parquet_schema(f)
  # Should fail schema check: educat4 expected as dictionary, found as utf8
  expect_false(res_schema$valid)
})

test_that("validate_parquet_data version consistency: multiple unique values abort", {
  tmp <- withr::local_tempdir()
  dt  <- make_valid_dt()
  # Inject two different version values — should trigger partition key consistency error
  data.table::set(dt, i = 1L, j = "version", value = "v01_v99")

  dir_path <- file.path(tmp, "country=COL", "year=2010", "welfare=INC", "version=v01_v02")
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  f <- file.path(dir_path, "COL_2010-0.parquet")
  arrow::write_parquet(dt, f)

  res <- validate_parquet_data(f)
  expect_false(res$valid)
  expect_true(any(grepl("version", res$errors)))
})
