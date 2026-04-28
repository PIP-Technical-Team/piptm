# Tests for table_maker.R
#
# Strategy: reuse write_fixture_parquet / write_fixture_manifest helpers from
# test-load-data.R (sourced via testthat helper mechanism — these are
# accessible because they live in the same test directory via local() or
# defined inline here as they are not in a helper file).
#
# All tests use temp directories and defer env cleanup so package state is
# never permanently mutated.

library(data.table)
library(arrow)
library(jsonlite)

# ---------------------------------------------------------------------------
# Re-export fixture builders (inline copies of the helpers in test-load-data.R)
# ---------------------------------------------------------------------------

write_fixture_parquet_tm <- function(arrow_root,
                                     country_code,
                                     year,
                                     welfare_type,
                                     version,
                                     pip_id,
                                     survey_acronym,
                                     n_rows  = 10L,
                                     welfare = NULL,
                                     weight  = NULL,
                                     extra_cols = character(0L)) {

  dir_path <- file.path(
    arrow_root,
    paste0("country_code=",  country_code),
    paste0("surveyid_year=", year),
    paste0("welfare_type=",  welfare_type),
    paste0("version=",       version)
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

  if (is.null(welfare)) welfare <- seq(1, by = 1, length.out = n_rows)
  if (is.null(weight))  weight  <- rep(1.0, n_rows)

  dt <- data.table(
    country_code   = country_code,
    surveyid_year  = as.integer(year),
    welfare_type   = welfare_type,
    version        = version,
    pip_id         = pip_id,
    survey_acronym = survey_acronym,
    welfare        = as.numeric(welfare),
    weight         = as.numeric(weight)
  )

  for (col in extra_cols) {
    if (col == "gender") dt[, gender := factor(
      rep_len(c("male", "female"), n_rows), levels = c("male", "female"))]
    if (col == "area")   dt[, area   := factor(
      rep(c("urban", "urban", "rural", "rural"), length.out = n_rows),
      levels = c("urban", "rural"))]
    if (col == "age")    dt[, age    := as.integer(seq(10L, by = 10L, length.out = n_rows))]
    if (col == "educat4") dt[, educat4 := factor(
      rep_len(c("Primary", "Secondary", "Tertiary (complete or incomplete)", "No education"), n_rows))]
  }

  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))
  invisible(pip_id)
}

write_fixture_manifest_tm <- function(manifest_dir, release, entries,
                                      set_current = TRUE) {
  manifest <- list(
    release      = release,
    generated_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    entries      = entries
  )
  fname <- file.path(manifest_dir, paste0("manifest_", release, ".json"))
  jsonlite::write_json(manifest, fname, auto_unbox = TRUE, pretty = TRUE)
  if (set_current) {
    jsonlite::write_json(
      list(current_release = release),
      file.path(manifest_dir, "current_release.json"),
      auto_unbox = TRUE
    )
  }
  invisible(fname)
}

reset_piptm_env <- function() {
  env <- getNamespace("piptm")$.piptm_env
  env$arrow_root      <- NULL
  env$manifest_dir    <- NULL
  env$manifests       <- list()
  env$current_release <- NULL
}

# ---------------------------------------------------------------------------
# Shared fixture factory for table_maker tests
#
# Builds three surveys:
#   S1: COL / 2010 / INC — has gender + area   (n=10, welfare 1-10)
#   S2: BOL / 2000 / INC — no extra dims       (n=10, welfare 1-10)
#   S3: COL / 2015 / INC — has age only        (n=10, welfare 1-10)
# ---------------------------------------------------------------------------

make_tm_fixtures <- function(env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_parquet_tm(
    arrow_root    = tmp_arrow,
    country_code  = "COL",
    year          = 2010L,
    welfare_type  = "INC",
    version       = "v01_v01",
    pip_id        = "COL_2010_ECH_INC_ALL",
    survey_acronym = "ECH",
    extra_cols    = c("gender", "area")
  )

  write_fixture_parquet_tm(
    arrow_root    = tmp_arrow,
    country_code  = "BOL",
    year          = 2000L,
    welfare_type  = "INC",
    version       = "v01_v01",
    pip_id        = "BOL_2000_ECH_INC_ALL",
    survey_acronym = "ECH"
  )

  write_fixture_parquet_tm(
    arrow_root    = tmp_arrow,
    country_code  = "COL",
    year          = 2015L,
    welfare_type  = "INC",
    version       = "v01_v01",
    pip_id        = "COL_2015_ECH_INC_ALL",
    survey_acronym = "ECH",
    extra_cols    = c("age")
  )

  entries <- list(
    list(pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
         year = 2010L, welfare_type = "INC", version = "v01_v01",
         survey_acronym = "ECH", module = "ALL",
         dimensions = list("gender", "area")),
    list(pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
         year = 2000L, welfare_type = "INC", version = "v01_v01",
         survey_acronym = "ECH", module = "ALL",
         dimensions = list()),
    list(pip_id = "COL_2015_ECH_INC_ALL", survey_id = "S3", country_code = "COL",
         year = 2015L, welfare_type = "INC", version = "v01_v01",
         survey_acronym = "ECH", module = "ALL",
         dimensions = list("age"))
  )

  write_fixture_manifest_tm(tmp_manifest, "20260206", entries)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

# Activate fixtures and return a cleanup function
activate_tm_fixtures <- function(fx) {
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
}

# ===========================================================================
# pip_lookup() tests
# ===========================================================================

test_that("pip_lookup() resolves a single triplet correctly", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  ids <- piptm::pip_lookup("COL", 2010L, "INC")
  expect_identical(ids, "COL_2010_ECH_INC_ALL")
})

test_that("pip_lookup() resolves multiple triplets", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  ids <- piptm::pip_lookup(
    country_code = c("COL", "BOL"),
    year         = c(2010L, 2000L),
    welfare_type = c("INC", "INC")
  )
  expect_length(ids, 2L)
  expect_true("COL_2010_ECH_INC_ALL" %in% ids)
  expect_true("BOL_2000_ECH_INC_ALL" %in% ids)
})

test_that("pip_lookup() warns on unmatched triplet", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_warning(
    ids <- piptm::pip_lookup("ZZZ", 1999L, "INC"),
    regexp = "not found in manifest"
  )
  expect_length(ids, 0L)
})

test_that("pip_lookup() errors on mismatched vector lengths", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::pip_lookup(c("COL", "BOL"), 2010L, c("INC", "INC")),
    regexp = "same length"
  )
})

# ===========================================================================
# table_maker() — dual-input dispatch
# ===========================================================================

test_that("table_maker() accepts pip_id directly", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )
  expect_s3_class(res, "data.table")
  expect_true(all(res$pip_id == "COL_2010_ECH_INC_ALL"))
})

test_that("table_maker() accepts triplets and produces same result as pip_id path", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res_id <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )
  res_triplet <- piptm::table_maker(
    country_code = "COL",
    year         = 2010L,
    welfare_type = "INC",
    measures     = "mean"
  )
  expect_equal(res_id$value, res_triplet$value)
  expect_equal(res_id$measure, res_triplet$measure)
})

test_that("table_maker() errors when neither pip_id nor triplets are provided", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(measures = "mean"),
    regexp = "Provide either"
  )
})

test_that("table_maker() errors when only some triplet params are provided", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(country_code = "COL", measures = "mean"),
    regexp = "Provide either"
  )
})

test_that("table_maker() prefers pip_id when both pip_id and triplets supplied", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  # pip_id points to COL_2010; triplets point to BOL_2000
  # Result should use COL_2010 only
  res <- piptm::table_maker(
    pip_id       = "COL_2010_ECH_INC_ALL",
    country_code = "BOL",
    year         = 2000L,
    welfare_type = "INC",
    measures     = "mean"
  )
  expect_true(all(res$pip_id == "COL_2010_ECH_INC_ALL"))
})

# ===========================================================================
# table_maker() — output shape and column order
# ===========================================================================

test_that("table_maker() returns correct column order without by", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id        = "COL_2010_ECH_INC_ALL",
    measures      = c("headcount", "mean"),
    poverty_lines = 5.0
  )
  expected_leading <- c("pip_id", "country_code", "surveyid_year", "welfare_type",
                         "poverty_line", "measure", "value", "population")
  expect_identical(names(res), expected_leading)
})

test_that("table_maker() returns correct column order with by", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    by       = c("gender", "area")
  )
  expected <- c("pip_id", "country_code", "surveyid_year", "welfare_type",
                "gender", "area", "poverty_line", "measure", "value", "population")
  expect_identical(names(res), expected)
})

test_that("table_maker() returns data for each survey when multiple pip_ids given", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    measures = "mean"
  )
  expect_true("COL_2010_ECH_INC_ALL" %in% res$pip_id)
  expect_true("BOL_2000_ECH_INC_ALL" %in% res$pip_id)
})

# ===========================================================================
# table_maker() — measures and poverty lines
# ===========================================================================

test_that("table_maker() returns correct welfare mean for known fixture data", {
  # Fixture welfare = 1:10, weight = 1 each → mean = 5.5
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )
  row <- res[measure == "mean"]
  expect_equal(row$value, 5.5, tolerance = 1e-9)
})

test_that("table_maker() returns one row per poverty line for poverty measures", {
  # 2 poverty lines × 1 measure = 2 rows (no by)
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id        = "COL_2010_ECH_INC_ALL",
    measures      = "headcount",
    poverty_lines = c(3.0, 7.0)
  )
  expect_equal(nrow(res), 2L)
  expect_setequal(res$poverty_line, c(3.0, 7.0))
})

test_that("table_maker() sets poverty_line = NA for non-poverty measures", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = c("mean", "gini")
  )
  expect_true(all(is.na(res$poverty_line)))
})

test_that("table_maker() returns correct headcount for poverty line = 5 (50% poor)", {
  # welfare = 1:10, poverty_line = 5 → rows 1-4 poor (< 5) → headcount = 4/10
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id        = "COL_2010_ECH_INC_ALL",
    measures      = "headcount",
    poverty_lines = 5.0
  )
  expect_equal(res$value, 0.4, tolerance = 1e-9)
})

# ===========================================================================
# table_maker() — by dimensions
# ===========================================================================

test_that("table_maker() cross-tabulates by gender and area", {
  # 10 rows: gender alternates male/female, area alternates urban/rural
  # 4 combinations: male/urban, male/rural, female/urban, female/rural
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    by       = c("gender", "area")
  )
  expect_true("gender" %in% names(res))
  expect_true("area"   %in% names(res))
  expect_equal(nrow(res), 4L)
})

test_that("table_maker() bins age and returns age_group column", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  # COL_2015_ECH_INC_ALL has age dimension
  res <- piptm::table_maker(
    pip_id   = "COL_2015_ECH_INC_ALL",
    measures = "mean",
    by       = "age"
  )
  expect_true("age_group" %in% names(res))
  expect_false("age" %in% names(res))
  expect_true(all(res$age_group %in% c("0-14", "15-24", "25-64", "65+", NA)))
})

# ===========================================================================
# table_maker() — dimension pre-filter (partial / zero overlap)
# ===========================================================================

test_that("table_maker() warns and fills NA for partial dimension match", {
  # COL_2010 has gender+area; BOL_2000 has no dimensions
  # Request by=c("gender") → BOL has zero overlap → dropped with warning
  # COL has full match → kept
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_warning(
    res <- piptm::table_maker(
      pip_id   = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
      measures = "mean",
      by       = "gender"
    ),
    regexp = "Excluding"
  )
  # Only COL survives
  expect_true(all(res$pip_id == "COL_2010_ECH_INC_ALL"))
})

test_that("table_maker() fills missing dim with NA for partial match survey", {
  # COL_2010 has gender+area; COL_2015 has age only
  # Request by=c("gender") → COL_2015 has 0 overlap → dropped
  # Request by=c("area") + "gender":
  # Build a special fixture where S3 has gender but not area
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  write_fixture_parquet_tm(tmp_arrow, "COL", 2010L, "INC", "v01_v01",
                            "COL_2010_ECH_INC_ALL", "ECH",
                            extra_cols = c("gender", "area"))
  write_fixture_parquet_tm(tmp_arrow, "PER", 2010L, "INC", "v01_v01",
                            "PER_2010_ECH_INC_ALL", "ECH",
                            extra_cols = c("gender"))  # area missing

  entries <- list(
    list(pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1",
         country_code = "COL", year = 2010L, welfare_type = "INC",
         version = "v01_v01", survey_acronym = "ECH", module = "ALL",
         dimensions = list("gender", "area")),
    list(pip_id = "PER_2010_ECH_INC_ALL", survey_id = "S2",
         country_code = "PER", year = 2010L, welfare_type = "INC",
         version = "v01_v01", survey_acronym = "ECH", module = "ALL",
         dimensions = list("gender"))  # partial: missing area
  )
  write_fixture_manifest_tm(tmp_manifest, "20260206", entries)
  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_piptm_env())

  # PER has gender but not area → partial match warning
  expect_warning(
    res <- piptm::table_maker(
      pip_id   = c("COL_2010_ECH_INC_ALL", "PER_2010_ECH_INC_ALL"),
      measures = "mean",
      by       = c("gender", "area")
    ),
    regexp = "missing some requested dimensions"
  )

  # Both surveys in result
  expect_true("COL_2010_ECH_INC_ALL" %in% res$pip_id)
  expect_true("PER_2010_ECH_INC_ALL" %in% res$pip_id)

  # PER rows have NA in area
  per_rows <- res[pip_id == "PER_2010_ECH_INC_ALL"]
  expect_true(all(is.na(per_rows$area)))
})

test_that("table_maker() drops and warns for zero-overlap survey", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  # BOL_2000 has no dimensions → zero overlap with by="gender"
  expect_warning(
    res <- piptm::table_maker(
      pip_id   = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
      measures = "mean",
      by       = "gender"
    ),
    regexp = "Excluding"
  )
  expect_false("BOL_2000_ECH_INC_ALL" %in% res$pip_id)
})

test_that("table_maker() errors when ALL surveys have zero dimension overlap", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  # BOL_2000 has no dimensions → zero overlap
  expect_error(
    suppressWarnings(piptm::table_maker(
      pip_id   = "BOL_2000_ECH_INC_ALL",
      measures = "mean",
      by       = "gender"
    )),
    regexp = "All requested surveys were excluded"
  )
})

# ===========================================================================
# table_maker() — validation errors
# ===========================================================================

test_that("table_maker() errors on unknown measure name", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(pip_id = "COL_2010_ECH_INC_ALL", measures = "NOT_A_MEASURE"),
    regexp = "Unknown measure"
  )
})

test_that("table_maker() errors on poverty measure without poverty_lines", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(pip_id = "COL_2010_ECH_INC_ALL", measures = "headcount"),
    regexp = "poverty"
  )
})

test_that("table_maker() errors on >4 dimensions", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(
      pip_id   = "COL_2010_ECH_INC_ALL",
      measures = "mean",
      by       = c("gender", "area", "age", "educat4", "educat5")
    ),
    regexp = "4"
  )
})

test_that("table_maker() errors on multiple education columns", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(
      pip_id   = "COL_2010_ECH_INC_ALL",
      measures = "mean",
      by       = c("educat4", "educat5")
    ),
    regexp = "education"
  )
})

test_that("table_maker() errors on pip_id not found in manifest", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(pip_id = "ZZZ_9999_FAKE_INC_ALL", measures = "mean"),
    regexp = "No matching surveys"
  )
})

# ===========================================================================
# table_maker() — JSON serialization
# ===========================================================================

test_that("table_maker() output serializes to valid JSON with na='null'", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res  <- piptm::table_maker(
    pip_id        = "COL_2010_ECH_INC_ALL",
    measures      = c("headcount", "mean"),
    poverty_lines = 5.0
  )
  json <- jsonlite::toJSON(res, na = "null")
  parsed <- jsonlite::fromJSON(json)

  expect_true(is.data.frame(parsed))
  expect_true("measure" %in% names(parsed))
  expect_true("value"   %in% names(parsed))
  # Non-poverty rows should have null poverty_line in JSON
  expect_true(any(is.na(parsed$poverty_line)))
})
