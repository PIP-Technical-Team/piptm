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

# Fixture builders write_fixture_parquet_tm() and write_fixture_manifest_tm()
# are defined in tests/testthat/helper-fixtures.R (auto-loaded by testthat).

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
    extra_cols    = c("gender", "area")
  )

  write_fixture_parquet_tm(
    arrow_root    = tmp_arrow,
    country_code  = "BOL",
    year          = 2000L,
    welfare_type  = "INC",
    version       = "v01_v01",
    pip_id        = "BOL_2000_ECH_INC_ALL"
  )

  write_fixture_parquet_tm(
    arrow_root    = tmp_arrow,
    country_code  = "COL",
    year          = 2015L,
    welfare_type  = "INC",
    version       = "v01_v01",
    pip_id        = "COL_2015_ECH_INC_ALL",
    extra_cols    = c("age")
  )

  entries <- list(
    list(pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
         year = 2010L, welfare_type = "INC", version = "v01_v01",
         survey_acronym = "ECH", module = "ALL",
         dimensions = list("gender", "area"),
         welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L),
    list(pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
         year = 2000L, welfare_type = "INC", version = "v01_v01",
         survey_acronym = "ECH", module = "ALL",
         dimensions = list(),
         welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L),
    list(pip_id = "COL_2015_ECH_INC_ALL", survey_id = "S3", country_code = "COL",
         year = 2015L, welfare_type = "INC", version = "v01_v01",
         survey_acronym = "ECH", module = "ALL",
         dimensions = list("age"),
         welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L)
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

test_that("table_maker() batch result matches sequential single-survey calls", {
  # Regression test: Approach B (batch) must be numerically identical to
  # calling table_maker() once per survey and rbindlisting the results.
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  ids <- c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL")

  batch <- piptm::table_maker(
    pip_id        = ids,
    measures      = c("mean", "headcount", "gini"),
    poverty_lines = c(3.0, 7.0)
  )

  sequential <- data.table::rbindlist(lapply(ids, function(pid) {
    piptm::table_maker(
      pip_id        = pid,
      measures      = c("mean", "headcount", "gini"),
      poverty_lines = c(3.0, 7.0)
    )
  }), fill = TRUE)

  # Align row order before comparing
  key_cols <- c("pip_id", "measure", "poverty_line")
  data.table::setkeyv(batch,      key_cols)
  data.table::setkeyv(sequential, key_cols)

  expect_equal(batch$value,      sequential$value,      tolerance = 1e-10)
  expect_equal(batch$population, sequential$population, tolerance = 1e-10)
  expect_equal(batch$pip_id,     sequential$pip_id)
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

test_that("table_maker() drops partial-match survey (missing some dims) with warning", {
  # COL_2010 has gender+area; PER_2010 has gender only (missing area)
  # Under new behaviour PER should be excluded, not included with NA area.
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
         dimensions = list("gender", "area"),
         welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L),
    list(pip_id = "PER_2010_ECH_INC_ALL", survey_id = "S2",
         country_code = "PER", year = 2010L, welfare_type = "INC",
         version = "v01_v01", survey_acronym = "ECH", module = "ALL",
         dimensions = list("gender"),  # partial: missing area
         welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L)
  )
  write_fixture_manifest_tm(tmp_manifest, "20260206", entries)
  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_piptm_env())

  # PER has gender but not area → partial match → excluded with warning
  expect_warning(
    res <- piptm::table_maker(
      pip_id   = c("COL_2010_ECH_INC_ALL", "PER_2010_ECH_INC_ALL"),
      measures = "mean",
      by       = c("gender", "area")
    ),
    regexp = "Excluding"
  )

  # Only COL (full match) survives
  expect_true("COL_2010_ECH_INC_ALL" %in% res$pip_id)
  expect_false("PER_2010_ECH_INC_ALL" %in% res$pip_id)

  # No NA values in area column for COL rows
  expect_false(any(is.na(res$area)))
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

# ===========================================================================
# table_maker() — by = NULL aggregate mode
# ===========================================================================

test_that("table_maker() returns aggregate result when by = NULL", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id        = "COL_2010_ECH_INC_ALL",
    measures      = c("headcount", "mean"),
    poverty_lines = 2.15,
    by            = NULL
  )
  # No dimension columns present
  expect_false(any(c("area", "gender", "educat4") %in% names(res)))
  # Required output columns present
  expect_true(all(c("pip_id", "measure", "value", "population") %in% names(res)))
  # One row for mean (no poverty_line) + one row for headcount at 2.15
  expect_equal(nrow(res), 2L)
  # mean row: poverty_line is NA
  expect_true(is.na(res[measure == "mean"]$poverty_line))
  # headcount row: poverty_line = 2.15
  expect_equal(res[measure == "headcount"]$poverty_line, 2.15)
})

test_that("table_maker() aggregate population equals sum of survey weights", {
  # Fixture weight = 1 each, n = 10 → population = 10
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    by       = NULL
  )
  expect_equal(res$population, 10)
})

# ---------------------------------------------------------------------------
# ppp argument — table_maker() pass-through
# ---------------------------------------------------------------------------

#' Write a multi-welfare Parquet + manifest fixture for table_maker ppp tests.
make_tm_ppp_fixtures <- function(env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  dp <- file.path(
    tmp_arrow,
    "country_code=COL", "surveyid_year=2010",
    "welfare_type=INC",  "version=v01_v01"
  )
  dir.create(dp, recursive = TRUE)
  dt <- data.table::data.table(
    country_code   = "COL", surveyid_year  = 2010L,
    welfare_type   = "INC", version = "v01_v01",
    pip_id         = "COL_2010_ECH_INC_ALL",
    welfare_lcu              = seq(100, by = 100, length.out = 10L),
    welfare_ppp_2021_01_02   = seq(3,   by = 1,   length.out = 10L),
    welfare_ppp_2017_01_02   = seq(1,   by = 1,   length.out = 10L),
    welfare_ppp_2011_01_01   = seq(2,   by = 1,   length.out = 10L),
    weight         = rep(1.0, 10L)
  )
  arrow::write_parquet(dt, file.path(dp, "data.parquet"))

  entries <- list(list(
    pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S",
    country_code = "COL", year = 2010L, welfare_type = "INC",
    version = "v01_v01", survey_acronym = "ECH", module = "ALL",
    dimensions = list(),
    welfare_vars = list("welfare_lcu", "welfare_ppp_2021_01_02", "welfare_ppp_2017_01_02", "welfare_ppp_2011_01_01"),
    ppp_sort = 2021L
  ))
  write_fixture_manifest_tm(tmp_manifest, "20260206", entries)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

test_that("table_maker() ppp=2017 computes on welfare_ppp_2017_01_02 column", {
  fx <- make_tm_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    ppp      = 2017L
  )
  expect_s3_class(res, "data.table")
  # welfare_ppp_2017_01_02 = 1..10, weight = 1 each → mean = 5.5
  expect_equal(res$value, 5.5)
})

test_that("table_maker() default ppp=2021 computes on welfare_ppp_2021 column", {
  fx <- make_tm_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_piptm_env())

  # No explicit ppp — uses the default 2021L
  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean"
  )
  # welfare_ppp_2021_01_02 = 3..12, weight = 1 each → mean = 7.5
  expect_equal(res$value, 7.5)
})

test_that("table_maker() ppp=2011 computes on welfare_ppp_2011_01_01 column", {
  fx <- make_tm_ppp_fixtures()
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    ppp      = 2011L
  )
  # welfare_ppp_2011_01_01 = 2..11, mean = 6.5
  expect_equal(res$value, 6.5)
})

# ---------------------------------------------------------------------------
# Column pruning — table_maker() passes only needed cols to load_surveys()
# ---------------------------------------------------------------------------

test_that("table_maker() filter_base errors on invalid variable names", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(
      pip_id       = "COL_2010_ECH_INC_ALL",
      measures     = "mean",
      filter_base  = list(not_a_real_dim = 1L)
    ),
    regexp = "Invalid .*filter_base.*variable"
  )
})

test_that("table_maker() filter_base excludes surveys missing required dimensions", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_warning(
    res <- piptm::table_maker(
      pip_id      = c("COL_2010_ECH_INC_ALL", "COL_2015_ECH_INC_ALL"),
      measures    = "mean",
      filter_base = list(age = 20L)
    ),
    regexp = "Excluding .*filter_base"
  )

  expect_true(all(res$pip_id == "COL_2015_ECH_INC_ALL"))
})

test_that("table_maker() filter_base all-excluded case aborts loudly", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    piptm::table_maker(
      pip_id      = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
      measures    = "mean",
      filter_base = list(age = 20L)
    ),
    regexp = "All requested surveys were excluded: none have all required .*filter_base.* dimensions"
  )
})

test_that("table_maker() result does not expose extra Parquet columns (column pruning)", {
  # Build a fixture with an extra column that should never reach the output.
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()

  dir_path <- file.path(
    tmp_arrow,
    "country_code=COL", "surveyid_year=2010",
    "welfare_type=INC",  "version=v01_v02"
  )
  dir.create(dir_path, recursive = TRUE)

  n <- 10L
  dt_file <- data.table::data.table(
    country_code   = "COL",
    surveyid_year  = 2010L,
    welfare_type   = "INC",
    version        = "v01_v02",
    pip_id         = "COL_2010_ECH_INC_ALL",
    survey_acronym = "ECH",
    welfare_ppp_2021_01_02 = seq_len(n) * 1.0,
    weight         = rep(1.0, n),
    gender         = rep(c("male", "female"), length.out = n),
    # extra column not used in any computation — should not appear in output
    extra_unused   = rep("noise", n)
  )
  arrow::write_parquet(dt_file, file.path(dir_path, "data.parquet"))

  entries_list <- list(list(
    pip_id         = "COL_2010_ECH_INC_ALL",
    survey_id      = "COL_2010_ECH_v01_M_v02_A_GMD_ALL",
    country_code   = "COL",
    year           = 2010L,
    welfare_type   = "INC",
    version        = "v01_v02",
    survey_acronym = "ECH",
    module         = "ALL",
    dimensions     = list("gender"),
    welfare_vars   = list("welfare_ppp_2021_01_02"),
    ppp_sort       = 2021L
  ))

  write_fixture_manifest_tm(tmp_manifest, "20260206", entries_list, set_current = TRUE)
  piptm::set_manifest_dir(tmp_manifest)
  piptm::set_arrow_root(tmp_arrow)
  withr::defer(reset_piptm_env())

  res <- piptm::table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = "mean",
    by       = "gender"
  )

  expect_s3_class(res, "data.table")
  # output is long-format — extra_unused must never appear
  expect_false("extra_unused" %in% names(res))
  expect_true("gender" %in% names(res))
  expect_true("value"  %in% names(res))
})

# ===========================================================================
# pop_share_threshold suppression
# ===========================================================================

test_that("pop_share_threshold suppresses non-share measures for small cells", {
  # S1 fixture: 10 rows, 5 male / 5 female, equal weight → 50/50 split.
  # With threshold 0.6, both cells (0.5 each) are below → headcount suppressed.
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_warning(
    res <- table_maker(
      pip_id   = "COL_2010_ECH_INC_ALL",
      measures = c("pop_share", "headcount"),
      by       = "gender",
      poverty_lines = 5,
      pop_share_threshold = 0.6
    ),
    "Suppressing measures"
  )

  # pop_share rows retained
  expect_equal(nrow(res[measure == "pop_share"]), 2L)
  # headcount rows suppressed (both cells below 0.6)
  expect_equal(nrow(res[measure == "headcount"]), 0L)
})

test_that("pop_share_threshold = NULL disables suppression", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  res <- table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = c("pop_share", "headcount"),
    by       = "gender",
    poverty_lines = 5,
    pop_share_threshold = NULL
  )

  # All rows present: 2 pop_share + 2 headcount
  expect_equal(nrow(res), 4L)
})

test_that("no suppression when pop_share not in measures", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  # Even with low threshold, if pop_share isn't requested, no suppression
  res <- table_maker(
    pip_id   = "COL_2010_ECH_INC_ALL",
    measures = c("headcount"),
    by       = "gender",
    poverty_lines = 5,
    pop_share_threshold = 0.99
  )

  expect_equal(nrow(res[measure == "headcount"]), 2L)
})

test_that("obs_share rows are retained when cell is suppressed", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_warning(
    res <- table_maker(
      pip_id   = "COL_2010_ECH_INC_ALL",
      measures = c("pop_share", "obs_share", "mean"),
      by       = "gender",
      pop_share_threshold = 0.6
    ),
    "Suppressing"
  )

  # Both share measures retained (2 each), mean suppressed
  expect_equal(nrow(res[measure == "pop_share"]), 2L)
  expect_equal(nrow(res[measure == "obs_share"]), 2L)
  expect_equal(nrow(res[measure == "mean"]), 0L)
})

test_that("pop_share_threshold validation rejects invalid values", {
  fx <- make_tm_fixtures()
  activate_tm_fixtures(fx)
  withr::defer(reset_piptm_env())

  expect_error(
    table_maker(
      pip_id = "COL_2010_ECH_INC_ALL",
      measures = c("pop_share", "mean"),
      by = "gender",
      pop_share_threshold = 1.5
    ),
    "pop_share_threshold"
  )
})
