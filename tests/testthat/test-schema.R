# Tests for schema.R
# Plan: .cg-docs/plans/2026-04-02-version-partition-and-manifest-resolution.md (Step 2)
#
# Test coverage:
#   - pip_arrow_schema(): version field present, educat4/5/7 present,
#     education absent, levels correct
#   - pip_required_cols(): version included, 8 required cols
#   - pip_allowed_cols(): educat4/5/7 included, education absent, 14 cols total

# ===========================================================================
# pip_arrow_schema()
# ===========================================================================

test_that("pip_arrow_schema returns a list with fields and levels", {
  s <- pip_arrow_schema()
  expect_type(s, "list")
  expect_named(s, c("fields", "levels"))
})

test_that("pip_arrow_schema includes version as a required utf8 field", {
  s <- pip_arrow_schema()
  expect_true("version" %in% names(s$fields))
  expect_true(s$fields$version$required)
  expect_identical(s$fields$version$type$ToString(), arrow::utf8()$ToString())
})

test_that("pip_arrow_schema includes educat4, educat5, educat7 as optional dict fields", {
  s        <- pip_arrow_schema()
  dict_str <- arrow::dictionary(arrow::int32(), arrow::utf8())$ToString()
  for (col in c("educat4", "educat5", "educat7")) {
    expect_true(col %in% names(s$fields), label = paste(col, "in fields"))
    expect_false(s$fields[[col]]$required, label = paste(col, "is optional"))
    expect_identical(
      s$fields[[col]]$type$ToString(), dict_str,
      label = paste(col, "is dictionary type")
    )
  }
})

test_that("pip_arrow_schema does NOT contain education field", {
  s <- pip_arrow_schema()
  expect_false("education" %in% names(s$fields))
})

test_that("pip_arrow_schema$levels does not contain education", {
  s <- pip_arrow_schema()
  expect_false("education" %in% names(s$levels))
})

test_that("pip_arrow_schema$levels contains gender and area with correct values", {
  s <- pip_arrow_schema()
  expect_identical(s$levels$gender, c("male", "female"))
  expect_identical(s$levels$area,   c("urban", "rural"))
})

test_that("pip_arrow_schema contains all 6 required columns", {
  s         <- pip_arrow_schema()
  req_names <- names(Filter(function(f) isTRUE(f$required), s$fields))
  expected  <- c(
    "country_code", "surveyid_year", "welfare_type", "version",
    "pip_id", "weight"
  )
  expect_setequal(req_names, expected)
})

# ===========================================================================
# pip_required_cols()
# ===========================================================================

test_that("pip_required_cols returns exactly 6 columns including version", {
  cols <- pip_required_cols()
  expect_length(cols, 6L)
  expect_true("version" %in% cols)
  expect_false("welfare" %in% cols)
})

test_that("pip_required_cols does not include optional columns", {
  cols <- pip_required_cols()
  for (opt in c("gender", "area", "educat4", "educat5", "educat7", "age")) {
    expect_false(opt %in% cols, label = paste(opt, "not in required"))
  }
})

# ===========================================================================
# pip_allowed_cols()
# ===========================================================================

test_that("pip_allowed_cols includes all required and optional base columns", {
  cols     <- pip_allowed_cols()
  expected <- c(
    "country_code", "surveyid_year", "welfare_type", "version",
    "pip_id", "weight",
    "gender", "area", "educat4", "educat5", "educat7", "age"
  )
  expect_setequal(cols, expected)
})

test_that("pip_allowed_cols does not include education", {
  expect_false("education" %in% pip_allowed_cols())
})

test_that("pip_allowed_cols does not include welfare column", {
  expect_false("welfare" %in% pip_allowed_cols())
})

test_that("pip_allowed_cols returns 12 base columns total", {
  expect_length(pip_allowed_cols(), 12L)
})

test_that("pip_allowed_cols with welfare_vars appends welfare columns", {
  wv   <- c("welfare_lcu", "welfare_ppp_2017_01_02")
  cols <- pip_allowed_cols(welfare_vars = wv)
  expect_length(cols, 14L)
  expect_true("welfare_lcu"            %in% cols)
  expect_true("welfare_ppp_2017_01_02" %in% cols)
})

# ===========================================================================
# pip_welfare_schema()
# ===========================================================================

test_that("pip_welfare_schema returns float64 field specs for all supplied cols", {
  wv  <- c("welfare_lcu", "welfare_ppp_2017_01_02")
  wfs <- pip_welfare_schema(wv)
  expect_named(wfs, wv)
  for (nm in wv) {
    expect_identical(wfs[[nm]]$type$ToString(), arrow::float64()$ToString())
    expect_true(wfs[[nm]]$required)
  }
})

test_that("pip_welfare_schema errors on empty welfare_vars", {
  expect_error(pip_welfare_schema(character(0L)), regexp = "non-empty")
  expect_error(pip_welfare_schema(NULL),          regexp = "non-empty")
})

test_that("pip_welfare_schema handles single column", {
  wfs <- pip_welfare_schema("welfare_lcu")
  expect_length(wfs, 1L)
  expect_named(wfs, "welfare_lcu")
})
