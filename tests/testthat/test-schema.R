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

test_that("pip_arrow_schema contains all 8 required columns", {
  s         <- pip_arrow_schema()
  req_names <- names(Filter(function(f) isTRUE(f$required), s$fields))
  expected  <- c(
    "country_code", "surveyid_year", "welfare_type", "version",
    "survey_id", "survey_acronym", "welfare", "weight"
  )
  expect_setequal(req_names, expected)
})

# ===========================================================================
# pip_required_cols()
# ===========================================================================

test_that("pip_required_cols returns exactly 8 columns including version", {
  cols <- pip_required_cols()
  expect_length(cols, 8L)
  expect_true("version" %in% cols)
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

test_that("pip_allowed_cols includes all required and optional columns", {
  cols     <- pip_allowed_cols()
  expected <- c(
    "country_code", "surveyid_year", "welfare_type", "version",
    "survey_id", "survey_acronym", "welfare", "weight",
    "gender", "area", "educat4", "educat5", "educat7", "age"
  )
  expect_setequal(cols, expected)
})

test_that("pip_allowed_cols does not include education", {
  expect_false("education" %in% pip_allowed_cols())
})

test_that("pip_allowed_cols returns 14 columns total", {
  expect_length(pip_allowed_cols(), 14L)
})
