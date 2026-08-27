# Test: Description helper functions

test_that("format_welfare_type() maps codes to labels correctly", {
  expect_equal(format_welfare_type("INC"), "Income")
  expect_equal(format_welfare_type("CON"), "Consumption")
  expect_equal(format_welfare_type(c("INC", "CON")), c("Income", "Consumption"))
})


test_that("format_welfare_type() falls back to input for unknown codes", {
  result <- format_welfare_type("UNKNOWN")
  expect_equal(result, "UNKNOWN")
  mixed <- format_welfare_type(c("INC", "UNKNOWN", "CON"))
  expect_equal(mixed, c("Income", "UNKNOWN", "Consumption"))
})


test_that("format_slot_label() maps internal slots to display labels", {
  expect_equal(format_slot_label("columns"), "Columns")
  expect_equal(format_slot_label("rows"), "Rows")
  expect_equal(format_slot_label("super_columns"), "Super Columns")
  expect_equal(format_slot_label("super_rows"), "Super Rows")
})


test_that("format_slot_label() handles vector input", {
  slots <- c("columns", "rows")
  result <- format_slot_label(slots)
  expect_equal(result, c("Columns", "Rows"))
})


test_that("format_covariate_description() formats single covariate", {
  covariates_dt <- data.table(
    slot = "columns",
    varname = "gender",
    ui_label = "Gender",
    n_categories = 2L
  )
  
  result <- format_covariate_description("gender", covariates_dt)
  expect_equal(result, "Gender")
})


test_that("format_covariate_description() formats two covariates with × separator", {
  covariates_dt <- data.table(
    slot = c("columns", "rows"),
    varname = c("gender", "area"),
    ui_label = c("Gender", "Area"),
    n_categories = c(2L, 2L)
  )
  
  result <- format_covariate_description(c("gender", "area"), covariates_dt)
  expect_equal(result, "Gender × Area")
})


test_that("format_covariate_description() formats three covariates", {
  covariates_dt <- data.table(
    slot = c("columns", "rows", "super_columns"),
    varname = c("gender", "area", "education"),
    ui_label = c("Gender", "Area", "Education"),
    n_categories = c(2L, 2L, 3L)
  )
  
  result <- format_covariate_description(c("gender", "area", "education"), covariates_dt)
  expect_equal(result, "Gender × Area × Education")
})


test_that("format_covariate_description() returns empty string for NULL input", {
  covariates_dt <- data.table(
    slot = character(0),
    varname = character(0),
    ui_label = character(0),
    n_categories = integer(0)
  )
  
  result <- format_covariate_description(NULL, covariates_dt)
  expect_equal(result, "")
})


test_that("format_covariate_description() returns empty string for empty vector", {
  covariates_dt <- data.table(
    slot = character(0),
    varname = character(0),
    ui_label = character(0),
    n_categories = integer(0)
  )
  
  result <- format_covariate_description(character(0), covariates_dt)
  expect_equal(result, "")
})


test_that("format_covariate_description() falls back to varname when ui_label missing", {
  covariates_dt <- data.table(
    slot = "columns",
    varname = "unknown_var",
    ui_label = NA_character_,
    n_categories = 2L
  )
  
  result <- format_covariate_description("unknown_var", covariates_dt)
  expect_equal(result, "unknown_var")
})


test_that("format_covariate_description() falls back to varname when not in registry", {
  covariates_dt <- data.table(
    slot = character(0),
    varname = character(0),
    ui_label = character(0),
    n_categories = integer(0)
  )
  
  result <- format_covariate_description("unlisted_var", covariates_dt)
  expect_equal(result, "unlisted_var")
})


test_that("format_covariate_description() handles missing covariate table", {
  result <- format_covariate_description(c("x", "y"), NULL)
  expect_equal(result, "x × y")
})
