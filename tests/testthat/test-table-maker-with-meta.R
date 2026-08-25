library(data.table)
library(arrow)
library(jsonlite)

# Fixtures are in helper-description.R (shared)

# ── Tests ─────────────────────────────────────────────────────────────────────

test_that("with_meta = FALSE returns data.table (backward compat)", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini")
  )

  expect_true(data.table::is.data.table(res))
  expect_true(all(c("pip_id", "measure", "value", "population") %in% names(res)))
})

test_that("with_meta = TRUE returns list with 5 fields", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    with_meta = TRUE
  )

  expect_true(is.list(res))
  expect_true(all(c("data", "specification", "execution", "provenance", "warnings") %in% names(res)))
})

test_that("requested_pip_id captured at function entry", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("requested_pip_id" %in% names(exec))
  expect_equal(exec$requested_pip_id, c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"))
})

test_that("loaded_surveys populated after all exclusions", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  # Request with dimension that one survey lacks
  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    by = "gender",  # BOL doesn't have gender
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("loaded_surveys" %in% names(exec))
  expect_true(data.table::is.data.table(exec$loaded_surveys))
  
  # Should only include COL (BOL excluded for missing gender)
  expect_equal(nrow(exec$loaded_surveys), 1L)
  expect_equal(exec$loaded_surveys$pip_id, "COL_2010_ECH_INC_ALL")
  expect_true(all(c("pip_id", "country_code", "surveyid_year", "welfare_type") %in% names(exec$loaded_surveys)))
  
  # Type and completeness checks
  expect_true(is.character(exec$loaded_surveys$pip_id))
  expect_true(is.character(exec$loaded_surveys$country_code))
  expect_true(is.integer(exec$loaded_surveys$surveyid_year) || is.numeric(exec$loaded_surveys$surveyid_year))
  expect_true(is.character(exec$loaded_surveys$welfare_type))
  expect_false(any(is.na(exec$loaded_surveys$pip_id)))
  expect_false(any(is.na(exec$loaded_surveys$country_code)))
})

test_that("excluded_surveys with stage tracking", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  # Request with dimension that BOL lacks
  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    by = "gender",
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("excluded_surveys" %in% names(exec))
  expect_true(data.table::is.data.table(exec$excluded_surveys))
  
  # BOL should be excluded
  expect_equal(nrow(exec$excluded_surveys), 1L)
  expect_equal(exec$excluded_surveys$pip_id, "BOL_2000_ECH_INC_ALL")
  expect_true(all(c("pip_id", "reason", "stage") %in% names(exec$excluded_surveys)))
  expect_equal(exec$excluded_surveys$stage, "dimension_pre")
  expect_true(grepl("Missing dimensions.*gender", exec$excluded_surveys$reason))
})

test_that("excluded_surveys manifest stage for unknown pip_id", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "UNKNOWN_ID"),
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  exec <- res$execution
  expect_equal(nrow(exec$excluded_surveys), 1L)
  expect_equal(exec$excluded_surveys$pip_id, "UNKNOWN_ID")
  expect_equal(exec$excluded_surveys$stage, "manifest")
  expect_equal(exec$excluded_surveys$reason, "Not found in manifest")
})

test_that("resolved_release tracked consistently", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  exec <- res$execution
  prov <- res$provenance
  
  expect_true("resolved_release" %in% names(exec))
  expect_equal(exec$resolved_release, prov$release)
  expect_equal(exec$resolved_release, "20260401_TEST")  # current release from fixture
})

test_that("resolved_ppp and ppp_column_used tracked", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    ppp = 2021L,
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("resolved_ppp" %in% names(exec))
  expect_true("ppp_column_used" %in% names(exec))
  expect_equal(exec$resolved_ppp, 2021L)
  expect_equal(exec$ppp_column_used, "welfare_ppp_2021_01_02")  # from fixture manifest
})

test_that("measures_computed stores measure names not families", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini", "median"),
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("measures_computed" %in% names(exec))
  expect_equal(exec$measures_computed, c("mean", "gini", "median"))
  
  # Should NOT contain family names
  expect_false("summary_stats" %in% exec$measures_computed)
  expect_false("inequality" %in% exec$measures_computed)
})

test_that("filters_applied populated from normalized_filter_base", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  # Test that filters_applied captures normalized_filter_base
  # Use a pass-through filter that includes all data (no actual filtering)
  # We test the filter logic elsewhere; here we just verify metadata capture
  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("filters_applied" %in% names(exec))
  expect_true(is.list(exec$filters_applied))
  
  # When no filter_base provided, filters_applied should be an empty list
  expect_equal(length(exec$filters_applied), 0L)
})

test_that("warnings captured via registered handler", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  # Trigger exclusion warning
  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    by = "gender",  # BOL doesn't have gender
    with_meta = TRUE
  )

  expect_true("warnings" %in% names(res))
  expect_true(length(res$warnings) > 0)
  
  # Should contain dimension exclusion warning
  warning_text <- paste(res$warnings, collapse = " ")
  expect_true(grepl("Excluding.*survey.*lack.*dimension", warning_text))
})

test_that("specification contains requested pip_id not loaded", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    by = "gender",  # BOL excluded
    with_meta = TRUE
  )

  spec <- res$specification
  exec <- res$execution
  
  # Specification should have requested (both surveys)
  expect_equal(spec$pip_id, c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"))
  
  # Execution should have only loaded (just COL)
  expect_equal(exec$requested_pip_id, c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"))
  expect_equal(nrow(exec$loaded_surveys), 1L)
  expect_equal(exec$loaded_surveys$pip_id, "COL_2010_ECH_INC_ALL")
})

test_that("new schema fields all present in execution block", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  exec <- res$execution
  expected_fields <- c(
    "requested_pip_id",
    "loaded_surveys",
    "excluded_surveys",
    "resolved_release",
    "resolved_ppp",
    "ppp_column_used",
    "filters_applied",
    "measures_computed",
    "suppression"
  )
  
  expect_true(all(expected_fields %in% names(exec)))
})

test_that("all surveys excluded returns appropriate response", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())
  
  # Request dimension that no survey has
  # We need to use a dimension that doesn't exist or create a scenario
  # where all surveys are filtered out
  # Since we can't easily create a nonexistent dimension test,
  # let's test with a scenario where all surveys lack a dimension
  res <- piptm::table_maker(
    pip_id = "BOL_2000_ECH_INC_ALL",  # BOL doesn't have gender
    analysis_var = "welfare",
    measures = "mean",
    by = "gender",
    with_meta = TRUE
  )
  
  exec <- res$execution
  expect_equal(nrow(exec$loaded_surveys), 0L)
  expect_equal(nrow(exec$excluded_surveys), 1L)
  expect_true(all(exec$excluded_surveys$stage == "dimension_pre"))
  expect_equal(exec$excluded_surveys$pip_id, "BOL_2000_ECH_INC_ALL")

  markdown <- piptm::render_description_markdown(
    piptm::build_description_model(res)
  )
  expect_match(markdown, "No surveys contributed data to this table")
  expect_false(grepl("| Country |", markdown, fixed = TRUE))
})

test_that("filter_pre stage exclusions captured", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())
  
  # Neither fixture has the age dimension, so both should be excluded before
  # Arrow loading at the filter_pre stage.
  res <- piptm::table_maker(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    filter_base = list(age_group = c(1L)),
    with_meta = TRUE
  )

  exec <- res$execution
  expect_equal(nrow(exec$loaded_surveys), 0L)
  expect_equal(nrow(exec$excluded_surveys), 2L)
  expect_true(all(exec$excluded_surveys$stage == "filter_pre"))
})

test_that("default PPP resolution uses fixed 2021 reference", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())
  
  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  exec <- res$execution
  expect_true("resolved_ppp" %in% names(exec))
  expect_identical(exec$resolved_ppp, 2021L)
  expect_identical(exec$ppp_column_used, "welfare_ppp_2021_01_02")
})

test_that("aggregate mode (by=NULL) metadata tracked correctly", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())
  
  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    by = NULL,  # Aggregate mode
    with_meta = TRUE
  )
  
  spec <- res$specification
  exec <- res$execution
  
  # Specification should reflect by=NULL
  expect_null(spec$by)
  
  # Execution should still track loaded surveys
  expect_true(data.table::is.data.table(exec$loaded_surveys))
  expect_equal(nrow(exec$loaded_surveys), 1L)
  expect_equal(exec$loaded_surveys$pip_id, "COL_2010_ECH_INC_ALL")
})
