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

test_that("with_meta = TRUE data field is a data.table", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    with_meta = TRUE
  )

  expect_true(data.table::is.data.table(res$data))
  expect_setequal(unique(res$data$measure), c("mean", "gini"))
})

test_that("with_meta = TRUE specification contains labeled inputs", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    by = c("gender", "area"),
    with_meta = TRUE
  )

  spec <- res$specification
  expect_true(is.list(spec))
  expect_equal(spec$pip_id, "COL_2010_ECH_INC_ALL")
  expect_true(is.list(spec$analysis_var))
  expect_true("name" %in% names(spec$analysis_var))
  expect_true("label" %in% names(spec$analysis_var))
  expect_equal(spec$analysis_var$name, "welfare")
  expect_true(is.list(spec$measures))
  expect_equal(length(spec$measures), 2)
  expect_true(all(vapply(spec$measures, function(m) all(c("name", "label") %in% names(m)), logical(1))))
  expect_equal(spec$ppp, 2021L)
  expect_null(spec$poverty_line)
  expect_true(is.list(spec$by))
  expect_equal(length(spec$by), 2)
})

test_that("with_meta = TRUE execution tracks included surveys", {
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
  expect_true(is.list(exec))
  expect_true(is.data.table(exec$included_surveys))
  expect_true("pip_id" %in% names(exec$included_surveys))
  expect_equal(exec$included_surveys$pip_id, "COL_2010_ECH_INC_ALL")
  expect_true(is.data.table(exec$excluded_surveys))
  expect_equal(nrow(exec$excluded_surveys), 0)
})

test_that("with_meta = TRUE execution tracks excluded surveys", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  expect_warning(
    res <- piptm::table_maker(
      pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
      analysis_var = "welfare",
      measures = "mean",
      by = "gender",
      with_meta = TRUE
    ),
    "Excluding"
  )

  exec <- res$execution
  expect_true(nrow(exec$excluded_surveys) > 0)
  expect_true("pip_id" %in% names(exec$excluded_surveys))
  expect_true("reason" %in% names(exec$excluded_surveys))
  expect_equal(exec$excluded_surveys$pip_id, "BOL_2000_ECH_INC_ALL")
})

test_that("with_meta = TRUE provenance contains release and version", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  prov <- res$provenance
  expect_true(is.list(prov))
  expect_true("release" %in% names(prov))
  expect_true("package_version" %in% names(prov))
})

test_that("with_meta = TRUE warnings field is a list", {
  fx <- make_tm_fixtures_meta()
  activate_tm_fixtures_meta(fx)
  withr::defer(reset_piptm_env_meta())

  res <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    with_meta = TRUE
  )

  expect_true(is.list(res$warnings))
})
