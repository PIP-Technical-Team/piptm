library(data.table)
library(arrow)
library(jsonlite)

# ── Fixtures ──────────────────────────────────────────────────────────────────

reset_piptm_env_meta <- function() {
  env <- getNamespace("piptm")$.piptm_env
  env$arrow_root <- NULL
  env$manifest_dir <- NULL
  env$manifests <- list()
  env$current_release <- NULL
}

make_tm_fixtures_meta <- function(env = parent.frame()) {
  tmp_arrow <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "COL",
    year = 2010L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL",
    extra_cols = c("gender", "area")
  )

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow,
    country_code = "BOL",
    year = 2000L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "BOL_2000_ECH_INC_ALL"
  )

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
      year = 2010L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
      year = 2000L, welfare_type = "INC", version = "v01_v01",
      survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
  )

  write_fixture_manifest_tm(tmp_manifest, "20260401_TEST", entries)

  list(tmp_arrow = tmp_arrow, tmp_manifest = tmp_manifest)
}

activate_tm_fixtures_meta <- function(fx) {
  piptm::set_manifest_dir(fx$tmp_manifest)
  piptm::set_arrow_root(fx$tmp_arrow)
}

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
