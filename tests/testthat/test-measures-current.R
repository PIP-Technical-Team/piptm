library(testthat)

.with_covariate_registry <- function(code) {
  env <- get(".piptm_env", envir = asNamespace("piptm"))
  old_reg <- env$registries
  old_rel <- env$current_release
  rel <- old_rel
  if (is.null(rel) || !nzchar(rel)) {
    rel <- "TEST_RELEASE"
    env$current_release <- rel
  }
  reg <- list(
    pov_status = list(varname = "pov_status", ui_label = "Poverty status", tm_type = "poverty", roles = c("covariate"), n_categories = 2L),
    gender = list(varname = "gender", ui_label = "Gender", tm_type = "binary", roles = c("covariate"), n_categories = 2L),
    area = list(varname = "area", ui_label = "Area", tm_type = "binary", roles = c("covariate"), n_categories = 2L),
    wquintile = list(varname = "wquintile", ui_label = "Welfare quintile", tm_type = "categorical", roles = c("covariate"), n_categories = 5L),
    educat4 = list(varname = "educat4", ui_label = "Education 4", tm_type = "categorical", roles = c("covariate"), n_categories = 4L)
  )
  env$registries <- list()
  env$registries[[rel]] <- reg
  on.exit({ env$registries <- old_reg; env$current_release <- old_rel }, add = TRUE)
  force(code)
}

test_that(".classify_measures groups mixed requests in canonical order", {
  out <- piptm:::.classify_measures(c("headcount", "gini", "mean", "pop_share"))
  expect_equal(names(out), c("poverty", "inequality", "summary_stats", "shares"))
})

test_that(".classify_measures errors on unknown measure", {
  expect_error(piptm:::.classify_measures(c("mean", "bad_measure")), "Unknown measure")
})

test_that(".validate_poverty_lines enforces poverty-line requirements", {
  expect_error(piptm:::.validate_poverty_lines(NULL, families = "poverty"), "require")
  expect_error(piptm:::.validate_poverty_lines("2.15", families = "poverty"), "numeric")
  expect_error(piptm:::.validate_poverty_lines(c(2.15, -1), families = "poverty"), "positive finite")
  expect_silent(piptm:::.validate_poverty_lines(2.15, families = "poverty"))
})

test_that(".validate_by accepts known covariates", {
  .with_covariate_registry({
    expect_silent(piptm:::.validate_by(NULL))
    expect_silent(piptm:::.validate_by(c("gender", "area")))
  })
})

test_that(".validate_by rejects unknown and >4 dimensions", {
  .with_covariate_registry({
    expect_error(piptm:::.validate_by("not_a_dim"), "Unknown dimension")
    expect_error(piptm:::.validate_by(c("gender", "area", "wquintile", "educat4", "pov_status")), "At most 4")
  })
})

test_that(".validate_by warns on unavailable survey dimensions", {
  .with_covariate_registry({
    expect_warning(piptm:::.validate_by(c("gender", "area"), dimensions = c("gender")), "not available")
  })
})
