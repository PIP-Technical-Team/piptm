library(testthat)

.registry_fixture <- function() {
  list(
    welfare = list(varname = "welfare", ui_label = "Welfare", tm_type = "welfare", roles = c("analysis_var"), stat_groups = c("summary_stats", "inequality"), n_categories = NULL, categories = NULL),
    pov_status = list(varname = "pov_status", ui_label = "Poverty status", tm_type = "poverty", roles = c("analysis_var", "covariate"), stat_groups = c("poverty"), n_categories = 2L, categories = NULL),
    age_group = list(varname = "age_group", ui_label = "Age group", tm_type = "categorical", roles = c("filter", "covariate"), stat_groups = character(0L), n_categories = 4L, categories = list(list(code = "0-14", label = "0 to 14"), list(code = "15-24", label = "15 to 24"), list(code = "25-64", label = "25 to 64"), list(code = "65+", label = "65 and above"))),
    measure_spec = list(summary_stats = list(label = "Summary Statistics", measures = list(mean = list(label = "Mean"), median = list(label = "Median"))), poverty = list(label = "Poverty", measures = list(headcount = list(label = "Headcount"))))
  )
}

.with_registry <- function(reg, code) {
  env <- get(".piptm_env", envir = asNamespace("piptm"))
  old_reg <- env$registries
  old_rel <- env$current_release
  rel <- old_rel
  if (is.null(rel) || !nzchar(rel)) {
    rel <- "TEST_RELEASE"
    env$current_release <- rel
  }
  env$registries <- list()
  env$registries[[rel]] <- reg
  on.exit({ env$registries <- old_reg; env$current_release <- old_rel }, add = TRUE)
  force(code)
}

test_that("piptm_analysis_variables returns role-filtered fields", {
  .with_registry(.registry_fixture(), {
    out <- piptm::piptm_analysis_variables()
    vars <- vapply(out, `[[`, character(1L), "varname")
    expect_true("welfare" %in% vars)
    expect_true("pov_status" %in% vars)
    expect_false("age_group" %in% vars)
  })
})

test_that("piptm_filter_categories returns filter variables with subcategories", {
  .with_registry(.registry_fixture(), {
    out <- piptm::piptm_filter_categories()
    expect_equal(length(out), 1L)
    expect_identical(out[[1L]]$varname, "age_group")
    expect_equal(vapply(out[[1L]]$subcategories, `[[`, character(1L), "code"), c("0-14", "15-24", "25-64", "65+"))
  })
})

test_that("piptm_layout_covariates returns covariate fields", {
  .with_registry(.registry_fixture(), {
    out <- piptm::piptm_layout_covariates()
    vars <- vapply(out, `[[`, character(1L), "varname")
    expect_setequal(vars, c("pov_status", "age_group"))
  })
})

test_that("piptm_stat_groups returns grouped measure catalogue", {
  .with_registry(.registry_fixture(), {
    out <- piptm::piptm_stat_groups()
    groups <- vapply(out, `[[`, character(1L), "group")
    expect_true("summary_stats" %in% groups)
    expect_true("poverty" %in% groups)
  })
})
