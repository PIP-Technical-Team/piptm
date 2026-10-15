library(testthat)

# Phase 1 red/green tests for:
# - tm_variable_spec.yaml inline categories
# - R/registry.R core helpers/functions
# - zzz.R registry load wiring

spec_file <- function() {
  system.file("extdata", "tm_variable_spec.yaml", package = "piptm")
}

read_spec_text <- function() {
  readLines(spec_file(), warn = FALSE)
}

test_that("tm_variable_spec.yaml includes inline categories for derived vars", {
  expect_true(nzchar(spec_file()))

  txt <- read_spec_text()
  merged <- paste(txt, collapse = "\n")

  expect_match(merged, "age_group:\\n(?:.|\\n)*?categories:")
  expect_match(merged, "hsize_group:\\n(?:.|\\n)*?categories:")
  expect_match(merged, "wquintile:\\n(?:.|\\n)*?categories:")
})

test_that("registry core functions exist in namespace", {
  ns <- asNamespace("piptm")

  expect_true(exists("build_variable_registry", where = ns, inherits = FALSE))
  expect_true(exists("piptm_variable_registry", where = ns, inherits = FALSE))
  expect_true(exists("piptm_load_registry", where = ns, inherits = FALSE))
  expect_true(exists(".mapping_to_categories", where = ns, inherits = FALSE))
  expect_true(exists(".build_registry_entry", where = ns, inherits = FALSE))
})

test_that("mapping helper converts named list to code-label pairs", {
  ns <- asNamespace("piptm")
  fn <- get(".mapping_to_categories", envir = ns)

  out <- fn(list("1" = "Male", "0" = "Female"))

  expect_type(out, "list")
  expect_equal(out[[1L]]$code, "1")
  expect_equal(out[[1L]]$label, "Male")
  expect_equal(out[[2L]]$code, "0")
  expect_equal(out[[2L]]$label, "Female")
})

test_that("build helper gives special-case pov_status n_categories=2", {
  ns <- asNamespace("piptm")
  fn <- get(".build_registry_entry", envir = ns)

  spec_var <- list(
    ui_label = "Poverty status",
    roles = c("analysis_var", "covariate"),
    stat_groups = c("poverty")
  )

  entry <- fn("pov_status", spec_var, pip_vars = list())

  expect_equal(entry$tm_type, "poverty")
  expect_identical(entry$n_categories, 2L)
  expect_null(entry$categories)
})

test_that("package load initializes registries slot", {
  ns <- asNamespace("piptm")
  expect_true(exists(".piptm_env", where = ns, inherits = FALSE))
  env <- get(".piptm_env", envir = ns)
  expect_true("registries" %in% names(env))
  expect_type(env$registries, "list")
})

test_that("registry view accessors exist and error on missing release", {
  ns <- asNamespace("piptm")

  expect_true(exists("piptm_analysis_variables", where = ns, inherits = FALSE))
  expect_true(exists("piptm_filter_categories", where = ns, inherits = FALSE))
  expect_true(exists("piptm_layout_covariates", where = ns, inherits = FALSE))

  # Accessors should error informatively when the release is not available
  expect_error(piptm::piptm_analysis_variables("BOGUS_RELEASE"), "No variable registry found")
  expect_error(piptm::piptm_filter_categories("BOGUS_RELEASE"), "No variable registry found")
  expect_error(piptm::piptm_layout_covariates("BOGUS_RELEASE"), "No variable registry found")
})

test_that(".sentence_case uppercases first letter and lowercases the rest", {
  ns <- asNamespace("piptm")
  f <- get(".sentence_case", envir = ns)
  expect_equal(f(c("male", "Paid Employee", "  YES ")), c("Male", "Paid employee", "Yes"))
  expect_true(is.na(f(NA_character_)))
  expect_null(f(NULL))
})

test_that("YAML category labels override pipdata labels by code; pipdata labels are cased", {
  ns <- asNamespace("piptm")
  build <- get(".build_registry_entry", envir = ns)
  pip_vars <- list(
    wquintile = list(
      type = "factor", recode_type = "quantile_from_continuous",
      mapping = list(`1` = "quintile1", `2` = "quintile2")
    ),
    gender = list(
      type = "factor", recode_type = "binary_map",
      mapping = list(`0` = "female", `1` = "male")
    )
  )
  spec_q <- list(
    ui_label = "Welfare quintile", roles = list("filter"),
    categories = list(
      list(code = "1", label = "Q1 (bottom 20%)"),
      list(code = "2", label = "Q2")
    )
  )
  spec_g <- list(ui_label = "Gender", roles = list("filter"))

  q <- build("wquintile", spec_q, pip_vars)
  expect_equal(vapply(q$categories, `[[`, "", "label"), c("Q1 (bottom 20%)", "Q2"))
  expect_equal(vapply(q$categories, `[[`, "", "code"), c("1", "2"))

  g <- build("gender", spec_g, pip_vars)
  expect_equal(vapply(g$categories, `[[`, "", "label"), c("Female", "Male"))
})
