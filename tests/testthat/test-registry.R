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

test_that("zzz.R initializes registries slot", {
  zzz_path <- testthat::test_path("..", "..", "R", "zzz.R")
  expect_true(file.exists(zzz_path))

  zzz <- readLines(zzz_path, warn = FALSE)
  merged <- paste(zzz, collapse = "\n")
  expect_match(merged, "\\.piptm_env\\$registries\\s*<-\\s*list\\(\\)")
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
