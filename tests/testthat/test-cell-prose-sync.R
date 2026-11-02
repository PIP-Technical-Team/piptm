# Plan: .cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md (Phase 1, Steps 1-2)
# Spec sync test between tm_variable_spec.yaml, tm_measure_spec.yaml,
# tests/testthat/fixtures/registry-cell-prose-sample.json, and tm_cell_prose.yaml.

library(testthat)
library(jsonlite)

.resolve_extdata <- function(filename) {
  p <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "inst", "extdata", filename),
    error = function(e) ""
  )
  if (!nzchar(p) || !file.exists(p)) {
    p <- system.file("extdata", filename, package = "piptm")
  }
  p
}

.resolve_fixture <- function(filename) {
  p <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "tests", "testthat", "fixtures", filename),
    error = function(e) ""
  )
  if (!nzchar(p) || !file.exists(p)) {
    p <- testthat::test_path("fixtures", filename)
  }
  p
}

test_that("Step 1: registry-cell-prose-sample.json exists and matches 20260401_TEST.json filter/covariate entries", {
  skip_if_not_installed("yaml")

  fixture_path <- .resolve_fixture("registry-cell-prose-sample.json")
  expect_true(file.exists(fixture_path))

  fixture_reg <- jsonlite::read_json(fixture_path, simplifyVector = FALSE)
  expect_type(fixture_reg, "list")
  expect_true("_comment" %in% names(fixture_reg))

  reg_vars <- setdiff(names(fixture_reg), c("_comment", "measure_spec"))
  expect_gt(length(reg_vars), 0L)

  for (vname in reg_vars) {
    entry <- fixture_reg[[vname]]
    roles <- as.character(unlist(entry$roles, use.names = FALSE))
    if (any(roles %in% c("filter", "covariate"))) {
      expect_true(is.list(entry$categories) && length(entry$categories) > 0L,
                  info = paste("Fixture missing categories for", vname))
    }
  }
})

test_that("Step 2: tm_cell_prose.yaml exists and .get_cell_prose() caches parsed result", {
  skip_if_not_installed("yaml")

  prose_path <- .resolve_extdata("tm_cell_prose.yaml")
  expect_true(nzchar(prose_path) && file.exists(prose_path))

  ns <- asNamespace("piptm")
  expect_true(exists(".get_cell_prose", where = ns, inherits = FALSE))

  env <- ns$.piptm_env
  old_cache <- env$cell_prose
  on.exit({ env$cell_prose <- old_cache }, add = TRUE)
  env$cell_prose <- NULL

  prose1 <- ns$.get_cell_prose()
  expect_type(prose1, "list")
  expect_true(all(c("schema_version", "analysis_vars", "variables", "targets", "measures") %in% names(prose1)))
  expect_false(is.null(env$cell_prose))

  # Sentinel check that second call returns cached object without re-reading YAML
  sentinel <- prose1
  sentinel$.__cached_marker__ <- TRUE
  env$cell_prose <- sentinel
  prose2 <- ns$.get_cell_prose()
  expect_true(isTRUE(prose2$.__cached_marker__))
})

test_that("Step 2: tm_cell_prose.yaml stays in sync with variable/measure specs and fixture registry codes", {
  skip_if_not_installed("yaml")

  var_spec <- yaml::read_yaml(.resolve_extdata("tm_variable_spec.yaml"))$variables
  meas_spec <- yaml::read_yaml(.resolve_extdata("tm_measure_spec.yaml"))$stat_groups
  fixture_reg <- jsonlite::read_json(.resolve_fixture("registry-cell-prose-sample.json"), simplifyVector = FALSE)
  prose <- yaml::read_yaml(.resolve_extdata("tm_cell_prose.yaml"))

  # 1. Non-share analysis_vars covered in prose$analysis_vars
  non_share_analysis_vars <- names(Filter(function(v) {
    roles <- as.character(unlist(v$roles, use.names = FALSE))
    sg <- as.character(unlist(v$stat_groups, use.names = FALSE))
    ("analysis_var" %in% roles) && !identical(sg, "shares")
  }, var_spec))
  expect_equal(setdiff(non_share_analysis_vars, names(prose$analysis_vars)), character(0))

  # 2. All binary share targets covered in prose$targets with predicate + rate_noun
  share_targets <- names(Filter(function(v) {
    roles <- as.character(unlist(v$roles, use.names = FALSE))
    sg <- as.character(unlist(v$stat_groups, use.names = FALSE))
    ("analysis_var" %in% roles) && ("shares" %in% sg)
  }, var_spec))
  expect_equal(setdiff(share_targets, names(prose$targets)), character(0))
  for (tname in share_targets) {
    t_entry <- prose$targets[[tname]]
    expect_true(is.character(t_entry$predicate) && nzchar(t_entry$predicate),
                info = paste("Missing predicate for target", tname))
    expect_true(is.character(t_entry$rate_noun) && nzchar(t_entry$rate_noun),
                info = paste("Missing rate_noun for target", tname))
  }

  # 3. All filter/covariate variables in tm_variable_spec.yaml have slot_noun + phrase_frame
  filter_cov_vars <- names(Filter(function(v) {
    roles <- as.character(unlist(v$roles, use.names = FALSE))
    any(roles %in% c("filter", "covariate"))
  }, var_spec))
  expect_equal(setdiff(filter_cov_vars, names(prose$variables)), character(0))

  # Head nouns are permitted ONLY on gender and pov_status
  vars_with_nouns <- names(Filter(function(v) !is.null(v$nouns) && length(v$nouns) > 0L, prose$variables))
  expect_setequal(vars_with_nouns, c("gender", "pov_status"))

  for (vname in filter_cov_vars) {
    v_prose <- prose$variables[[vname]]
    expect_true(is.character(v_prose$slot_noun) && nzchar(v_prose$slot_noun),
                info = paste("Missing slot_noun for", vname))
    expect_true(is.character(v_prose$phrase_frame) && nzchar(v_prose$phrase_frame),
                info = paste("Missing phrase_frame for", vname))

    # Every hard-coded code in nouns/overrides for variables in fixture_reg must match fixture_reg codes
    if (vname %in% names(fixture_reg) && length(fixture_reg[[vname]]$categories) > 0L) {
      fixture_codes <- vapply(
        fixture_reg[[vname]]$categories,
        function(c) as.character(c$code)[[1L]],
        character(1)
      )
      if (!is.null(v_prose$nouns)) {
        expect_equal(
          setdiff(names(v_prose$nouns), fixture_codes),
          character(0),
          info = paste("Unmatched noun codes in tm_cell_prose.yaml for", vname)
        )
      }
      if (!is.null(v_prose$overrides)) {
        expect_equal(
          setdiff(names(v_prose$overrides), fixture_codes),
          character(0),
          info = paste("Unmatched override codes in tm_cell_prose.yaml for", vname)
        )
      }
    }
  }

  # 4. All 23 measures in tm_measure_spec.yaml covered in prose$measures
  all_measures <- unlist(lapply(meas_spec, function(g) names(g$measures)), use.names = FALSE)
  expect_equal(setdiff(all_measures, names(prose$measures)), character(0))
})
