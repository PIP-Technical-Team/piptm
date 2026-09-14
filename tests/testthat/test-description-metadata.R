# Test: .build_description_metadata() field correctness
# Requirement: R3 — execution log capture; plan applies to Phase 1, Step 4
# These tests use synthetic result/excluded data so they run WITHOUT a
# configured data directory or registry, providing a real functional baseline.

test_that("build_description_metadata assembles schema-expected fields", {
  result <- data.table::data.table(
    pip_id = c("COL_2020_GEIH_INC_ALL", "COL_2020_GEIH_INC_ALL"),
    country_code = "COL",
    surveyid_year = 2020L,
    welfare_type = "INC",
    measure = c("mean", "gini"),
    value = c(100.5, 0.55),
    population = 1e7
  )

  params <- list(
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    poverty_line = NULL,
    by = NULL,
    filter_base = NULL,
    ppp = 2021L,
    release = NULL,
    pop_share_threshold = 0.01
  )

  excluded <- data.table::data.table(
    pip_id = c("PER_2019_ENAHO_ALL"),
    reason = c("missing gender")
  )

  meta <- piptm:::.build_description_metadata(
    params = params,
    result = result,
    release = "20260206",
    excluded_surveys = excluded,
    suppressed_cells = NULL,
    captured_warnings = c("warning one", "warning two")
  )

  expect_type(meta, "list")
  expect_true(all(c("params", "provenance", "surveys", "resolved_labels",
                    "execution") %in% names(meta)))

  # provenance
  expect_identical(meta$provenance$release, "20260206")
  expect_identical(meta$provenance$ppp_year, 2021L)
  expect_true(nzchar(meta$provenance$generated_at))

  # surveys.loaded derived from result (NOT manifest) -> schema columns present
  expect_s3_class(meta$surveys$loaded, "data.table")
  expect_true(all(c("pip_id", "country_code", "surveyid_year", "welfare_type") %in%
                    names(meta$surveys$loaded)))
  expect_identical(nrow(meta$surveys$loaded), 1L)

  # surveys.excluded carries through the tracker
  expect_s3_class(meta$surveys$excluded, "data.table")
  expect_true(all(c("pip_id", "reason") %in% names(meta$surveys$excluded)))
  expect_identical(nrow(meta$surveys$excluded), 1L)

  # resolved_labels
  expect_true(all(c("analysis_var", "measures", "filters",
                    "covariates") %in% names(meta$resolved_labels)))
  expect_true(all(c("varname", "ui_label", "tm_type") %in%
                    names(meta$resolved_labels$analysis_var)))
  expect_s3_class(meta$resolved_labels$measures, "data.table")

  # execution log counts
  expect_identical(meta$execution$n_surveys_loaded, 1L)
  expect_identical(meta$execution$n_surveys_excluded, 1L)
  expect_identical(meta$execution$n_measures_computed, 2L)
  expect_true(all(c("suppression", "warnings") %in% names(meta$execution)))
  expect_identical(meta$execution$warnings, c("warning one", "warning two"))
  expect_false(meta$execution$suppression$triggered)
})

test_that("build_description_metadata handles empty lists and no warnings", {
  result <- data.table::data.table(
    pip_id = "BRA_2015_PNADC_ALL",
    country_code = "BRA",
    surveyid_year = 2015L,
    welfare_type = "CON",
    measure = "headcount",
    value = 0.2,
    population = 2e7
  )

  params <- list(
    analysis_var = "pov_status",
    measures = "headcount",
    poverty_line = 2.15,
    by = NULL,
    filter_base = NULL,
    ppp = 2023,
    release = "TEST_RELEASE",
    pop_share_threshold = NULL
  )

  meta <- piptm:::.build_description_metadata(
    params = params,
    result = result,
    release = "TEST_RELEASE",
    excluded_surveys = data.table::data.table(pip_id = character(0), reason = character(0)),
    suppressed_cells = NULL,
    captured_warnings = character(0)
  )

  # warnings field is NULL when none captured
  expect_null(meta$execution$warnings)
  # analysis variable falls back to raw name when registry unavailable
  expect_identical(meta$resolved_labels$analysis_var$varname, "pov_status")
})

test_that(".build_description_metadata degrades when registries fail", {
  result <- data.table::data.table(
    pip_id = "TEST_SURVEY",
    country_code = "TST",
    surveyid_year = 2020L,
    welfare_type = "CON",
    measure = "mean",
    value = 1,
    population = 100
  )

  params <- list(
    analysis_var = "welfare",
    measures = "mean",
    poverty_line = NULL,
    by = NULL,
    filter_base = NULL,
    ppp = 2021L,
    release = NULL,
    pop_share_threshold = NULL
  )

  with_mocked_bindings(
    piptm_variable_registry = function(...) stop("registry missing"),
    piptm_stat_groups = function(...) stop("registry missing"),
    piptm_filter_categories = function(...) stop("registry missing"),
    piptm_layout_covariates = function(...) stop("registry missing"),
    {
      meta <- piptm:::.build_description_metadata(
        params = params,
        result = result,
        release = "20260206",
        excluded_surveys = data.table::data.table(pip_id = character(0), reason = character(0)),
        suppressed_cells = NULL,
        captured_warnings = character(0)
      )
      expect_identical(meta$resolved_labels$analysis_var$tm_type, "unknown")
      expect_null(meta$resolved_labels$filters)
      expect_s3_class(meta$resolved_labels$covariates, "data.table")
      expect_identical(nrow(meta$resolved_labels$covariates), 0L)
    }
  )
})
