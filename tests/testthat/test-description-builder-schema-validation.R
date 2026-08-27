# Test: Schema validation for description builder functions
# Tests for P0 fixes from 2026-08-27 review

# =============================================================================
# P0-1: .build_surveys_content() NULL and empty list handling
# =============================================================================

test_that(".build_surveys_content() handles NULL loaded surveys", {
  meta <- list(
    surveys = list(loaded = NULL, excluded = NULL),
    execution = list(n_surveys_loaded = 0L, n_surveys_excluded = 0L)
  )
  result <- piptm:::.build_surveys_content(meta)
  expect_equal(result$n_loaded, 0L)
  expect_equal(nrow(result$loaded_list), 0L)
})

test_that(".build_surveys_content() handles loaded as empty list", {
  meta <- list(
    surveys = list(loaded = list(), excluded = NULL),  # JSON converts empty DT to list()
    execution = list(n_surveys_loaded = 0L, n_surveys_excluded = 0L)
  )
  result <- piptm:::.build_surveys_content(meta)
  expect_equal(result$n_loaded, 0L)
})

test_that(".build_surveys_content() handles excluded as NULL", {
  meta <- list(
    surveys = list(
      loaded = data.table(
        pip_id = "TEST_2020",
        country_code = "TST",
        surveyid_year = "2020",
        welfare_type = "CON"
      ),
      excluded = NULL
    ),
    execution = list(n_surveys_loaded = 1L, n_surveys_excluded = 0L)
  )
  result <- piptm:::.build_surveys_content(meta)
  expect_equal(result$n_loaded, 1L)
  expect_equal(result$n_excluded, 0L)
  expect_null(result$excluded_list)
})

# =============================================================================
# P0-2: .build_filters_content() schema validation
# =============================================================================

test_that(".build_filters_content() aborts when filters_dt missing ui_label", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(varname = "age_group", selected_labels = list(c("0-14")))
      # missing ui_label
    )
  )
  expect_error(
    piptm:::.build_filters_content(meta),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

test_that(".build_filters_content() aborts when filters_dt missing selected_labels", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(varname = "age_group", ui_label = "Age group")
      # missing selected_labels
    )
  )
  expect_error(
    piptm:::.build_filters_content(meta),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

test_that(".build_filters_content() aborts when filters_dt missing both ui_label and selected_labels", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(varname = "age_group")
    )
  )
  expect_error(
    piptm:::.build_filters_content(meta),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

# =============================================================================
# P0-3: .build_filters_content() list-column type normalization
# =============================================================================

test_that(".build_filters_content() normalizes NULL elements in selected_labels", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(
        varname = "gender",
        ui_label = "Gender",
        selected_labels = list(NULL)  # JSON round-trip can produce this
      )
    )
  )
  result <- piptm:::.build_filters_content(meta)
  expect_equal(result$filters$selected_categories, "(unspecified)")
})

test_that(".build_filters_content() normalizes non-character elements in selected_labels", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(
        varname = "age_group",
        ui_label = "Age group",
        selected_labels = list(c(1L, 2L))  # integers from JSON
      )
    )
  )
  # Should not error; as.character() coerces integers
  expect_no_error(piptm:::.build_filters_content(meta))
  result <- piptm:::.build_filters_content(meta)
  expect_equal(result$filters$selected_categories, "1, 2")
})

test_that(".build_filters_content() normalizes empty character vector in selected_labels", {
  meta <- list(
    resolved_labels = list(
      filters = data.table(
        varname = "gender",
        ui_label = "Gender",
        selected_labels = list(character(0))
      )
    )
  )
  result <- piptm:::.build_filters_content(meta)
  expect_equal(result$filters$selected_categories, "(unspecified)")
})

# =============================================================================
# P1-4: .build_statistics_content() defensive checks
# =============================================================================

test_that(".build_statistics_content() aborts when measures_dt is NULL", {
  meta <- list(resolved_labels = list(measures = NULL, analysis_var = list(ui_label = "Welfare", tm_type = "continuous")))
  params <- list(poverty_line = NULL)
  expect_error(
    piptm:::.build_statistics_content(meta, params),
    class = "rlang_error",
    regexp = "must be a non-empty data.frame"
  )
})

test_that(".build_statistics_content() aborts when measures_dt is empty", {
  meta <- list(
    resolved_labels = list(
      measures = data.table(measure = character(0), ui_label = character(0), stat_group = character(0)),
      analysis_var = list(ui_label = "Welfare", tm_type = "continuous")
    )
  )
  params <- list(poverty_line = NULL)
  expect_error(
    piptm:::.build_statistics_content(meta, params),
    class = "rlang_error",
    regexp = "must be a non-empty data.frame"
  )
})

test_that(".build_statistics_content() aborts when measures_dt is not a data.frame", {
  meta <- list(
    resolved_labels = list(
      measures = "not_a_dataframe",
      analysis_var = list(ui_label = "Welfare", tm_type = "continuous")
    )
  )
  params <- list(poverty_line = NULL)
  expect_error(
    piptm:::.build_statistics_content(meta, params),
    class = "rlang_error",
    regexp = "must be a non-empty data.frame"
  )
})

# =============================================================================
# P1-6: .build_layout_content() type coercion
# =============================================================================

test_that(".build_layout_content() coerces factor columns to character", {
  meta <- list(
    resolved_labels = list(
      covariates = data.table(
        slot = "columns",
        varname = factor("gender"),  # JSON round-trip could produce factor
        ui_label = factor("Gender"),
        n_categories = 2L
      )
    )
  )
  # Should not error; as.character() coerces factors
  expect_no_error(piptm:::.build_layout_content(meta))
  result <- piptm:::.build_layout_content(meta)
  expect_type(result$layout$varname, "character")
  expect_type(result$layout$ui_label, "character")
})

test_that(".build_layout_content() coerces numeric n_categories to integer", {
  meta <- list(
    resolved_labels = list(
      covariates = data.table(
        slot = "columns",
        varname = "gender",
        ui_label = "Gender",
        n_categories = 2.0  # numeric, not integer
      )
    )
  )
  # Should not error; as.integer() coerces numeric
  result <- piptm:::.build_layout_content(meta)
  expect_type(result$layout$n_categories, "integer")
})

test_that(".build_layout_content() handles NA in n_categories after coercion", {
  meta <- list(
    resolved_labels = list(
      covariates = data.table(
        slot = "columns",
        varname = "gender",
        ui_label = "Gender",
        n_categories = NA_real_
      )
    )
  )
  result <- piptm:::.build_layout_content(meta)
  expect_equal(result$layout$n_categories, 0L)
})
