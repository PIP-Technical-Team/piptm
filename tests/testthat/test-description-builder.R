# Test: build_description_model() section visibility and content

test_that("build_description_model() produces complete model with all sections", {
  # Construct minimal synthetic metadata
  meta <- list(
    params = list(
      pip_id = c("TEST_2020_SURVEY_CON_ALL"),
      analysis_var = "welfare",
      measures = c("mean", "gini"),
      poverty_line = NULL,
      ppp = 2021L,
      release = "TEST_2024",
      by = c("gender"),
      filter_base = NULL,
      pop_share_threshold = NULL
    ),
    provenance = list(
      release = "TEST_2024",
      ppp_year = 2021L,
      generated_at = as.POSIXct("2026-08-26 10:00:00", tz = "UTC")
    ),
    surveys = list(
      loaded = data.table(
        pip_id = "TEST_2020_SURVEY_CON_ALL",
        country_code = "TST",
        surveyid_year = "2020",
        welfare_type = "CON"
      ),
      excluded = data.table(pip_id = character(0), reason = character(0))
    ),
    resolved_labels = list(
      analysis_var = list(
        varname = "welfare",
        ui_label = "Welfare",
        tm_type = "continuous"
      ),
      measures = data.table(
        measure = c("mean", "gini"),
        ui_label = c("Mean", "Gini index"),
        stat_group = c("summary_statistics", "inequality")
      ),
      filters = NULL,
      covariates = data.table(
        slot = "columns",
        varname = "gender",
        ui_label = "Gender",
        n_categories = 2L
      )
    ),
    execution = list(
      n_surveys_loaded = 1L,
      n_surveys_excluded = 0L,
      n_filters_applied = 0L,
      n_measures_computed = 2L,
      suppression = list(
        triggered = FALSE,
        threshold = NULL,
        n_cells_suppressed = 0L,
        suppressed_cells = NULL
      ),
      warnings = NULL
    )
  )
  
  params <- meta$params
  
  # Call build_description_model()
  model <- build_description_model(meta, params)
  
  # Check structure: all 9 sections present
  expect_equal(length(model), 9L)
  expect_named(model, c(
    "overview", "provenance", "surveys_selected",
    "filters_applied", "statistics_selected",
    "layout_configuration", "cell_definition",
    "execution_summary", "warnings"
  ))
  
  # Check each section has visible, title, content
  for (section_name in names(model)) {
    expect_true("visible" %in% names(model[[section_name]]))
    expect_true("title" %in% names(model[[section_name]]))
    expect_true("content" %in% names(model[[section_name]]))
  }
  
  # Check visibility flags
  expect_true(model$overview$visible)
  expect_true(model$provenance$visible)
  expect_true(model$surveys_selected$visible)
  expect_false(model$filters_applied$visible)  # No filters
  expect_true(model$statistics_selected$visible)
  expect_true(model$layout_configuration$visible)  # Has covariates
  expect_true(model$cell_definition$visible)
  expect_true(model$execution_summary$visible)
  expect_false(model$warnings$visible)  # No warnings
})

test_that("build_description_model() rejects malformed metadata", {
  minimal_params <- list()
  malformed <- list(
    params = list(),
    provenance = list(),
    surveys = list(),
    resolved_labels = list()
    # execution missing
  )
  expect_error(
    build_description_model(malformed, minimal_params),
    "missing required"
  )

  malformed$execution <- NULL
  expect_error(
    build_description_model(malformed, minimal_params),
    "missing required"
  )

  malformed$execution <- NULL
  malformed$provenance <- NULL
  expect_error(
    build_description_model(malformed, minimal_params),
    "missing required"
  )

  malformed2 <- list(
    params = list(),
    provenance = list(),
    surveys = list(),
    resolved_labels = list(),
    execution = NULL
  )
  expect_error(
    build_description_model(malformed2, minimal_params),
    "malformed field"
  )
})


test_that(paste("build_description_model() sets filters_applied visible",
                "when filter_base present"), {
  meta <- list(
    params = list(
      pip_id = c("TEST_2020_SURVEY_CON_ALL"),
      analysis_var = "welfare",
      measures = c("mean"),
      poverty_line = NULL,
      ppp = 2021L,
      release = "TEST_2024",
      by = NULL,
      filter_base = list(age_group = c(1L, 2L)),
      pop_share_threshold = NULL
    ),
    provenance = list(
      release = "TEST_2024",
      ppp_year = 2021L,
      generated_at = as.POSIXct("2026-08-26 10:00:00", tz = "UTC")
    ),
    surveys = list(
      loaded = data.table(
        pip_id = "TEST_2020_SURVEY_CON_ALL",
        country_code = "TST",
        surveyid_year = "2020",
        welfare_type = "CON"
      ),
      excluded = data.table(pip_id = character(0), reason = character(0))
    ),
    resolved_labels = list(
      analysis_var = list(
        varname = "welfare",
        ui_label = "Welfare",
        tm_type = "continuous"
      ),
      measures = data.table(
        measure = "mean",
        ui_label = "Mean",
        stat_group = "summary_statistics"
      ),
      filters = data.table(
        varname = "age_group",
        ui_label = "Age group",
        selected_codes = list(c(1L, 2L)),
        selected_labels = list(c("0 to 14", "15 to 24"))
      ),
      covariates = data.table(
        slot = character(0),
        varname = character(0),
        ui_label = character(0),
        n_categories = integer(0)
      )
    ),
    execution = list(
      n_surveys_loaded = 1L,
      n_surveys_excluded = 0L,
      n_filters_applied = 1L,
      n_measures_computed = 1L,
      suppression = list(
        triggered = FALSE,
        threshold = NULL,
        n_cells_suppressed = 0L,
        suppressed_cells = NULL
      ),
      warnings = NULL
    )
  )
  
  params <- meta$params
  model <- build_description_model(meta, params)
  
  # filters_applied should be visible
  expect_true(model$filters_applied$visible)
  expect_type(model$filters_applied$content, "list")
  expect_true("filters" %in% names(model$filters_applied$content))
})


test_that("build_description_model() sets layout_configuration invisible when by is NULL", {
  meta <- list(
    params = list(
      pip_id = c("TEST_2020_SURVEY_CON_ALL"),
      analysis_var = "welfare",
      measures = c("mean"),
      poverty_line = NULL,
      ppp = 2021L,
      release = "TEST_2024",
      by = NULL,
      filter_base = NULL,
      pop_share_threshold = NULL
    ),
    provenance = list(
      release = "TEST_2024",
      ppp_year = 2021L,
      generated_at = as.POSIXct("2026-08-26 10:00:00", tz = "UTC")
    ),
    surveys = list(
      loaded = data.table(
        pip_id = "TEST_2020_SURVEY_CON_ALL",
        country_code = "TST",
        surveyid_year = "2020",
        welfare_type = "CON"
      ),
      excluded = data.table(pip_id = character(0), reason = character(0))
    ),
    resolved_labels = list(
      analysis_var = list(
        varname = "welfare",
        ui_label = "Welfare",
        tm_type = "continuous"
      ),
      measures = data.table(
        measure = "mean",
        ui_label = "Mean",
        stat_group = "summary_statistics"
      ),
      filters = NULL,
      covariates = data.table(
        slot = character(0),
        varname = character(0),
        ui_label = character(0),
        n_categories = integer(0)
      )
    ),
    execution = list(
      n_surveys_loaded = 1L,
      n_surveys_excluded = 0L,
      n_filters_applied = 0L,
      n_measures_computed = 1L,
      suppression = list(
        triggered = FALSE,
        threshold = NULL,
        n_cells_suppressed = 0L,
        suppressed_cells = NULL
      ),
      warnings = NULL
    )
  )
  
  params <- meta$params
  model <- build_description_model(meta, params)
  
  # layout_configuration should be invisible
  expect_false(model$layout_configuration$visible)
  expect_null(model$layout_configuration$content)
})


test_that("build_description_model() sets warnings visible when warnings present", {
  meta <- list(
    params = list(
      pip_id = c("TEST_2020_SURVEY_CON_ALL"),
      analysis_var = "welfare",
      measures = c("mean"),
      poverty_line = NULL,
      ppp = 2021L,
      release = "TEST_2024",
      by = NULL,
      filter_base = NULL,
      pop_share_threshold = NULL
    ),
    provenance = list(
      release = "TEST_2024",
      ppp_year = 2021L,
      generated_at = as.POSIXct("2026-08-26 10:00:00", tz = "UTC")
    ),
    surveys = list(
      loaded = data.table(
        pip_id = "TEST_2020_SURVEY_CON_ALL",
        country_code = "TST",
        surveyid_year = "2020",
        welfare_type = "CON"
      ),
      excluded = data.table(pip_id = character(0), reason = character(0))
    ),
    resolved_labels = list(
      analysis_var = list(
        varname = "welfare",
        ui_label = "Welfare",
        tm_type = "continuous"
      ),
      measures = data.table(
        measure = "mean",
        ui_label = "Mean",
        stat_group = "summary_statistics"
      ),
      filters = NULL,
      covariates = data.table(
        slot = character(0),
        varname = character(0),
        ui_label = character(0),
        n_categories = integer(0)
      )
    ),
    execution = list(
      n_surveys_loaded = 1L,
      n_surveys_excluded = 0L,
      n_filters_applied = 0L,
      n_measures_computed = 1L,
      suppression = list(
        triggered = FALSE,
        threshold = NULL,
        n_cells_suppressed = 0L,
        suppressed_cells = NULL
      ),
      warnings = c("Sample warning 1", "Sample warning 2")
    )
  )
  
  params <- meta$params
  model <- build_description_model(meta, params)
  
  # warnings should be visible
  expect_true(model$warnings$visible)
  expect_type(model$warnings$content, "list")
  expect_equal(model$warnings$content$warnings, c("Sample warning 1", "Sample warning 2"))
})
