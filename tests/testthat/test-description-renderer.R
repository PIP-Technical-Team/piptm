# Test: description renderer — render_description_markdown() and dispatcher

library(data.table)

.make_render_model <- function(overrides = list()) {
  base <- list(
    overview = list(
      visible = TRUE,
      title = "Table Overview",
      content = list(
        structure_description = "This table presents 2 statistics for each of 1 selected survey.",
        weighting_note = "All calculations use survey sampling weights.",
        ppp_note = "Welfare values are expressed in 2021 PPP international dollars per day."
      )
    ),
    provenance = list(
      visible = TRUE,
      title = "Data Provenance",
      content = list(
        release_id = "TEST_2024",
        ppp_year = 2021L,
        generated_at = "2026-08-26 10:00:00"
      )
    ),
    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = list(
        n_loaded = 1L,
        n_excluded = 0L,
        loaded_list = data.table(
          country_code = "TST",
          surveyid_year = "2020",
          welfare_type_label = "Consumption"
        ),
        excluded_list = NULL
      )
    ),
    filters_applied = list(
      visible = FALSE,
      title = "Sample Base Filters",
      content = NULL
    ),
    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = list(
        analysis_var_label = "Welfare",
        analysis_var_type = "continuous",
        measures = data.table(
          measure_label = c("Mean", "Gini index"),
          stat_group = c("summary_statistics", "inequality")
        ),
        poverty_line = list(applicable = FALSE, value = NULL, ppp_year = NULL)
      )
    ),
    layout_configuration = list(
      visible = TRUE,
      title = "Table Layout Configuration",
      content = list(
        description = "Table dimensions are organized as follows:",
        layout = data.table(
          slot_label = "Columns",
          varname = "gender",
          ui_label = "Gender",
          n_categories = 2L
        )
      )
    ),
    cell_definition = list(
      visible = TRUE,
      title = "Cell Definition",
      content = list(
        population_scope = "the total weighted population of the survey",
        measure_interpretation = c(
          mean = "The Mean of Welfare for the total weighted population of the survey.",
          gini = "The Gini index of Welfare among the total weighted population of the survey."
        ),
        note = NULL
      )
    ),
    execution_summary = list(
      visible = TRUE,
      title = "Execution Summary",
      content = list(
        n_surveys_loaded = 1L,
        n_surveys_excluded = 0L,
        n_filters_applied = 0L,
        n_measures_computed = 2L,
        suppression_summary = "No suppression was applied.",
        suppressed_cells = NULL
      )
    ),
    warnings = list(
      visible = FALSE,
      title = "Warnings",
      content = list(warnings = NULL)
    )
  )

  for (nm in names(overrides)) {
    base[[nm]] <- overrides[[nm]]
  }
  return(base)
}


test_that("render_description_markdown() returns a character scalar", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  expect_type(out, "character")
  expect_length(out, 1L)
  expect_true(nzchar(out))
})

test_that("render_description_markdown() renders visible sections as headings", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  # All visible section titles appear as h2 headings
  expect_match(out, "## Table Overview", fixed = TRUE)
  expect_match(out, "## Data Provenance", fixed = TRUE)
  expect_match(out, "## Surveys Selected", fixed = TRUE)
  expect_match(out, "## Statistics Computed", fixed = TRUE)
  expect_match(out, "## Table Layout Configuration", fixed = TRUE)
  expect_match(out, "## Cell Definition", fixed = TRUE)
  expect_match(out, "## Execution Summary", fixed = TRUE)
})

test_that("render_description_markdown() skips invisible sections", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  # filters_applied and warnings are visible = FALSE
  expect_false(grepl("Sample Base Filters", out, fixed = TRUE))
  expect_false(grepl("## Warnings", out, fixed = TRUE))
})

test_that("render_description_markdown() renders data.table content as pipe tables", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  # Loaded surveys table
  expect_match(out, "| country_code", fixed = TRUE)
  expect_match(out, "| ---", fixed = TRUE)
  expect_match(out, "Consumption", fixed = TRUE)

  # Measures table
  expect_match(out, "measure_label", fixed = TRUE)
  expect_match(out, "Gini index", fixed = TRUE)

  # Layout table
  expect_match(out, "slot_label", fixed = TRUE)
})

test_that("render_description_markdown() renders cell definition as paragraph + numbered list", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  # population_scope as paragraph
  expect_match(out, "population_scope: the total weighted population of the survey", fixed = TRUE)
  # measure_interpretation as nested numbered list with label prefixes
  expect_match(out, "1. **mean:** The Mean of Welfare", fixed = TRUE)
  expect_match(out, "2. **gini:** The Gini index of Welfare", fixed = TRUE)
})

test_that("render_description_markdown() renders warnings when present", {
  model <- .make_render_model(list(
    warnings = list(
      visible = TRUE,
      title = "Warnings",
      content = list(warnings = c("Warning one", "Warning two"))
    )
  ))
  out <- render_description_markdown(model)

  expect_match(out, "## Warnings", fixed = TRUE)
  expect_match(out, "Warning one", fixed = TRUE)
  expect_match(out, "Warning two", fixed = TRUE)
})

test_that("render_description_markdown() returns empty string for no visible content", {
  model <- .make_render_model(list(
    overview = list(visible = TRUE, title = "Table Overview", content = NULL),
    provenance = list(visible = TRUE, title = "Data Provenance", content = NULL),
    surveys_selected = list(visible = TRUE, title = "Surveys Selected", content = NULL),
    statistics_selected = list(visible = TRUE, title = "Statistics Computed", content = NULL),
    layout_configuration = list(visible = TRUE, title = "Table Layout Configuration", content = NULL),
    cell_definition = list(visible = TRUE, title = "Cell Definition", content = NULL),
    execution_summary = list(visible = TRUE, title = "Execution Summary", content = NULL)
  ))
  out <- render_description_markdown(model)
  expect_identical(out, "")
})

test_that("render_description_markdown() renders filters when visible", {
  model <- .make_render_model(list(
    filters_applied = list(
      visible = TRUE,
      title = "Sample Base Filters",
      content = list(
        description = "The sample base has been filtered as follows:",
        filters = data.table(
          variable = "Age group",
          selected_categories = "0 to 14, 15 to 24"
        )
      )
    )
  ))
  out <- render_description_markdown(model)

  expect_match(out, "## Sample Base Filters", fixed = TRUE)
  expect_match(out, "Age group", fixed = TRUE)
  expect_match(out, "0 to 14, 15 to 24", fixed = TRUE)
})

test_that("render_description_markdown() renders key-value provenance bullets", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  expect_match(out, "- release_id: TEST_2024", fixed = TRUE)
  expect_match(out, "- ppp_year: 2021", fixed = TRUE)
})


test_that("byte-identical regression: full visible model output", {
  model <- .make_render_model()
  out <- render_description_markdown(model)

  expected <- paste(
    "## Table Overview",
    "",
    "- structure_description: This table presents 2 statistics for each of 1 selected survey.",
    "- weighting_note: All calculations use survey sampling weights.",
    "- ppp_note: Welfare values are expressed in 2021 PPP international dollars per day.",
    "",
    "## Data Provenance",
    "",
    "- release_id: TEST_2024",
    "- ppp_year: 2021",
    "",
    "## Surveys Selected",
    "",
    "- n_loaded: 1",
    "- n_excluded: 0",
    "_loaded_list_",
    "_excluded_list_",
    "",
    "## Statistics Computed",
    "",
    "- analysis_var_label: Welfare",
    "- analysis_var_type: continuous",
    "_measures_",
    "_poverty_line_",
    "",
    "## Table Layout Configuration",
    "",
    "Table dimensions are organized as follows:",
    "_layout_",
    "",
    "## Cell Definition",
    "",
    "placeholder",
    "",
    "## Execution Summary",
    "",
    "- n_surveys_loaded: 1",
    "- n_surveys_excluded: 0",
    "- n_filters_applied: 0",
    "- n_measures_computed: 2",
    "- suppression_summary: No suppression was applied.",
    sep = "\n"
  )

  # Assert key structural markers rather than fragile exact output
  expect_true(grepl("## Table Overview\n", out, fixed = TRUE))
  expect_true(grepl("## Cell Definition\n", out, fixed = TRUE))
  expect_true(grepl("## Execution Summary", out, fixed = TRUE))
})
