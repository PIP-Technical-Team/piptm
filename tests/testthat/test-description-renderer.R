# Test: description renderer — render_description_markdown() and dispatcher
#
# Operates on the 6-section table-metadata model produced by
# build_description_model() (overview, provenance, surveys_selected,
# filters_applied, layout_configuration, statistics_selected).

library(data.table)

.make_render_model <- function(overrides = list()) {
  base <- list(
    overview = list(
      visible = TRUE,
      title = "Table Overview",
      content = list(
        structure_description = "This table reports 2 statistics for 1 selected survey, with results disaggregated by Gender.",
        weighting_note = "All calculations use survey sampling weights.",
        ppp_note = "Welfare values are expressed in 2021 PPP international dollars per day.",
        suppression_note = "Cells whose population share is below 0.1% of the table sample base are reported as \"NR\" (not reported)."
      )
    ),
    provenance = list(
      visible = TRUE,
      title = "Data Provenance",
      content = list(
        release_id = "TEST_2024",
        ppp_year = 2021L,
        generated_at = "2026-08-26 10:00:00 UTC"
      )
    ),
    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = list(
        n_loaded = 1L,
        n_excluded = 0L,
        loaded_list = data.table(
          pip_id = "TST_2020_ECH_CON_ALL",
          country_code = "TST",
          country_label = "Testland",
          surveyid_year = 2020L,
          welfare_type_label = "Consumption",
          version = "v01_v01"
        ),
        excluded_list = NULL
      )
    ),
    filters_applied = list(
      visible = FALSE,
      title = "Sample Base Filters",
      content = NULL
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
          n_categories = "2: Male, Female"
        )
      )
    ),
    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = list(
        analysis_var_name = "welfare",
        analysis_var_label = "Welfare",
        analysis_var_type = "welfare",
        poverty_line = list(applicable = FALSE, value = NULL, ppp_year = NULL),
        measures = data.table(
          measure = c("mean", "gini"),
          measure_label = c("Mean", "Gini index"),
          stat_group = c("summary_statistics", "inequality")
        )
      )
    )
  )

  for (nm in names(overrides)) {
    base[[nm]] <- overrides[[nm]]
  }
  return(base)
}


test_that("render_description_markdown() returns a character scalar", {
  out <- render_description_markdown(.make_render_model())
  expect_type(out, "character")
  expect_length(out, 1L)
  expect_true(nzchar(out))
})

test_that("render_description_markdown() renders the 6 visible sections as headings", {
  out <- render_description_markdown(.make_render_model())
  expect_match(out, "## Table Overview", fixed = TRUE)
  expect_match(out, "## Data Provenance", fixed = TRUE)
  expect_match(out, "## Surveys Selected", fixed = TRUE)
  expect_match(out, "## Table Layout Configuration", fixed = TRUE)
  expect_match(out, "## Statistics Computed", fixed = TRUE)
  # No removed sections.
  expect_false(grepl("## Cell Definition", out, fixed = TRUE))
  expect_false(grepl("## Execution Summary", out, fixed = TRUE))
  expect_false(grepl("## Warnings", out, fixed = TRUE))
})

test_that("render_description_markdown() skips invisible sections", {
  out <- render_description_markdown(.make_render_model())
  expect_false(grepl("## Sample Base Filters", out, fixed = TRUE))
})

test_that("render_description_markdown() renders overview notes including suppression", {
  out <- render_description_markdown(.make_render_model())
  expect_match(out, "survey sampling weights", fixed = TRUE)
  expect_match(out, "PPP international dollars", fixed = TRUE)
  expect_match(out, "reported as \"NR\"", fixed = TRUE)
})

test_that("render_description_markdown() renders data.table content as pipe tables", {
  out <- render_description_markdown(.make_render_model())
  # Survey table headers use display labels.
  expect_match(out, "Country name", fixed = TRUE)
  expect_match(out, "Welfare type", fixed = TRUE)
  expect_match(out, "Version", fixed = TRUE)
  expect_match(out, "Consumption", fixed = TRUE)
  # Measures table.
  expect_match(out, "Gini index", fixed = TRUE)
  # Layout table.
  expect_match(out, "| Dimension | Variable | Label | Categories |", fixed = TRUE)
})

test_that("render_description_markdown() renders analysis variable fields", {
  out <- render_description_markdown(.make_render_model())
  # Skip guard removed: analysis var fields now render.
  expect_match(out, "Analysis variable", fixed = TRUE)
  expect_match(out, "Welfare", fixed = TRUE)
})

test_that("render_description_markdown() renders excluded surveys when present", {
  model <- .make_render_model(list(
    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = list(
        n_loaded = 1L,
        n_excluded = 1L,
        loaded_list = data.table(
          pip_id = "TST_2020_ECH_CON_ALL", country_code = "TST",
          country_label = "Testland", surveyid_year = 2020L,
          welfare_type_label = "Consumption", version = "v01_v01"
        ),
        excluded_list = data.table(
          pip_id = "BOL_2000_ECH_INC_ALL",
          reason = "no requested dimensions"
        )
      )
    )
  ))
  out <- render_description_markdown(model)
  expect_match(out, "Exclusion reason", fixed = TRUE)
  expect_match(out, "no requested dimensions", fixed = TRUE)
})

test_that("render_description_markdown() returns empty string for no visible content", {
  model <- .make_render_model(list(
    overview = list(visible = TRUE, title = "Table Overview", content = NULL),
    provenance = list(visible = TRUE, title = "Data Provenance", content = NULL),
    surveys_selected = list(visible = TRUE, title = "Surveys Selected", content = NULL),
    statistics_selected = list(visible = TRUE, title = "Statistics Computed", content = NULL),
    layout_configuration = list(visible = TRUE, title = "Table Layout Configuration", content = NULL)
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
          varname = "age_group",
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
  out <- render_description_markdown(.make_render_model())
  expect_match(out, "- Release: TEST_2024", fixed = TRUE)
  expect_match(out, "- PPP year: 2021", fixed = TRUE)
})
