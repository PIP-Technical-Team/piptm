# Test: HTML description renderer

library(data.table)

.make_render_model_html <- function(overrides = list()) {
  base <- list(
    overview = list(
      visible = TRUE,
      title = "Table Overview",
      content = list(
        structure_description = "This table reports 2 statistics for 1 selected survey.",
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
        generated_at = "2026-09-09 16:00:00"
      )
    ),
    surveys_selected = list(
      visible = TRUE,
      title = "Surveys Selected",
      content = list(
        n_loaded = 1L,
        n_excluded = 0L,
        loaded_list = data.table(
          pip_id = "TEST_2020_SURVEY_CON_ALL",
          country_code = "TST",
          surveyid_year = "2020",
          welfare_type_label = "Consumption"
        )
      )
    ),
    filters_applied = list(
      visible = TRUE,
      title = "Sample Base Filters",
      content = list(
        description = "The weighted sample is restricted to observations that meet all criteria.",
        filters = data.table(variable = "Age group", selected_categories = "0 to 14")
      )
    ),
    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = list(
        analysis_var_label = "Welfare",
        analysis_var_type = "continuous",
        measures = data.table(
          measure_label = c("Mean", "Median"),
          stat_group = c("summary_statistics", "poverty"),
          analysis_var_label = c("Welfare", "Welfare")
        ),
        poverty_line = list(applicable = TRUE, value = 2.15, ppp_year = 2021L)
      )
    ),
    layout_configuration = list(
      visible = TRUE,
      title = "Table Layout Configuration",
      content = list(
        description = "Table dimensions are organized as follows:",
        layout = data.table(slot_label = "Columns", varname = "gender", ui_label = "Gender", n_categories = 2L)
      )
    ),
    cell_definition = list(
      visible = TRUE,
      title = "Cell Definition",
      content = list(
        population_scope = "survey-weighted individuals in the selected survey",
        measure_interpretation = c(
          "The Mean of Welfare for survey-weighted individuals in the selected survey."
        )
      )
    ),
    execution_summary = list(
      visible = TRUE,
      title = "Execution Summary",
      content = list(
        n_surveys_loaded = 1L,
        n_surveys_excluded = 0L,
        n_filters_applied = 1L,
        n_measures_computed = 1L,
        suppression_summary = "No suppression was applied."
      )
    ),
    warnings = list(
      visible = FALSE,
      title = "Warnings",
      content = NULL
    )
  )

  for (nm in names(overrides)) {
    base[[nm]] <- overrides[[nm]]
  }
  base
}


test_that("render_description_html() renders sections and human labels", {
  model <- .make_render_model_html()
  out <- render_description_html(model)

  expect_type(out, "character")
  expect_length(out, 1L)
  expect_true(nzchar(out))

  expect_match(out, "<h2[^>]*>Table Overview</h2>", perl = TRUE)
  expect_match(out, "Description:</b>", fixed = TRUE)
  expect_match(out, "Population note:</b>", fixed = TRUE)
  expect_match(out, "Welfare values are expressed in 2021 PPP international dollars per day.", fixed = TRUE)

  expect_false(grepl("structure_description", out, fixed = TRUE))
  expect_false(grepl("weighting_note", out, fixed = TRUE))
  expect_false(grepl("n_surveys_loaded", out, fixed = TRUE))

  idx_loaded <- regexpr("Surveys loaded:</b>", out, fixed = TRUE)[1]
  idx_included <- regexpr("Included surveys", out, fixed = TRUE)[1]
  expect_true(idx_loaded > 0)
  expect_true(idx_included > 0)
  expect_lt(idx_loaded, idx_included)

  expect_match(out, "<th[^>]*>Country</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Survey year</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Welfare type</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Survey identifier</th>", perl = TRUE)
  expect_match(out, "TEST_2020_SURVEY_CON_ALL", fixed = TRUE)
  expect_match(out, "<th[^>]*>Measure</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Group</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Variable of analysis</th>", perl = TRUE)
  expect_match(out, "Summary statistics", fixed = TRUE)
  expect_match(out, "Poverty", fixed = TRUE)
  expect_false(grepl("summary_statistics", out, fixed = TRUE))
  expect_match(out, "<th[^>]*>Dimension</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Variable</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Label</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Categories</th>", perl = TRUE)

  expect_false(grepl("<th[^>]*>country_code</th>", out, perl = TRUE))
  expect_false(grepl("<th[^>]*>surveyid_year</th>", out, perl = TRUE))
  expect_false(grepl("<th[^>]*>welfare_type_label</th>", out, perl = TRUE))
})


test_that("render_description_html() handles poverty_line block per approved rules", {
  model_yes <- .make_render_model_html()
  out_yes <- render_description_html(model_yes)

  expect_match(out_yes, "Poverty line", fixed = TRUE)
  expect_match(out_yes, "Value:</b> 2.15", fixed = TRUE)
  expect_match(out_yes, "PPP year:</b> 2021", fixed = TRUE)
  expect_false(grepl("Applicable:", out_yes, fixed = TRUE))

  model_no <- .make_render_model_html(list(
    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = list(
        analysis_var_label = "Welfare",
        analysis_var_type = "continuous",
        measures = data.table(
          measure_label = c("Mean", "Median"),
          stat_group = c("summary_statistics", "poverty"),
          analysis_var_label = c("Welfare", "Welfare")
        ),
        poverty_line = list(applicable = FALSE, value = NULL, ppp_year = NULL)
      )
    )
  ))
  out_no <- render_description_html(model_no)

  expect_false(grepl("Poverty line", out_no, fixed = TRUE))
  expect_false(grepl("Applicable", out_no, fixed = TRUE))
})


test_that("render_description_html() escapes special characters including apostrophe", {
  model <- .make_render_model_html(list(
    overview = list(
      visible = TRUE,
      title = "Table Overview",
      content = list(
        structure_description = "Tom & Jerry's <test> \"quote\""
      )
    )
  ))

  out <- render_description_html(model)
  expect_match(out, "Tom &amp; Jerry&#39;s &lt;test&gt; &quot;quote&quot;", fixed = TRUE)
})


test_that("render_description_html() renders ppp_note as unlabeled paragraph", {
  model <- .make_render_model_html()
  out <- render_description_html(model)

  expect_match(out, "<p[^>]*>Welfare values are expressed in 2021 PPP international dollars per day.</p>", perl = TRUE)
  expect_false(grepl("PPP note:", out, fixed = TRUE))
})


test_that("render_description_html() omits bottom border on final visible section", {
  model <- .make_render_model_html()
  out <- render_description_html(model)

  section_styles <- regmatches(
    out,
    gregexpr("<section style=\"[^\"]+\"", out, perl = TRUE)
  )[[1]]

  expect_gte(length(section_styles), 1L)
  expect_true(grepl("border-bottom", section_styles[[1]], fixed = TRUE))
  expect_false(grepl("border-bottom", section_styles[[length(section_styles)]], fixed = TRUE))
})


# ── Structured shares payload rendering ─────────────────────────────────────

test_that("render_description_html() renders shares-only structured payload as an escaped table with footer", {
  model <- .make_render_model_html(list(
    cell_definition = list(
      visible = TRUE,
      title = "Cell Definition",
      content = list(
        population_scope = "survey-weighted individuals in the selected survey",
        measure_interpretation = list(
          prose = character(0),
          shares_table = data.table(
            measure = c("Population share", "Target share within cell"),
            denominator = c("d1", "d2"),
            numerator = c("n1 & <cell>", "n2 \"quoted\""),
            plain_meaning = c("What share, p1?", "What share, p2?")
          ),
          shares_footer = paste0(
            "For cells with positive weighted population: Target share in ",
            "sample base = Population share \u00d7 Target share within cell."
          )
        )
      )
    )
  ))
  out <- render_description_html(model)

  expect_match(out, "<table[^>]*>", perl = TRUE)
  expect_match(out, "<th[^>]*>Measure</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Denominator</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Numerator</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Plain meaning</th>", perl = TRUE)

  # Dynamic cell values must be HTML-escaped.
  expect_match(out, "n1 &amp; &lt;cell&gt;", fixed = TRUE)
  expect_match(out, "n2 &quot;quoted&quot;", fixed = TRUE)

  # Footer present (2 shares requested).
  expect_match(
    out,
    "For cells with positive weighted population: Target share in sample base",
    fixed = TRUE
  )
})

test_that("render_description_html() renders mixed prose + shares table, one share has no footer", {
  model <- .make_render_model_html(list(
    cell_definition = list(
      visible = TRUE,
      title = "Cell Definition",
      content = list(
        population_scope = "survey-weighted individuals in the selected survey",
        measure_interpretation = list(
          prose = "The Mean of Welfare for survey-weighted individuals in the selected survey.",
          shares_table = data.table(
            measure = "Population share",
            denominator = "d1",
            numerator = "n1",
            plain_meaning = "What share, p1?"
          ),
          shares_footer = NULL
        )
      )
    )
  ))
  out <- render_description_html(model)

  mean_pos <- regexpr("The Mean of Welfare", out, fixed = TRUE)
  table_pos <- regexpr("Plain meaning", out, fixed = TRUE)
  expect_true(mean_pos > 0)
  expect_true(table_pos > 0)
  expect_true(mean_pos < table_pos)

  # Only one share requested: no identity footer.
  expect_false(grepl("Population share \u00d7 Target share within cell", out, fixed = TRUE))
})

test_that("render_description_html() leaves non-share measure_interpretation rendering unchanged", {
  model <- .make_render_model_html()
  out <- render_description_html(model)

  expect_match(
    out,
    "The Mean of Welfare for survey-weighted individuals in the selected survey.",
    fixed = TRUE
  )
  expect_false(grepl("<th[^>]*>Plain meaning</th>", out, perl = TRUE))
})
