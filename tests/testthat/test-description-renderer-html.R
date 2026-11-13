# Test: HTML description renderer (6-section table-metadata model)

library(data.table)

.make_render_model_html <- function(overrides = list()) {
  base <- list(
    provenance = list(
      visible = TRUE,
      title = "Data Provenance",
      content = list(
        release_id = "TEST_2024",
        ppp_year = 2021L,
        generated_at = "2026-09-09 16:00:00 UTC"
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
          country_label = "Testland",
          surveyid_year = 2020L,
          welfare_type_label = "Consumption",
          vermast = "v01",
          veralt = "v02"
        ),
        excluded_list = NULL
      )
    ),
    filters_applied = list(
      visible = TRUE,
      title = "Sample Base Filters",
      content = list(
        description = "The weighted sample is restricted to observations that meet all criteria.",
        filters = data.table(
          varname = "age_group",
          variable = "Age group",
          selected_categories = "0 to 14"
        )
      )
    ),
    layout_configuration = list(
      visible = TRUE,
      title = "Table Layout Configuration",
      content = list(
        description = "Table dimensions are organized as follows:",
        layout = data.table(
          slot_label = "Columns", varname = "gender", ui_label = "Gender",
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
        poverty_line = list(applicable = TRUE, value = "2.15", ppp_year = 2021L),
        measures = data.table(
          measure = c("mean", "median"),
          measure_label = c("Mean", "Median"),
          stat_group = c("summary_statistics", "poverty")
        )
      )
    ),
    notes = list(
      visible = TRUE,
      title = "Notes",
      content = list(
        weighting_note = "All calculations use survey sampling weights.",
        version_note = "Survey versions report the master version (vermast, the original survey vintage in the Datalibweb master collection) and the alternative version (veralt, the harmonized adaptation built on that master survey).",
        ppp_note = "Welfare values are expressed in 2021 PPP international dollars per day.",
        suppression_note = "Cells whose population share is below 0.1% of the table sample base are reported as \"NR\" (not reported)."
      )
    )
  )

  for (nm in names(overrides)) {
    base[[nm]] <- overrides[[nm]]
  }
  base
}


test_that("render_description_html() renders Metadata title, the 6 sections, and unlabeled notes bullets", {
  out <- render_description_html(.make_render_model_html())

  expect_type(out, "character")
  expect_length(out, 1L)
  expect_true(nzchar(out))

  expect_match(out, "<h1[^>]*>Metadata</h1>", perl = TRUE)
  expect_match(out, "<h2[^>]*>Data Provenance</h2>", perl = TRUE)
  expect_match(out, "<h2[^>]*>Notes</h2>", perl = TRUE)
  expect_lt(
    regexpr("Statistics Computed", out, fixed = TRUE),
    regexpr(">Notes<", out, fixed = TRUE)
  )
  expect_match(out, "<th[^>]*>Master version</th><th[^>]*>Alternative version</th>", perl = TRUE)
  expect_match(out, "<li[^>]*>All calculations use survey sampling weights.</li>", perl = TRUE)
  expect_match(out, "<li[^>]*>Survey versions report the master version \\(vermast", perl = TRUE)
  expect_match(out, "<li[^>]*>Welfare values are expressed in 2021 PPP international dollars per day.</li>", perl = TRUE)
  expect_match(out, "<li[^>]*>Cells whose population share is below 0.1% of the table sample base are reported as &quot;NR&quot; \\(not reported\\).</li>", perl = TRUE)

  # No key/label prefixes in Notes.
  expect_false(grepl("Description:</b>", out, fixed = TRUE))
  expect_false(grepl("Population note:</b>", out, fixed = TRUE))
  expect_false(grepl("Cell suppression:</b>", out, fixed = TRUE))
  expect_false(grepl("Table Overview", out, fixed = TRUE))

  # Internal keys never leak.
  expect_false(grepl("structure_description", out, fixed = TRUE))
  expect_false(grepl("weighting_note", out, fixed = TRUE))
  expect_false(grepl("suppression_note", out, fixed = TRUE))

  # No removed sections.
  expect_false(grepl("Cell Definition", out, fixed = TRUE))
  expect_false(grepl("Execution Summary", out, fixed = TRUE))
  expect_false(grepl(">Warnings<", out, fixed = TRUE))

  # Survey table headers use display labels.
  expect_match(out, "<th[^>]*>Country name</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Survey year</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Welfare type</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Master version</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Alternative version</th>", perl = TRUE)
  expect_match(out, "TEST_2020_SURVEY_CON_ALL", fixed = TRUE)

  # Measures and layout headers.
  expect_match(out, "<th[^>]*>Measure</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Group</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Dimension</th>", perl = TRUE)
  expect_match(out, "<th[^>]*>Categories</th>", perl = TRUE)
})


test_that("render_description_html() renders analysis variable fields (skip guard removed)", {
  out <- render_description_html(.make_render_model_html())
  expect_match(out, "Analysis variable:</b>", fixed = TRUE)
  expect_match(out, "Welfare", fixed = TRUE)
})


test_that("render_description_html() handles poverty_line block per approved rules", {
  out_yes <- render_description_html(.make_render_model_html())
  expect_match(out_yes, "Poverty line", fixed = TRUE)
  expect_match(out_yes, "Value:</b> 2.15", fixed = TRUE)
  expect_match(out_yes, "PPP year:</b> 2021", fixed = TRUE)
  expect_false(grepl("Applicable:", out_yes, fixed = TRUE))

  model_no <- .make_render_model_html(list(
    statistics_selected = list(
      visible = TRUE,
      title = "Statistics Computed",
      content = list(
        analysis_var_name = "welfare",
        analysis_var_label = "Welfare",
        analysis_var_type = "welfare",
        poverty_line = list(applicable = FALSE, value = NULL, ppp_year = NULL),
        measures = data.table(
          measure = c("mean", "median"),
          measure_label = c("Mean", "Median"),
          stat_group = c("summary_statistics", "poverty")
        )
      )
    )
  ))
  out_no <- render_description_html(model_no)
  expect_false(grepl("Poverty line", out_no, fixed = TRUE))
})


test_that("render_description_html() escapes special characters including apostrophe", {
  model <- .make_render_model_html(list(
    notes = list(
      visible = TRUE,
      title = "Notes",
      content = list(
        weighting_note = "Tom & Jerry's <test> \"quote\""
      )
    )
  ))
  out <- render_description_html(model)
  expect_match(out, "Tom &amp; Jerry&#39;s &lt;test&gt; &quot;quote&quot;", fixed = TRUE)
})


test_that("render_description_html() escapes dynamic table cell values", {
  model <- .make_render_model_html(list(
    filters_applied = list(
      visible = TRUE,
      title = "Sample Base Filters",
      content = list(
        description = "Filtered.",
        filters = data.table(
          varname = "x",
          variable = "X & <var>",
          selected_categories = "a \"b\" <c>"
        )
      )
    )
  ))
  out <- render_description_html(model)
  expect_match(out, "X &amp; &lt;var&gt;", fixed = TRUE)
  expect_match(out, "a &quot;b&quot; &lt;c&gt;", fixed = TRUE)
})


test_that("render_description_html() renders ppp_note as unlabeled list item in Notes", {
  out <- render_description_html(.make_render_model_html())
  expect_match(out, "<li[^>]*>Welfare values are expressed in 2021 PPP international dollars per day.</li>", perl = TRUE)
  expect_false(grepl("PPP note:", out, fixed = TRUE))
})


test_that("render_description_html() omits bottom border on final visible section", {
  out <- render_description_html(.make_render_model_html())
  section_styles <- regmatches(
    out,
    gregexpr("<section style=\"[^\"]+\"", out, perl = TRUE)
  )[[1]]
  expect_gte(length(section_styles), 1L)
  expect_true(grepl("border-bottom", section_styles[[1]], fixed = TRUE))
  expect_false(grepl("border-bottom", section_styles[[length(section_styles)]], fixed = TRUE))
})
