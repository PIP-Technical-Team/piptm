# Plan: .cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md (Phase 1, Step 3)
# Unit tests for build_cell_explanation()

library(testthat)
library(jsonlite)

.with_fixture_registry <- function(release_id = "FIXTURE_2026", code) {
  ns <- asNamespace("piptm")
  env <- ns$.piptm_env
  old_regs  <- env$registries
  old_cur   <- env$current_release
  old_arrow <- env$arrow_root
  old_cache <- env$cell_prose
  on.exit({
    env$registries      <- old_regs
    env$current_release <- old_cur
    env$arrow_root      <- old_arrow
    env$cell_prose      <- old_cache
  }, add = TRUE)

  fixture_path <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "tests", "testthat", "fixtures", "registry-cell-prose-sample.json"),
    error = function(e) testthat::test_path("fixtures", "registry-cell-prose-sample.json")
  )
  fixture_reg <- jsonlite::read_json(fixture_path, simplifyVector = FALSE)
  fixture_reg[["_comment"]] <- NULL

  env$registries <- list()
  env$registries[[release_id]] <- fixture_reg
  env$current_release <- release_id
  env$arrow_root <- NULL
  env$cell_prose <- NULL

  force(code)
}

test_that("Example 1: Welfare, three measures, three slots, long filter (2 filter vars)", {
  .with_fixture_registry("FIXTURE_2026", {
    res <- build_cell_explanation(
      analysis_var = "welfare",
      measures = c("mean", "median", "gini"),
      row = "educat4",
      column = "gender",
      superColumn = "area",
      filter_base = list(age_group = "3", educat7 = "4"),
      ppp = 2021L,
      pop_share_threshold = NULL,
      welfare_type = "INC",
      release = "FIXTURE_2026"
    )

    expect_equal(res$schema_version, "1.0")
    expect_equal(res$who_is_in_table$title, "Who is in the table")
    expect_true(res$who_is_in_table$filtered)
    expect_equal(
      res$who_is_in_table$text,
      "People aged 25\u201364 who also have incomplete secondary education. Anyone outside this group is excluded before the table is built."
    )

    expect_equal(res$what_a_cell_is$title, "What a cell is")
    expect_length(res$what_a_cell_is$dimensions, 3L)
    expect_equal(
      res$what_a_cell_is$structure_text,
      "Each cell is one combination of education level (row), gender (column), and area (column group)."
    )
    # In 20260401_TEST.json, first categories are: gender 1=Male ("men"), educat4 1=Incomplete secondary, area 1=Urban
    expect_equal(
      res$what_a_cell_is$example_cell_label,
      "men with incomplete secondary education living in urban areas"
    )
    expect_match(res$what_a_cell_is$example_text, "within the filtered group above", fixed = TRUE)

    expect_equal(res$what_is_shown$title, "What is shown in that cell")
    expect_null(res$what_is_shown$intro)
    expect_length(res$what_is_shown$measures, 3L)

    m_mean <- res$what_is_shown$measures[[1L]]
    expect_equal(m_mean$key, "mean")
    expect_equal(m_mean$label, "Mean")
    expect_null(m_mean$counts)
    expect_null(m_mean$out_of)
    expect_null(m_mean$total_note)
    expect_equal(
      m_mean$text,
      "Average daily income of men with incomplete secondary education living in urban areas (2021 PPP dollars)."
    )

    m_med <- res$what_is_shown$measures[[2L]]
    expect_equal(m_med$key, "median")
    expect_equal(m_med$label, "Median")
    expect_equal(
      m_med$text,
      "Daily income of the middle person among men with incomplete secondary education living in urban areas, with half below and half above (2021 PPP dollars)."
    )

    m_gini <- res$what_is_shown$measures[[3L]]
    expect_equal(m_gini$key, "gini")
    expect_equal(m_gini$label, "Gini")
    expect_equal(
      m_gini$text,
      "Income inequality within men with incomplete secondary education living in urban areas (0 = everyone equal, 1 = one person has it all)."
    )
  })
})

test_that("Example 2: Shares, two slots, short filter (filter appears only once, in total population)", {
  .with_fixture_registry("FIXTURE_2026", {
    res <- build_cell_explanation(
      analysis_var = "employed",
      measures = c("pop_share", "target_within_group_share", "target_survey_share"),
      row = "area",
      column = "gender",
      filter_base = list(age_group = "3"),
      ppp = 2021L,
      pop_share_threshold = NULL,
      release = "FIXTURE_2026"
    )

    expect_true(res$who_is_in_table$filtered)
    expect_equal(
      res$who_is_in_table$text,
      "All people aged 25\u201364 in the survey. That group is the 100% for the table."
    )

    # Cell group does NOT repeat the filter ("aged 25-64"); filter stays in total population
    expect_equal(res$what_a_cell_is$example_cell_label, "men living in urban areas")
    expect_equal(
      res$what_a_cell_is$example_text,
      "For example, the first cell is men living in urban areas \u2014 within the filtered group above."
    )

    expect_match(res$what_is_shown$intro, "100%", fixed = TRUE)
    expect_length(res$what_is_shown$measures, 3L)

    m_pop <- res$what_is_shown$measures[[1L]]
    expect_equal(m_pop$key, "pop_share")
    expect_equal(m_pop$counts, "men living in urban areas (everyone in the cell)")
    expect_equal(m_pop$out_of, "all people aged 25\u201364 in the survey (everyone in the table)")
    expect_equal(
      m_pop$text,
      "Out of all people aged 25\u201364 in the survey, the percentage who are men living in urban areas."
    )
    expect_equal(m_pop$total_note, "All cells in the table add up to 100%.")

    m_within <- res$what_is_shown$measures[[2L]]
    expect_equal(m_within$key, "target_within_group_share")
    expect_equal(m_within$counts, "employed men living in urban areas")
    expect_equal(m_within$out_of, "men living in urban areas (everyone in that one cell)")
    expect_equal(
      m_within$text,
      "Out of men living in urban areas, the percentage who are employed."
    )
    expect_equal(
      m_within$total_note,
      "Does not add up to 100% (each cell is its own employment rate)."
    )

    m_survey <- res$what_is_shown$measures[[3L]]
    expect_equal(m_survey$key, "target_survey_share")
    expect_equal(m_survey$counts, "employed men living in urban areas")
    expect_equal(m_survey$out_of, "all people aged 25\u201364 in the survey (everyone in the table)")
    expect_equal(
      m_survey$text,
      "Out of all people aged 25\u201364 in the survey, the percentage who are employed men living in urban areas."
    )
    expect_equal(
      m_survey$total_note,
      "All cells together add up to the overall employment rate of people aged 25\u201364."
    )
  })
})

test_that("Example 3: Poverty measures, two slots, no filter", {
  .with_fixture_registry("FIXTURE_2026", {
    res <- build_cell_explanation(
      analysis_var = "pov_status",
      measures = c("headcount", "poverty_gap", "npoor"),
      row = "area",
      column = "gender",
      filter_base = NULL,
      poverty_line = 2.15,
      ppp = 2021L,
      pop_share_threshold = NULL,
      release = "FIXTURE_2026"
    )

    expect_false(res$who_is_in_table$filtered)
    expect_equal(res$who_is_in_table$text, "Everyone in the survey. No filter is applied.")
    expect_equal(res$what_a_cell_is$example_cell_label, "men living in urban areas")

    expect_length(res$what_is_shown$measures, 3L)
    expect_equal(
      res$what_is_shown$measures[[1L]]$text,
      "Out of all men living in urban areas, the percentage living on less than $2.15 a day (2021 PPP)."
    )
    expect_equal(
      res$what_is_shown$measures[[2L]]$text,
      "How far below $2.15 a day men living in urban areas fall on average, expressed as a percentage of the $2.15 a day line."
    )
    expect_equal(
      res$what_is_shown$measures[[3L]]$text,
      "The number of men living in urban areas living on less than $2.15 a day (2021 PPP)."
    )
  })
})

test_that("Example 4: Poverty status used as a row, with a welfare measure + head-noun precedence (R6)", {
  .with_fixture_registry("FIXTURE_2026", {
    # Both gender (column, first cat 1=Male -> "men") and pov_status (row, first cat 0=Non-poor -> "non-poor people")
    # have noun entries. Precedence: column > row > superColumn > superRow -> gender supplies head noun "men",
    # pov_status becomes trailing phrase "who are non-poor".
    res <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      row = "pov_status",
      column = "gender",
      filter_base = list(area = "0"),
      poverty_line = 3.65,
      ppp = 2021L,
      pop_share_threshold = NULL,
      welfare_type = "INC",
      release = "FIXTURE_2026"
    )

    expect_true(res$who_is_in_table$filtered)
    expect_match(res$what_a_cell_is$structure_text, "$3.65 a day (2021 PPP)", fixed = TRUE)
    expect_equal(res$what_a_cell_is$example_cell_label, "non-poor men living in rural areas")
    expect_equal(
      res$what_is_shown$measures[[1L]]$text,
      "Average daily income of non-poor men living in rural areas (2021 PPP dollars)."
    )
    expect_true(any(grepl("3.65", res$notes, fixed = TRUE) & grepl("split", res$notes, fixed = TRUE)))

    # Reverse slot assignment: column = pov_status, row = gender -> pov_status wins head noun ("non-poor people")
    res_rev <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      column = "pov_status",
      row = "gender",
      filter_base = NULL,
      poverty_line = 3.65,
      ppp = 2021L,
      pop_share_threshold = NULL,
      welfare_type = "INC",
      release = "FIXTURE_2026"
    )
    expect_equal(res_rev$what_a_cell_is$example_cell_label, "non-poor people who are men")
  })
})

test_that("Example 5 & single-cell shares: No rows or columns selected (by = NULL)", {
  .with_fixture_registry("FIXTURE_2026", {
    res <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      filter_base = list(age_group = "3"),
      ppp = 2021L,
      pop_share_threshold = 0.001,
      welfare_type = "INC",
      release = "FIXTURE_2026"
    )

    expect_length(res$what_a_cell_is$dimensions, 0L)
    expect_equal(
      res$what_a_cell_is$structure_text,
      "No row or column variables are selected, so the table has a single cell covering people aged 25\u201364."
    )
    expect_null(res$what_a_cell_is$example_text)
    expect_equal(
      res$what_is_shown$measures[[1L]]$text,
      "Average daily income of people aged 25\u201364 (2021 PPP dollars)."
    )
    # Parity with table_maker(): when by = NULL, pop_share_threshold has no effect -> no NR note
    expect_false(any(grepl("NR", res$notes, fixed = TRUE)))

    # Single-cell shares (by = NULL): target_within_group_share and target_survey_share collapse cleanly
    res_sh <- build_cell_explanation(
      analysis_var = "employed",
      measures = c("pop_share", "target_within_group_share", "target_survey_share"),
      filter_base = NULL,
      release = "FIXTURE_2026"
    )
    expect_equal(
      res_sh$what_is_shown$measures[[1L]]$total_note,
      "Always 100% because the table has a single cell."
    )
    expect_equal(
      res_sh$what_is_shown$measures[[2L]]$counts,
      "employed people in the survey"
    )
    expect_equal(
      res_sh$what_is_shown$measures[[2L]]$text,
      "Out of everyone in the survey, the percentage who are employed."
    )
    expect_match(
      res_sh$what_is_shown$measures[[2L]]$total_note,
      "collapse to the same overall employment rate",
      fixed = TRUE
    )
    expect_equal(
      res_sh$what_is_shown$measures[[3L]]$text,
      "Out of everyone in the survey, the percentage who are employed."
    )
    expect_match(
      res_sh$what_is_shown$measures[[3L]]$total_note,
      "collapse to the same overall employment rate",
      fixed = TRUE
    )

    # Single share measure with by = NULL does NOT mention the unselected share measure
    res_sh1 <- build_cell_explanation(
      analysis_var = "employed",
      measures = "target_survey_share",
      filter_base = NULL,
      release = "FIXTURE_2026"
    )
    expect_equal(res_sh1$what_is_shown$measures[[1L]]$counts, "employed people in the survey")
    expect_false(grepl("Target share within cell", res_sh1$what_is_shown$measures[[1L]]$total_note, fixed = TRUE))
    expect_equal(
      res_sh1$what_is_shown$measures[[1L]]$total_note,
      "With no row or column breakdown, the single cell equals the overall employment rate of everyone in the survey."
    )
  })
})

test_that("R5: No-registry fallback with PIPTM_ARROW_ROOT unset degrades every label to raw codes", {
  ns <- asNamespace("piptm")
  env <- ns$.piptm_env
  old_regs  <- env$registries
  old_cur   <- env$current_release
  old_arrow <- env$arrow_root
  old_env_arrow <- Sys.getenv("PIPTM_ARROW_ROOT", unset = NA_character_)
  on.exit({
    env$registries      <- old_regs
    env$current_release <- old_cur
    env$arrow_root      <- old_arrow
    if (is.na(old_env_arrow)) {
      Sys.unsetenv("PIPTM_ARROW_ROOT")
    } else {
      Sys.setenv(PIPTM_ARROW_ROOT = old_env_arrow)
    }
  }, add = TRUE)

  Sys.unsetenv("PIPTM_ARROW_ROOT")
  env$registries <- list()
  env$current_release <- NULL
  env$arrow_root <- NULL

  res <- expect_no_error(
    build_cell_explanation(
      analysis_var = "welfare",
      measures = c("mean", "median", "gini"),
      row = "educat4",
      column = "gender",
      superColumn = "area",
      filter_base = list(age_group = "3", educat7 = "4"),
      ppp = 2021L,
      pop_share_threshold = 0.001,
      welfare_type = "INC",
      release = "MISSING_REL"
    )
  )

  expect_true(any(grepl("Variable registry unavailable for release MISSING_REL; using fallback labels.", res$warnings, fixed = TRUE)))
  expect_equal(
    res$who_is_in_table$text,
    "People whose age group is code 3 and whose education level is code 4. Anyone outside this group is excluded before the table is built."
  )
  expect_equal(
    res$what_a_cell_is$structure_text,
    "Each cell is one combination of education level (row), gender (column), and area (column group)."
  )
  expect_equal(
    res$what_a_cell_is$example_cell_label,
    "people whose education level is code 1, whose gender is code 1, and whose area is code 1"
  )
  expect_true(any(grepl("NR", res$notes, fixed = TRUE)))
})

test_that("R8(d): Non-monetary analysis variables (age, educy, hsize) omit PPP dollars; welfare_type selects income/consumption/welfare; 1-slot structure_text", {
  .with_fixture_registry("FIXTURE_2026", {
    for (v in c("age", "educy", "hsize")) {
      res_v <- build_cell_explanation(
        analysis_var = v,
        measures = c("mean", "median"),
        column = "gender",
        ppp = 2021L,
        release = "FIXTURE_2026"
      )
      expect_equal(res_v$what_a_cell_is$structure_text, "Each cell is one gender group (column).")
      expect_false(grepl("PPP", res_v$what_is_shown$measures[[1L]]$text, fixed = TRUE),
                   info = paste("PPP should be omitted for", v))
      expect_false(grepl("PPP", res_v$what_is_shown$measures[[2L]]$text, fixed = TRUE),
                   info = paste("PPP should be omitted for", v))
    }

    res_con <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      ppp = 2017L,
      welfare_type = "CON",
      release = "FIXTURE_2026"
    )
    expect_equal(res_con$what_is_shown$measures[[1L]]$text, "Average daily consumption of men (2017 PPP dollars).")

    res_def <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      ppp = 2021L,
      welfare_type = NULL,
      release = "FIXTURE_2026"
    )
    expect_equal(res_def$what_is_shown$measures[[1L]]$text, "Average daily welfare of men (2021 PPP dollars).")
  })
})

test_that("R8(b): Filter variable also used as a layout slot emits overlap note and avoids repeating 'people'", {
  .with_fixture_registry("FIXTURE_2026", {
    res <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      row = "educat4",
      filter_base = list(educat4 = c("1", "2")),
      ppp = 2021L,
      welfare_type = "INC",
      release = "FIXTURE_2026"
    )
    expect_equal(
      res$who_is_in_table$text,
      "People with incomplete or general secondary education. Anyone outside this group is excluded before the table is built."
    )
    expect_true(any(grepl("Education level is used both as a sample filter and as a row.", res$notes, fixed = TRUE)))

    # Multi-variable filter (Case 4.3): "who also have incomplete or general secondary education"
    res_43 <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      filter_base = list(age_group = c("2", "3"), educat4 = c("1", "2")),
      ppp = 2021L,
      welfare_type = "INC",
      release = "FIXTURE_2026"
    )
    expect_equal(
      res_43$who_is_in_table$text,
      "People aged 15\u201324 or 25\u201364 who also have incomplete or general secondary education. Anyone outside this group is excluded before the table is built."
    )
  })
})
