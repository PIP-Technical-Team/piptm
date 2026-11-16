# Plan: .cg-docs/plans/2026-11-02-page2-cell-definition-endpoint.md (Phase 1, Step 3)
# Unit tests for build_cell_explanation()

.with_fixture_registry <- function(release_id = "FIXTURE_2026", code) {
  skip_if_not_installed("yaml")
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
      "Each cell is one combination of education level (row), gender (column), and area (column group) \u2014 20 cells in total:"
    )
    # 20 cells (> 4) -> per-slot breakdown, one line per active slot (<= 4 lines)
    expect_length(res$what_a_cell_is$items, 3L)
    expect_equal(res$what_a_cell_is$items[[2L]], "Column (gender): men or women.")
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

test_that("what_a_cell_is$items: <=4-cell enumeration, >4-cell per-slot breakdown, fallback, and overlap", {
  .with_fixture_registry("FIXTURE_2026", {
    # (A) 2x2 (area x gender) = 4 cells -> full combined enumeration, outer=rows, inner=columns.
    # Registry order: area 1=Urban then 0=Rural; gender 1=Male("men") then 0=Female("women").
    res_2x2 <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      row = "area",
      column = "gender",
      release = "FIXTURE_2026"
    )
    expect_match(res_2x2$what_a_cell_is$structure_text, "4 cells in total:", fixed = TRUE)
    expect_type(res_2x2$what_a_cell_is$items, "list")
    expect_length(res_2x2$what_a_cell_is$items, 4L)
    expect_equal(
      unlist(res_2x2$what_a_cell_is$items),
      c(
        "Men living in urban areas",
        "Women living in urban areas",
        "Men living in rural areas",
        "Women living in rural areas"
      )
    )

    # (B) 1 slot, 3 categories (lstatus) -> <=4 mode, 3 enumerated items.
    res_lstatus <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      row = "lstatus",
      release = "FIXTURE_2026"
    )
    expect_match(res_lstatus$what_a_cell_is$structure_text, "3 cells in total:", fixed = TRUE)
    expect_length(res_lstatus$what_a_cell_is$items, 3L)

    # (C) 1 slot, 5 categories (educat4) -> >4 mode, single per-slot breakdown line.
    res_educ <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      row = "educat4",
      release = "FIXTURE_2026"
    )
    expect_match(res_educ$what_a_cell_is$structure_text, "5 cells in total:", fixed = TRUE)
    expect_length(res_educ$what_a_cell_is$items, 1L)
    expect_match(res_educ$what_a_cell_is$items[[1L]], "Row (education level): people with ", fixed = TRUE)
    expect_match(res_educ$what_a_cell_is$items[[1L]], "education.", fixed = TRUE)

    # (D) >4 cells (educat4 (5) x gender (2) = 10) -> 2 breakdown lines; gender uses nouns.
    res_10 <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      row = "educat4",
      column = "gender",
      release = "FIXTURE_2026"
    )
    expect_match(res_10$what_a_cell_is$structure_text, "10 cells in total:", fixed = TRUE)
    expect_length(res_10$what_a_cell_is$items, 2L)
    expect_equal(res_10$what_a_cell_is$items[[2L]], "Column (gender): men or women.")
    expect_lte(length(res_10$what_a_cell_is$items), 4L)

    # (E) 0 slots -> empty items, null example_text, no cell-count suffix.
    res_0 <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      release = "FIXTURE_2026"
    )
    expect_length(res_0$what_a_cell_is$items, 0L)
    expect_null(res_0$what_a_cell_is$example_text)
    expect_false(grepl("cells in total", res_0$what_a_cell_is$structure_text, fixed = TRUE))

    # (F) Filter overlap: column = area, filter to Urban only ("1") -> 1 cell in total (singular),
    # 1 item, example cell is Urban (not Rural).
    res_f1 <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      column = "area",
      filter_base = list(area = "1"),
      release = "FIXTURE_2026"
    )
    expect_match(res_f1$what_a_cell_is$structure_text, "1 cell in total:", fixed = TRUE)
    expect_false(grepl("1 cells in total", res_f1$what_a_cell_is$structure_text, fixed = TRUE))
    expect_length(res_f1$what_a_cell_is$items, 1L)
    expect_match(res_f1$what_a_cell_is$items[[1L]], "urban", ignore.case = TRUE)
    expect_false(grepl("rural", res_f1$what_a_cell_is$items[[1L]], ignore.case = TRUE))

    # (G) Filter overlap in 2x2: row=area filtered to Rural only ("0"), column=gender ->
    # 2 cells in total, first cell is a rural men cell (not urban).
    res_f2 <- build_cell_explanation(
      analysis_var = "welfare",
      measures = "mean",
      row = "area",
      column = "gender",
      filter_base = list(area = "0"),
      release = "FIXTURE_2026"
    )
    expect_match(res_f2$what_a_cell_is$structure_text, "2 cells in total:", fixed = TRUE)
    expect_length(res_f2$what_a_cell_is$items, 2L)
    expect_true(all(grepl("rural", unlist(res_f2$what_a_cell_is$items), ignore.case = TRUE)))
    expect_false(any(grepl("urban", unlist(res_f2$what_a_cell_is$items), ignore.case = TRUE)))

    # items never exceeds 4 across every multi-slot scenario above
    scenarios <- list(
      "2x2"     = res_2x2,
      "lstatus" = res_lstatus,
      "educ"    = res_educ,
      "10"      = res_10,
      "0"       = res_0,
      "f1"      = res_f1,
      "f2"      = res_f2
    )
    for (nm in names(scenarios)) {
      expect_lte(length(scenarios[[nm]]$what_a_cell_is$items), 4L, label = nm)
    }
  })
})

test_that("what_a_cell_is$items is empty with no cell-count suffix when the registry is unavailable", {
  skip_if_not_installed("yaml")
  ns <- asNamespace("piptm")
  env <- ns$.piptm_env
  old_regs  <- env$registries
  old_cur   <- env$current_release
  old_arrow <- env$arrow_root
  old_cache <- env$cell_prose
  old_env_arrow <- Sys.getenv("PIPTM_ARROW_ROOT", unset = NA_character_)
  on.exit({
    env$registries      <- old_regs
    env$current_release <- old_cur
    env$arrow_root      <- old_arrow
    env$cell_prose      <- old_cache
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
  env$cell_prose <- NULL

  res <- build_cell_explanation(
    analysis_var = "welfare",
    measures = "mean",
    row = "educat4",
    column = "gender",
    superColumn = "area",
    release = "MISSING_REL"
  )
  expect_length(res$what_a_cell_is$items, 0L)
  expect_false(grepl("cells in total", res$what_a_cell_is$structure_text, fixed = TRUE))
  expect_false(grepl("cell in total", res$what_a_cell_is$structure_text, fixed = TRUE))
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
    expect_match(res$what_a_cell_is$structure_text, "4 cells in total:", fixed = TRUE)
    expect_equal(res$what_a_cell_is$example_cell_label, "non-poor men living in rural areas")
    expect_length(res$what_a_cell_is$items, 4L)
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
  skip_if_not_installed("yaml")
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
      expect_equal(res_v$what_a_cell_is$structure_text, "Each cell is one gender group (column) \u2014 2 cells in total:")
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

test_that("R9(P3.1): 3+ filter variables join multiple 'who also' clauses with 'and'", {
  .with_fixture_registry("FIXTURE_2026", {
    res <- build_cell_explanation(
      analysis_var = "welfare",
      measures     = "mean",
      filter_base  = list(age_group = "3", educat7 = "4", area = "0"),
      release      = "FIXTURE_2026"
    )
    expect_equal(
      res$who_is_in_table$text,
      paste0(
        "People aged 25\u201364 who also have incomplete secondary education ",
        "and who also live in rural areas. Anyone outside this group is excluded before the table is built."
      )
    )
  })
})

test_that("R8(c) / V5: NR suppression note trigger matches R/table_response.R active_slots condition and R/table_maker.R", {
  # 1. Inspect the real source files to verify the structural condition has not drifted
  tr_path <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "R", "table_response.R"),
    error = function(e) ""
  )
  tm_path <- tryCatch(
    file.path(rprojroot::find_package_root_file(), "R", "table_maker.R"),
    error = function(e) ""
  )
  if (nzchar(tr_path) && file.exists(tr_path)) {
    tr_src <- paste(readLines(tr_path, warn = FALSE), collapse = "\n")
    # R/table_response.R defines active_slots as non-null, non-empty trimmed slot strings
    # and branches on if (length(active_slots) == 0L) for single-cell vs multi-cell
    expect_match(tr_src, "active_slots <- Filter\\(", perl = TRUE)
    expect_match(tr_src, "if \\(length\\(active_slots\\) == 0L\\)", perl = TRUE)
  }
  if (nzchar(tm_path) && file.exists(tm_path)) {
    tm_src <- paste(readLines(tm_path, warn = FALSE), collapse = "\n")
    # R/table_maker.R masks cells as "NR" iff !is.null(pop_share_threshold) && length(dim_cols) > 0L
    expect_match(tm_src, "if \\(!is\\.null\\(pop_share_threshold\\)\\)", perl = TRUE)
    expect_match(tm_src, "if \\(length\\(dim_cols\\) > 0L\\)", perl = TRUE)
  }

  # 2. Functional parity across (slots x pop_share_threshold) combinations
  .with_fixture_registry("FIXTURE_2026", {
    grid <- list(
      list(slots = list(column = NULL, row = NULL, superColumn = NULL, superRow = NULL), threshold = 0.001, expect_nr_note = FALSE),
      list(slots = list(column = NULL, row = NULL, superColumn = NULL, superRow = NULL), threshold = NULL,  expect_nr_note = FALSE),
      list(slots = list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL), threshold = 0.001, expect_nr_note = TRUE),
      list(slots = list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL), threshold = NULL,  expect_nr_note = FALSE),
      list(slots = list(column = "gender", row = "area", superColumn = NULL, superRow = NULL), threshold = 0.005, expect_nr_note = TRUE)
    )

    for (g in grid) {
      active_slots_tr <- Filter(
        function(k) !is.null(g$slots[[k]]) && nzchar(trimws(g$slots[[k]])),
        c("column", "row", "superColumn", "superRow")
      )
      expected_by_tr_and_tm <- (length(active_slots_tr) > 0L) && !is.null(g$threshold)
      expect_identical(expected_by_tr_and_tm, g$expect_nr_note)

      exp_out <- build_cell_explanation(
        analysis_var        = "welfare",
        measures            = "mean",
        column              = g$slots$column,
        row                 = g$slots$row,
        superColumn         = g$slots$superColumn,
        superRow            = g$slots$superRow,
        pop_share_threshold = g$threshold,
        release             = "FIXTURE_2026"
      )
      has_nr_note <- any(grepl("\"NR\" (Not Reported)", unlist(exp_out$notes), fixed = TRUE))
      expect_identical(has_nr_note, expected_by_tr_and_tm)
    }
  })
})

test_that("Edge cases: long-filter shares, modifier=null targets, 4 slots, 1-slot age_group, duplicate/empty filters, all 23 measures, and entry validation", {
  .with_fixture_registry("FIXTURE_2026", {
    # 1. Long filter (1 var with 3 categories) + shares: out_of never duplicates "everyone in the table"
    res_long_sh <- build_cell_explanation(
      analysis_var = "in_labor_force",
      measures     = c("pop_share", "target_within_group_share", "target_survey_share"),
      column       = "gender",
      filter_base  = list(educat4 = c("1", "2", "3")),
      release      = "FIXTURE_2026"
    )
    expect_equal(
      res_long_sh$what_is_shown$measures[[1L]]$out_of,
      "everyone in the table (the filtered group above)"
    )
    expect_equal(
      res_long_sh$what_is_shown$measures[[2L]]$counts,
      "men who are in the labor force"
    )
    expect_equal(
      res_long_sh$what_is_shown$measures[[3L]]$out_of,
      "everyone in the table (the filtered group above)"
    )

    # 2. All 4 layout slots active (column, row, superColumn, superRow)
    res_4slots <- build_cell_explanation(
      analysis_var = "welfare",
      measures     = "mean",
      column       = "gender",
      row          = "educat4",
      superColumn  = "area",
      superRow     = "age_group",
      release      = "FIXTURE_2026"
    )
    expect_length(res_4slots$what_a_cell_is$dimensions, 4L)
    expect_match(res_4slots$what_a_cell_is$structure_text, "age group (row group)", fixed = TRUE)
    # educat4(5) x gender(2) x area(2) x age_group(4) = 80 cells (>4) -> 4 per-slot breakdown lines
    expect_length(res_4slots$what_a_cell_is$items, 4L)

    # 3. 1-slot age_group does not double the word "group"; duplicate filter codes deduplicated; empty filter ignored
    res_age_slot <- build_cell_explanation(
      analysis_var = "welfare",
      measures     = "mean",
      column       = "age_group",
      filter_base  = list(area = c("0", "0"), empty_var = character(0)),
      release      = "FIXTURE_2026"
    )
    expect_equal(res_age_slot$what_a_cell_is$structure_text, "Each cell is one age group (column) \u2014 4 cells in total:")
    expect_equal(
      res_age_slot$who_is_in_table$text,
      "People living in rural areas. Anyone outside this group is excluded before the table is built."
    )

    # 4. All 23 canonical measures in tm_measure_spec.yaml render non-empty text with no unexpanded {...}
    meas_spec <- yaml::read_yaml(
      system.file("extdata", "tm_measure_spec.yaml", package = "piptm")
    )$stat_groups
    for (g in meas_spec) {
      mkeys <- names(g$measures)
      avar <- if (identical(g$id, "shares")) {
        "imp_wat_rec"
      } else if (identical(g$id, "poverty")) {
        "pov_status"
      } else {
        "welfare"
      }
      res_m <- build_cell_explanation(
        analysis_var = avar,
        measures     = mkeys,
        column       = "gender",
        poverty_line = 2.15,
        release      = "FIXTURE_2026"
      )
      for (m_item in res_m$what_is_shown$measures) {
        expect_true(nzchar(m_item$text), info = paste("Empty text for", m_item$key))
        expect_false(grepl("\\{[A-Za-z_]+\\}", m_item$text), info = paste("Unexpanded placeholder in", m_item$key))
      }
    }

    # 5. Entry-point argument validation for direct R callers
    expect_error(build_cell_explanation(analysis_var = NULL, measures = "mean"), "analysis_var")
    expect_error(build_cell_explanation(analysis_var = "welfare", measures = character(0)), "measures")
    expect_error(build_cell_explanation(analysis_var = "pov_status", measures = "headcount", poverty_line = -1), "poverty_line")
    expect_error(build_cell_explanation(analysis_var = "welfare", measures = "mean", ppp = 2017.5), "ppp")
  })
})
