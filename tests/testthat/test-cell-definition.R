# Test: build_cell_definition() — SAFETY-CRITICAL tests
# These tests serve as the primary defense against incorrect cell content.
# All 4 worked examples must produce byte-identical output.

# Helper to construct resolved_labels for cell definition tests
.make_resolved_labels <- function(analysis_var_info, measures_info, filters_info = NULL,
                                   covariates_info = NULL) {
  list(
    analysis_var = analysis_var_info,
    measures = measures_info,
    filters = filters_info,
    covariates = covariates_info
  )
}


# ── Example 1: Simple Aggregate Mean ─────────────────────────────────────────

test_that("Example 1: Simple aggregate mean produces exact expected output", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = NULL,
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  # Expected output from spec §4.3 Example 1
  expected_sentence <- paste(
    "The Mean of Welfare for survey-weighted individuals",
    "in the selected survey."
  )

  expect_equal(result$population_scope, "survey-weighted individuals in the selected survey")
  expect_length(result$measure_interpretation, 1L)
  expect_equal(result$measure_interpretation[1], expected_sentence)
  expect_null(result$note)
})


# ── Example 2: Filtered Poverty Analysis with Covariates ────────────────────

test_that("Example 2: Filtered poverty with covariates produces exact expected output", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = c("headcount", "poverty_gap"),
      ui_label = c("Poverty headcount", "Poverty gap index"),
      stat_group = c("poverty", "poverty")
    ),
    filters_info = data.table(
      varname = "age_group",
      ui_label = "Age group",
      selected_codes = list(c(1L, 2L)),
      selected_labels = list(c("0 to 14", "15 to 24"))
    ),
    covariates_info = data.table(
      slot = c("columns", "rows"),
      varname = c("gender", "area"),
      ui_label = c("Gender", "Area"),
      n_categories = c(2L, 2L)
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = c("headcount", "poverty_gap"),
    filter_base = list(age_group = c(1L, 2L)),
    by = c("gender", "area"),
    poverty_line = 2.15,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  # Expected outputs from spec §4.3 Example 2
  expected_pop_scope <- paste(
    "survey-weighted individuals for whom Age group is among [0 to 14, 15 to 24],",
    "within each Gender × Area group"
  )
  expected_sentence_1 <- paste(
    "The Poverty headcount at $2.15/day (PPP 2021) for survey-weighted",
    "individuals for whom Age group is among [0 to 14, 15 to 24],",
    "within each Gender × Area group."
  )
  expected_sentence_2 <- paste(
    "The Poverty gap index at $2.15/day (PPP 2021) for survey-weighted",
    "individuals for whom Age group is among [0 to 14, 15 to 24],",
    "within each Gender × Area group."
  )
  
  expect_equal(result$population_scope, expected_pop_scope)
  expect_length(result$measure_interpretation, 2L)
  expect_equal(result$measure_interpretation[1], expected_sentence_1)
  expect_equal(result$measure_interpretation[2], expected_sentence_2)
  expect_null(result$note)
})


# ── Example 3: Binary Analysis Variable with Target Shares ──────────────────

test_that("Example 3: Binary var with target shares produces exact expected output", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "imp_wat_rec",
      ui_label = "Improved water source",
      tm_type = "binary"
    ),
    measures_info = data.table(
      measure = c("target_within_group_share", "target_survey_share"),
      ui_label = c("Target share within cell", "Target share in total survey"),
      stat_group = c("shares", "shares")
    ),
    filters_info = data.table(
      varname = "age_group",
      ui_label = "Age group",
      selected_codes = list(1L),
      selected_labels = list("0 to 14")
    ),
    covariates_info = data.table(
      slot = "columns",
      varname = "area",
      ui_label = "Area",
      n_categories = 2L
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "imp_wat_rec",
    measures = c("target_within_group_share", "target_survey_share"),
    filter_base = list(age_group = 1L),
    by = c("area"),
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  # Expected outputs from spec §4.3 Example 3 (NO interpretation blocks)
  expected_pop_scope <- paste(
    "survey-weighted individuals for whom Age group is among [0 to 14],",
    "within each Area group"
  )
  expected_sentence_1 <- paste(
    "Within survey-weighted individuals for whom Age group is among [0 to 14],",
    "within each Area group, the share for which Improved water source",
    "is true."
  )
  expected_sentence_2 <- paste(
    "The share of the total survey-weighted population represented",
    "by survey-weighted individuals for whom Age group is among [0 to 14],",
    "within each Area group where Improved water source is true."
  )
  
  expect_equal(result$population_scope, expected_pop_scope)
  expect_length(result$measure_interpretation, 2L)
  expect_equal(result$measure_interpretation[1], expected_sentence_1)
  expect_equal(result$measure_interpretation[2], expected_sentence_2)
  expect_null(result$note)
})


# ── Example 4: Poverty Status as Covariate ──────────────────────────────────

test_that("Example 4: pov_status covariate produces exact expected output with note", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = c("mean", "gini"),
      ui_label = c("Mean", "Gini index"),
      stat_group = c("summary_statistics", "inequality")
    ),
    covariates_info = data.table(
      slot = "columns",
      varname = "pov_status",
      ui_label = "Poverty status",
      n_categories = 2L
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    filter_base = NULL,
    by = c("pov_status"),
    poverty_line = 6.85,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  # Expected outputs from spec §4.3 Example 4 (NO interpretation blocks)
  expected_pop_scope <- paste(
    "survey-weighted individuals in the selected survey, within each",
    "Poverty status group (below/above $6.85/day PPP 2021)"
  )
  expected_sentence_1 <- paste(
    "The Mean of Welfare for survey-weighted individuals in",
    "the selected survey, within each Poverty status group",
    "(below/above $6.85/day PPP 2021)."
  )
  expected_sentence_2 <- paste(
    "The Gini index of Welfare among survey-weighted individuals",
    "in the selected survey, within each Poverty status group",
    "(below/above $6.85/day PPP 2021)."
  )
  expected_note <- paste(
    "Note: Poverty status groups are defined using a threshold",
    "of $6.85/day (PPP 2021)."
  )
  
  expect_equal(result$population_scope, expected_pop_scope)
  expect_length(result$measure_interpretation, 2L)
  expect_equal(result$measure_interpretation[1], expected_sentence_1)
  expect_equal(result$measure_interpretation[2], expected_sentence_2)
  expect_equal(result$note, expected_note)
})

test_that("build_cell_definition handles NULL filters safely", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "unknown"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    ),
    filters_info = NULL
  )

  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = list(age_group = 1L),
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST",
    resolved_labels = resolved_labels
  )

  expect_match(result$population_scope, "survey-weighted individuals", fixed = TRUE)
  expect_length(result$measure_interpretation, 1L)
})

test_that("build_cell_definition annotates unknown tm_type", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "mystery",
      ui_label = "mystery",
      tm_type = "unknown"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    )
  )

  result <- build_cell_definition(
    analysis_var = "mystery",
    measures = "mean",
    filter_base = NULL,
    by = "pov_status",
    poverty_line = 2.15,
    ppp = 2021L,
    release = "TEST",
    resolved_labels = resolved_labels
  )

  expect_true(grepl("Poverty status", result$population_scope, fixed = TRUE))
  expect_match(result$note, "metadata", fixed = FALSE)
})


# ── Negative Tests: Ensure WRONG phrasing is NOT produced ───────────────────

test_that("Negative test: Inequality measures use 'among' not 'for'", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "gini",
      ui_label = "Gini index",
      stat_group = "inequality"
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "gini",
    filter_base = NULL,
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  sentence <- result$measure_interpretation[1]
  expect_match(sentence, "among", fixed = TRUE)
  expect_false(grepl(" for ", sentence, fixed = TRUE))
})


test_that("Negative test: Summary stats use 'of' and 'for', not 'among'", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = NULL,
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  sentence <- result$measure_interpretation[1]
  expect_match(sentence, " of ", fixed = TRUE)
  expect_match(sentence, " for ", fixed = TRUE)
  expect_false(grepl(" among ", sentence, fixed = TRUE))
})


test_that("Negative test: Poverty measures include dollar sign and PPP year", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "headcount",
      ui_label = "Poverty headcount",
      stat_group = "poverty"
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "headcount",
    filter_base = NULL,
    by = NULL,
    poverty_line = 2.15,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  sentence <- result$measure_interpretation[1]
  expect_match(sentence, "$", fixed = TRUE)
  expect_match(sentence, "PPP 2021", fixed = TRUE)
})


test_that("Negative test: target_within_group_share with by=NULL uses 'total weighted survey population'", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "imp_wat_rec",
      ui_label = "Improved water source",
      tm_type = "binary"
    ),
    measures_info = data.table(
      measure = "target_within_group_share",
      ui_label = "Target share within cell",
      stat_group = "shares"
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "imp_wat_rec",
    measures = "target_within_group_share",
    filter_base = NULL,
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  sentence <- result$measure_interpretation[1]
  expect_match(sentence, "total weighted survey population", fixed = TRUE)
})


# ── Edge Cases ───────────────────────────────────────────────────────────────

test_that("Edge case: Multiple filters use AND separator", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    ),
    filters_info = data.table(
      varname = c("age_group", "area"),
      ui_label = c("Age group", "Area"),
      selected_codes = list(c(1L), c(1L)),
      selected_labels = list("0 to 14", "Urban")
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = list(age_group = 1L, area = 1L),
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  pop_scope <- result$population_scope
  expect_match(pop_scope, " AND ", fixed = TRUE)
  expect_match(pop_scope, "Age group is among [0 to 14]", fixed = TRUE)
  expect_match(pop_scope, "Area is among [Urban]", fixed = TRUE)
})


test_that("Edge case: 3+ covariates use × join", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    ),
    covariates_info = data.table(
      slot = c("columns", "rows", "super_columns"),
      varname = c("gender", "area", "education"),
      ui_label = c("Gender", "Area", "Education"),
      n_categories = c(2L, 2L, 3L)
    )
  )
  
  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = NULL,
    by = c("gender", "area", "education"),
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )
  
  pop_scope <- result$population_scope
  expect_match(pop_scope, "Gender × Area × Education", fixed = TRUE)
})


test_that("Edge case: Binary analysis var with non-share measures handled gracefully", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "imp_wat_rec",
      ui_label = "Improved water source",
      tm_type = "binary"
    ),
    measures_info = data.table(
      measure = "mean",
      ui_label = "Mean",
      stat_group = "summary_statistics"
    )
  )
  
  # This should not error (UI gates it, but builder should handle)
  expect_no_error({
    result <- build_cell_definition(
      analysis_var = "imp_wat_rec",
      measures = "mean",
      filter_base = NULL,
      by = NULL,
      poverty_line = NULL,
      ppp = 2021L,
      release = "TEST_2024",
      resolved_labels = resolved_labels
    )
  })
  
  expect_length(result$measure_interpretation, 1L)
})
# P0-4: Schema validation for build_cell_definition()
# Tests for filters_dt column validation

test_that("build_cell_definition() aborts when filters_dt missing varname", {
  resolved_labels <- list(
    analysis_var = list(varname = "welfare", ui_label = "Welfare", tm_type = "continuous"),
    measures = data.table(measure = "mean", ui_label = "Mean", stat_group = "summary_statistics"),
    filters = data.table(
      # missing varname
      ui_label = "Age group",
      selected_labels = list(c("0-14"))
    )
  )
  expect_error(
    build_cell_definition(
      analysis_var = "welfare", measures = "mean",
      filter_base = list(age_group = 1L), by = NULL,
      poverty_line = NULL, ppp = 2021L, release = "TEST",
      resolved_labels = resolved_labels
    ),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

test_that("build_cell_definition() aborts when filters_dt missing ui_label", {
  resolved_labels <- list(
    analysis_var = list(varname = "welfare", ui_label = "Welfare", tm_type = "continuous"),
    measures = data.table(measure = "mean", ui_label = "Mean", stat_group = "summary_statistics"),
    filters = data.table(
      varname = "age_group",
      # missing ui_label
      selected_labels = list(c("0-14"))
    )
  )
  expect_error(
    build_cell_definition(
      analysis_var = "welfare", measures = "mean",
      filter_base = list(age_group = 1L), by = NULL,
      poverty_line = NULL, ppp = 2021L, release = "TEST",
      resolved_labels = resolved_labels
    ),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

test_that("build_cell_definition() aborts when filters_dt missing selected_labels", {
  resolved_labels <- list(
    analysis_var = list(varname = "welfare", ui_label = "Welfare", tm_type = "continuous"),
    measures = data.table(measure = "mean", ui_label = "Mean", stat_group = "summary_statistics"),
    filters = data.table(
      varname = "age_group",
      ui_label = "Age group"
      # missing selected_labels
    )
  )
  expect_error(
    build_cell_definition(
      analysis_var = "welfare", measures = "mean",
      filter_base = list(age_group = 1L), by = NULL,
      poverty_line = NULL, ppp = 2021L, release = "TEST",
      resolved_labels = resolved_labels
    ),
    class = "rlang_error",
    regexp = "missing required columns"
  )
})

test_that("build_cell_definition() handles NULL filters_dt gracefully", {
  resolved_labels <- list(
    analysis_var = list(varname = "welfare", ui_label = "Welfare", tm_type = "continuous"),
    measures = data.table(measure = "mean", ui_label = "Mean", stat_group = "summary_statistics"),
    filters = NULL
  )
  # Should not error when filters is NULL (no filter-base case)
  expect_no_error({
    result <- build_cell_definition(
      analysis_var = "welfare", measures = "mean",
      filter_base = NULL, by = NULL,
      poverty_line = NULL, ppp = 2021L, release = "TEST",
      resolved_labels = resolved_labels
    )
  })
  expect_match(result$population_scope, "survey-weighted individuals in the selected survey", fixed = TRUE)
})
