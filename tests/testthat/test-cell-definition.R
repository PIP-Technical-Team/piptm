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
      ui_label = c("Target share within cell", "Target share in sample base"),
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
  
  # Expected outputs: shares now render as a structured payload, not prose.
  expected_pop_scope <- paste(
    "survey-weighted individuals for whom Age group is among [0 to 14],",
    "within each Area group"
  )
  expected_base_pop <- "survey-weighted individuals for whom Age group is among [0 to 14]"
  expected_cell_numerator <- paste0(
    "Individuals in this cell (survey-weighted individuals for whom ",
    "Age group is among [0 to 14], within each Area group)"
  )
  expected_target_numerator <- "Cell members for whom Improved water source is true"

  expect_equal(result$population_scope, expected_pop_scope)
  expect_null(result$note)
  expect_type(result$measure_interpretation, "list")
  expect_named(
    result$measure_interpretation,
    c("prose", "shares_table", "shares_footer")
  )
  expect_length(result$measure_interpretation$prose, 0L)

  shares_table <- result$measure_interpretation$shares_table
  expect_s3_class(shares_table, "data.table")
  expect_equal(nrow(shares_table), 2L)
  expect_named(
    shares_table,
    c("measure", "denominator", "numerator", "plain_meaning")
  )

  within_row <- shares_table[measure == "Target share within cell"]
  expect_equal(within_row$denominator, expected_cell_numerator)
  expect_equal(within_row$numerator, expected_target_numerator)
  expect_equal(
    within_row$plain_meaning,
    paste(
      "Among all individuals for whom Age group is among [0 to 14] in this",
      "Area group, what fraction have Improved water source?"
    )
  )

  survey_row <- shares_table[measure == "Target share in sample base"]
  expect_equal(survey_row$denominator, expected_base_pop)
  expect_equal(survey_row$numerator, expected_target_numerator)
  expect_equal(
    survey_row$plain_meaning,
    paste(
      "Among all individuals for whom Age group is among [0 to 14], what",
      "fraction fall in this Area group and have Improved water source?"
    )
  )

  # Footer must name the measures using the SAME labels as the table above:
  # "Target share in sample base" (this test's ui_label for
  # target_survey_share), and the canonical fallback "Population share"
  # since pop_share was not requested and has no row in `measures_dt` here.
  expect_equal(
    result$measure_interpretation$shares_footer,
    paste0(
      "For cells with positive weighted population: Target share in sample ",
      "base = Population share \u00d7 Target share within cell."
    )
  )
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


test_that("Negative test: target_within_group_share with by=NULL uses generic total-survey phrasing", {
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
  
  shares_table <- result$measure_interpretation$shares_table
  expect_equal(nrow(shares_table), 1L)
  expect_null(result$measure_interpretation$shares_footer)
  expect_equal(
    shares_table$plain_meaning,
    "Among the total survey population, what fraction have Improved water source?"
  )
})


# ── Structured shares table: additional scenarios ───────────────────────────

test_that("Structured shares table: all three shares, no filter, with grouping", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "imp_wat_rec",
      ui_label = "Improved water source",
      tm_type = "binary"
    ),
    measures_info = data.table(
      measure = c("pop_share", "target_within_group_share", "target_survey_share"),
      ui_label = c(
        "Population share",
        "Target share within cell",
        "Sample base share"
      ),
      stat_group = c("shares", "shares", "shares")
    ),
    covariates_info = data.table(
      slot = "columns",
      varname = "gender",
      ui_label = "Gender",
      n_categories = 2L
    )
  )

  result <- build_cell_definition(
    analysis_var = "imp_wat_rec",
    measures = c("pop_share", "target_within_group_share", "target_survey_share"),
    filter_base = NULL,
    by = c("gender"),
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )

  expected_base_pop <- "survey-weighted individuals in the selected survey"
  expected_cell_numerator <- paste0(
    "Individuals in this cell (survey-weighted individuals in the ",
    "selected survey, within each Gender group)"
  )
  expected_target_numerator <- "Cell members for whom Improved water source is true"

  shares_table <- result$measure_interpretation$shares_table
  expect_equal(nrow(shares_table), 3L)

  pop_row <- shares_table[measure == "Population share"]
  expect_equal(pop_row$denominator, expected_base_pop)
  expect_equal(pop_row$numerator, expected_cell_numerator)
  expect_equal(
    pop_row$plain_meaning,
    "Among the total survey population, what fraction fall in this Gender group?"
  )

  within_row <- shares_table[measure == "Target share within cell"]
  expect_equal(within_row$denominator, expected_cell_numerator)
  expect_equal(within_row$numerator, expected_target_numerator)
  expect_equal(
    within_row$plain_meaning,
    paste(
      "Among the total survey population in this Gender group, what",
      "fraction have Improved water source?"
    )
  )

  survey_row <- shares_table[measure == "Sample base share"]
  expect_equal(survey_row$denominator, expected_base_pop)
  expect_equal(survey_row$numerator, expected_target_numerator)
  expect_equal(
    survey_row$plain_meaning,
    paste(
      "Among the total survey population, what fraction fall in this",
      "Gender group and have Improved water source?"
    )
  )

  # Regression: the footer must reference each requested share measure using
  # the SAME resolved ui_label shown in its table row, not an independently
  # hardcoded name. This fixture deliberately uses a custom label
  # ("Sample base share") for target_survey_share, distinct from both the
  # real piptm registry default ("Target share in sample base",
  # inst/extdata/tm_measure_spec.yaml) and the earlier incorrect hardcoded
  # footer text ("Target share in total survey"), to prove the footer is not
  # hardcoded to either.
  footer <- result$measure_interpretation$shares_footer
  expect_false(is.null(footer))
  expect_match(footer, "Population share", fixed = TRUE)
  expect_match(footer, "Target share within cell", fixed = TRUE)
  expect_match(footer, "Sample base share", fixed = TRUE)
  expect_false(grepl("Target share in sample base", footer, fixed = TRUE))
  expect_false(grepl("Target share in total survey", footer, fixed = TRUE))
})


test_that("Structured shares table: filtered with no grouping uses honest degenerate wording", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "imp_wat_rec",
      ui_label = "Improved water source",
      tm_type = "binary"
    ),
    measures_info = data.table(
      measure = c("pop_share", "target_within_group_share", "target_survey_share"),
      ui_label = c(
        "Population share",
        "Target share within cell",
        "Target share in sample base"
      ),
      stat_group = c("shares", "shares", "shares")
    ),
    filters_info = data.table(
      varname = "age_group",
      ui_label = "Age group",
      selected_codes = list(1L),
      selected_labels = list("0 to 14")
    )
  )

  result <- build_cell_definition(
    analysis_var = "imp_wat_rec",
    measures = c("pop_share", "target_within_group_share", "target_survey_share"),
    filter_base = list(age_group = 1L),
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )

  shares_table <- result$measure_interpretation$shares_table
  expect_equal(nrow(shares_table), 3L)

  pop_row <- shares_table[measure == "Population share"]
  expect_equal(pop_row$plain_meaning, "This value is always 1 when there is no grouping.")

  within_row <- shares_table[measure == "Target share within cell"]
  survey_row <- shares_table[measure == "Target share in sample base"]

  expect_equal(
    within_row$plain_meaning,
    paste(
      "Among all individuals for whom Age group is among [0 to 14], what",
      "fraction have Improved water source?"
    )
  )
  # target_survey_share carries a parenthetical cross-reference so the two
  # rows are never textually indistinguishable, even though the underlying
  # values are numerically identical when there is no grouping.
  expect_equal(
    survey_row$plain_meaning,
    paste0(
      "Among all individuals for whom Age group is among [0 to 14], what ",
      "fraction have Improved water source? (Equivalent to Target share ",
      "within cell when no grouping is applied.)"
    )
  )
  expect_false(identical(within_row$plain_meaning, survey_row$plain_meaning))
})


test_that("Structured shares table: pov_status group label matches population_scope wording", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = "pop_share",
      ui_label = "Population share",
      stat_group = "shares"
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
    measures = "pop_share",
    filter_base = NULL,
    by = c("pov_status"),
    poverty_line = 6.85,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )

  # The group label used in plain_meaning ("Poverty status") must match the
  # group label embedded in population_scope's qualifier -- same source
  # (.resolve_group_label()), so they can never drift.
  expect_match(result$population_scope, "Poverty status", fixed = TRUE)
  shares_table <- result$measure_interpretation$shares_table
  expect_equal(
    shares_table$plain_meaning,
    "Among the total survey population, what fraction fall in this Poverty status group?"
  )
})


test_that("Structured shares table: mixed shares and non-share measures", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "continuous"
    ),
    measures_info = data.table(
      measure = c("mean", "pop_share"),
      ui_label = c("Mean", "Population share"),
      stat_group = c("summary_statistics", "shares")
    )
  )

  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = c("mean", "pop_share"),
    filter_base = NULL,
    by = NULL,
    poverty_line = NULL,
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )

  expect_type(result$measure_interpretation, "list")
  expect_equal(
    result$measure_interpretation$prose,
    "The Mean of Welfare for survey-weighted individuals in the selected survey."
  )

  shares_table <- result$measure_interpretation$shares_table
  expect_equal(nrow(shares_table), 1L)
  expect_equal(shares_table$measure, "Population share")

  # Only one share measure requested: no footer.
  expect_null(result$measure_interpretation$shares_footer)
})


test_that("No shares requested: measure_interpretation remains a character vector", {
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

  expect_type(result$measure_interpretation, "character")
  expect_length(result$measure_interpretation, 1L)
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


test_that("Edge case: Poverty measures support multiple poverty lines", {
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
    poverty_line = c(2.15, 6.85),
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )

  expect_equal(result$population_scope, "survey-weighted individuals in the selected survey")
  expect_identical(
    result$measure_interpretation,
    c("The Poverty headcount at $2.15/day / $6.85/day (PPP 2021) for survey-weighted individuals in the selected survey.")
  )
})


test_that("Edge case: pov_status grouping supports multiple poverty lines", {
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
      slot = "columns",
      varname = "pov_status",
      ui_label = "Poverty status",
      n_categories = 2L
    )
  )

  result <- build_cell_definition(
    analysis_var = "welfare",
    measures = "mean",
    filter_base = NULL,
    by = c("pov_status"),
    poverty_line = c(2.15, 6.85),
    ppp = 2021L,
    release = "TEST_2024",
    resolved_labels = resolved_labels
  )

  expect_match(
    result$population_scope,
    "below/above poverty lines \\$2.15/day / \\$6.85/day PPP 2021"
  )
  expect_match(
    result$note,
    "thresholds of \\$2.15/day / \\$6.85/day \\(PPP 2021\\)"
  )
})


test_that("build_cell_definition unwraps list-wrapped analysis variable metadata after JSON coercion", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = list("Welfare"),
      tm_type = list("continuous")
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

  expect_match(result$measure_interpretation[1], "The Mean of Welfare", fixed = TRUE)
  expect_null(result$note)
})


test_that("build_cell_definition falls back to raw analysis_var/'unknown' when JSON coercion yields empty metadata", {
  resolved_labels <- .make_resolved_labels(
    analysis_var_info = list(
      varname = "welfare",
      ui_label = list(),
      tm_type = list()
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

  expect_match(result$measure_interpretation[1], "The Mean of welfare", fixed = TRUE)
  expect_match(result$note, "metadata", fixed = FALSE)
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
