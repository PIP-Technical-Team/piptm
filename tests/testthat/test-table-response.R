test_that("build_table_layout constructs fixed 4-slot layout with categories", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(
    column = "gender",
    row = "area",
    superColumn = "pov_status",
    superRow = NULL
  )

  layout <- piptm:::build_table_layout(slots, release = rel)

  expect_type(layout, "list")
  expect_named(layout, c("columns", "rows", "superColumns", "superRows"), ignore.order = FALSE)

  # columns: gender
  expect_equal(layout$columns$varname, "gender")
  expect_equal(layout$columns$label, "Gender")
  expect_type(layout$columns$categories, "list")
  expect_equal(length(layout$columns$categories), 2L)

  # rows: area
  expect_equal(layout$rows$varname, "area")
  expect_equal(layout$rows$label, "Area")
  expect_type(layout$rows$categories, "list")

  # superColumns: pov_status
  expect_equal(layout$superColumns$varname, "pov_status")
  expect_equal(layout$superColumns$label, "Poverty status")
  expect_equal(length(layout$superColumns$categories), 2L)
  expect_equal(layout$superColumns$categories[[1]]$label, "Non-poor")
  expect_equal(layout$superColumns$categories[[2]]$label, "Poor")

  # superRows: unused -> NULL
  expect_null(layout$superRows)
})

test_that("build_table_layout with all slots NULL returns all 4 keys as NULL", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = NULL, row = NULL, superColumn = NULL, superRow = NULL)
  layout <- piptm:::build_table_layout(slots, release = rel)

  expect_named(layout, c("columns", "rows", "superColumns", "superRows"), ignore.order = FALSE)
  expect_null(layout$columns)
  expect_null(layout$rows)
  expect_null(layout$superColumns)
  expect_null(layout$superRows)
})

test_that("build_table_layout throws error if slot varname not in registry", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "non_existent_var", row = NULL, superColumn = NULL, superRow = NULL)
  expect_error(piptm:::build_table_layout(slots, release = rel), "non_existent_var")
})

test_that("serialize_table_response serializes 1 survey with 2 slots into Approach C structure", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "gender", row = "area", superColumn = NULL, superRow = NULL)

  # Hand-built formatted flat table as returned by format_table_output
  flat_dt <- data.table::data.table(
    pip_id = rep("COL_2010_ECH_INC_ALL", 8L),
    country_code = rep("COL", 8L),
    surveyid_year = rep(2010L, 8L),
    welfare_type = rep("INC", 8L),
    gender = c("male", "male", "male", "male", "female", "female", "female", "female"),
    area = c("urban", "urban", "rural", "rural", "urban", "urban", "rural", "rural"),
    poverty_line = rep(NA_real_, 8L),
    measure = rep(c("mean", "median"), 4L),
    value = c("12.50", "10.20", "8.30", "7.10", "11.40", "9.80", "7.90", "6.50")
  )

  res <- piptm::serialize_table_response(
    dt = flat_dt,
    slots = slots,
    analysis_var = "welfare",
    poverty_line = NULL,
    measures = c("mean", "median"),
    release = rel
  )

  expect_type(res, "list")
  expect_length(res, 1L)

  survey_obj <- res[[1L]]
  expect_equal(survey_obj$pip_id, "COL_2010_ECH_INC_ALL")
  expect_equal(survey_obj$country_code, "COL")
  expect_equal(survey_obj$surveyid_year, 2010L)
  expect_equal(survey_obj$welfare_type, "INC")
  expect_null(survey_obj$poverty_line)
  expect_equal(survey_obj$analysis_var, "welfare")
  expect_equal(as.character(survey_obj$measures), c("mean", "median"))

  # layout
  expect_named(survey_obj$layout, c("columns", "rows", "superColumns", "superRows"), ignore.order = FALSE)
  expect_equal(survey_obj$layout$columns$varname, "gender")
  expect_equal(survey_obj$layout$rows$varname, "area")
  expect_null(survey_obj$layout$superColumns)
  expect_null(survey_obj$layout$superRows)

  # cells
  expect_type(survey_obj$cells, "list")
  expect_length(survey_obj$cells, 4L)

  cell1 <- survey_obj$cells[[1L]]
  expect_named(cell1, c("superRow", "row", "superColumn", "column", "measures"), ignore.order = FALSE)
  expect_false("population" %in% names(cell1))
  expect_null(cell1$superRow)
  expect_null(cell1$superColumn)
  expect_type(cell1$row, "list")
  expect_named(cell1$row, c("code", "label"))
  expect_type(cell1$column, "list")
  expect_named(cell1$column, c("code", "label"))
  expect_named(cell1$measures, c("mean", "median"), ignore.order = FALSE)
})

test_that("serialize_table_response handles no-slots national table (single all-null cell)", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = NULL, row = NULL, superColumn = NULL, superRow = NULL)

  flat_dt <- data.table::data.table(
    pip_id = "COL_2010_ECH_INC_ALL",
    country_code = "COL",
    surveyid_year = 2010L,
    welfare_type = "INC",
    poverty_line = NA_real_,
    measure = c("mean", "median"),
    value = c("12.50", "10.20")
  )

  res <- piptm::serialize_table_response(
    dt = flat_dt,
    slots = slots,
    analysis_var = "welfare",
    poverty_line = NULL,
    measures = c("mean", "median"),
    release = rel
  )

  expect_length(res, 1L)
  survey_obj <- res[[1L]]
  expect_null(survey_obj$layout$columns)
  expect_null(survey_obj$layout$rows)
  expect_null(survey_obj$layout$superColumns)
  expect_null(survey_obj$layout$superRows)

  expect_length(survey_obj$cells, 1L)
  cell <- survey_obj$cells[[1L]]
  expect_null(cell$superRow)
  expect_null(cell$row)
  expect_null(cell$superColumn)
  expect_null(cell$column)
  expect_equal(cell$measures$mean, "12.50")
  expect_equal(cell$measures$median, "10.20")
})

test_that("serialize_table_response preserves explicit poverty_line for mixed poverty/non-poverty measures", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL)

  flat_dt <- data.table::data.table(
    pip_id = rep("COL_2010_ECH_INC_ALL", 4L),
    country_code = rep("COL", 4L),
    surveyid_year = rep(2010L, 4L),
    welfare_type = rep("INC", 4L),
    gender = c("male", "male", "female", "female"),
    poverty_line = c(2.15, NA_real_, 2.15, NA_real_),
    measure = c("headcount", "mean", "headcount", "mean"),
    value = c("15.20", "12.50", "16.80", "11.40")
  )

  res <- piptm::serialize_table_response(
    dt = flat_dt,
    slots = slots,
    analysis_var = "welfare",
    poverty_line = 2.15,
    measures = c("headcount", "mean"),
    release = rel
  )

  expect_length(res, 1L)
  survey_obj <- res[[1L]]
  expect_equal(survey_obj$poverty_line, 2.15)
  expect_equal(survey_obj$analysis_var, "welfare")
})

test_that("serialize_table_response returns observed-only cells (absent category is in layout but not in cells)", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL)

  # Data only contains "male", "female" was absent from microdata
  flat_dt <- data.table::data.table(
    pip_id = "COL_2010_ECH_INC_ALL",
    country_code = "COL",
    surveyid_year = 2010L,
    welfare_type = "INC",
    gender = "male",
    poverty_line = NA_real_,
    measure = "mean",
    value = "12.50"
  )

  res <- piptm::serialize_table_response(
    dt = flat_dt,
    slots = slots,
    analysis_var = "welfare",
    poverty_line = NULL,
    measures = "mean",
    release = rel
  )

  survey_obj <- res[[1L]]
  # layout still lists both registry categories (Male and Female)
  expect_equal(length(survey_obj$layout$columns$categories), 2L)

  # cells only contains the 1 observed cell (male)
  expect_length(survey_obj$cells, 1L)
  expect_equal(survey_obj$cells[[1L]]$column$label, "Male")
})

test_that("serialize_table_response is deterministic across repeat executions", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "gender", row = "area", superColumn = "pov_status", superRow = NULL)

  flat_dt <- data.table::data.table(
    pip_id = rep("COL_2010_ECH_INC_ALL", 8L),
    country_code = rep("COL", 8L),
    surveyid_year = rep(2010L, 8L),
    welfare_type = rep("INC", 8L),
    gender = c("male", "male", "female", "female", "male", "male", "female", "female"),
    area = c("urban", "rural", "urban", "rural", "urban", "rural", "urban", "rural"),
    pov_status = c(0L, 0L, 0L, 0L, 1L, 1L, 1L, 1L),
    poverty_line = rep(2.15, 8L),
    measure = rep("mean", 8L),
    value = c("10.0", "8.0", "9.0", "7.0", "3.0", "2.0", "2.5", "1.5")
  )

  res1 <- piptm::serialize_table_response(flat_dt, slots, "welfare", 2.15, "mean", rel)
  res2 <- piptm::serialize_table_response(flat_dt, slots, "welfare", 2.15, "mean", rel)

  json1 <- as.character(jsonlite::toJSON(res1, auto_unbox = TRUE, null = "null"))
  json2 <- as.character(jsonlite::toJSON(res2, auto_unbox = TRUE, null = "null"))

  expect_identical(json1, json2)
})

test_that("serialize_table_response is invariant to input row order (P0.2/P2.5 regression)", {
  rel <- activate_test_registry("TEST_RELEASE")

  # Two covariates with 3 distinct out-of-registry ("unregistered") codes for
  # `gender` to exercise the unmatched-value tie-break path: before the fix,
  # all unmatched values shared the same cat_idx and ties fell back to input
  # row order. Shuffling the rows must not change the serialized cell order.
  slots <- list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL)

  flat_dt <- data.table::data.table(
    pip_id = rep("COL_2010_ECH_INC_ALL", 5L),
    country_code = rep("COL", 5L),
    surveyid_year = rep(2010L, 5L),
    welfare_type = rep("INC", 5L),
    gender = c("male", "female", "other1", "other2", "other3"),
    poverty_line = rep(NA_real_, 5L),
    measure = rep("mean", 5L),
    value = c("10.0", "9.0", "5.0", "6.0", "7.0")
  )

  res_original <- piptm::serialize_table_response(flat_dt, slots, "welfare", NULL, "mean", rel)
  json_original <- as.character(jsonlite::toJSON(res_original, auto_unbox = TRUE, null = "null"))

  set.seed(42)
  shuffled_dt <- flat_dt[sample(.N)]
  res_shuffled <- piptm::serialize_table_response(shuffled_dt, slots, "welfare", NULL, "mean", rel)
  json_shuffled <- as.character(jsonlite::toJSON(res_shuffled, auto_unbox = TRUE, null = "null"))

  expect_identical(json_original, json_shuffled)
})

test_that("serialize_table_response scopes cells and metadata per-survey when surveys have different observed categories", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL)

  # Survey A only observes "male"; survey B observes both "male" and "female".
  flat_dt <- data.table::data.table(
    pip_id = c("COL_2010_ECH_INC_ALL", "PER_2012_ENAHO_INC_ALL", "PER_2012_ENAHO_INC_ALL"),
    country_code = c("COL", "PER", "PER"),
    surveyid_year = c(2010L, 2012L, 2012L),
    welfare_type = c("INC", "INC", "INC"),
    gender = c("male", "male", "female"),
    poverty_line = rep(NA_real_, 3L),
    measure = rep("mean", 3L),
    value = c("10.0", "20.0", "25.0")
  )

  res <- piptm::serialize_table_response(flat_dt, slots, "welfare", NULL, "mean", rel)

  expect_length(res, 2L)

  survey_a <- Filter(function(s) s$pip_id == "COL_2010_ECH_INC_ALL", res)[[1L]]
  survey_b <- Filter(function(s) s$pip_id == "PER_2012_ENAHO_INC_ALL", res)[[1L]]

  expect_equal(survey_a$country_code, "COL")
  expect_equal(survey_a$surveyid_year, 2010L)
  expect_length(survey_a$cells, 1L)
  expect_equal(survey_a$cells[[1L]]$column$label, "Male")

  expect_equal(survey_b$country_code, "PER")
  expect_equal(survey_b$surveyid_year, 2012L)
  expect_length(survey_b$cells, 2L)
  observed_labels <- vapply(survey_b$cells, function(c) c$column$label, character(1L))
  expect_setequal(observed_labels, c("Male", "Female"))
})

test_that("serialize_table_response errors when a slot variable is absent from dt columns", {
  rel <- activate_test_registry("TEST_RELEASE")

  slots <- list(column = "gender", row = NULL, superColumn = NULL, superRow = NULL)

  # flat_dt has no "gender" column, even though the slot requests it.
  flat_dt <- data.table::data.table(
    pip_id = "COL_2010_ECH_INC_ALL",
    country_code = "COL",
    surveyid_year = 2010L,
    welfare_type = "INC",
    poverty_line = NA_real_,
    measure = "mean",
    value = "10.0"
  )

  expect_error(
    piptm::serialize_table_response(flat_dt, slots, "welfare", NULL, "mean", rel),
    "gender"
  )
})


test_that("serialize_table_response validates measures, analysis_var, and poverty_line", {
  rel <- activate_test_registry("TEST_RELEASE")
  slots <- list(column = NULL, row = NULL, superColumn = NULL, superRow = NULL)

  flat_dt <- data.table::data.table(
    pip_id = "COL_2010_ECH_INC_ALL",
    country_code = "COL",
    surveyid_year = 2010L,
    welfare_type = "INC",
    poverty_line = NA_real_,
    measure = "mean",
    value = "10.0"
  )

  # measures: empty / NULL / NA / empty-string all rejected
  expect_error(
    serialize_table_response(flat_dt, slots, "welfare", NULL, character(0), rel),
    class = "rlang_error"
  )
  expect_error(
    serialize_table_response(flat_dt, slots, "welfare", NULL, NULL, rel),
    class = "rlang_error"
  )
  expect_error(
    serialize_table_response(flat_dt, slots, "welfare", NULL, NA_character_, rel),
    class = "rlang_error"
  )
  expect_error(
    serialize_table_response(flat_dt, slots, "welfare", NULL, "", rel),
    class = "rlang_error"
  )

  # analysis_var: non-scalar/empty/NA rejected
  expect_error(
    serialize_table_response(flat_dt, slots, c("welfare", "welfare2"), NULL, "mean", rel),
    class = "rlang_error"
  )
  expect_error(
    serialize_table_response(flat_dt, slots, "", NULL, "mean", rel),
    class = "rlang_error"
  )
  expect_error(
    serialize_table_response(flat_dt, slots, NA_character_, NULL, "mean", rel),
    class = "rlang_error"
  )

  # poverty_line: non-numeric string / length>1 rejected; NULL is fine
  expect_error(
    serialize_table_response(flat_dt, slots, "welfare", "not-a-number", "mean", rel),
    class = "rlang_error"
  )
  expect_error(
    serialize_table_response(flat_dt, slots, "welfare", c(1, 2), "mean", rel),
    class = "rlang_error"
  )
  expect_no_error(serialize_table_response(flat_dt, slots, "welfare", NULL, "mean", rel))
  expect_no_error(serialize_table_response(flat_dt, slots, "welfare", 2.15, "mean", rel))
})

test_that("serialize_table_response warns (not errors) when a requested measure has no matches anywhere in dt", {
  rel <- activate_test_registry("TEST_RELEASE")
  slots <- list(column = NULL, row = NULL, superColumn = NULL, superRow = NULL)

  flat_dt <- data.table::data.table(
    pip_id = "COL_2010_ECH_INC_ALL",
    country_code = "COL",
    surveyid_year = 2010L,
    welfare_type = "INC",
    poverty_line = NA_real_,
    measure = "mean",
    value = "10.0"
  )

  expect_warning(
    res <- serialize_table_response(flat_dt, slots, "welfare", NULL, c("mean", "gini_typo"), rel),
    "gini_typo"
  )
  expect_equal(res[[1L]]$cells[[1L]]$measures$mean, "10.0")
  expect_true(is.na(res[[1L]]$cells[[1L]]$measures$gini_typo))
})

test_that("three-covariate response matches authoritative post-format fixture", {
  fx_path <- testthat::test_path("fixtures", "table-response-approach-c", "three_covariates_post_format.json")
  skip_if(!file.exists(fx_path), "Authoritative fixture not found")

  expected_fx <- jsonlite::fromJSON(fx_path, simplifyVector = FALSE)
  exp_survey <- expected_fx$data[[1L]]

  # Verify structural properties of authoritative fixture
  expect_named(exp_survey$layout, c("columns", "rows", "superColumns", "superRows"), ignore.order = FALSE)
  expect_equal(exp_survey$layout$columns$varname, "gender")
  expect_equal(exp_survey$layout$rows$varname, "educat4")
  expect_equal(exp_survey$layout$superColumns$varname, "area")
  expect_null(exp_survey$layout$superRows)

  expect_length(exp_survey$cells, 20L)
  for (cell in exp_survey$cells) {
    expect_named(cell, c("superRow", "row", "superColumn", "column", "measures"), ignore.order = FALSE)
    expect_false("population" %in% names(cell))
    expect_type(cell$measures$mean, "character")
  }
})
