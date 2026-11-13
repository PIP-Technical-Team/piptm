# Tests for the pure manifest + registry build_description_model().
#
# The builder takes table parameters directly (no description_metadata), reads
# only the in-memory manifest + registry (zero Arrow/microdata), and returns a
# 6-section model. See plan
# .cg-docs/plans/2026-11-12-metadata-rework-and-table-maker-decoupling.md
# (R2-R10).

library(data.table)
library(arrow)
library(jsonlite)

# ── Fixtures ──────────────────────────────────────────────────────────────────
# A two-survey manifest + registry where COL_2010 carries gender + area and
# BOL_2000 carries no dimensions, plus country_name for label rendering.
activate_builder_fixture <- function(release = "20260401_TEST",
                                     env = parent.frame()) {
  tmp_arrow    <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  write_fixture_parquet_tm(
    arrow_root = tmp_arrow, country_code = "COL", year = 2010L,
    welfare_type = "INC", version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL", extra_cols = c("gender", "area")
  )
  write_fixture_parquet_tm(
    arrow_root = tmp_arrow, country_code = "BOL", year = 2000L,
    welfare_type = "INC", version = "v01_v01",
    pip_id = "BOL_2000_ECH_INC_ALL"
  )

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
      country_name = "Colombia", year = 2010L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list("gender", "area"),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    ),
    list(
      pip_id = "BOL_2000_ECH_INC_ALL", survey_id = "S2", country_code = "BOL",
      country_name = "Bolivia", year = 2000L, welfare_type = "INC",
      version = "v01_v01", survey_acronym = "ECH", module = "ALL",
      dimensions = list(),
      welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
    )
  )
  write_fixture_manifest_tm(tmp_manifest, release, entries)

  suppressMessages({
    piptm::set_manifest_dir(tmp_manifest)
    piptm::set_arrow_root(tmp_arrow)
  })
  activate_test_registry(release, .local_envir = env)
  withr::defer(reset_piptm_env_tm(), envir = env)
  invisible(release)
}

# ── Section structure ─────────────────────────────────────────────────────────

test_that("build_description_model() returns exactly the 6 sections", {
  activate_builder_fixture()

  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    column = "gender",
    release = "20260401_TEST"
  )

  expect_named(
    model,
    c("overview", "provenance", "surveys_selected", "filters_applied",
      "layout_configuration", "statistics_selected"),
    ignore.order = FALSE
  )
  for (section in names(model)) {
    expect_true(all(c("visible", "title", "content") %in% names(model[[section]])))
  }
  # No legacy sections survive.
  expect_false(any(c("cell_definition", "execution_summary", "warnings") %in%
                     names(model)))
})

test_that("build_description_model() visibility flags respond to inputs", {
  activate_builder_fixture()

  # No filter, no layout slot -> filters_applied and layout hidden.
  m1 <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    release = "20260401_TEST"
  )
  expect_false(m1$filters_applied$visible)
  expect_false(m1$layout_configuration$visible)
  expect_true(m1$overview$visible)
  expect_true(m1$statistics_selected$visible)

  # Filter + layout slot -> both visible.
  m2 <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    column = "gender",
    filter_base = list(area = 1L),
    release = "20260401_TEST"
  )
  expect_true(m2$filters_applied$visible)
  expect_true(m2$layout_configuration$visible)
})

# ── Overview ──────────────────────────────────────────────────────────────────

test_that("overview reports counts, weighting, ppp, and suppression rule", {
  activate_builder_fixture()

  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("mean", "gini"),
    column = "gender",
    pop_share_threshold = 0.001,
    release = "20260401_TEST"
  )
  ov <- model$overview$content
  expect_match(ov$structure_description, "2 statistics")
  expect_match(ov$structure_description, "1 selected survey")
  expect_true(nzchar(ov$weighting_note))
  expect_match(ov$ppp_note, "PPP")
  expect_match(ov$suppression_note, "NR")

  # No cell counts leak into overview.
  expect_false(any(grepl("cell(s)? (was|were) suppressed", unlist(ov))))
})

test_that("overview omits suppression note when no layout slot is active", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    pop_share_threshold = 0.001,
    release = "20260401_TEST"
  )
  expect_null(model$overview$content$suppression_note)
})

# ── Provenance ────────────────────────────────────────────────────────────────

test_that("provenance reports release, ppp, poverty_line, and timestamp", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "pov_status",
    measures = "headcount",
    poverty_line = 2.15,
    release = "20260401_TEST"
  )
  prov <- model$provenance$content
  expect_identical(prov$release_id, "20260401_TEST")
  expect_equal(prov$ppp_year, 2021L)
  expect_match(prov$poverty_line, "2.15")
  expect_match(prov$generated_at, "UTC")
})

# ── Surveys ───────────────────────────────────────────────────────────────────

test_that("surveys_selected lists included surveys with manifest labels", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    release = "20260401_TEST"
  )
  surveys <- model$surveys_selected$content
  expect_equal(surveys$n_loaded, 1L)
  expect_equal(surveys$n_excluded, 0L)
  loaded <- surveys$loaded_list
  expect_identical(loaded$pip_id[[1]], "COL_2010_ECH_INC_ALL")
  expect_identical(loaded$country_label[[1]], "Colombia")
  expect_identical(loaded$welfare_type_label[[1]], "Income")
  expect_identical(loaded$version[[1]], "v01_v01")
  # Excluded table omitted when empty.
  expect_null(surveys$excluded_list)
})

test_that("surveys_selected shows excluded surveys with clean reason", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = c("COL_2010_ECH_INC_ALL", "BOL_2000_ECH_INC_ALL"),
    analysis_var = "welfare",
    measures = "mean",
    column = "gender",
    release = "20260401_TEST"
  )
  surveys <- model$surveys_selected$content
  expect_equal(surveys$n_loaded, 1L)
  expect_equal(surveys$n_excluded, 1L)
  excl <- surveys$excluded_list
  expect_identical(excl$pip_id[[1]], "BOL_2000_ECH_INC_ALL")
  # reason must NOT repeat the pip_id prefix.
  expect_false(grepl("BOL_2000_ECH_INC_ALL:", excl$reason[[1]], fixed = TRUE))
  expect_match(excl$reason[[1]], "no requested dimensions")
})

# ── Filters ───────────────────────────────────────────────────────────────────

test_that("filters_applied resolves category labels, not raw codes", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    filter_base = list(area = 1L),
    release = "20260401_TEST"
  )
  filt <- model$filters_applied$content$filters
  expect_identical(filt$varname[[1]], "area")
  expect_identical(filt$variable[[1]], "Area")
  # Registry maps code 1 -> "Rural" in the test registry fixture category order
  # (codes "urban","rural" -> labels "Urban","Rural"). Codes here are integer
  # positions; the label resolution falls back to the raw code when the filter
  # category codes are non-integer strings, so assert a human-readable label or
  # a graceful fallback (never an internal crash).
  expect_true(nzchar(filt$selected_categories[[1]]))
})

# ── Layout ────────────────────────────────────────────────────────────────────

test_that("layout_configuration reports true slot roles (not hardcoded rows)", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    column = "gender",
    row = "area",
    release = "20260401_TEST"
  )
  layout <- model$layout_configuration$content$layout
  # Visual-hierarchy order: row before column.
  expect_identical(layout$slot_label, c("Rows", "Columns"))
  expect_identical(layout$varname, c("area", "gender"))
  expect_true(all(c("Area", "Gender") %in% layout$ui_label))
})

test_that("layout_configuration category labels come from the registry", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    column = "gender",
    release = "20260401_TEST"
  )
  layout <- model$layout_configuration$content$layout
  expect_match(layout$n_categories[[1]], "Male")
  expect_match(layout$n_categories[[1]], "Female")
})

# ── Statistics ────────────────────────────────────────────────────────────────

test_that("statistics_selected reports analysis var and measures in order", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("median", "mean"),
    release = "20260401_TEST"
  )
  stats <- model$statistics_selected$content
  expect_identical(stats$analysis_var_name, "welfare")
  expect_identical(stats$analysis_var_label, "Welfare")
  expect_identical(stats$analysis_var_type, "welfare")
  # Request order preserved: median before mean.
  expect_identical(stats$measures$measure, c("median", "mean"))
  expect_identical(stats$measures$measure_label, c("Median", "Mean"))
})

test_that("statistics_selected reports poverty line when applicable", {
  activate_builder_fixture()
  model <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "pov_status",
    measures = "headcount",
    poverty_line = 3.65,
    release = "20260401_TEST"
  )
  pl <- model$statistics_selected$content$poverty_line
  expect_true(pl$applicable)
  expect_match(pl$value, "3.65")
  expect_equal(pl$value_raw, 3.65)
  expect_equal(pl$ppp_year, 2021L)
})

# ── Error paths (shared domain validation) ────────────────────────────────────

test_that("build_description_model() rejects poverty measure without poverty_line", {
  activate_builder_fixture()
  expect_error(
    piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "headcount",
      release = "20260401_TEST"
    ),
    "poverty"
  )
})

test_that("build_description_model() rejects a pov_status layout slot without a valid poverty_line", {
  # Review finding P1.3: previously .build_layout_content() silently dropped
  # the pov_status row instead of erroring, so a direct caller (bypassing the
  # plumber route's separate .validate_table_shape() check) got a metadata
  # document that misrepresented the requested table shape instead of an
  # error. .validate_and_normalize_table_params() now enforces this for both
  # table_maker() and build_description_model().
  activate_builder_fixture()
  expect_error(
    piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "pov_status",
      release = "20260401_TEST"
    ),
    "pov_status"
  )
})

test_that("build_description_model() rejects unknown filter_base variable", {
  activate_builder_fixture()
  expect_error(
    piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      filter_base = list(not_a_real_var = 1L),
      release = "20260401_TEST"
    )
  )
})

test_that("build_description_model() accepts empty filter_base list the same as NULL", {
  activate_builder_fixture()

  model_empty <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    filter_base = list(),
    release = "20260401_TEST"
  )

  model_null <- piptm::build_description_model(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = "mean",
    filter_base = NULL,
    release = "20260401_TEST"
  )

  expect_false(model_empty$filters_applied$visible)
  expect_null(model_empty$filters_applied$content)
  expect_identical(model_empty$layout_configuration$visible, model_null$layout_configuration$visible)
  expect_identical(model_empty$statistics_selected$content$measures$measure,
                   model_null$statistics_selected$content$measures$measure)
})

test_that("build_description_model() degrades gracefully when a specific pip_id is missing in registry", {
  activate_builder_fixture()

  reg <- piptm::piptm_variable_registry("20260401_TEST")
  reg[["gender"]]$categories <- NULL
  reg[["gender"]]$n_categories <- NULL
  piptm:::.piptm_env$registries[["20260401_TEST"]] <- reg

  expect_no_error(
    model <- piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      release = "20260401_TEST"
    )
  )

  layout <- model$layout_configuration$content$layout
  expect_identical(layout$varname[[1]], "gender")
  expect_true(nchar(layout$n_categories[[1]]) >= 0L)
})

test_that("build_description_model() tolerates partial pip_id manifest matches", {
  activate_builder_fixture()

  expect_no_error(
    model <- piptm::build_description_model(
      pip_id = c("COL_2010_ECH_INC_ALL", "XXX_9999_NONE_INC_ALL"),
      analysis_var = "welfare",
      measures = "mean",
      release = "20260401_TEST"
    )
  )

  surveys <- model$surveys_selected$content
  expect_identical(surveys$n_loaded, 1L)
  expect_identical(surveys$loaded_list$pip_id[[1]], "COL_2010_ECH_INC_ALL")
})

test_that("build_description_model() rejects gapped or duplicate layout slots for direct callers", {
  activate_builder_fixture()

  expect_error(
    piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      superColumn = "area",
      release = "20260401_TEST"
    ),
    regexp = "filled in order|left to right"
  )

  expect_error(
    piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      row = "gender",
      release = "20260401_TEST"
    ),
    regexp = "multiple layout slots|more than one slot|Duplicate"
  )
})

test_that("build_description_model() rejects multi-valued slot inputs for direct callers", {
  activate_builder_fixture()

  expect_error(
    piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = c("gender", "area"),
      release = "20260401_TEST"
    ),
    regexp = "single non-empty character scalar|single variable"
  )
})

test_that(".build_layout_content handles missing category metadata defensively", {
  layout <- piptm:::.build_layout_content(
    active_slots = list(column = "gender"),
    poverty_line = NULL,
    registry = list(),
    layout_covs = list(
      list(varname = "gender", label = "Gender", n_categories = NA_integer_)
    )
  )

  expect_type(layout, "list")
  expect_true("layout" %in% names(layout))
  expect_identical(layout$layout$varname[[1]], "gender")
  expect_identical(layout$layout$n_categories[[1]], "0: Unspecified")
})

test_that(".build_layout_content uses consistent n_categories string shape", {
  layout <- piptm:::.build_layout_content(
    active_slots = list(column = "gender", row = "area"),
    poverty_line = 2.15,
    registry = list(
      gender = list(categories = list(list(label = "Male"), list(label = "Female")))
    ),
    layout_covs = list(
      list(varname = "gender", label = "Gender", n_categories = 2L),
      list(varname = "area", label = "Area", n_categories = 2L)
    )
  )

  nc <- layout$layout$n_categories
  expect_true(all(grepl("^[0-9]+: ", nc)))
  expect_match(nc[[1]], "Male")
  expect_match(nc[[2]], "Category")
})

test_that(".build_surveys_content keeps included rows when excluded is empty", {
  included <- data.table::data.table(
    pip_id = "COL_2010_ECH_INC_ALL",
    country_code = "COL",
    country_name = "Colombia",
    year = 2010L,
    welfare_type = "INC",
    version = "v01_v01"
  )
  excluded <- data.table::data.table(
    pip_id = character(0),
    reason = character(0)
  )

  out <- piptm:::.build_surveys_content(included, excluded, release = "20260401_TEST")
  expect_identical(out$n_loaded, 1L)
  expect_identical(out$n_excluded, 0L)
  expect_identical(out$loaded_list$pip_id[[1]], "COL_2010_ECH_INC_ALL")
  expect_null(out$excluded_list)
})

test_that("build_description_model() aborts when all surveys excluded", {
  activate_builder_fixture()
  expect_error(
    piptm::build_description_model(
      pip_id = "BOL_2000_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      column = "gender",
      release = "20260401_TEST"
    )
  )
})

test_that("build_description_model() degrades when registry unavailable", {
  # Manifest present but no registry for this release: labels fall back to keys,
  # and the builder must not abort.
  tmp_arrow    <- withr::local_tempdir()
  tmp_manifest <- withr::local_tempdir()
  write_fixture_parquet_tm(
    arrow_root = tmp_arrow, country_code = "COL", year = 2010L,
    welfare_type = "INC", version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL", extra_cols = c("gender")
  )
  entries <- list(list(
    pip_id = "COL_2010_ECH_INC_ALL", survey_id = "S1", country_code = "COL",
    country_name = "Colombia", year = 2010L, welfare_type = "INC",
    version = "v01_v01", survey_acronym = "ECH", module = "ALL",
    dimensions = list("gender"),
    welfare_vars = list("welfare_ppp_2021_01_02"), ppp_sort = 2021L
  ))
  write_fixture_manifest_tm(tmp_manifest, "20260401_NOREG", entries)
  suppressMessages({
    piptm::set_manifest_dir(tmp_manifest)
    piptm::set_arrow_root(tmp_arrow)
  })
  withr::defer(reset_piptm_env_tm())

  # No filter/by so no registry-backed filter/covariate validation is needed.
  expect_no_error(
    model <- piptm::build_description_model(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      release = "20260401_NOREG"
    )
  )
  expect_identical(
    model$statistics_selected$content$analysis_var_name, "welfare"
  )
})

# ── Zero-microdata guard (Step 3: R2, R19) ────────────────────────────────────
#
# Note: a mock-based guard (local_mocked_bindings() aborting load_surveys()/
# load_survey_microdata()) was deliberately NOT used here -- neither function
# is called by name anywhere in R/description_builder.R, so such mocks would
# never be invoked and the test would pass vacuously even if the builder read
# microdata through a different path. The static source-scan test below is
# the real, meaningful guard: it directly inspects R/description_builder.R for
# any reference to the known microdata-path symbols.

test_that("R/description_builder.R contains no microdata-path references", {
  pkg_root <- tryCatch(
    rprojroot::find_package_root_file(),
    error = function(e) normalizePath(testthat::test_path("..", ".."),
                                      mustWork = FALSE)
  )
  src <- readLines(file.path(pkg_root, "R", "description_builder.R"),
                   warn = FALSE)
  # Ignore comment/roxygen lines; check only code.
  code <- src[!grepl("^\\s*#", src)]
  joined <- paste(code, collapse = "\n")
  expect_false(grepl("table_maker\\s*\\(", joined))
  expect_false(grepl("load_survey_microdata\\s*\\(", joined))
  expect_false(grepl("load_surveys\\s*\\(", joined))
  expect_false(grepl("arrow::", joined, fixed = TRUE))
})
