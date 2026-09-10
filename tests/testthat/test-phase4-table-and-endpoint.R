library(data.table)
library(jsonlite)

.reset_phase4_env <- function() {
  env <- getNamespace("piptm")$.piptm_env
  env$arrow_root <- NULL
  env$manifest_dir <- NULL
  env$manifests <- list()
  env$current_release <- NULL
}

.make_phase4_fixtures <- function(env = parent.frame()) {
  skip_if_not_installed("arrow")

  tmp_arrow <- withr::local_tempdir(.local_envir = env)
  tmp_manifest <- withr::local_tempdir(.local_envir = env)

  dir_path <- file.path(
    tmp_arrow,
    "country_code=COL",
    "surveyid_year=2010",
    "welfare_type=INC",
    "version=v01_v01"
  )
  dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)

  dt <- data.table(
    country_code = "COL",
    surveyid_year = 2010L,
    welfare_type = "INC",
    version = "v01_v01",
    pip_id = "COL_2010_ECH_INC_ALL",
    welfare_ppp_2021_01_02 = c(1, 2, 3, 4, 5, 6),
    weight = c(1, 1, 1, 1, 1, 1),
    gender = factor(c("male", "female", "male", "female", "male", "female")),
    area = factor(c("urban", "urban", "rural", "rural", "urban", "rural")),
    age = c(20L, 22L, 35L, 40L, 55L, 60L)
  )

  arrow::write_parquet(dt, file.path(dir_path, "data.parquet"))

  entries <- list(
    list(
      pip_id = "COL_2010_ECH_INC_ALL",
      survey_id = "S1",
      country_code = "COL",
      country_name = "Colombia",
      region_code = "LCN",
      region_name = "Latin America & Caribbean",
      year = 2010L,
      welfare_type = "INC",
      version = "v01_v01",
      survey_acronym = "ECH",
      module = "ALL",
      dimensions = list("gender", "area", "age"),
      welfare_vars = list("welfare_ppp_2021_01_02"),
      ppp_sort = 2021L
    )
  )

  write_fixture_manifest_tm(tmp_manifest, "20260206_PHASE4", entries, set_current = TRUE)

  suppressMessages({
    piptm::set_manifest_dir(tmp_manifest)
    piptm::set_arrow_root(tmp_arrow)
  })

  withr::defer(.reset_phase4_env(), envir = env)

  list(release = "20260206_PHASE4")
}

.make_phase4_req <- function(method = "GET", path = "/", query = list()) {
  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      paste0(k, "=", as.character(query[[k]]))
    }))
    paste(parts, collapse = "&")
  } else {
    ""
  }

  req <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- toupper(method)
  req$PATH_INFO <- path
  req$QUERY_STRING <- qs
  req$HTTP_ACCEPT <- "application/json"
  req$CONTENT_TYPE <- ""
  req$CONTENT_LENGTH <- "0"
  req$HTTP_HOST <- "localhost"
  req$rook.input <- list(
    read_lines = function() "",
    read = function(l = -1L) raw(0L),
    rewind = function() invisible(NULL)
  )
  req
}

.parse_phase4_res <- function(res) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = TRUE)
}

.phase4_plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
if (!nzchar(.phase4_plumber_path)) {
  .phase4_plumber_path <- file.path(
    rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"
  )
}

.phase4_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.phase4_plumber_path))
} else {
  NULL
}

test_that("phase4: table_maker welfare analysis by gender/area works", {
  .make_phase4_fixtures()
  activate_test_registry("20260206_PHASE4")

  out <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "welfare",
    measures = c("gini", "mean"),
    by = c("gender", "area")
  )

  expect_true(all(c("gini", "mean") %in% out$measure))
  expect_true(all(c("gender", "area", "population") %in% names(out)))
})

test_that("phase4: table_maker poverty analysis with pov_status uses poverty_line", {
  .make_phase4_fixtures()

  out <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "pov_status",
    measures = c("headcount", "poverty_gap"),
    poverty_line = 2.15
  )

  expect_true(all(c("headcount", "poverty_gap") %in% out$measure))
  expect_true(all(!is.na(out[measure %in% c("headcount", "poverty_gap")]$poverty_line)))
})

test_that("phase4: table_maker requires poverty_line when by includes pov_status", {
  .make_phase4_fixtures()

  expect_error(
    piptm::table_maker(
      pip_id = "COL_2010_ECH_INC_ALL",
      analysis_var = "welfare",
      measures = "mean",
      by = "pov_status"
    ),
    "poverty_line"
  )
})

test_that("phase4: table_maker continuous analysis_var works", {
  .make_phase4_fixtures()
  activate_test_registry("20260206_PHASE4")

  out <- piptm::table_maker(
    pip_id = "COL_2010_ECH_INC_ALL",
    analysis_var = "age",
    measures = c("mean", "median"),
    by = "gender"
  )

  expect_setequal(unique(out$measure), c("mean", "median"))
  expect_true("gender" %in% names(out))
})

test_that("phase4: GET /table validates and computes with analysis_var", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.phase4_router), "Router could not be created")
  .make_phase4_fixtures()
  activate_test_registry("20260206_PHASE4")

  res <- .phase4_router$call(.make_phase4_req("GET", "/table", query = list(
    analysis_var = "welfare",
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = c("gini", "mean")
  )))

  expect_equal(res$status, 200L)
  body <- .parse_phase4_res(res)
  expect_equal(body$status, "success")
  expect_true(all(c("gini", "mean") %in% body$data$measure))
})

test_that("phase4: GET /table rejects pov_status usage without poverty_line", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.phase4_router), "Router could not be created")

  res <- .phase4_router$call(.make_phase4_req("GET", "/table", query = list(
    analysis_var = "pov_status",
    pip_id = "COL_2010_ECH_INC_ALL",
    measures = "headcount"
  )))

  expect_equal(res$status, 400L)
  body <- .parse_phase4_res(res)
  expect_equal(body$status, "error")
  expect_true(any(grepl("poverty_line", body$errors, ignore.case = TRUE)))
})
