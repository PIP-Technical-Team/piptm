# Tests — POST /table/download (stateless converter; no Arrow data needed)

.dl_path <- tryCatch(
  file.path(rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R"),
  error = function(e) ""
)
if (!nzchar(.dl_path) || !file.exists(.dl_path)) {
  .dl_path <- system.file("plumber", "plumber.R", package = "piptm")
}
.dl_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.dl_path))
} else {
  NULL
}

.dl_req <- function(body_json) {
  raw_body <- charToRaw(body_json)
  req <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- "POST"
  req$PATH_INFO      <- "/table/download"
  req$QUERY_STRING   <- ""
  req$HTTP_ACCEPT    <- "*/*"
  req$CONTENT_TYPE   <- "application/json"
  req$HTTP_CONTENT_TYPE <- "application/json"
  req$CONTENT_LENGTH <- as.character(length(raw_body))
  req$HTTP_HOST      <- "localhost"
  req$rook.input     <- list(
    read_lines = function() body_json,
    read       = function(l = -1L) raw_body,
    rewind     = function() invisible(NULL)
  )
  req$postBody <- body_json
  req
}

# Canonical Approach C survey payload for download unit tests
.dl_data <- list(
  list(
    pip_id        = "COL_2010_ECH_INC_ALL",
    country_code  = "COL",
    surveyid_year = 2010L,
    welfare_type  = "INC",
    poverty_line  = 2.15,
    analysis_var  = "welfare",
    measures      = list("mean", "gini"),
    layout        = list(
      columns = list(
        varname    = "gender",
        label      = "Gender",
        categories = list(
          list(code = 0L, label = "Female"),
          list(code = 1L, label = "Male")
        )
      ),
      rows         = NULL,
      superColumns = NULL,
      superRows    = NULL
    ),
    cells = list(
      list(
        superRow    = NULL,
        row         = NULL,
        superColumn = NULL,
        column      = list(code = 0L, label = "Female"),
        measures    = list(mean = "4.50", gini = "45.67")
      ),
      list(
        superRow    = NULL,
        row         = NULL,
        superColumn = NULL,
        column      = list(code = 1L, label = "Male"),
        measures    = list(mean = "5.15", gini = NULL)
      )
    )
  )
)

.dl_json <- function(format, data = .dl_data) {
  jsonlite::toJSON(list(format = format, data = data), auto_unbox = TRUE, null = "null")
}

.dl_body <- function(res) {
  jsonlite::fromJSON(rawToChar(res$body), simplifyVector = TRUE)
}

test_that("/table/download returns a Shape A CSV attachment", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req(.dl_json("csv")))
  expect_equal(res$status, 200L)
  expect_match(res$headers[["Content-Type"]], "text/csv")
  expect_match(res$headers[["Content-Disposition"]], "table\\.csv")
  df <- data.table::fread(text = rawToChar(res$body), colClasses = "character")
  expect_equal(nrow(df), 2L)
  expect_named(
    df,
    c("pip_id", "country_code", "surveyid_year", "welfare_type", "analysis_var", "poverty_line", "gender", "mean", "gini"),
    ignore.order = FALSE
  )
  expect_equal(df$gender, c("Female", "Male"))
  expect_equal(df$mean, c("4.50", "5.15"))
  expect_equal(df$gini, c("45.67", ""))
})

test_that("/table/download returns a readable Shape A RDS with character measures and numeric poverty_line", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req(.dl_json("rds")))
  expect_equal(res$status, 200L)
  expect_equal(res$headers[["Content-Type"]], "application/octet-stream")
  expect_match(res$headers[["Content-Disposition"]], "table\\.rds")
  tmp <- tempfile(fileext = ".rds")
  on.exit(unlink(tmp), add = TRUE)
  writeBin(res$body, tmp)
  df <- readRDS(tmp)
  expect_s3_class(df, "data.frame")
  expect_equal(nrow(df), 2L)
  expect_named(
    df,
    c("pip_id", "country_code", "surveyid_year", "welfare_type", "analysis_var", "poverty_line", "gender", "mean", "gini"),
    ignore.order = FALSE
  )
  expect_type(df$poverty_line, "double")
  expect_equal(df$poverty_line, c(2.15, 2.15))
  expect_type(df$mean, "character")
  expect_type(df$gini, "character")
  expect_equal(df$mean, c("4.50", "5.15"))
  expect_equal(df$gini[1L], "45.67")
  expect_true(is.na(df$gini[2L]))
})

test_that("/table/download rejects an unsupported format with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req(.dl_json("xlsx")))
  expect_equal(res$status, 400L)
  expect_match(res$headers[["Content-Type"]], "application/json")
  payload <- .dl_body(res)
  expect_equal(payload$status, "error")
  expect_length(payload$warnings, 0L)
  expect_true(length(payload$errors) >= 1L)
})

test_that("/table/download rejects empty data with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req('{"format":"csv","data":[]}'))
  expect_equal(res$status, 400L)
  expect_match(res$headers[["Content-Type"]], "application/json")
  payload <- .dl_body(res)
  expect_equal(payload$status, "error")
  expect_length(payload$warnings, 0L)
  expect_true(length(payload$errors) >= 1L)
})

test_that("/table/download rejects a missing/malformed body with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req(""))
  expect_equal(res$status, 400L)
  expect_match(res$headers[["Content-Type"]], "application/json")
  payload <- .dl_body(res)
  expect_equal(payload$status, "error")
  expect_length(payload$warnings, 0L)
  expect_true(length(payload$errors) >= 1L)
})

test_that("/table/download rejects invalid or non-object JSON bodies with 400", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  bad_cases <- c("{", "[]", '"x"', '{"format":"csv"}', '{"format":"csv","data":"x"}')

  for (body_json in bad_cases) {
    res <- .dl_router$call(.dl_req(body_json))
    expect_equal(res$status, 400L)
    expect_match(res$headers[["Content-Type"]], "application/json")
    payload <- .dl_body(res)
    expect_equal(payload$status, "error")
    expect_true(length(payload$errors) >= 1L)
  }
})

test_that("/table/download defaults to CSV when format is omitted", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req(jsonlite::toJSON(list(data = .dl_data), auto_unbox = TRUE, null = "null")))
  expect_equal(res$status, 200L)
  expect_match(res$headers[["Content-Type"]], "text/csv")
  expect_match(res$headers[["Content-Disposition"]], "table\\.csv")
})

test_that("/table/download rejects oversized payloads with 413", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  req <- .dl_req(.dl_json("csv"))
  req$CONTENT_LENGTH <- as.character((25 * 1024 * 1024) + 1L)
  res <- .dl_router$call(req)

  expect_equal(res$status, 413L)
  expect_match(res$headers[["Content-Type"]], "application/json")
  payload <- .dl_body(res)
  expect_equal(payload$status, "error")
  expect_true(length(payload$errors) >= 1L)
})

test_that("/table/download preserves formatted measure strings verbatim (NR sentinel, scaled percentage)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  nr_data <- .dl_data
  nr_data[[1L]]$measures <- list("gini", "headcount")
  nr_data[[1L]]$cells[[1L]]$measures <- list(gini = "NR", headcount = "45.67")
  nr_data[[1L]]$cells[[2L]]$measures <- list(gini = "38.12", headcount = "NR")

  res <- .dl_router$call(.dl_req(.dl_json("csv", data = nr_data)))
  expect_equal(res$status, 200L)
  df <- data.table::fread(text = rawToChar(res$body), colClasses = "character")
  expect_equal(nrow(df), 2L)
  expect_equal(df$gini, c("NR", "38.12"))
  expect_equal(df$headcount, c("45.67", "NR"))
})

test_that("/table/download round-trips the 3-covariate Approach C fixture into CSV and RDS", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  fx_path <- testthat::test_path("fixtures", "table-response-approach-c", "three_covariates_post_format.json")
  skip_if(!file.exists(fx_path), "Authoritative fixture not found")
  fx <- jsonlite::fromJSON(fx_path, simplifyVector = FALSE)

  # CSV round-trip
  res_csv <- .dl_router$call(.dl_req(.dl_json("csv", data = fx$data)))
  expect_equal(res_csv$status, 200L)
  df_csv <- data.table::fread(text = rawToChar(res_csv$body), colClasses = "character")
  expect_equal(nrow(df_csv), 20L)
  expect_named(
    df_csv,
    c("pip_id", "country_code", "surveyid_year", "welfare_type", "analysis_var", "poverty_line", "educat4", "area", "gender", "mean"),
    ignore.order = FALSE
  )
  expect_equal(df_csv$educat4[1L], "None")
  expect_equal(df_csv$area[1L], "Rural")
  expect_equal(df_csv$gender[1L], "Female")
  expect_equal(df_csv$mean[1L], "4.50")

  # RDS round-trip
  res_rds <- .dl_router$call(.dl_req(.dl_json("rds", data = fx$data)))
  expect_equal(res_rds$status, 200L)
  tmp <- tempfile(fileext = ".rds")
  on.exit(unlink(tmp), add = TRUE)
  writeBin(res_rds$body, tmp)
  df_rds <- readRDS(tmp)
  expect_equal(nrow(df_rds), 20L)
  expect_equal(df_rds$mean[20L], "10.87")
  expect_equal(df_rds$educat4[20L], "Unknown")
})

test_that("/table/download rejects legacy flat-row payloads with 400 (never 500)", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  legacy_payload <- list(
    format = "csv",
    data = list(
      list(pip_id = "COL_2010_ECH_INC_ALL", measure = "mean", value = "4.50", poverty_line = NULL)
    )
  )

  res <- .dl_router$call(.dl_req(jsonlite::toJSON(legacy_payload, auto_unbox = TRUE, null = "null")))
  expect_equal(res$status, 400L)
  expect_match(res$headers[["Content-Type"]], "application/json")
  payload <- .dl_body(res)
  expect_equal(payload$status, "error")
  expect_true(length(payload$errors) >= 1L)
})
