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

.dl_rows <- list(
  list(pip_id = "AAA_2020", measure = "mean", value = 1.5, population = 100, poverty_line = NULL),
  list(pip_id = "AAA_2020", measure = "gini", value = NULL, population = 100, poverty_line = NULL)
)

.dl_json <- function(format, data = .dl_rows) {
  jsonlite::toJSON(list(format = format, data = data), auto_unbox = TRUE, null = "null")
}

.dl_body <- function(res) {
  jsonlite::fromJSON(rawToChar(res$body), simplifyVector = TRUE)
}

test_that("/table/download returns a CSV attachment", {
  skip_if_not_installed("plumber")
  skip_if(is.null(.dl_router), "Router could not be created")

  res <- .dl_router$call(.dl_req(.dl_json("csv")))
  expect_equal(res$status, 200L)
  expect_match(res$headers[["Content-Type"]], "text/csv")
  expect_match(res$headers[["Content-Disposition"]], "table\\.csv")
  df <- data.table::fread(text = rawToChar(res$body))
  expect_equal(nrow(df), 2L)
  expect_true(all(c("pip_id", "measure", "value", "population") %in% names(df)))
})

test_that("/table/download returns a readable RDS with numeric value", {
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
  expect_true(is.numeric(df$value))
  expect_true(is.na(df$value[2L]))
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

  res <- .dl_router$call(.dl_req(jsonlite::toJSON(list(data = .dl_rows), auto_unbox = TRUE, null = "null")))
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
