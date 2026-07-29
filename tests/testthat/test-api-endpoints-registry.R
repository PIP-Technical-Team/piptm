library(testthat)

skip_if_not_installed("plumber")

# Build a router for these tests (router is stateless; state lives in .piptm_env)
.ep_plumber_path <- system.file("plumber", "plumber.R", package = "piptm")
if (!nzchar(.ep_plumber_path)) {
  .ep_plumber_path <- file.path(rprojroot::find_package_root_file(), "inst", "plumber", "plumber.R")
}

.ep_router <- if (requireNamespace("plumber", quietly = TRUE)) {
  suppressMessages(plumber::plumb(.ep_plumber_path))
} else {
  NULL
}

make_api_req <- function(method = "GET", path = "/", query = list(), body = NULL) {
  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      paste0(k, "=", as.character(query[[k]]))
    }))
    paste(parts, collapse = "&")
  } else {
    ""
  }

  body_raw <- if (!is.null(body)) charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE)) else raw(0L)

  req                <- new.env(parent = emptyenv())
  req$REQUEST_METHOD <- toupper(method)
  req$PATH_INFO      <- path
  req$QUERY_STRING   <- qs
  req$HTTP_ACCEPT    <- "application/json"
  req$CONTENT_TYPE   <- if (!is.null(body)) "application/json" else ""
  req$CONTENT_LENGTH <- as.character(length(body_raw))
  req$HTTP_HOST      <- "localhost"
  req$rook.input     <- list(
    read_lines = function() rawToChar(body_raw),
    read       = function(l = -1L) body_raw,
    rewind     = function() invisible(NULL)
  )
  req
}

parse_api_res <- function(res, simplify = TRUE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}

# Helper to inject a synthetic registry for the current release
.inject_registry <- function(registry) {
  env <- get('.piptm_env', envir = asNamespace('piptm'))
  release <- piptm::piptm_current_release()
  if (is.null(release) || !nzchar(release)) {
    stop("No current release is set. Tests require a configured working release (e.g. 20260401_TEST).")
  }
  old <- env$registries
  env$registries <- list()
  env$registries[[release]] <- registry
  list(env = env, old = old, release = release)
}

.restore_registry <- function(state) {
  state$env$registries <- state$old
}

# Build a minimal registry matching expected shapes
.make_registry_fixture <- function() {
  list(
    welfare = list(
      varname = "welfare",
      ui_label = "Welfare",
      tm_type = "welfare",
      roles = c("analysis_var"),
      stat_groups = c("summary_statistics", "inequality"),
      n_categories = NULL,
      categories = NULL
    ),
    pov_status = list(
      varname = "pov_status",
      ui_label = "Poverty status",
      tm_type = "poverty",
      roles = c("analysis_var", "covariate"),
      stat_groups = c("poverty"),
      n_categories = 2L,
      categories = NULL
    ),
    age_group = list(
      varname = "age_group",
      ui_label = "Age group",
      tm_type = "categorical",
      roles = c("filter", "covariate"),
      stat_groups = character(0L),
      n_categories = 4L,
      categories = list(
        list(code = "0-14", label = "0 to 14"),
        list(code = "15-24", label = "15 to 24"),
        list(code = "25-64", label = "25 to 64"),
        list(code = "65+", label = "65 and above")
      )
    )
  )
}

test_that("GET /analysis-variables returns success and expected pov_status fields", {
  skip_if(is.null(.ep_router), "Router could not be created")
  reg <- .make_registry_fixture()
  state <- .inject_registry(reg)
  on.exit(.restore_registry(state))

  res <- .ep_router$call(make_api_req("GET", "/analysis-variables"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")

  data <- body$data
  get_varname <- function(entry) {
    v <- entry$varname
    if (is.list(v)) v <- unlist(v)
    as.character(v[[1]])
  }
  varnames <- vapply(data, get_varname, character(1))
  expect_true("pov_status" %in% varnames)

  pov <- data[[which(varnames == "pov_status")]]
  pov_type <- pov$type
  if (is.list(pov_type)) pov_type <- unlist(pov_type)
  expect_identical(as.character(pov_type[[1L]]), "poverty")
  expect_true("stat_groups" %in% names(pov))
})

test_that("GET /categories returns filters with subcategories", {
  skip_if(is.null(.ep_router), "Router could not be created")
  reg <- .make_registry_fixture()
  state <- .inject_registry(reg)
  on.exit(.restore_registry(state))

  res <- .ep_router$call(make_api_req("GET", "/categories"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")

  data <- body$data
  get_varname <- function(entry) {
    v <- entry$varname
    if (is.list(v)) v <- unlist(v)
    as.character(v[[1]])
  }
  varnames <- vapply(data, get_varname, character(1))
  expect_true("age_group" %in% varnames)

  age <- data[[which(varnames == "age_group")]]
  # subcategories may be boxed as list-of-lists
  subs <- age$subcategories
  expect_true(length(subs) == 4)
  codes <- vapply(subs, function(x) as.character(x$code[[1]]), character(1))
  expect_true(all(c("0-14", "15-24", "25-64", "65+") %in% codes))
})

test_that("GET /covariates returns pov_status with n_categories=2", {
  skip_if(is.null(.ep_router), "Router could not be created")
  reg <- .make_registry_fixture()
  state <- .inject_registry(reg)
  on.exit(.restore_registry(state))

  res <- .ep_router$call(make_api_req("GET", "/covariates"))
  expect_equal(res$status, 200L)
  body <- parse_api_res(res, simplify = FALSE)
  status_val <- unlist(body$status)
  expect_equal(status_val, "success")

  data <- body$data
  get_varname <- function(entry) {
    v <- entry$varname
    if (is.list(v)) v <- unlist(v)
    as.character(v[[1]])
  }
  varnames <- vapply(data, get_varname, character(1))
  expect_true("pov_status" %in% varnames)

  pov <- data[[which(varnames == "pov_status")]]
  ncat <- pov$n_categories
  if (is.list(ncat)) ncat <- unlist(ncat)
  expect_true(identical(as.integer(ncat), 2L))
  expect_false("pov_status_mutex" %in% names(pov))
})
