# Shared API testing helpers
#
# Common helper functions for testing plumber API endpoints.

library(jsonlite)

#' Create a mock plumber request object
#' 
#' @param method HTTP method (GET, POST, etc.)
#' @param path Request path
#' @param query Named list of query parameters
#' @param body Request body (will be JSON-encoded)
#' @param defaults Named list of default query parameters to add
make_api_req <- function(method = "GET", path = "/",
                         query = list(), body = NULL, defaults = list()) {
  
  # Merge defaults into query
  query <- modifyList(defaults, query)
  
  qs <- if (length(query) > 0L) {
    parts <- unlist(lapply(names(query), function(k) {
      paste0(k, "=", as.character(query[[k]]))
    }))
    paste(parts, collapse = "&")
  } else {
    ""
  }

  body_raw <- if (!is.null(body)) {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
  } else {
    raw(0L)
  }

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

#' Parse a plumber response object
#'
#' @param res Plumber response object
#' @param simplify Whether to simplify JSON arrays/objects to vectors/data.frames
parse_api_res <- function(res, simplify = TRUE) {
  body <- res$body
  if (is.raw(body)) body <- rawToChar(body)
  jsonlite::fromJSON(body, simplifyVector = simplify)
}

#' Create a request for /table endpoint with default analysis_var
make_table_req <- function(method = "GET", query = list(), body = NULL) {
  defaults <- list(analysis_var = "welfare")
  make_api_req(method, "/table", query, body, defaults)
}