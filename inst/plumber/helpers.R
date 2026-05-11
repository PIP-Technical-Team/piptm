# API Helper Utilities — shared across all plumber endpoint handlers
#
# Plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md (Step 1)
#
# These functions are sourced by inst/plumber/plumber.R at startup.  They are
# NOT exported from the package namespace — they live only in the plumber
# process's global environment after being source()d.
#
# Functions
# ---------
#   api_response()            — build a success envelope
#   api_error()               — build an error envelope and set HTTP status
#   resolve_release()         — validate + resolve a release param
#   validate_table_input()    — validate /table endpoint parameters
#   validate_lookup_input()   — validate /lookup endpoint parameters
#   capture_with_warnings()   — run an expression, collect warnings, catch errors

# ── Response envelopes ────────────────────────────────────────────────────────

#' Build a structured success response envelope
#'
#' @param data     The payload to include in the `data` field.
#' @param warnings Character vector of warning messages to echo (default
#'   empty).
#' @param meta     Named list of metadata (e.g. release ID, survey count).
#'   Defaults to an empty list.
#'
#' @return A named list with fields: `status`, `data`, `warnings`, `errors`,
#'   `meta`.
api_response <- function(data, warnings = character(), meta = list()) {
  list(
    status   = "success",
    data     = data,
    warnings = warnings,
    errors   = character(),
    meta     = meta
  )
}

#' Build a structured error response envelope and set the HTTP status code
#'
#' @param errors      Character vector of human-readable error messages.
#' @param status_code Integer HTTP status code to apply to `res` (e.g. 400L,
#'   422L).
#' @param res         The plumber response object.  Its `$status` field is set
#'   before returning.
#'
#' @return A named list with fields: `status`, `data`, `warnings`, `errors`,
#'   `meta`.
api_error <- function(errors, status_code, res) {
  res$status <- status_code
  list(
    status   = "error",
    data     = NULL,
    warnings = character(),
    errors   = errors,
    meta     = list()
  )
}

# ── Release resolver ──────────────────────────────────────────────────────────

#' Resolve and validate a release parameter
#'
#' When `release` is `NULL`, returns the current release via
#' [piptm::piptm_current_release()].  When `release` is a non-NULL string,
#' validates it against [piptm::piptm_manifests()] and aborts loudly via
#' [cli::cli_abort()] if the value is not a known release.
#'
#' This function always returns a single character string.  Any error it
#' raises will be caught by the enclosing [capture_with_warnings()] call in
#' each endpoint handler, producing a 422 response.
#'
#' @param release Character scalar release ID, or `NULL`.
#'
#' @return Character scalar release ID.
resolve_release <- function(release) {
  if (is.null(release)) {
    return(piptm::piptm_current_release())
  }

  known <- names(piptm::piptm_manifests())
  if (!release %in% known) {
    cli::cli_abort(
      c(
        "Release {.val {release}} is not a known release.",
        "i" = "Available releases: {.val {sort(known)}}."
      )
    )
  }

  release
}

# ── Input validators ──────────────────────────────────────────────────────────

#' Validate inputs for the /table endpoint
#'
#' Checks all user-supplied parameters for the `/table` endpoint.  Returns a
#' list so the handler can act on the validation result and use the coerced
#' `poverty_lines` value without re-coercing.
#'
#' Checks performed:
#' \itemize{
#'   \item `pip_id` is a non-NULL character vector of length 1–15
#'   \item `measures` is a non-NULL character vector with all elements in
#'     [piptm::pip_measures()]
#'   \item `poverty_lines`, if not `NULL`, coerces cleanly to numeric (no NAs
#'     introduced), and all values are positive and finite
#'   \item `by`, if not `NULL`, is a subset of [piptm::.VALID_DIMENSIONS]
#' }
#'
#' Note: `release` validation is handled separately by [resolve_release()].
#'
#' @param pip_id       Character vector of survey identifiers.
#' @param measures     Character vector of measure names.
#' @param poverty_lines Numeric (or coercible) vector of poverty lines, or
#'   `NULL`.
#' @param by           Character vector of disaggregation dimensions, or
#'   `NULL`.
#'
#' @return A named list:
#'   \describe{
#'     \item{`valid`}{`TRUE` when all checks pass; `FALSE` otherwise.}
#'     \item{`errors`}{Character vector of error messages (empty when valid).}
#'     \item{`poverty_lines`}{The coerced numeric vector, or `NULL`.  Use this
#'       in the handler rather than the original input.}
#'   }
validate_table_input <- function(pip_id, measures, poverty_lines = NULL,
                                 by = NULL) {
  errors <- character()

  # ── pip_id ─────────────────────────────────────────────────────────────────
  if (is.null(pip_id) || !is.character(pip_id) || length(pip_id) == 0L) {
    errors <- c(errors, "`pip_id` must be a non-empty character vector.")
  } else if (length(pip_id) > 15L) {
    errors <- c(
      errors,
      paste0(
        "`pip_id` may contain at most 15 surveys per request; ",
        length(pip_id), " were supplied."
      )
    )
  }

  # ── measures ───────────────────────────────────────────────────────────────
  valid_measures <- names(piptm::pip_measures())
  if (is.null(measures) || !is.character(measures) || length(measures) == 0L) {
    errors <- c(errors, "`measures` must be a non-empty character vector.")
  } else {
    unknown <- setdiff(measures, valid_measures)
    if (length(unknown) > 0L) {
      errors <- c(
        errors,
        paste0(
          "Unknown measure(s): ",
          paste(unknown, collapse = ", "),
          ". Valid measures: ",
          paste(valid_measures, collapse = ", "),
          "."
        )
      )
    }
  }

  # ── poverty_lines ──────────────────────────────────────────────────────────
  coerced_pl <- NULL
  if (!is.null(poverty_lines)) {
    coerced_pl <- suppressWarnings(as.numeric(poverty_lines))
    if (anyNA(coerced_pl) && !anyNA(poverty_lines)) {
      # Coercion introduced NAs — original values were non-numeric strings
      errors <- c(
        errors,
        paste0(
          "`poverty_lines` must be numeric; could not coerce: ",
          paste(poverty_lines[is.na(coerced_pl)], collapse = ", "),
          "."
        )
      )
      coerced_pl <- NULL
    } else if (!is.null(coerced_pl)) {
      bad <- !is.finite(coerced_pl) | coerced_pl <= 0
      if (any(bad)) {
        errors <- c(
          errors,
          paste0(
            "`poverty_lines` must contain only positive finite values; ",
            "problematic values: ",
            paste(coerced_pl[bad], collapse = ", "),
            "."
          )
        )
        coerced_pl <- NULL
      }
    }
  }

  # ── by ─────────────────────────────────────────────────────────────────────
  if (!is.null(by)) {
    valid_dims <- piptm:::.VALID_DIMENSIONS
    unknown_dims <- setdiff(by, valid_dims)
    if (length(unknown_dims) > 0L) {
      errors <- c(
        errors,
        paste0(
          "Unknown dimension(s): ",
          paste(unknown_dims, collapse = ", "),
          ". Valid dimensions: ",
          paste(valid_dims, collapse = ", "),
          "."
        )
      )
    }
  }

  list(
    valid         = length(errors) == 0L,
    errors        = errors,
    poverty_lines = coerced_pl
  )
}

#' Validate inputs for the /lookup endpoint
#'
#' Checks that `country_code`, `year`, and `welfare_type` are all provided,
#' have the same length, contain valid `welfare_type` values, and that `year`
#' is coercible to integer.
#'
#' @param country_code Character vector of ISO3 country codes.
#' @param year         Integer (or coercible) vector of survey years.
#' @param welfare_type Character vector of welfare types.
#'
#' @return A named list with fields `valid` (logical) and `errors` (character
#'   vector).
validate_lookup_input <- function(country_code, year, welfare_type) {
  errors <- character()

  # ── presence ───────────────────────────────────────────────────────────────
  if (is.null(country_code) || length(country_code) == 0L) {
    errors <- c(errors, "`country_code` must be provided.")
  }
  if (is.null(year) || length(year) == 0L) {
    errors <- c(errors, "`year` must be provided.")
  }
  if (is.null(welfare_type) || length(welfare_type) == 0L) {
    errors <- c(errors, "`welfare_type` must be provided.")
  }

  # Short-circuit: if any are missing, length checks are meaningless
  if (length(errors) > 0L) {
    return(list(valid = FALSE, errors = errors))
  }

  # ── equal lengths ──────────────────────────────────────────────────────────
  n <- length(country_code)
  if (length(year) != n || length(welfare_type) != n) {
    errors <- c(
      errors,
      paste0(
        "`country_code`, `year`, and `welfare_type` must all have the same ",
        "length. Got: country_code = ", n,
        ", year = ", length(year),
        ", welfare_type = ", length(welfare_type), "."
      )
    )
  }

  # ── welfare_type values ────────────────────────────────────────────────────
  bad_wt <- setdiff(welfare_type, c("INC", "CON"))
  if (length(bad_wt) > 0L) {
    errors <- c(
      errors,
      paste0(
        "`welfare_type` must be \"INC\" or \"CON\"; invalid value(s): ",
        paste(bad_wt, collapse = ", "),
        "."
      )
    )
  }

  # ── year coercible to integer ──────────────────────────────────────────────
  coerced_year <- suppressWarnings(as.integer(year))
  if (anyNA(coerced_year) && !anyNA(year)) {
    errors <- c(
      errors,
      paste0(
        "`year` must be coercible to integer; problematic values: ",
        paste(year[is.na(coerced_year)], collapse = ", "),
        "."
      )
    )
  }

  list(valid = length(errors) == 0L, errors = errors)
}

# ── Warning capture ───────────────────────────────────────────────────────────

#' Run an expression, collect warnings, and catch errors
#'
#' Wraps `expr` in a [base::withCallingHandlers()] (to intercept warnings
#' without stopping execution) nested inside a [base::tryCatch()] (to catch
#' errors and aborts).  This is the single error-catching boundary for all
#' domain logic in endpoint handlers.
#'
#' `resolve_release()` aborts and `table_maker()` domain errors are both
#' caught here and surfaced as the `error` field.
#'
#' @param expr An expression to evaluate (passed with `substitute()`; do not
#'   wrap in `quote()` at the call site).
#'
#' @return A named list:
#'   \describe{
#'     \item{`result`}{The return value of `expr`, or `NULL` if an error
#'       occurred.}
#'     \item{`warnings`}{Character vector of warning/message strings collected
#'       during evaluation (in order of occurrence).}
#'     \item{`error`}{`NULL` on success, or a single character string
#'       describing the caught error.}
#'   }
capture_with_warnings <- function(expr) {
  collected <- character()

  result <- tryCatch(
    withCallingHandlers(
      expr,
      warning = function(w) {
        collected <<- c(collected, conditionMessage(w))
        invokeRestart("muffleWarning")
      },
      message = function(m) {
        # Capture cli informational messages too (they come through as
        # conditions of class "message").
        msg <- conditionMessage(m)
        # Only collect non-empty messages; strip trailing newlines.
        msg <- trimws(msg, which = "right")
        if (nzchar(msg)) {
          collected <<- c(collected, msg)
        }
        invokeRestart("muffleMessage")
      }
    ),
    error = function(e) {
      list(.__error__ = conditionMessage(e))
    }
  )

  # Distinguish a caught error from a normal return value
  if (is.list(result) && !is.null(result$.__error__)) {
    return(list(result = NULL, warnings = collected, error = result$.__error__))
  }

  list(result = result, warnings = collected, error = NULL)
}
