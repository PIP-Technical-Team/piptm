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

# Maximum number of surveys accepted per /table request.
.MAX_SURVEYS_PER_REQUEST <- 15L
.SESSION_TTL_SECONDS <- 3600L
.SESSION_STORE <- new.env(parent = emptyenv())
.PIP_ID_PATTERN <- "^[A-Z]{3}_[0-9]{4}_[A-Z0-9_-]{1,40}$"

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

#' Validate and normalize pip_id values for session storage
#'
#' @param pip_id Character vector of survey identifiers.
#'
#' @return A named list with fields `valid`, `errors`, and normalized `pip_id`.
validate_session_pip_id <- function(pip_id) {
  errors <- character()

  if (is.null(pip_id) || length(pip_id) == 0L) {
    errors <- c(errors, "`pip_id` must be a non-empty character vector.")
    return(list(valid = FALSE, errors = errors, pip_id = NULL))
  }

  pip_id <- as.character(unlist(pip_id, use.names = FALSE))
  pip_id <- trimws(pip_id)
  pip_id <- pip_id[nzchar(pip_id)]

  if (length(pip_id) == 0L) {
    errors <- c(errors, "`pip_id` must contain at least one non-empty value.")
    return(list(valid = FALSE, errors = errors, pip_id = NULL))
  }

  bad_ids <- pip_id[!grepl(.PIP_ID_PATTERN, pip_id)]
  if (length(bad_ids) > 0L) {
    errors <- c(
      errors,
      paste0(
        "`pip_id` value(s) contain invalid characters or format: ",
        paste(unique(bad_ids), collapse = ", "),
        ". Expected pattern: ISO3_YYYY_<survey-info> (uppercase letters, ",
        "digits, hyphens, underscores; max 50 characters)."
      )
    )
  }

  list(
    valid = length(errors) == 0L,
    errors = errors,
    pip_id = unique(pip_id)
  )
}

#' Create and store a session containing pip_id values
#'
#' @param pip_id Character vector of validated survey identifiers.
#'
#' @return Character scalar session ID.
create_session <- function(pip_id) {
  session_id <- NULL
  tries <- 0L

  while (is.null(session_id) && tries < 20L) {
    candidate <- paste0(sample(c(letters, 0:9), size = 12L, replace = TRUE), collapse = "")
    if (!exists(candidate, envir = .SESSION_STORE, inherits = FALSE)) {
      session_id <- candidate
    }
    tries <- tries + 1L
  }

  if (is.null(session_id)) {
    cli::cli_abort("Could not allocate a unique session ID. Please retry.")
  }

  .SESSION_STORE[[session_id]] <- list(
    pip_id = pip_id,
    created_at = Sys.time()
  )

  session_id
}

#' Fetch a non-expired session's pip_id values
#'
#' @param session_id Character scalar session identifier.
#'
#' @return Character vector of pip_id values, or `NULL` when missing/expired.
get_session_surveys <- function(session_id) {
  if (!is.character(session_id) || length(session_id) != 1L || !nzchar(session_id)) {
    return(NULL)
  }

  if (!exists(session_id, envir = .SESSION_STORE, inherits = FALSE)) {
    return(NULL)
  }

  entry <- .SESSION_STORE[[session_id]]
  age_sec <- as.numeric(difftime(Sys.time(), entry$created_at, units = "secs"))

  if (is.na(age_sec) || age_sec > .SESSION_TTL_SECONDS) {
    rm(list = session_id, envir = .SESSION_STORE)
    return(NULL)
  }

  as.character(entry$pip_id)
}

# List of helpers that return JSON/TEXT and set res$status. Used by the
# /description endpoint whose serializer is text (so error envelopes are
# serialized explicitly as JSON strings).

#' Build a JSON-serialized error envelope and set the HTTP status code
#'
#' Unlike [api_error()] (which returns a list for a JSON serializer), this
#' returns a pre-serialized JSON string suitable for text-serialized endpoints.
#'
#' @param errors      Character vector of human-readable error messages.
#' @param status_code Integer HTTP status code to apply to `res`.
#' @param res         The plumber response object.
#' @return A JSON string of the standard error envelope.
error_json <- function(errors, status_code, res) {
  res$status <- status_code
  jsonlite::toJSON(
    list(
      status   = "error",
      data     = NULL,
      warnings = character(),
      errors   = errors,
      meta     = list()
    ),
    auto_unbox = TRUE
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
#' `poverty_line` and `ppp` values without re-coercing.
#'
#' Checks performed:
#' \itemize{
#'   \item `pip_id` is a non-NULL character vector of length 1–15
#'   \item `measures` is a non-NULL character vector with all elements in
#'   \'\' elements in [piptm:::.MEASURE_REGISTRY]
#'   \item `analysis_var` is a single non-empty character value
#'   \item `poverty_line`, if not `NULL`, coerces cleanly to a single numeric
#'     value and is positive and finite
#'   \item `poverty_line` is required when `analysis_var == "pov_status"` or
#'     when `"pov_status"` is included in `by`
#'   \item `by`, if not `NULL`, is a subset of [piptm::.VALID_DIMENSIONS]
#'   \item `ppp`, if not `NULL`, is a single value coercible to a positive
#'     integer (PPP reference year, e.g. `2017`)
#' }
#'
#' Note: `release` validation is handled separately by [resolve_release()].
#'
#' @param analysis_var Character scalar analysis variable name.
#' @param pip_id       Character vector of survey identifiers.
#' @param measures     Character vector of measure names.
#' @param poverty_line Numeric (or coercible) scalar poverty line, or `NULL`.
#' @param by           Character vector of disaggregation dimensions, or
#'   `NULL`.
#' @param ppp          Integer scalar PPP reference year (e.g. `2017`), or
#'   `NULL` to use the manifest default.  Must be a single positive integer
#'   value when provided.
#'
#' @return A named list:
#'   \describe{
#'     \item{`valid`}{`TRUE` when all checks pass; `FALSE` otherwise.}
#'     \item{`errors`}{Character vector of error messages (empty when valid).}
#'     \item{`poverty_line`}{The coerced numeric scalar, or `NULL`.  Use this
#'       in the handler rather than the original input.}
#'     \item{`ppp`}{The coerced integer scalar, or `NULL`.  Use this in the
#'       handler rather than the original input.}
#'   }
validate_table_input <- function(analysis_var = NULL, pip_id, measures, poverty_line = NULL,
                                 by = NULL, ppp = NULL,
                                 pop_share_threshold = NULL) {
  errors <- character()

  # ── analysis_var ───────────────────────────────────────────────────────────
  if (is.null(analysis_var) || !is.character(analysis_var) ||
      length(analysis_var) != 1L || !nzchar(trimws(analysis_var))) {
    errors <- c(errors, "`analysis_var` must be a single non-empty character value.")
  } else {
    allowed_analysis_vars <- unique(c("welfare", "pov_status", piptm::pip_optional_dims()))
    if (!analysis_var %in% allowed_analysis_vars) {
      errors <- c(
        errors,
        paste0(
          "Unknown `analysis_var`: ", analysis_var,
          ". Valid analysis variables: ",
          paste(allowed_analysis_vars, collapse = ", "),
          "."
        )
      )
    }
  }

  # ── pip_id ─────────────────────────────────────────────────────────────────
  if (is.null(pip_id) || !is.character(pip_id) || length(pip_id) == 0L) {
    errors <- c(errors, "`pip_id` must be a non-empty character vector.")
  } else if (length(pip_id) > .MAX_SURVEYS_PER_REQUEST) {
    errors <- c(
      errors,
      paste0(
        "`pip_id` may contain at most ", .MAX_SURVEYS_PER_REQUEST,
        " surveys per request; ",
        length(pip_id), " were supplied."
      )
    )
  } else {
    # P1.10 — allowlist: pip_id values must match the canonical survey-ID
    # pattern (ISO3 _ year _ survey-acronym _ welfare-type _ area-code).
    # This prevents path-traversal, shell-injection, and garbage input from
    # reaching the filesystem or computation layer.
    bad_ids <- pip_id[!grepl(.PIP_ID_PATTERN, pip_id)]
    if (length(bad_ids) > 0L) {
      errors <- c(
        errors,
        paste0(
          "`pip_id` value(s) contain invalid characters or format: ",
          paste(bad_ids, collapse = ", "),
          ". Expected pattern: ISO3_YYYY_<survey-info> (uppercase letters, ",
          "digits, hyphens, underscores; max 50 characters)."
        )
      )
    }
  }

  # ── measures ───────────────────────────────────────────────────────────────
  valid_measures <- names(piptm:::.MEASURE_REGISTRY)
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

  # ── poverty_line ───────────────────────────────────────────────────────────
  coerced_pl <- NULL
  if (!is.null(poverty_line)) {
    coerced_pl <- suppressWarnings(as.numeric(poverty_line))
    if (length(coerced_pl) != 1L || is.na(coerced_pl)) {
      errors <- c(
        errors,
        "`poverty_line` must be a single positive numeric value when provided."
      )
      coerced_pl <- NULL
    } else {
      bad <- !is.finite(coerced_pl) | coerced_pl <= 0
      if (any(bad)) {
        errors <- c(
          errors,
          "`poverty_line` must be positive and finite."
        )
        coerced_pl <- NULL
      }
    }
  }

  # ── by ──────────────────────────────────────────────────────────────────────────────
  if (!is.null(by)) {
    valid_dims <- piptm::pip_valid_dimensions()
    by_check <- setdiff(by, "pov_status")
    unknown_dims <- setdiff(by_check, valid_dims)
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

  # ── cross-validation: poverty_line required for pov_status usage ─────────
  if ((identical(analysis_var, "pov_status") || (!is.null(by) && "pov_status" %in% by)) &&
      is.null(coerced_pl)) {
    errors <- c(
      errors,
      "`poverty_line` is required when analysis_var is 'pov_status' or when 'pov_status' is used as a disaggregation dimension."
    )
  }

  # ── ppp ────────────────────────────────────────────────────────────────────
  # Optional PPP reference year (e.g. 2017). Must be a single positive integer
  # when supplied. Query params arrive as character; coerce before checking.
  #
  # Validation layers (applied in order):
  #   1. Type guard  — rejects non-numeric/non-character atomics (e.g. TRUE/FALSE).
  #                    Logical inputs would otherwise coerce: TRUE→1L, FALSE→0L.
  #   2. Length guard — rejects vectors; repeated query params arrive as vectors.
  #   3. Digit guard  — rejects strings containing non-digit characters after
  #                    trimming whitespace.  This blocks scientific notation
  #                    ("1e5"→100000L), hex strings ("0x7FF"→2047L), decimal
  #                    strings ("2017.5"→2017L) and embedded newlines — all of
  #                    which as.integer() silently accepts.
  #   4. Positive guard — rejects zero and negative years.
  coerced_ppp <- NULL
  if (!is.null(ppp)) {
    # Layer 1: type guard — only character and numeric atomics are accepted.
    # Logical scalars (TRUE/FALSE) and lists must be rejected before length
    # or coercion checks, since as.integer(TRUE) == 1L passes all later tests.
    if (!is.character(ppp) && !is.numeric(ppp)) {
      errors <- c(
        errors,
        paste0(
          "`ppp` must be a numeric or character value; ",
          "got type: ", class(ppp)[[1L]], "."
        )
      )
      coerced_ppp <- NULL
    } else if (length(ppp) != 1L) {
      # Layer 2: scalar guard.
      errors <- c(
        errors,
        paste0(
          "`ppp` must be a single scalar value; ",
          length(ppp), " value(s) were supplied."
        )
      )
      coerced_ppp <- NULL
    } else {
      # Layer 3: digit-only guard for character inputs.
      # as.integer() silently parses hex ("0x7FF"→2047L), scientific notation
      # ("1e5"→100000L), and decimal strings ("2017.5"→2017L).  We reject any
      # string that is not purely decimal digits (after trimming whitespace).
      # Numeric inputs (already integer or double) skip this guard and go
      # straight to the positive check — a numeric 2017.0 is fine; 2017.9
      # truncates to 2017L which is acceptable for a typed R call.
      ppp_safe <- substr(as.character(ppp), 1L, 40L)  # bound length for echo
      if (is.character(ppp) && !grepl("^[0-9]+$", trimws(ppp))) {
        errors <- c(
          errors,
          paste0(
            "`ppp` must be a plain integer year (digits only, e.g. 2017); ",
            "got: ", ppp_safe, "."
          )
        )
        coerced_ppp <- NULL
      } else {
        # Layer 4: coerce and check positive.
        # suppressWarnings() silences the overflow warning when a numeric value
        # exceeds .Machine$integer.max — the resulting NA_integer_ is caught
        # by the is.na() check below.
        coerced_ppp <- suppressWarnings(as.integer(ppp))
        if (is.na(coerced_ppp)) {
          errors <- c(
            errors,
            paste0(
              "`ppp` must be coercible to an integer PPP year (e.g. 2017); ",
              "got: ", ppp_safe, "."
            )
          )
          coerced_ppp <- NULL
        } else if (coerced_ppp <= 0L) {
          errors <- c(
            errors,
            paste0(
              "`ppp` must be a positive integer PPP year; ",
              "got: ", coerced_ppp, "."
            )
          )
          coerced_ppp <- NULL
        }
      }
    }
  }

  # ── pop_share_threshold ─────────────────────────────────────────────────────

  # Optional numeric in (0, 1). Empty string or NULL disables suppression.
  # Query params arrive as character; coerce before checking.

  coerced_threshold <- NULL
  if (!is.null(pop_share_threshold) && !identical(pop_share_threshold, "")) {
    coerced_threshold <- suppressWarnings(as.numeric(pop_share_threshold))
    if (is.na(coerced_threshold)) {
      errors <- c(
        errors,
        paste0(
          "`pop_share_threshold` must be a numeric value in (0, 1); ",
          "got: ", substr(as.character(pop_share_threshold), 1L, 40L), "."
        )
      )
      coerced_threshold <- NULL
    } else if (coerced_threshold <= 0 || coerced_threshold >= 1) {
      errors <- c(
        errors,
        paste0(
          "`pop_share_threshold` must be strictly between 0 and 1; ",
          "got: ", coerced_threshold, "."
        )
      )
      coerced_threshold <- NULL
    }
  }

  list(
    valid               = length(errors) == 0L,
    errors              = errors,
    poverty_line        = coerced_pl,
    ppp                 = coerced_ppp,
    pop_share_threshold = coerced_threshold
  )
}

#' Validate inputs for the /description fallback path
#'
#' Ensures the fallback body contains at least the required table parameters.
#' The heavy lifting is delegated to [validate_table_input()]; this helper
#' mainly checks the distinction between the fast and fallback paths and
#' rejects ambiguous bodies.
#'
#' @param body Parsed JSON request body (a list).
#'
#' @return A named list with `valid` (logical), `errors` (character vector),
#'   and `mode` ("metadata" | "fallback").
validate_description_input <- function(body) {
  errors <- character()

  if (!is.list(body) || length(body) == 0L) {
    return(list(
      valid = FALSE,
      errors = "Request body must be a non-empty JSON object.",
      mode = NA_character_
    ))
  }

  fmt <- body$format %||% NULL
  if (!is.null(fmt)) {
    if (!is.character(fmt) || length(fmt) != 1L || is.na(fmt)) {
      errors <- c(errors, "`format` must be a single string when provided.")
    } else {
      fmt <- tolower(trimws(fmt))
      if (!(fmt %in% c("markdown", "html"))) {
        errors <- c(errors, "`format` must be one of: markdown, html.")
      }
    }
  }

  has_metadata <- "description_metadata" %in% names(body)
  param_fields <- intersect(
    names(body),
    c("pip_id", "analysis_var", "measures", "poverty_line", "by",
      "filter_base", "ppp", "release", "pop_share_threshold")
  )
  has_params <- length(param_fields) > 0L

  if (has_metadata && has_params) {
    errors <- c(errors,
      "Request must provide either `description_metadata` OR table parameters, not both.")
  }

  mode <- if (has_metadata) "metadata" else if (has_params) "fallback" else NA_character_
  if (is.na(mode)) {
    errors <- c(errors,
      "Request body must contain `description_metadata` or table parameters.")
  }

  list(valid = length(errors) == 0L, errors = errors, mode = mode)
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

  # ── year coercible to integer (coerce here; return result for handler reuse) ─
  coerced_year <- suppressWarnings(as.integer(year))
  if (anyNA(coerced_year)) {
    bad_orig <- year[is.na(coerced_year)]
    errors <- c(
      errors,
      paste0(
        "`year` must be coercible to integer; problematic values: ",
        paste(bad_orig, collapse = ", "),
        "."
      )
    )
    coerced_year <- NULL
  }

  list(
    valid  = length(errors) == 0L,
    errors = errors,
    year   = coerced_year
  )
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
#' @param expr An expression to evaluate. Passed and evaluated normally;
#'   do not wrap in `quote()`.
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
      # Use a typed S3 class instead of a list-key sentinel to avoid
      # colliding with domain functions that legitimately return list(.__error__ = ...).
      structure(list(message = conditionMessage(e)), class = "cw_error")
    }
  )

  # Distinguish a caught error from a normal return value via S3 class.
  if (inherits(result, "cw_error")) {
    return(list(result = NULL, warnings = collected, error = result$message))
  }

  list(result = result, warnings = collected, error = NULL)
}
