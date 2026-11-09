# ── User-facing API error constants ──────────────────────────────────────────

#' Canonical generic user-facing fallback error sentence
#'
#' Returned in API `errors[]` envelopes whenever a caught condition does not
#' carry a specific `user_message` attribute.
#'
#' @keywords internal
#' @noRd
.PIPTM_GENERIC_USER_ERROR <- "Something went wrong processing this request."

#' Canonical user-facing error sentence for invalid /table/download payloads
#'
#' Returned in HTTP 400 API `errors[]` envelopes when `POST /table/download`
#' receives an incomplete or malformed `data` array.
#'
#' @keywords internal
#' @noRd
.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR <- paste0(
  "The table data could not be exported because the download payload is ",
  "incomplete or invalid."
)

#' Shared user-facing error constants across domain modules
#'
#' @keywords internal
#' @noRd
.PIPTM_DESCRIPTION_METADATA_USER_ERROR <- paste0(
  "The table description could not be generated because the provided table ",
  "metadata is incomplete or invalid."
)

#' @keywords internal
#' @noRd
.PIPTM_SAMPLE_FILTER_UNREADABLE_USER_ERROR <- paste0(
  "The selected sample filter could not be read. Please reset your filters ",
  "and try again."
)

#' @keywords internal
#' @noRd
.PIPTM_SAMPLE_FILTER_VALUES_USER_ERROR <-
  "One or more selected sample filter values are invalid."

#' @keywords internal
#' @noRd
.PIPTM_POVERTY_LINE_USER_ERROR <-
  "A valid positive poverty line is required to compute poverty statistics."

#' @keywords internal
#' @noRd
.PIPTM_SURVEY_YEAR_USER_ERROR <-
  "Survey years must be valid whole-number years (for example, 2019)."

#' @keywords internal
#' @noRd
.PIPTM_NO_ACTIVE_RELEASE_USER_ERROR <-
  "Survey data is temporarily unavailable because no active data release is loaded."

#' Format user-facing error message for an unavailable data release
#'
#' @param release Release value passed by the caller.
#' @return Single character string.
#' @keywords internal
#' @noRd
.piptm_unknown_release_user_error <- function(release) {
  rel_label <- if (length(release) == 1L && !is.na(release)) as.character(release) else ""
  sprintf("The requested data release ('%s') is not available.", rel_label)
}

# ── Condition constructors and extractors ────────────────────────────────────

#' Abort with both a developer diagnostic message and a UI-safe user message
#'
#' Thin wrapper around [cli::cli_abort()] that preserves `conditionMessage(e)`
#' for interactive R sessions, server logs, and existing `expect_error()` unit
#' tests while embedding a plain-English `user_message` field on the condition
#' object (`class = c(class, "piptm_error")`) for the Plumber API boundary.
#'
#' @param message Character vector of developer-facing cli bullets passed
#'   directly to [cli::cli_abort()].
#' @param ... Additional condition fields forwarded to [cli::cli_abort()].
#' @param user_message Optional single non-empty character string suitable for
#'   display to non-technical UI users, or `NULL`.
#' @param class Optional character vector of custom condition subclasses
#'   prepended to `"piptm_error"`.
#' @param .envir Environment in which to evaluate glue/cli expressions in
#'   `message`. Defaults to `parent.frame()`.
#' @param call Execution environment of the currently running function.
#'   Defaults to `rlang::caller_env()`.
#'
#' @return Never returns; raises an error condition of class
#'   `c(class, "piptm_error", "rlang_error", "error", "condition")`.
#'
#' @keywords internal
#' @noRd
piptm_abort <- function(message,
                        ...,
                        user_message = NULL,
                        class = NULL,
                        .envir = parent.frame(),
                        call = rlang::caller_env()) {
  if (!is.null(user_message)) {
    if (!is.character(user_message) ||
        length(user_message) != 1L ||
        is.na(user_message)) {
      cli::cli_abort(
        "{.arg user_message} must be {.code NULL} or a single non-empty character string.",
        call = call
      )
    }
    user_message <- trimws(cli::ansi_strip(user_message))
    if (!nzchar(user_message)) {
      cli::cli_abort(
        "{.arg user_message} must be {.code NULL} or a single non-empty character string.",
        call = call
      )
    }
  }

  cli::cli_abort(
    message = message,
    ...,
    class = unique(c(class, "piptm_error")),
    user_message = user_message,
    .envir = .envir,
    call = call
  )
}

#' Extract a UI-safe error message from a caught condition
#'
#' Returns the condition's `user_message` attribute when present and non-empty;
#' otherwise returns `fallback` (defaults to [.PIPTM_GENERIC_USER_ERROR]).
#' Never returns raw `conditionMessage(cnd)`.
#'
#' @param cnd A condition object caught by `tryCatch()`.
#' @param fallback Single character string (or `NULL`) returned when `cnd` does
#'   not carry a valid `user_message`.
#'
#' @return Single plain-text character string safe for UI display (or `fallback`
#'   when `fallback = NULL`).
#'
#' @keywords internal
#' @noRd
piptm_user_error_message <- function(cnd,
                                     fallback = .PIPTM_GENERIC_USER_ERROR) {
  um <- if (is.list(cnd) || inherits(cnd, "condition")) {
    cnd[["user_message"]]
  } else {
    NULL
  }

  if (is.character(um) && length(um) == 1L && !is.na(um)) {
    um_clean <- trimws(cli::ansi_strip(um))
    if (nzchar(um_clean)) {
      return(um_clean)
    }
  }

  fallback
}
