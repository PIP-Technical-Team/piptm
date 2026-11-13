#' @importFrom data.table data.table rbindlist
#' @importFrom cli cli_abort cli_warn
NULL

# ── Shared table-parameter helpers ────────────────────────────────────────────
#
# These helpers hold the domain validation and the manifest-driven survey
# partitioning that used to live inline inside table_maker(). They are consumed
# by BOTH table_maker() (the computation path) and build_description_model()
# (the metadata document path) so the two can never disagree on which inputs
# are valid or on which surveys are included/excluded for a given request.
#
# Both helpers are pure with respect to microdata: they read only the in-memory
# manifest and variable registry, never Arrow/Parquet.

#' Validate and normalize table-shaping parameters
#'
#' Classifies the requested measures, validates poverty-line coherence and
#' release-scoped breakdown covariates, and validates + normalizes the
#' `filter_base` sample filter into an integer-coded named list. All failures
#' raise via [piptm_abort()] with a user-facing message, matching the behavior
#' previously inlined in [table_maker()].
#'
#' @param analysis_var Character scalar analysis variable name, or `NULL`.
#' @param measures Character vector of measure names.
#' @param poverty_line Numeric scalar/vector poverty line(s), or `NULL`.
#' @param by Character vector of breakdown covariates, or `NULL`.
#' @param column,row,superColumn,superRow Optional layout-slot values used by
#'   API-adjacent direct callers (for example, [build_description_model()]).
#'   Each must be `NULL` or a single non-blank character scalar.
#' @param filter_base Named list sample filter, or `NULL`.
#' @param release Character scalar release ID, or `NULL`.
#'
#' @return A named list with elements `families` (measure family classification),
#'   `by_validate` (`by` with `pov_status` removed, or `NULL`),
#'   `normalized_filter_base` (integer-coded named list, or `NULL`), and
#'   `filter_vars` (character vector of filter variable names).
#' @keywords internal
.validate_and_normalize_table_params <- function(analysis_var,
                                                 measures,
                                                 poverty_line = NULL,
                                                 by = NULL,
                                                 column = NULL,
                                                 row = NULL,
                                                 superColumn = NULL,
                                                 superRow = NULL,
                                                 filter_base = NULL,
                                                 release = NULL) {
  families <- .classify_measures(measures)
  .validate_poverty_lines(poverty_line, names(families))

  # `pov_status` as a breakdown dimension requires a valid poverty line
  # regardless of which measures were requested (poverty-family measures are
  # already covered by .validate_poverty_lines() above via `families`, but a
  # non-poverty measure disaggregated by pov_status would otherwise slip
  # through). This check is shared so table_maker() and
  # build_description_model() can never disagree on when a poverty line is
  # required for a pov_status breakdown.
  if (!is.null(by) && "pov_status" %in% by) {
    if (is.null(poverty_line) || !is.numeric(poverty_line) ||
        length(poverty_line) != 1L || !is.finite(poverty_line) ||
        poverty_line <= 0) {
      piptm_abort(
        c(
          "{.arg poverty_line} is required when {.val pov_status} is used as a disaggregation dimension.",
          "i" = "Provide a single positive numeric scalar."
        ),
        user_message = "A valid positive poverty line is required when breaking down results by poverty status."
      )
    }
  }

  # Optional slot-level validation for direct callers that pass layout slots
  # rather than a pre-built `by` vector.
  slot_values <- list(
    column = column,
    row = row,
    superColumn = superColumn,
    superRow = superRow
  )
  if (any(!vapply(slot_values, is.null, logical(1)))) {
    slot_order <- c("column", "row", "superColumn", "superRow")
    filled <- character(0)
    slot_dims <- character(0)

    for (slot_name in slot_order) {
      value <- slot_values[[slot_name]]
      if (is.null(value)) {
        next
      }

      if (!is.character(value) || length(value) != 1L ||
          is.na(value) || !nzchar(trimws(value))) {
        piptm_abort(
          c(
            "{.arg {slot_name}} must be NULL or a single non-empty character scalar.",
            "i" = "Example: {.code {slot_name} = 'gender'}"
          ),
          user_message = "Each table layout slot must contain exactly one valid variable name."
        )
      }

      value <- trimws(value)
      filled <- c(filled, slot_name)
      slot_dims <- c(slot_dims, value)
    }

    if (length(filled) > 0L) {
      expected_prefix <- slot_order[seq_along(filled)]
      if (!identical(filled, expected_prefix)) {
        missing_slot <- setdiff(expected_prefix, filled)[[1L]]
        piptm_abort(
          c(
            "Layout slots must be filled in order (column, row, superColumn, superRow).",
            "i" = "{.arg {missing_slot}} is empty but a later slot is set."
          ),
          user_message = "Layout fields must be filled from left to right with no gaps."
        )
      }

      if (anyDuplicated(slot_dims) > 0L) {
        dup_vals <- unique(slot_dims[duplicated(slot_dims)])
        piptm_abort(
          c(
            "A layout variable is assigned to more than one slot.",
            "i" = "Duplicate value{?s}: {.val {dup_vals}}"
          ),
          user_message = "The same variable cannot be assigned to multiple layout slots."
        )
      }
    }
  }

  by_validate <- by
  if (!is.null(by_validate) && anyDuplicated(by_validate) > 0L) {
    dup_vals <- unique(by_validate[duplicated(by_validate)])
    piptm_abort(
      c(
        "Duplicate variable(s) in {.arg by}: {.val {dup_vals}}.",
        "i" = "Each disaggregation variable may be used only once."
      ),
      user_message = "Each table disaggregation variable can be used only once."
    )
  }

  if (!is.null(by_validate)) {
    by_validate <- setdiff(by_validate, "pov_status")
    if (length(by_validate) == 0L) by_validate <- NULL
  }
  .validate_by(by_validate, release = release)

  normalized_filter_base <- NULL
  filter_vars <- character(0L)
  if (!is.null(filter_base)) {
    if (is.data.frame(filter_base)) {
      filter_base <- as.list(filter_base)
    }

    if (!is.list(filter_base)) {
      piptm_abort(
        "{.arg filter_base} must be NULL or a named list.",
        user_message = .PIPTM_SAMPLE_FILTER_UNREADABLE_USER_ERROR
      )
    }
    if (length(filter_base) == 0L) {
      # Treat explicit empty list as an intentional "no filters" request.
      filter_base <- NULL
    }

    if (!is.null(filter_base)) {

      filter_vars <- names(filter_base)
      if (is.null(filter_vars) || anyNA(filter_vars) || any(!nzchar(filter_vars))) {
        piptm_abort(
          "{.arg filter_base} must have non-empty variable names.",
          user_message = .PIPTM_SAMPLE_FILTER_UNREADABLE_USER_ERROR
        )
      }

      allowed_filter_vars <- unique(
        vapply(piptm_filter_categories(release), `[[`, character(1), "varname")
      )

      invalid_vars <- setdiff(filter_vars, allowed_filter_vars)
      if (length(invalid_vars) > 0L) {
        piptm_abort(
          c(
            "Invalid {.arg filter_base} variable{?s}: {.val {invalid_vars}}.",
            "i" = "Allowed variables: {.val {allowed_filter_vars}}"
          ),
          user_message = "One or more selected sample filters are not valid for this data release."
        )
      }

      normalized_filter_base <- stats::setNames(
        lapply(filter_vars, function(varname) {
          vals <- unlist(filter_base[[varname]], use.names = FALSE)
          if (length(vals) == 0L) {
            piptm_abort(
              "{.arg filter_base} variable {.val {varname}} must include at least one value.",
              user_message = "Each sample filter must include at least one selected category."
            )
          }
          vals_numeric <- suppressWarnings(as.numeric(vals))
          bad_non_int  <- is.na(vals_numeric) | !is.finite(vals_numeric)
          if (any(bad_non_int)) {
            piptm_abort(
              c(
                "{.arg filter_base} variable {.val {varname}} has non-integer value{?s}.",
                "i" = "Values must be integer codes stored in Parquet.",
                "i" = "Offending value{?s}: {.val {unique(vals[bad_non_int])}}"
              ),
              user_message = .PIPTM_SAMPLE_FILTER_VALUES_USER_ERROR
            )
          }
          fractional_idx <- vals_numeric != floor(vals_numeric)
          if (any(fractional_idx)) {
            piptm_abort(
              c(
                "{.arg filter_base} variable {.val {varname}} has non-integer value{?s}.",
                "i" = "Values must be integer codes stored in Parquet.",
                "i" = "Offending value{?s}: {.val {unique(vals[fractional_idx])}}"
              ),
              user_message = .PIPTM_SAMPLE_FILTER_VALUES_USER_ERROR
            )
          }
          vals_int <- as.integer(vals_numeric)
          unique(vals_int)
        }),
        filter_vars
      )
    }
  }

  list(
    families = families,
    by_validate = by_validate,
    normalized_filter_base = normalized_filter_base,
    filter_vars = filter_vars
  )
}


#' Partition requested surveys into included vs. excluded by available dimensions
#'
#' Resolves the manifest for `release`, matches the requested `pip_id`s against
#' it, and applies the two-stage sequential pre-filter used by [table_maker()]:
#'
#' 1. **Filter-base stage** — drop surveys that lack all `filter_vars`
#'    dimensions; abort if none remain.
#' 2. **Breakdown stage** — on the survivors, drop surveys that lack all
#'    requested `by` dimensions (excluding `pov_status`, which is computed
#'    dynamically); abort if none remain.
#'
#' Partial `pip_id` matches warn and continue (when `warn = TRUE`), mirroring
#' `table_maker()`. The returned `excluded$reason` column carries the clean
#' reason (e.g. `"missing gender"`, `"no requested dimensions"`) WITHOUT the
#' `<pip_id>: ` prefix, while the emitted `cli_warn()` bullets keep the prefix
#' so existing warning text is unchanged.
#'
#' @param release Character scalar release ID, or `NULL` (resolved to current).
#' @param pip_id Character vector of requested survey identifiers.
#' @param normalized_filter_base Named list sample filter (integer-coded), or
#'   `NULL`.
#' @param filter_vars Character vector of filter variable names.
#' @param by Character vector of breakdown covariates, or `NULL`.
#' @param warn Logical; emit `cli_warn()` for excluded surveys and partial
#'   `pip_id` matches. Defaults to `TRUE` (computation path); the metadata
#'   builder passes `FALSE`.
#'
#' @return A named list with `release` (the resolved release),
#'   `included` (a manifest-entry `data.table`), and `excluded`
#'   (a `data.table` with columns `pip_id`, `reason`).
#' @keywords internal
.partition_surveys_by_dimensions <- function(release,
                                             pip_id,
                                             normalized_filter_base = NULL,
                                             filter_vars = character(0L),
                                             by = NULL,
                                             warn = TRUE) {
  if (is.null(release)) {
    release <- piptm_current_release()
  }
  mf      <- piptm_manifest(release)
  .ids    <- pip_id
  entries <- mf[pip_id %chin% .ids]

  if (data.table::uniqueN(entries, by = "pip_id") != nrow(entries)) {
    dup_ids <- entries[duplicated(entries, by = "pip_id"), pip_id]
    cli::cli_abort(
      c(
        "Manifest contains duplicate entries for {length(dup_ids)} pip_id{?s}.",
        "i" = "Each pip_id must map to exactly one manifest row.",
        "i" = "Duplicates: {.val {unique(dup_ids)}}"
      )
    )
  }

  if (nrow(entries) == 0L) {
    piptm_abort(
      c(
        "No matching surveys found in manifest for release {.val {mf$release[[1L]] %||% piptm_current_release()}}.",
        "i" = "Requested pip_id{?s}: {.val {(.ids)}}"
      ),
      user_message = "The requested country or survey is not available in the selected data release."
    )
  }

  # Warn about pip_ids that exist in the request but not in the manifest
  missing_ids <- setdiff(pip_id, entries$pip_id)
  if (length(missing_ids) && warn) {
    cli_warn(
      c(
        "{length(missing_ids)} pip_id{?s} not found in manifest:",
        "i" = "{.val {missing_ids}}"
      )
    )
  }

  excluded_surveys <- data.table::data.table(
    pip_id = character(0),
    reason = character(0)
  )

  # ── Stage 1: Filter-base manifest pre-filter ────────────────────────────────
  if (!is.null(normalized_filter_base)) {
    overlap_fb <- vapply(
      entries$dimensions,
      function(d) length(intersect(filter_vars, d)),
      integer(1L)
    )

    full_filter_idx <- overlap_fb == length(filter_vars)
    dropped_filter_idx <- !full_filter_idx

    if (any(dropped_filter_idx)) {
      dropped_entries <- entries[dropped_filter_idx]
      dropped_reason <- vapply(seq_len(nrow(dropped_entries)), function(i) {
        have <- dropped_entries$dimensions[[i]]
        miss <- setdiff(filter_vars, have)
        if (length(miss) == length(filter_vars)) {
          "no filter_base dimensions"
        } else {
          paste0("missing ", paste(miss, collapse = ", "))
        }
      }, character(1L))

      excluded_surveys <- data.table::rbindlist(list(
        excluded_surveys,
        data.table::data.table(
          pip_id = dropped_entries$pip_id,
          reason = dropped_reason
        )
      ), use.names = TRUE, fill = TRUE)

      if (warn) {
        dropped_info <- paste0(dropped_entries$pip_id, ": ", dropped_reason)
        cli_warn(
          c(
            "Excluding {length(dropped_info)} survey{?s} that lack all required {.arg filter_base} dimensions ({.val {filter_vars}}):",
            "i" = "{dropped_info}"
          )
        )
      }

      entries <- entries[full_filter_idx]
    }

    if (nrow(entries) == 0L) {
      piptm_abort(
        c(
          "All requested surveys were excluded: none have all required {.arg filter_base} dimensions ({.val {filter_vars}})."
        ),
        user_message = .piptm_covariate_exclusion_user_error(filter_vars, release)
      )
    }
  }

  # ── Stage 2: Breakdown-dimension pre-filter ─────────────────────────────────
  if (!is.null(by)) {
    by_check <- by[!by %in% "pov_status"]

    if (length(by_check) == 0L) {
      by_check <- NULL
    }

    if (!is.null(by_check)) {
      overlap <- vapply(
        entries$dimensions,
        function(d) length(intersect(by_check, d)),
        integer(1L)
      )

      full_idx <- overlap == length(by_check)
      dropped_idx <- !full_idx

      if (any(dropped_idx)) {
        dropped_entries <- entries[dropped_idx]
        dropped_reason <- vapply(seq_len(nrow(dropped_entries)), function(i) {
          have <- dropped_entries$dimensions[[i]]
          miss <- setdiff(by_check, have)
          if (length(miss) == length(by_check)) {
            "no requested dimensions"
          } else {
            paste0("missing ", paste(miss, collapse = ", "))
          }
        }, character(1L))

        excluded_surveys <- data.table::rbindlist(list(
          excluded_surveys,
          data.table::data.table(
            pip_id = dropped_entries$pip_id,
            reason = dropped_reason
          )
        ), use.names = TRUE, fill = TRUE)

        if (warn) {
          dropped_info <- paste0(dropped_entries$pip_id, ": ", dropped_reason)
          cli_warn(
            c(
              "Excluding {length(dropped_info)} survey{?s} that lack all requested dimensions ({.val {by_check}}):",
              "i" = "{dropped_info}"
            )
          )
        }
        entries <- entries[full_idx]
      }

      if (nrow(entries) == 0L) {
        piptm_abort(
          c(
            "All requested surveys were excluded: none have all of the requested dimensions ({.val {by_check}}).",
            "i" = "Check {.fn piptm_manifest} for available dimensions per survey."
          ),
          user_message = .piptm_covariate_exclusion_user_error(by_check, release)
        )
      }
    }
  }

  list(
    release = release,
    included = entries,
    excluded = excluded_surveys
  )
}
