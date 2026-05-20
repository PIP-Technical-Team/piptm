# Parquet Validation Functions
# Plan:  .cg-docs/plans/2026-03-17-arrow-data-preparation.md  (Steps 2 & 6)
# Schema: inst/schema/arrow-schema.json  (canonical reference — not read at
#         runtime; constraints are transcribed as R constants below)
#
# These functions validate Parquet files that have already been written to the
# Master Arrow Repository by {pipdata}. They NEVER write or modify files.
# All functions return a structured result list and never throw conditions,
# so they are safe to use inside batch pipelines.
#
# Exported functions
# ------------------
#   validate_parquet_schema()         — structural checks (schema only, fast)
#   validate_parquet_data()           — data-quality checks (reads all rows)
#   validate_partition_consistency()  — cross-file checks within one partition
#
# Return value contract (all three functions)
# -------------------------------------------
#   list(
#     valid    = TRUE | FALSE,   # FALSE if any errors; TRUE even with warnings
#     errors   = character(),    # hard failures — file must not be used
#     warnings = character(),    # soft issues — file usable but flagged
#     file     = <path>          # absolute path(s) checked, for traceability
#   )

# ---------------------------------------------------------------------------
# Internal constants — now derived from pip_arrow_schema()
# ---------------------------------------------------------------------------

.VP_REQUIRED_COLS  <- pip_required_cols()
.VP_ALLOWED_COLS   <- pip_allowed_cols()
.VP_PARTITION_KEYS <- c("country_code", "surveyid_year", "welfare_type", "version")

.schema            <- pip_arrow_schema()
.VP_GENDER_LEVELS  <- .schema$levels$gender
.VP_AREA_LEVELS    <- .schema$levels$area

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

#' Build the canonical Arrow schema covering all known columns
#'
#' Returns an `arrow::schema()` for ALL columns defined in the PIP Arrow
#' schema — both required and optional. Used by `validate_parquet_schema()`
#' to compare actual file types against expected types for any column found
#' in a given file.
#'
#' @return An `arrow::Schema` object.
#' @importFrom arrow schema field utf8 int32 float64 dictionary open_dataset read_parquet write_parquet
#' @importFrom dplyr any_of
#' @importFrom data.table as.data.table uniqueN
#' @keywords internal
.vp_canonical_schema <- function() {
  # int32 indices: matches Arrow R's default when converting R factors.
  dict_type <- arrow::dictionary(arrow::int32(), arrow::utf8())

  arrow::schema(
    # --- Required columns ---
    arrow::field("country_code",   arrow::utf8()),
    arrow::field("surveyid_year",  arrow::int32()),
    arrow::field("welfare_type",   arrow::utf8()),
    arrow::field("version",        arrow::utf8()),
    arrow::field("pip_id",         arrow::utf8()),
    arrow::field("survey_acronym", arrow::utf8()),
    arrow::field("welfare",        arrow::float64()),
    arrow::field("weight",         arrow::float64()),
    # --- Optional breakdown dimensions ---
    arrow::field("gender",         dict_type),
    arrow::field("area",           dict_type),
    arrow::field("educat4",        dict_type),
    arrow::field("educat5",        dict_type),
    arrow::field("educat7",        dict_type),
    # age: continuous int32 — NOT a dictionary column (per schema rules)
    arrow::field("age",            arrow::int32())
  )
}

#' Parse partition key values from a Parquet file path
#'
#' Extracts `country_code`, `surveyid_year`, and `welfare_type` from the
#' directory components of `file_path`, e.g.:
#'   `.../country_code=BOL/surveyid_year=2012/welfare_type=INC/BOL_2012_...-0.parquet`
#'
#' Returns `NULL` for any component that cannot be parsed (e.g. non-standard
#' path), so callers can skip the path-matching check gracefully.
#'
#' @param file_path Absolute path to a Parquet file.
#'
#' @return A named list with `country_code` (character), `surveyid_year`
#'   (integer), `welfare_type` (character), `version` (character). Any
#'   unparsable component is `NA`.
#' @keywords internal
.vp_parse_partition_path <- function(file_path) {
  parts <- strsplit(
    gsub("\\\\", "/", file_path),  # normalise Windows separators
    "/"
  )[[1L]]

  extract <- function(prefix) {
    hit <- grep(paste0("^", prefix, "="), parts, value = TRUE)
    if (length(hit) == 1L) sub(paste0("^", prefix, "="), "", hit) else NA_character_
  }

  country_raw  <- extract("country_code")
  year_raw     <- extract("surveyid_year")
  welfare_raw  <- extract("welfare_type")
  version_raw  <- extract("version")

  list(
    country_code  = if (!is.na(country_raw)) country_raw else NA_character_,
    surveyid_year = if (!is.na(year_raw)) suppressWarnings(as.integer(year_raw))
                    else NA_integer_,
    welfare_type  = if (!is.na(welfare_raw)) welfare_raw else NA_character_,
    version       = if (!is.na(version_raw)) version_raw else NA_character_
  )
}

#' Build an empty validation result list
#'
#' @param file Character scalar — file or directory path being validated.
#'
#' @return A list with `valid = TRUE`, empty `errors` and `warnings`, and
#'   the supplied `file` value.
#' @keywords internal
.vp_result <- function(file) {
  list(valid = TRUE, errors = character(), warnings = character(), file = file)
}

#' Record an error in a validation result and set valid = FALSE
#'
#' @param result A validation result list (modified in place via replacement).
#' @param msg    Error message string.
#'
#' @return Updated result list.
#' @keywords internal
.vp_add_error <- function(result, msg) {
  result$errors <- c(result$errors, msg)
  result$valid  <- FALSE
  result
}

#' Record a warning in a validation result (valid remains TRUE)
#'
#' @param result A validation result list.
#' @param msg    Warning message string.
#'
#' @return Updated result list.
#' @keywords internal
.vp_add_warning <- function(result, msg) {
  result$warnings <- c(result$warnings, msg)
  result
}

# ---------------------------------------------------------------------------
# validate_parquet_schema()   — Step 2 (structural checks)
# ---------------------------------------------------------------------------

#' Validate the schema of a single Parquet file
#'
#' Reads only the file **metadata** (not row data) and checks that the file
#' conforms to the canonical PIP Arrow schema defined in
#' `inst/schema/arrow-schema.json`. This is fast even for large files.
#'
#' Checks performed:
#' 1. File exists.
#' 2. File is a readable Parquet file.
#' 3. All required columns are present.
#' 4. No extra columns outside the allowed set.
#' 5. Column types match the canonical Arrow types.
#'
#' @param file_path Absolute path to a `.parquet` file.
#'
#' @return A named list:
#'   \describe{
#'     \item{`valid`}{`TRUE` if no errors; `FALSE` otherwise.}
#'     \item{`errors`}{Character vector of hard failures.}
#'     \item{`warnings`}{Character vector of soft issues.}
#'     \item{`file`}{`file_path`, for traceability.}
#'   }
#'
#' @family parquet-validation
#' @export
#' @examples
#' \dontrun{
#' res <- validate_parquet_schema("path/to/BOL_2012_EH_...-0.parquet")
#' res$valid
#' res$errors
#' }
validate_parquet_schema <- function(file_path) {
  stopifnot(is.character(file_path), length(file_path) == 1L)

  result <- .vp_result(file_path)

  # --- Check 1: file exists --------------------------------------------------
  if (!file.exists(file_path)) {
    result <- .vp_add_error(
      result, paste0("File does not exist: ", file_path)
    )
    return(result)  # cannot proceed further
  }

  # --- Check 2: readable as Parquet — read schema only ----------------------
  file_schema <- tryCatch(
    arrow::open_dataset(file_path)$schema,
    error = function(e) {
      NULL
    }
  )
  if (is.null(file_schema)) {
    result <- .vp_add_error(
      result,
      paste0("Cannot read file as Parquet: ", file_path)
    )
    return(result)
  }

  actual_cols   <- names(file_schema)
  canonical_sch <- .vp_canonical_schema()

  # --- Check 3: required columns present -------------------------------------
  missing_cols <- setdiff(.VP_REQUIRED_COLS, actual_cols)
  if (length(missing_cols) > 0L) {
    result <- .vp_add_error(
      result,
      paste0("Missing required columns: ", paste(missing_cols, collapse = ", "))
    )
  }

  # --- Check 4: no extra columns outside schema ------------------------------
  # Any column starting with "welfare" (old single-welfare OR new multi-welfare_*)
  # is considered allowed. Column-count enforcement is not the responsibility of
  # this check — welfare columns are enumerated per-survey in the manifest.
  welfare_cols_in_file <- grep("^welfare", actual_cols, value = TRUE)
  extra_cols <- setdiff(actual_cols, c(.VP_ALLOWED_COLS, welfare_cols_in_file))
  if (length(extra_cols) > 0L) {
    result <- .vp_add_error(
      result,
      paste0(
        "Column(s) not in the Arrow schema: ",
        paste(extra_cols, collapse = ", ")
      )
    )
  }

  # --- Check 5: column types match canonical schema --------------------------
  # Only check columns that are both present in the file AND in the canonical
  # schema (missing/extra already flagged above).
  cols_to_check <- intersect(actual_cols, .VP_ALLOWED_COLS)

  for (col in cols_to_check) {
    actual_field   <- file_schema$GetFieldByName(col)$type
    canonical_field <- canonical_sch$GetFieldByName(col)$type

    actual_type    <- actual_field$ToString()
    canonical_type <- canonical_field$ToString()

    # Dictionary columns: Arrow picks the smallest index integer type that
    # fits the number of distinct values (int8 for ≤127 levels, int16 for
    # ≤32767, int32 otherwise). Accept any integer-indexed dictionary over
    # utf8 values rather than requiring exactly int32.
    is_dict_mismatch <- (
      !identical(actual_type, canonical_type) &&
      inherits(canonical_field, "DictionaryType") &&
      inherits(actual_field,    "DictionaryType") &&
      identical(actual_field$value_type$ToString(), "string")
    )

    if (!is_dict_mismatch && !identical(actual_type, canonical_type)) {
      result <- .vp_add_error(
        result,
        paste0(
          "Column '", col, "': expected type <", canonical_type,
          ">, found <", actual_type, ">"
        )
      )
    }
  }

  return(result)
}

# ---------------------------------------------------------------------------
# validate_parquet_data()   — Step 2 (data-quality checks)
# ---------------------------------------------------------------------------

#' Validate the row data of a single Parquet file
#'
#' Reads the **full row data** and checks data-quality constraints defined in
#' `inst/schema/arrow-schema.json`. More expensive than
#' `validate_parquet_schema()` — do not call in hot paths.
#'
#' Checks performed:
#' 1. `welfare` — all finite, all ≥ 0; warn if any == 0.
#' 2. `weight` — all finite, all > 0, no NA.
#' 3. `welfare_type` — all values in `{"INC", "CON"}`.
#' 4. `country_code` — matches ISO3 pattern; matches partition directory.
#' 5. `surveyid_year` — matches `surveyid_year=` component of partition directory.
#' 6. `welfare_type` data value — matches `welfare_type=` component of partition
#'    directory.
#' 7. Partition key consistency — one unique value per key per file.
#' 8. Factor level conformance for `gender`, `area`, `education` (if present).
#' 9. `age` range \[0, 130\] (if present).
#'
#' @param file_path Absolute path to a `.parquet` file.
#'
#' @return A named list with `valid`, `errors`, `warnings`, `file`.
#'   See [validate_parquet_schema()] for the full structure.
#'
#' @family parquet-validation
#' @export
#' @examples
#' \dontrun{
#' res <- validate_parquet_data("path/to/BOL_2012_EH_...-0.parquet")
#' res$valid
#' res$warnings  # e.g. zero-welfare rows
#' }
validate_parquet_data <- function(file_path) {
  stopifnot(is.character(file_path), length(file_path) == 1L)

  result <- .vp_result(file_path)

  # --- Guard: file must exist and be readable --------------------------------
  if (!file.exists(file_path)) {
    result <- .vp_add_error(
      result, paste0("File does not exist: ", file_path)
    )
    return(result)
  }

  # Read full data as data.table
  dt <- tryCatch(
    data.table::as.data.table(arrow::read_parquet(file_path)),
    error = function(e) NULL
  )
  if (is.null(dt)) {
    result <- .vp_add_error(
      result, paste0("Cannot read Parquet row data: ", file_path)
    )
    return(result)
  }

  # Parse expected partition values from the directory path
  path_keys <- .vp_parse_partition_path(file_path)

  # --- Check 1: welfare -------------------------------------------------------
  welfare_col <- dt[["welfare"]]
  n_zero <- sum(welfare_col == 0, na.rm = TRUE)
  if (n_zero > 0L) {
    result <- .vp_add_warning(
      result,
      paste0(n_zero, " row(s) have welfare == 0.")
    )
  }
  if (!all(is.finite(welfare_col))) {
    result <- .vp_add_error(
      result, "welfare contains non-finite values (Inf / NaN / NA)."
    )
  }
  if (any(welfare_col < 0, na.rm = TRUE)) {
    result <- .vp_add_error(
      result, "welfare contains negative values."
    )
  }

  # --- Check 2: weight -------------------------------------------------------
  weight_col <- dt[["weight"]]
  if (any(is.na(weight_col))) {
    result <- .vp_add_error(result, "weight contains NA values.")
  }
  if (!all(is.finite(weight_col))) {
    result <- .vp_add_error(
      result, "weight contains non-finite values (Inf / NaN)."
    )
  }
  if (any(weight_col <= 0, na.rm = TRUE)) {
    result <- .vp_add_error(
      result, "weight contains non-positive values (must be strictly > 0)."
    )
  }

  # --- Check 3: welfare_type values ------------------------------------------
  invalid_wt <- setdiff(unique(dt[["welfare_type"]]), c("INC", "CON"))
  if (length(invalid_wt) > 0L) {
    result <- .vp_add_error(
      result,
      paste0(
        "welfare_type contains invalid value(s): ",
        paste(invalid_wt, collapse = ", "),
        " (must be 'INC' or 'CON')."
      )
    )
  }

  # --- Check 4: country_code format ------------------------------------------
  cc_col <- dt[["country_code"]]
  bad_cc <- unique(cc_col[!grepl("^[A-Z]{3}$", cc_col)])
  if (length(bad_cc) > 0L) {
    result <- .vp_add_error(
      result,
      paste0(
        "country_code does not match ISO3 format [A-Z]{3}: ",
        paste(bad_cc, collapse = ", ")
      )
    )
  }

  # --- Check 5–7: partition key consistency & path matching ------------------
  for (key in .VP_PARTITION_KEYS) {
    n_unique <- data.table::uniqueN(dt[[key]])
    if (n_unique != 1L) {
      result <- .vp_add_error(
        result,
        paste0(
          "Partition key '", key, "' has ", n_unique,
          " distinct value(s) in file (must be exactly 1)."
        )
      )
    }
  }

  # Path matching — only check when the path could be parsed
  data_cc  <- dt[["country_code"]][[1L]]
  data_yr  <- dt[["surveyid_year"]][[1L]]
  data_wt  <- dt[["welfare_type"]][[1L]]
  data_ver <- dt[["version"]][[1L]]

  if (!is.na(path_keys$country_code) &&
      !identical(data_cc, path_keys$country_code)) {
    result <- .vp_add_error(
      result,
      paste0(
        "country_code in data ('", data_cc,
        "') does not match partition directory ('",
        path_keys$country_code, "')."
      )
    )
  }
  if (!is.na(path_keys$surveyid_year) &&
      !identical(as.integer(data_yr), path_keys$surveyid_year)) {
    result <- .vp_add_error(
      result,
      paste0(
        "surveyid_year in data (", data_yr,
        ") does not match partition directory year (",
        path_keys$surveyid_year, ")."
      )
    )
  }
  if (!is.na(path_keys$welfare_type) &&
      !identical(data_wt, path_keys$welfare_type)) {
    result <- .vp_add_error(
      result,
      paste0(
        "welfare_type in data ('", data_wt,
        "') does not match partition directory ('",
        path_keys$welfare_type, "')."
      )
    )
  }
  if (isTRUE(!is.na(path_keys$version)) &&
      !identical(data_ver, path_keys$version)) {
    result <- .vp_add_error(
      result,
      paste0(
        "version in data ('", data_ver,
        "') does not match partition directory ('",
        path_keys$version, "')."
      )
    )
  }

  # --- Check 8: factor level conformance for breakdown dimensions ------------
  if ("gender" %in% names(dt)) {
    g <- as.character(dt[["gender"]])
    bad <- unique(g[!is.na(g) & !g %in% .VP_GENDER_LEVELS])
    if (length(bad) > 0L) {
      result <- .vp_add_error(
        result,
        paste0(
          "gender contains values outside allowed levels {",
          paste(.VP_GENDER_LEVELS, collapse = ", "), "}: ",
          paste(bad, collapse = ", ")
        )
      )
    }
  }
  if ("area" %in% names(dt)) {
    a <- as.character(dt[["area"]])
    bad <- unique(a[!is.na(a) & !a %in% .VP_AREA_LEVELS])
    if (length(bad) > 0L) {
      result <- .vp_add_error(
        result,
        paste0(
          "area contains values outside allowed levels {",
          paste(.VP_AREA_LEVELS, collapse = ", "), "}: ",
          paste(bad, collapse = ", ")
        )
      )
    }
  }
  # educat4/5/7: only check that they are factors when present — levels are
  # survey-specific and are NOT validated against a fixed set.
  for (edu_col in c("educat4", "educat5", "educat7")) {
    if (edu_col %in% names(dt) && !is.factor(dt[[edu_col]])) {
      result <- .vp_add_error(
        result,
        paste0(edu_col, " must be a factor column.")
      )
    }
  }

  # --- Check 9: age range ----------------------------------------------------
  if ("age" %in% names(dt)) {
    age_col <- dt[["age"]]
    bad_age <- unique(age_col[!is.na(age_col) & (age_col < 0L | age_col > 130L)])
    if (length(bad_age) > 0L) {
      result <- .vp_add_error(
        result,
        paste0(
          "age contains values out of range [0, 130]: ",
          paste(bad_age, collapse = ", ")
        )
      )
    }
  }

  return(result)
}

# ---------------------------------------------------------------------------
# validate_parquet()   — unified wrapper
# ---------------------------------------------------------------------------

#' Validate a Parquet file or partition directory
#'
#' A single-entry-point wrapper around the three lower-level validators.
#' Choose what to check via the `check` argument; results always follow the
#' same `list(valid, errors, warnings, file)` contract.
#'
#' | `check` value    | Delegates to                        | `path` type |
#' |------------------|-------------------------------------|-------------|
#' | `"schema"`       | [validate_parquet_schema()]         | file        |
#' | `"data"`         | [validate_parquet_data()]           | file        |
#' | `"consistency"`  | [validate_partition_consistency()]  | directory   |
#'
#' When multiple file-level checks are requested (e.g.
#' `check = c("schema", "data")`), both are run and their results are merged:
#' `valid` is `FALSE` if **either** check finds an error; `errors` and
#' `warnings` are concatenated.  Combining `"consistency"` with any other
#' check is not supported because `"consistency"` takes a directory while the
#' others take a file — pass `"consistency"` alone.
#'
#' @param path  For `check = "schema"` or `"data"`: absolute path to a
#'   `.parquet` file.  For `check = "consistency"`: absolute path to a
#'   partition directory (e.g.
#'   `.../country_code=BOL/surveyid_year=2012/welfare_type=INC/version=v01_v04`).
#' @param check Character vector, one or more of `"schema"`, `"data"`,
#'   `"consistency"`.  Defaults to `"schema"`.
#'
#' @return A named list:
#'   \describe{
#'     \item{`valid`}{`TRUE` if no errors were found across all requested
#'       checks; `FALSE` otherwise.}
#'     \item{`errors`}{Character vector of hard failures.}
#'     \item{`warnings`}{Character vector of soft issues.}
#'     \item{`file`}{`path`, for traceability.}
#'   }
#'   When `check = "consistency"` the result also contains
#'   `files_checked` (see [validate_partition_consistency()]).
#'
#' @family parquet-validation
#' @export
#' @examples
#' \dontrun{
#' # Schema check only (default — fast, metadata only)
#' validate_parquet("path/to/KAZ_2006_HBS_CON_ALL-0.parquet")
#'
#' # Data-quality check
#' validate_parquet("path/to/KAZ_2006_HBS_CON_ALL-0.parquet", check = "data")
#'
#' # Both schema and data in one call
#' res <- validate_parquet(
#'   "path/to/KAZ_2006_HBS_CON_ALL-0.parquet",
#'   check = c("schema", "data")
#' )
#' res$valid
#' res$errors
#'
#' # Cross-file consistency check for a partition directory
#' validate_parquet(
#'   "path/to/arrow/country_code=KAZ/surveyid_year=2006/welfare_type=CON/version=v01_v05",
#'   check = "consistency"
#' )
#' }
validate_parquet <- function(path, check = "schema") {
  stopifnot(is.character(path), length(path) == 1L)

  valid_checks <- c("schema", "data", "consistency")
  check <- unique(check)
  bad   <- setdiff(check, valid_checks)
  if (length(bad) > 0L) {
    stop(
      "Unknown check value(s): ", paste(bad, collapse = ", "),
      ". Must be one or more of: ", paste(valid_checks, collapse = ", "), "."
    )
  }

  if ("consistency" %in% check && length(check) > 1L) {
    stop(
      "'consistency' cannot be combined with other checks: it validates a ",
      "partition *directory*, while 'schema' and 'data' validate a single ",
      "*file*. Call validate_parquet() separately for each."
    )
  }

  # --- Single-check fast paths -----------------------------------------------
  if (identical(check, "schema")) {
    return(validate_parquet_schema(path))
  }
  if (identical(check, "data")) {
    return(validate_parquet_data(path))
  }
  if (identical(check, "consistency")) {
    return(validate_partition_consistency(path))
  }

  # --- Multiple file-level checks: run and merge -----------------------------
  results <- lapply(check, function(chk) {
    if (chk == "schema") validate_parquet_schema(path)
    else                 validate_parquet_data(path)
  })

  list(
    valid    = all(vapply(results, `[[`, logical(1L), "valid")),
    errors   = unlist(lapply(results, `[[`, "errors"),   use.names = FALSE),
    warnings = unlist(lapply(results, `[[`, "warnings"), use.names = FALSE),
    file     = path
  )
}

# ---------------------------------------------------------------------------
# validate_arrow()   — batch validator over the Arrow repository
# ---------------------------------------------------------------------------

#' Batch-validate Parquet files in the Arrow repository
#'
#' Discovers `.parquet` files under [piptm_arrow_root()] and runs
#' [validate_parquet()] on each one, returning a tidy `data.table` summary
#' (one row per file).
#'
#' **Filtering** — narrow the scope with one of:
#' - `country_code`: one or more ISO3 codes (e.g. `"KAZ"` or `c("KAZ","COL")`).
#' - `pip_ids`: one or more `pip_id` strings that determine the file names to
#'   look for.
#' - `surveys`: a `data.table` / `data.frame` with at least a `pip_id` column
#'   (e.g. the output of [piptm_manifest()]).
#'
#' Only one filter may be supplied at a time. If none is supplied the function
#' validates the entire repository (potentially slow).
#'
#' @param country_code Character vector of ISO3 country codes to validate.
#' @param pip_ids      Character vector of `pip_id` values to validate.
#' @param surveys      A `data.table` / `data.frame` with a `pip_id` column.
#' @param check        Character vector passed to [validate_parquet()].
#'   Defaults to `c("schema", "data")`.
#' @param arrow_root   Path to the Arrow repository root. Defaults to
#'   [piptm_arrow_root()].
#'
#' @return A `data.table` with one row per `.parquet` file and columns:
#'   \describe{
#'     \item{`pip_id`}{Survey identifier derived from the filename.}
#'     \item{`file`}{Absolute path to the `.parquet` file.}
#'     \item{`valid`}{`TRUE` / `FALSE`.}
#'     \item{`n_errors`}{Number of hard errors.}
#'     \item{`n_warnings`}{Number of soft warnings.}
#'     \item{`errors`}{Errors collapsed with `" | "`, or `""` if none.}
#'     \item{`warnings`}{Warnings collapsed with `" | "`, or `""` if none.}
#'   }
#'
#' @family parquet-validation
#' @export
#' @examples
#' \dontrun{
#' # All KAZ files — schema + data checks
#' validate_arrow("KAZ")
#'
#' # Several countries
#' validate_arrow(c("KAZ", "COL"))
#'
#' # From a pip_inv subset (has a pip_id column)
#' validate_arrow(surveys = pip_inv_subset)
#'
#' # Schema check only — much faster
#' validate_arrow("KAZ", check = "schema")
#'
#' # Show only failures
#' res <- validate_arrow("KAZ")
#' res[valid == FALSE]
#' }
validate_arrow <- function(country_code = NULL,
                           pip_ids      = NULL,
                           surveys      = NULL,
                           check        = c("schema", "data"),
                           arrow_root   = piptm_arrow_root()) {

  # --- Validate arguments ----------------------------------------------------
  n_filters <- (!is.null(country_code)) + (!is.null(pip_ids)) + (!is.null(surveys))
  if (n_filters > 1L) {
    stop("Supply at most one of 'country_code', 'pip_ids', or 'surveys'.")
  }

  if (is.null(arrow_root) || !nzchar(arrow_root)) {
    stop(
      "Arrow root is not configured. ",
      "Set PIPTM_ARROW_ROOT in ~/.Renviron or call set_arrow_root()."
    )
  }
  if (!dir.exists(arrow_root)) {
    stop("Arrow root directory does not exist: ", arrow_root)
  }

  # --- Resolve pip_ids from the surveys data.frame --------------------------
  if (!is.null(surveys)) {
    if (!is.data.frame(surveys) || !"pip_id" %in% names(surveys)) {
      stop("'surveys' must be a data.frame / data.table with a 'pip_id' column.")
    }
    pip_ids <- unique(as.character(surveys[["pip_id"]]))
  }

  # --- Discover parquet files ------------------------------------------------
  search_root <- if (!is.null(country_code)) {
    # One search per country — file.path vectorises over country_code
    file.path(arrow_root, paste0("country_code=", toupper(country_code)))
  } else {
    arrow_root
  }

  parquet_files <- unlist(lapply(search_root, function(root) {
    if (!dir.exists(root)) {
      warning("Directory not found (skipping): ", root)
      return(character(0L))
    }
    list.files(root, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
  }), use.names = FALSE)

  # --- Filter by pip_id if requested ----------------------------------------
  if (!is.null(pip_ids)) {
    # Parquet filename is <pip_id>-0.parquet (or -1, -2, … for multi-chunk)
    keep <- vapply(parquet_files, function(f) {
      any(startsWith(basename(f), paste0(pip_ids, "-")))
    }, logical(1L))
    parquet_files <- parquet_files[keep]
  }

  if (length(parquet_files) == 0L) {
    message("No .parquet files found matching the supplied filter.")
    return(data.table::data.table(
      pip_id    = character(0L),
      file      = character(0L),
      valid     = logical(0L),
      n_errors  = integer(0L),
      n_warnings = integer(0L),
      errors    = character(0L),
      warnings  = character(0L)
    ))
  }

  # --- Run validate_parquet() on each file ----------------------------------
  results <- lapply(parquet_files, validate_parquet, check = check)

  # --- Build tidy summary data.table ----------------------------------------
  data.table::rbindlist(lapply(results, function(r) {
    fname  <- basename(r$file)
    # Strip trailing -<chunk>.parquet to recover pip_id
    pip_id <- sub("-[0-9]+\\.parquet$", "", fname)
    data.table::data.table(
      pip_id     = pip_id,
      file       = r$file,
      valid      = r$valid,
      n_errors   = length(r$errors),
      n_warnings = length(r$warnings),
      errors     = paste(r$errors,   collapse = " | "),
      warnings   = paste(r$warnings, collapse = " | ")
    )
  }))
}

# ---------------------------------------------------------------------------
# validate_partition_consistency()   — Step 6 (cross-file checks)
# ---------------------------------------------------------------------------

#' Validate schema consistency across all Parquet files in a partition directory
#'
#' Reads the **schema only** (not row data) of every `.parquet` file in
#' `partition_dir` and checks:
#' 1. At least one `.parquet` file exists.
#' 2. All files share an identical schema (no schema drift between survey
#'    versions stored in the same partition).
#' 3. The partition key values embedded in each file's data match the
#'    directory path.
#' 4. No duplicate `survey_id` values across files in the partition.
#'
#' @param partition_dir Absolute path to a partition directory, e.g.
#'   `.../country_code=BOL/surveyid_year=2012/welfare_type=INC/`.
#'
#' @return A named list with `valid`, `errors`, `warnings`, `file` (set to
#'   `partition_dir`) and an extra element:
#'   \describe{
#'     \item{`files_checked`}{Character vector of `.parquet` file paths found
#'       in `partition_dir`.}
#'   }
#'
#' @family parquet-validation
#' @export
#' @examples
#' \dontrun{
#' res <- validate_partition_consistency(
#'   "path/to/arrow/country_code=BOL/surveyid_year=2012/welfare_type=INC/version=v01_v04"
#' )
#' res$valid
#' res$files_checked
#' }
validate_partition_consistency <- function(partition_dir) {
  stopifnot(is.character(partition_dir), length(partition_dir) == 1L)

  result               <- .vp_result(partition_dir)
  result$files_checked <- character()

  # --- Guard: directory must exist -------------------------------------------
  if (!dir.exists(partition_dir)) {
    result <- .vp_add_error(
      result,
      paste0("Partition directory does not exist: ", partition_dir)
    )
    return(result)
  }

  parquet_files <- list.files(
    partition_dir, pattern = "\\.parquet$",
    full.names = TRUE, recursive = FALSE
  )

  # --- Check 1: at least one file --------------------------------------------
  if (length(parquet_files) == 0L) {
    result <- .vp_add_error(
      result,
      paste0("No .parquet files found in: ", partition_dir)
    )
    return(result)
  }

  result$files_checked <- parquet_files

  # --- Read schemas for all files (metadata only — fast) --------------------
  schemas <- lapply(parquet_files, function(f) {
    tryCatch(
      arrow::open_dataset(f)$schema,
      error = function(e) NULL
    )
  })

  unreadable <- parquet_files[vapply(schemas, is.null, logical(1L))]
  if (length(unreadable) > 0L) {
    result <- .vp_add_error(
      result,
      paste0(
        "Cannot read schema for file(s): ",
        paste(basename(unreadable), collapse = ", ")
      )
    )
    # Remove unreadable files from further checks
    readable_idx  <- !vapply(schemas, is.null, logical(1L))
    parquet_files <- parquet_files[readable_idx]
    schemas       <- schemas[readable_idx]
  }

  if (length(schemas) == 0L) return(result)

  # --- Check 2: all schemas identical ----------------------------------------
  # Compare every schema against the first as the reference.
  ref_schema      <- schemas[[1L]]$ToString()
  ref_file        <- basename(parquet_files[[1L]])

  for (i in seq_along(schemas)[-1L]) {
    this_schema <- schemas[[i]]$ToString()
    if (!identical(ref_schema, this_schema)) {
      result <- .vp_add_error(
        result,
        paste0(
          "Schema mismatch: '", basename(parquet_files[[i]]),
          "' differs from reference file '", ref_file, "'."
        )
      )
    }
  }

  # --- Check 3: partition key values match directory path --------------------
  path_keys <- .vp_parse_partition_path(
    file.path(partition_dir, "dummy.parquet")
  )

  # Read only partition key columns — avoids loading full row data
  for (f in parquet_files) {
    pq_keys <- tryCatch(
      data.table::as.data.table(
        arrow::read_parquet(f, col_select = dplyr::any_of(.VP_PARTITION_KEYS))
      ),
      error = function(e) NULL
    )
    if (is.null(pq_keys)) next

    data_cc <- if ("country_code"  %in% names(pq_keys)) pq_keys[["country_code"]][[1L]]  else NA_character_
    data_yr <- if ("surveyid_year" %in% names(pq_keys)) pq_keys[["surveyid_year"]][[1L]] else NA_integer_
    data_wt <- if ("welfare_type"  %in% names(pq_keys)) pq_keys[["welfare_type"]][[1L]]  else NA_character_

    if (!is.na(path_keys$country_code) &&
        !identical(data_cc, path_keys$country_code)) {
      result <- .vp_add_error(
        result,
        paste0(
          basename(f), ": country_code in data ('", data_cc,
          "') does not match directory ('", path_keys$country_code, "')."
        )
      )
    }
    if (!is.na(path_keys$surveyid_year) &&
        !identical(as.integer(data_yr), path_keys$surveyid_year)) {
      result <- .vp_add_error(
        result,
        paste0(
          basename(f), ": surveyid_year in data (", data_yr,
          ") does not match directory year (", path_keys$surveyid_year, ")."
        )
      )
    }
    if (!is.na(path_keys$welfare_type) &&
        !identical(data_wt, path_keys$welfare_type)) {
      result <- .vp_add_error(
        result,
        paste0(
          basename(f), ": welfare_type in data ('", data_wt,
          "') does not match directory ('", path_keys$welfare_type, "')."
        )
      )
    }
  }

  # --- Check 4: no duplicate pip_id across files, and unique within each file --
  # For cross-file deduplication: collect one pip_id per file.
  # For within-file uniqueness (schema rule unique_per_file: true): read all
  # values and use uniqueN() — a first-value read would miss corrupted files
  # that contain mixed pip_id rows.
  pip_ids <- vapply(parquet_files, function(f) {
    tryCatch({
      vals <- arrow::read_parquet(f, col_select = "pip_id")[[1L]]
      if (data.table::uniqueN(vals) > 1L) {
        cli::cli_warn(
          "File {.path {basename(f)}} contains {data.table::uniqueN(vals)} distinct pip_id values (expected 1)."
        )
      }
      as.character(vals[[1L]])
    },
    error = function(e) NA_character_
    )
  }, character(1L))

  pip_ids_clean <- pip_ids[!is.na(pip_ids)]
  dupes         <- pip_ids_clean[duplicated(pip_ids_clean)]

  if (length(dupes) > 0L) {
    result <- .vp_add_error(
      result,
      paste0(
        "Duplicate pip_id value(s) found across files in partition: ",
        paste(unique(dupes), collapse = ", ")
      )
    )
  }

  return(result)
}
