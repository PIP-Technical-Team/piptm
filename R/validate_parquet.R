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
.VP_PARTITION_KEYS <- c("country_code", "surveyid_year", "welfare_type")

.schema            <- pip_arrow_schema()
.VP_GENDER_LEVELS  <- .schema$levels$gender
.VP_AREA_LEVELS    <- .schema$levels$area
.VP_EDU_LEVELS     <- .schema$levels$education

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
#' @keywords internal
.vp_canonical_schema <- function() {
  # int32 indices: matches Arrow R's default when converting R factors.
  dict_type <- arrow::dictionary(arrow::int32(), arrow::utf8())

  arrow::schema(
    # --- Required columns ---
    arrow::field("country_code",   arrow::utf8()),
    arrow::field("surveyid_year",  arrow::int32()),
    arrow::field("welfare_type",   arrow::utf8()),
    arrow::field("survey_id",      arrow::utf8()),
    arrow::field("survey_acronym", arrow::utf8()),
    arrow::field("welfare",        arrow::float64()),
    arrow::field("weight",         arrow::float64()),
    # --- Optional breakdown dimensions ---
    arrow::field("gender",         dict_type),
    arrow::field("area",           dict_type),
    arrow::field("education",      dict_type),
    # age: continuous int32 — NOT a dictionary column (per schema rules)
    arrow::field("age",            arrow::int32())
  )
}

#' Parse partition key values from a Parquet file path
#'
#' Extracts `country_code`, `surveyid_year`, and `welfare_type` from the
#' directory components of `file_path`, e.g.:
#'   `.../country=BOL/year=2012/welfare=INC/BOL_2012_...-0.parquet`
#'
#' Returns `NULL` for any component that cannot be parsed (e.g. non-standard
#' path), so callers can skip the path-matching check gracefully.
#'
#' @param file_path Absolute path to a Parquet file.
#'
#' @return A named list with `country_code` (character), `surveyid_year`
#'   (integer), `welfare_type` (character). Any unparsable component is `NA`.
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

  country_raw  <- extract("country")
  year_raw     <- extract("year")
  welfare_raw  <- extract("welfare")

  list(
    country_code  = if (!is.na(country_raw)) country_raw else NA_character_,
    surveyid_year = if (!is.na(year_raw)) suppressWarnings(as.integer(year_raw))
                    else NA_integer_,
    welfare_type  = if (!is.na(welfare_raw)) welfare_raw else NA_character_
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
  extra_cols <- setdiff(actual_cols, .VP_ALLOWED_COLS)
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
    actual_type    <- file_schema$GetFieldByName(col)$type$ToString()
    canonical_type <- canonical_sch$GetFieldByName(col)$type$ToString()

    if (!identical(actual_type, canonical_type)) {
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
#' 5. `surveyid_year` — matches `year=` component of partition directory.
#' 6. `welfare_type` data value — matches `welfare=` component of partition
#'    directory.
#' 7. Partition key consistency — one unique value per key per file.
#' 8. Factor level conformance for `gender`, `area`, `education` (if present).
#' 9. `age` range [0, 130] (if present).
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
  if ("education" %in% names(dt)) {
    e <- as.character(dt[["education"]])
    bad <- unique(e[!is.na(e) & !e %in% .VP_EDU_LEVELS])
    if (length(bad) > 0L) {
      result <- .vp_add_error(
        result,
        paste0(
          "education contains values outside allowed levels {",
          paste(.VP_EDU_LEVELS, collapse = ", "), "}: ",
          paste(bad, collapse = ", ")
        )
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
#'   `.../country=BOL/year=2012/welfare=INC/`.
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
#'   "path/to/arrow/country=BOL/year=2012/welfare=INC"
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

  # --- Check 4: no duplicate survey_id across files --------------------------
  survey_ids <- vapply(parquet_files, function(f) {
    tryCatch(
      arrow::read_parquet(f, col_select = "survey_id")[[1L]][[1L]],
      error = function(e) NA_character_
    )
  }, character(1L))

  survey_ids_clean <- survey_ids[!is.na(survey_ids)]
  dupes            <- survey_ids_clean[duplicated(survey_ids_clean)]

  if (length(dupes) > 0L) {
    result <- .vp_add_error(
      result,
      paste0(
        "Duplicate survey_id value(s) found across files in partition: ",
        paste(unique(dupes), collapse = ", ")
      )
    )
  }

  return(result)
}
