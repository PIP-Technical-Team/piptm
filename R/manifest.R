# Manifest Loading and Accessor Functions
#
# Plan: .cg-docs/plans/2026-04-03-manifest-generation-version-partition.md
#       (Step 4)
#
# Provides functions for loading, caching, and accessing PIP release manifests.
# The manifest system is the reproducibility contract between {pipdata} (data
# producer) and {piptm} (data consumer).
#
# Manifest JSON format (written by pipdata::generate_release_manifest()):
#
#   {
#     "release": "20260206",
#     "generated_at": "2026-04-03T17:00:00Z",
#     "entries": [
#       {
#         "pip_id":         "COL_2010_GEIH_INC_ALL",
#         "survey_id":      "COL_2010_GEIH_v01_M_v05_A_GMD_ALL",
#         "country_code":   "COL",
#         "year":           2010,
#         "welfare_type":   "INC",
#         "version":        "v01_v05",
#         "survey_acronym": "GEIH",
#         "module":         "ALL",
#         "dimensions":     ["area", "gender", "age"]
#       }
#     ]
#   }
#
# Exported functions
# ------------------
#   piptm_manifest_dir()    — configured manifest directory path
#   piptm_arrow_root()      — configured Arrow repository root path
#   piptm_manifests()       — named list of all loaded manifest data.tables
#   piptm_current_release() — default release ID
#   piptm_manifest()        — data.table for a specific release
#   set_manifest_dir()      — override manifest directory at runtime
#   set_arrow_root()        — override Arrow root at runtime
#
# Internal functions
# ------------------
#   .load_manifests()       — scan a directory, parse JSONs, cache in env

# ---------------------------------------------------------------------------
# Internal: parse and cache manifests from a directory
# ---------------------------------------------------------------------------

#' Scan a manifest directory, parse all manifest JSON files, and cache results
#'
#' Looks for files matching the glob `manifest_*.json` in `manifest_dir`,
#' parses each with [jsonlite::fromJSON()], converts the `entries` array to a
#' `data.table`, and stores all manifests in `.piptm_env$manifests` keyed by
#' `release` ID. Also reads `current_release.json` if present to set the
#' default release.
#'
#' The `dimensions` field in each entry is stored as a list column — each
#' element is a character vector of available breakdown dimension names.
#'
#' @param manifest_dir Absolute path to the manifest directory.
#'
#' @return Invisibly returns the number of manifests loaded.
#' @keywords internal
.load_manifests <- function(manifest_dir) {

  if (!dir.exists(manifest_dir)) {
    cli::cli_abort(
      "Manifest directory does not exist: {.path {manifest_dir}}"
    )
  }

  .piptm_env$manifest_dir <- manifest_dir

  manifest_files <- list.files(
    manifest_dir,
    pattern    = "^manifest_.*\\.json$",
    full.names = TRUE
  )

  manifests <- list()

  for (f in manifest_files) {
    parsed <- tryCatch(
      jsonlite::fromJSON(f, simplifyVector = FALSE),
      error = function(e) {
        cli::cli_warn(
          "Could not parse manifest file {.path {f}}: {conditionMessage(e)}"
        )
        NULL
      }
    )

    if (is.null(parsed)) next

    release_id <- parsed$release
    if (is.null(release_id) || !nzchar(release_id)) {
      cli::cli_warn(
        "Manifest file {.path {f}} has no 'release' field. Skipping."
      )
      next
    }

    entries <- parsed$entries
    if (is.null(entries) || length(entries) == 0L) {
      # Empty manifest — store an empty data.table with the correct schema
      manifests[[release_id]] <- .empty_manifest_dt()
      next
    }

    # Convert list-of-lists to data.table.
    # 'dimensions' is a list column (each element = character vector).
    dt <- data.table::data.table(
      pip_id         = vapply(entries, `[[`, character(1L), "pip_id"),
      survey_id      = vapply(entries, `[[`, character(1L), "survey_id"),
      country_code   = vapply(entries, `[[`, character(1L), "country_code"),
      year           = vapply(entries, function(e) as.integer(e$year), integer(1L)),
      welfare_type   = vapply(entries, `[[`, character(1L), "welfare_type"),
      version        = vapply(entries, `[[`, character(1L), "version"),
      survey_acronym = vapply(entries, `[[`, character(1L), "survey_acronym"),
      module         = vapply(entries, `[[`, character(1L), "module")
    )
    # dimensions is a list column — each entry is a character vector.
    # Use data.table::set() to avoid CEDTA errors before NAMESPACE is generated.
    dims_col <- lapply(entries, function(e) {
      d <- e$dimensions
      if (is.null(d)) character(0L) else as.character(unlist(d))
    })
    data.table::set(dt, j = "dimensions", value = dims_col)

    manifests[[release_id]] <- dt[]
  }

  .piptm_env$manifests <- manifests

  # --- current_release.json pointer -----------------------------------------
  pointer_path <- file.path(manifest_dir, "current_release.json")
  if (file.exists(pointer_path)) {
    pointer <- tryCatch(
      jsonlite::fromJSON(pointer_path),
      error = function(e) NULL
    )
    if (!is.null(pointer) && !is.null(pointer$current_release)) {
      .piptm_env$current_release <- as.character(pointer$current_release)
    }
  }

  # If no pointer file, default to the most recent release by name
  if (is.null(.piptm_env$current_release) && length(manifests) > 0L) {
    .piptm_env$current_release <- sort(names(manifests), decreasing = TRUE)[[1L]]
  }

  n <- length(manifests)
  cli::cli_inform(
    "Loaded {n} manifest{?s} from {.path {manifest_dir}}."
  )

  invisible(n)
}

#' Build an empty manifest data.table with the canonical column schema
#'
#' @return An empty `data.table` with all required manifest columns.
#' @keywords internal
.empty_manifest_dt <- function() {
  dt <- data.table::data.table(
    pip_id         = character(0L),
    survey_id      = character(0L),
    country_code   = character(0L),
    year           = integer(0L),
    welfare_type   = character(0L),
    version        = character(0L),
    survey_acronym = character(0L),
    module         = character(0L),
    dimensions     = list()
  )
  dt
}

# ---------------------------------------------------------------------------
# Accessor functions
# ---------------------------------------------------------------------------

#' Return the configured manifest directory path
#'
#' Returns the path set by `PIPTM_MANIFEST_DIR` at startup or the most recent
#' call to [set_manifest_dir()]. Returns `NULL` in dev mode (no manifest dir
#' configured).
#'
#' @return Character scalar path, or `NULL`.
#' @family manifest-accessors
#' @export
piptm_manifest_dir <- function() {
  .piptm_env$manifest_dir
}

#' Return the configured Arrow repository root path
#'
#' Returns the path set by `PIPTM_ARROW_ROOT` at startup or the most recent
#' call to [set_arrow_root()]. Returns `NULL` when not configured.
#'
#' @return Character scalar path, or `NULL`.
#' @family manifest-accessors
#' @export
piptm_arrow_root <- function() {
  .piptm_env$arrow_root
}

#' Return all loaded manifests as a named list
#'
#' Each element is a `data.table` of survey entries for one release, keyed by
#' `release` ID (e.g. `"20260206"`). Returns an empty list when no manifests
#' have been loaded.
#'
#' @return Named list of `data.table`s.
#' @family manifest-accessors
#' @export
piptm_manifests <- function() {
  .piptm_env$manifests
}

#' Return the default (current) release ID
#'
#' The current release is determined at load time by reading
#' `current_release.json` in the manifest directory. Falls back to the
#' lexicographically latest release ID when the pointer file is absent.
#'
#' @return Character scalar release ID, or `NULL` when no manifests are loaded.
#' @family manifest-accessors
#' @export
piptm_current_release <- function() {
  .piptm_env$current_release
}

#' Return the manifest data.table for a specific release
#'
#' Looks up the in-memory manifest cache and returns the `data.table` of
#' survey entries for the requested release. Each row is one survey and
#' contains the four Arrow partition filter keys (`country_code`, `year`,
#' `welfare_type`, `version`) plus `pip_id`, `survey_id`, `survey_acronym`,
#' `module`, and `dimensions` (list column of available breakdown columns).
#'
#' @param release Character scalar release ID (e.g. `"20260206"`). Defaults
#'   to the current release as returned by [piptm_current_release()].
#'
#' @return A `data.table` with columns: `pip_id`, `survey_id`, `country_code`,
#'   `year`, `welfare_type`, `version`, `survey_acronym`, `module`,
#'   `dimensions`.
#'
#' @family manifest-accessors
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' mf <- piptm_manifest("20260206")
#' mf[country_code == "COL"]
#' }
piptm_manifest <- function(release = NULL) {
  if (is.null(release)) {
    release <- piptm_current_release()
    if (is.null(release)) {
      cli::cli_abort(
        c(
          "No current release is set.",
          "i" = "Call {.fn set_manifest_dir} to load manifests, or pass {.arg release} explicitly."
        )
      )
    }
  }

  manifests <- .piptm_env$manifests
  if (!release %in% names(manifests)) {
    available <- if (length(manifests) > 0L) {
      paste(sort(names(manifests)), collapse = ", ")
    } else {
      "(none loaded)"
    }
    cli::cli_abort(
      c(
        "Release {.val {release}} not found in loaded manifests.",
        "i" = "Available releases: {available}",
        "i" = "Call {.fn set_manifest_dir} to load manifests from a directory."
      )
    )
  }

  manifests[[release]]
}

# ---------------------------------------------------------------------------
# Runtime configuration overrides
# ---------------------------------------------------------------------------

#' Override the manifest directory at runtime
#'
#' Sets the manifest directory and immediately rescans it, loading all
#' `manifest_*.json` files into memory. Replaces any previously loaded
#' manifests. Useful for development and testing when the `PIPTM_MANIFEST_DIR`
#' environment variable is not set.
#'
#' @param path Absolute path to the manifest directory. Must exist and contain
#'   at least one `manifest_*.json` file.
#'
#' @return Invisibly returns `path`.
#' @family manifest-accessors
#' @export
#' @examples
#' \dontrun{
#' set_manifest_dir("//server/manifests")
#' piptm_current_release()
#' }
set_manifest_dir <- function(path) {
  stopifnot(is.character(path), length(path) == 1L, !is.na(path))
  .load_manifests(path)
  invisible(path)
}

#' Override the Arrow repository root path at runtime
#'
#' Sets the Arrow root path used by [load_survey_microdata()] and
#' [load_surveys()]. Replaces the value from `PIPTM_ARROW_ROOT` (or a
#' previous call to this function). Useful for development and testing.
#'
#' @param path Absolute path to the root of the shared Arrow repository
#'   (the directory containing `country=*/year=*/welfare=*/version=*`
#'   subdirectories). Must exist.
#'
#' @return Invisibly returns `path`.
#' @family manifest-accessors
#' @export
#' @examples
#' \dontrun{
#' set_arrow_root("//server/pip/arrow")
#' }
set_arrow_root <- function(path) {
  stopifnot(is.character(path), length(path) == 1L, !is.na(path))
  if (!dir.exists(path)) {
    cli::cli_abort("Arrow root directory does not exist: {.path {path}}")
  }
  .piptm_env$arrow_root <- path
  invisible(path)
}
