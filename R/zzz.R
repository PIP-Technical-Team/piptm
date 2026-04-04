# Package environment and lifecycle hooks
#
# .piptm_env: private package environment used to cache manifests and
# configuration at load time. Never exported — access via accessor functions
# in manifest.R.
#
# Configuration is driven by two environment variables:
#   PIPTM_MANIFEST_DIR  — path to the directory containing manifest_*.json
#                         files and (optionally) current_release.json.
#                         When unset, the package runs in dev mode: no
#                         manifests are loaded at startup and set_manifest_dir()
#                         must be called before any data access.
#   PIPTM_ARROW_ROOT    — absolute path to the root of the shared Arrow
#                         repository (the Hive-partitioned Parquet dataset).
#                         When unset, data loading will error until
#                         set_arrow_root() is called.

.piptm_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {

  # Initialise all slots to NULL so accessors never see an unbound name
  .piptm_env$manifest_dir    <- NULL
  .piptm_env$arrow_root      <- NULL
  .piptm_env$manifests       <- list()
  .piptm_env$current_release <- NULL

  # --- Arrow root -----------------------------------------------------------
  arrow_root_env <- Sys.getenv("PIPTM_ARROW_ROOT", unset = "")
  if (nzchar(arrow_root_env)) {
    .piptm_env$arrow_root <- arrow_root_env
  }

  # --- Manifest directory ---------------------------------------------------
  manifest_dir_env <- Sys.getenv("PIPTM_MANIFEST_DIR", unset = "")
  if (!nzchar(manifest_dir_env)) {
    # Dev / testing mode — manifests loaded on demand via set_manifest_dir()
    return(invisible(NULL))
  }

  tryCatch(
    .load_manifests(manifest_dir_env),
    error = function(e) {
      packageStartupMessage(
        "[piptm] Failed to load manifests from PIPTM_MANIFEST_DIR: ",
        conditionMessage(e),
        "\n  Call piptm::set_manifest_dir() to retry after fixing the path."
      )
    }
  )

  invisible(NULL)
}
