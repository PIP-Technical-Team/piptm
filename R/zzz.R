# Package environment and lifecycle hooks
#
# .piptm_env: private package environment used to cache manifests and
# configuration at load time. Never exported — access via accessor functions
# in manifest.R.
#
# Path constants are hardcoded in .onLoad and assigned directly to .piptm_env:
#
#   arrow_root   = "Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow"
#   manifest_dir = "Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests"
#
# To point to a different location at runtime, call set_arrow_root() or
# set_manifest_dir() after loading the package.
#
# The current release is constructed from pipfun::get_wrk_release():
#   release_id <- paste(wrk$release, wrk$identity, sep = "_")
#   e.g. "20260206_TEST"

.piptm_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {

  # Initialise all slots to NULL so accessors never see an unbound name
  .piptm_env$manifest_dir    <- NULL
  .piptm_env$arrow_root      <- NULL
  .piptm_env$manifests       <- list()
  .piptm_env$current_release <- NULL

  # --- Canonical path constants ---------------------------------------------
  arrow_root_opt    <- "Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow"
  manifest_dir_opt  <- "Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests"

  # --- Arrow root -----------------------------------------------------------
  .piptm_env$arrow_root <- arrow_root_opt

  # --- Manifest directory ---------------------------------------------------
  if (!nzchar(manifest_dir_opt)) {
    # Dev / testing mode — manifests loaded on demand via set_manifest_dir()
    return(invisible(NULL))
  }

  tryCatch(
    .load_manifests(manifest_dir_opt),
    error = function(e) {
      packageStartupMessage(
        "[piptm] Failed to load manifests from pipdata.manifest_root: ",
        conditionMessage(e),
        "\n  Call piptm::set_manifest_dir() to retry after fixing the path."
      )
    }
  )

  # --- Current release (from pipfun::get_wrk_release()) ---------------------
  # Overrides whatever .load_manifests() resolved from current_release.json,
  # ensuring the session always tracks the user's active working release.
  tryCatch({
    wrk        <- pipfun::get_wrk_release()
    release_id <- paste(wrk$release, wrk$identity, sep = "_")
    .piptm_env$current_release <- release_id
  },
  error = function(e) {
    packageStartupMessage(
      "[piptm] Could not resolve current release from pipfun::get_wrk_release(): ",
      conditionMessage(e),
      "\n  Falling back to manifest-derived current release."
    )
  })

  invisible(NULL)
}
