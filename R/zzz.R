# Package environment and lifecycle hooks
#
# .piptm_env: private package environment used to cache manifests and
# configuration at load time. Never exported — access via accessor functions
# in manifest.R.
#
# Configuration is driven by two environment variables that each team member
# sets once in their ~/.Renviron (run usethis::edit_r_environ() to open it):
#
#   PIPTM_ARROW_ROOT=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow
#   PIPTM_MANIFEST_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests
#
# When either variable is unset (e.g. CI, new developer, different drive
# letter), the corresponding slot stays NULL and the package starts in
# dev / testing mode. Call set_arrow_root() or set_manifest_dir() at runtime
# to configure the paths manually.
#
# The current release is set by pipfun::get_wrk_release() at load time,
# overriding whatever current_release.json resolves, so each user's session
# automatically tracks their own active working release.
#   release_id <- paste(wrk$release, wrk$identity, sep = "_")
#   e.g. "20260206_TEST"

.piptm_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {

  # Initialise all slots to NULL so accessors never see an unbound name
  .piptm_env$manifest_dir    <- NULL
  .piptm_env$arrow_root      <- NULL
  .piptm_env$manifests       <- list()
  .piptm_env$current_release <- NULL

  # --- Read paths from environment variables ---------------------------------
  # Each team member sets PIPTM_ARROW_ROOT and PIPTM_MANIFEST_DIR in their
  # ~/.Renviron. When unset (CI, new developer), the slots stay NULL and
  # the package starts in dev / testing mode — configure at runtime via
  # set_arrow_root() / set_manifest_dir().
  arrow_root_opt   <- Sys.getenv("PIPTM_ARROW_ROOT",   unset = "")
  manifest_dir_opt <- Sys.getenv("PIPTM_MANIFEST_DIR", unset = "")

  # --- Arrow root -----------------------------------------------------------
  # Only assign if the resolved path actually exists on this machine.
  if (nzchar(arrow_root_opt) && dir.exists(arrow_root_opt)) {
    .piptm_env$arrow_root <- arrow_root_opt
  }

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
