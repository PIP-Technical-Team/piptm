# Package environment and lifecycle hooks
#
# .piptm_env: private package environment used to cache manifests and
# configuration at load time. Never exported — access via accessor functions
# in manifest.R.
#
# Configuration is driven by three environment variables that each team member
# sets once in their ~/.Renviron (run usethis::edit_r_environ() to open it):
#
#   PIPTM_ARROW_ROOT=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/arrow
#   PIPTM_MANIFEST_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/manifests
#   PIPTM_REGISTRY_DIR=Y:/PIP_ingestion_pipeline_v2/pip_repository/tm_data/registry
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

  # --- Package options -------------------------------------------------------
  # Defines the curated allowlist of optional breakdown dimensions admitted
  # into the variable registry and Arrow schema. This is the single place a
  # piptm maintainer edits to add or remove optional variables.
  #
  # The "only set if not already set" guard means a developer can temporarily
  # override this at runtime via options(piptm.optional_vars = c(...)) without
  # touching package source. Setting it to NULL at runtime admits no optional
  # variables (required fields only).
  op_piptm <- list(
    piptm.optional_vars = c(
      "gender", "area", 
      "educat4", "educat5", "educat7", 
      "age", "age_group", "age_group2",
      "hsize", "hsize_group",
      "wquintile",
      "imp_wat_rec", "imp_san_rec", "electricity",
      "lstatus", 
      "empstat", 
      "industrycat10", 
      "industrycat4",

      # Derived
      "female","male",
      "urban","rural",
      "youth", "prime_work_age", "elderly",
      "employed", "unemployed", "in_labor_force",
      "self_emp", "paid_emp", "inf_emp", "employer",
      "emp_agri","emp_indu","emp_serv","emp_others"
    )
  )
  toset <- !(names(op_piptm) %in% names(options()))
  if (any(toset)) options(op_piptm[toset])

  # --- Initialise package environment ----------------------------------------
  # All slots set to NULL so accessors never see an unbound name.
  .piptm_env$manifest_dir    <- NULL
  .piptm_env$arrow_root      <- NULL
  .piptm_env$manifests       <- list()
  .piptm_env$current_release <- NULL
  .piptm_env$registry_dir    <- NULL
  .piptm_env$registries      <- list()

  # --- Read paths from environment variables ---------------------------------
  # Each team member sets these in their ~/.Renviron. When unset (CI, new
  # developer), slots stay NULL and the package starts in dev / testing mode.
  arrow_root_opt   <- Sys.getenv("PIPTM_ARROW_ROOT",   unset = "")
  manifest_dir_opt <- Sys.getenv("PIPTM_MANIFEST_DIR", unset = "")
  registry_dir_opt <- Sys.getenv("PIPTM_REGISTRY_DIR", unset = "")
  data_dir_opt     <- Sys.getenv("PIPTM_DATA_DIR",     unset = "")

  missing_required <- c()
  if (!nzchar(arrow_root_opt))   missing_required <- c(missing_required, "PIPTM_ARROW_ROOT")
  if (!nzchar(manifest_dir_opt)) missing_required <- c(missing_required, "PIPTM_MANIFEST_DIR")
  if (!nzchar(registry_dir_opt)) missing_required <- c(missing_required, "PIPTM_REGISTRY_DIR")
  if (length(missing_required) > 0) {
    packageStartupMessage(
      "[piptm] Missing environment variables: ",
      paste(missing_required, collapse = ", "),
      ". Copy .Renviron.example or run usethis::edit_r_environ() to configure paths."
    )
  }
  if (!nzchar(data_dir_opt)) {
    packageStartupMessage(
      "[piptm] PIPTM_DATA_DIR is not set — integration tests that require survey fixtures will be skipped."
    )
  }

  # --- Arrow root ------------------------------------------------------------
  # Only assign if the resolved path actually exists on this machine.
  if (nzchar(arrow_root_opt) && dir.exists(arrow_root_opt)) {
    .piptm_env$arrow_root <- arrow_root_opt
  }

  # --- collapse threading ----------------------------------------------------
  # Benchmark (benchmarks/orchestration-strategy.R, 2026-04-28) showed that
  # nthreads = 4 is optimal for typical 15-survey workloads (0.12s vs 0.13s
  # for nthreads = 1). We cap at the physical core count so we never
  # over-subscribe on a 2-core CI runner. na.rm = TRUE guards against
  # detectCores() returning NA on virtualised / CRAN platforms. Falls back
  # silently when the package was compiled without OpenMP support.
  tryCatch({
    n <- min(4L, max(1L, parallel::detectCores(logical = FALSE), na.rm = TRUE))
    collapse::set_collapse(nthreads = n)
  },
  error = function(e) NULL)

  # --- Manifest directory ----------------------------------------------------
  if (nzchar(manifest_dir_opt) && dir.exists(manifest_dir_opt)) {
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
  }

  # --- Variable registry -----------------------------------------------------
  # Only load when configured and present. Missing directory is not fatal.
  if (nzchar(registry_dir_opt) && dir.exists(registry_dir_opt)) {
    .piptm_env$registry_dir <- registry_dir_opt

    tryCatch(
      piptm_load_registry(registry_dir_opt),
      error = function(e) {
        packageStartupMessage(
          "[piptm] Failed to load variable registries from: ", registry_dir_opt,
          "\n  ", conditionMessage(e)
        )
      }
    )
  }

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
