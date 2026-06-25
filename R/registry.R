# Variable registry build/load/access helpers
#
# The variable registry is a release-specific metadata object that merges:
#  1) pipdata recode spec (automated type + mapping metadata)
#  2) UI YAML specification (labels, roles, stat groups)
#
# Only variables listed in the `piptm.optional_vars` R option (plus the
# hardcoded special-case variables: welfare, pov_status, weight) are admitted
# into the registry. The option is initialised in .onLoad() and can be
# overridden at runtime via options(piptm.optional_vars = c(...)).

#' @importFrom cli cli_abort cli_warn cli_inform
NULL

.SPECIAL_CASE_VARNAMES <- c("welfare", "pov_status", "weight")


#' Convert a named mapping list to code-label category entries
#'
#' @param mapping Named list where names are codes and values are labels.
#'
#' @return `NULL` when mapping is `NULL` or empty; otherwise a list of
#'   lists with `code` and `label` character fields.
#' @keywords internal
.mapping_to_categories <- function(mapping) {
  if (is.null(mapping) || length(mapping) == 0L) {
    return(NULL)
  }

  codes <- names(mapping)
  if (is.null(codes) || length(codes) == 0L) {
    return(NULL)
  }

  lapply(codes, function(code) {
    list(
      code  = as.character(code),
      label = as.character(mapping[[code]])
    )
  })
}


#' Build one variable-registry entry
#'
#' @param varname Character scalar variable name.
#' @param spec_var List entry from `tm_variable_spec.yaml`.
#' @param pip_vars Full `pip_dict$variables` list.
#'
#' @return A named list registry entry or `NULL` when a non-special variable
#'   is missing in both pipdata and inline YAML categories.
#' @keywords internal
.build_registry_entry <- function(varname, spec_var, pip_vars) {
  stat_groups <- spec_var$stat_groups
  if (is.null(stat_groups)) {
    stat_groups <- character(0L)
  } else {
    stat_groups <- as.character(unlist(stat_groups, use.names = FALSE))
  }

  if (varname %in% .SPECIAL_CASE_VARNAMES) {
    tm_type <- switch(
      varname,
      welfare    = "welfare",
      pov_status = "poverty",
      weight     = "continuous"
    )

    n_categories <- if (identical(varname, "pov_status")) 2L else NULL

    return(list(
      varname      = varname,
      ui_label     = as.character(spec_var$ui_label),
      tm_type      = tm_type,
      roles        = as.character(unlist(spec_var$roles, use.names = FALSE)),
      stat_groups  = stat_groups,
      n_categories = n_categories,
      categories   = NULL
    ))
  }

  inline_categories <- spec_var$categories
  if (!is.null(inline_categories) && length(inline_categories) > 0L) {
    categories <- lapply(inline_categories, function(entry) {
      list(
        code  = as.character(entry$code),
        label = as.character(entry$label)
      )
    })

    return(list(
      varname      = varname,
      ui_label     = as.character(spec_var$ui_label),
      tm_type      = "categorical",
      roles        = as.character(unlist(spec_var$roles, use.names = FALSE)),
      stat_groups  = stat_groups,
      n_categories = as.integer(length(categories)),
      categories   = categories
    ))
  }

  pip_var <- pip_vars[[varname]]
  if (is.null(pip_var)) {
    cli::cli_warn(
      c(
        "Variable {.val {varname}} is in tm_variable_spec.yaml but absent in pipdata recode spec.",
        "i" = "Skipping this variable in registry build."
      )
    )
    return(NULL)
  }

  pip_type    <- as.character(pip_var$type)
  recode_type <- as.character(pip_var$recode_type)

  tm_type <- NULL
  if (identical(pip_type, "numeric")) {
    tm_type <- "continuous"
  } else if (identical(pip_type, "factor")) {
    if (identical(recode_type, "binary_map")) {
      tm_type <- "binary"
    } else {
      tm_type <- "categorical"
    }
  } else {
    cli::cli_abort(
      c(
        "Unknown pipdata type {.val {pip_type}} for variable {.val {varname}}.",
        "i" = "Expected one of {.val {c('numeric', 'factor')}}."
      )
    )
  }

  categories   <- NULL
  n_categories <- NULL
  if (!identical(tm_type, "continuous")) {
    categories <- .mapping_to_categories(pip_var$mapping)
    if (!is.null(categories)) {
      n_categories <- as.integer(length(categories))
    }
  }

  list(
    varname      = varname,
    ui_label     = as.character(spec_var$ui_label),
    tm_type      = tm_type,
    roles        = as.character(unlist(spec_var$roles, use.names = FALSE)),
    stat_groups  = stat_groups,
    n_categories = n_categories,
    categories   = categories
  )
}


#' Build and write a release-specific variable registry JSON
#'
#' Merges the UI YAML spec (`tm_variable_spec.yaml`) with the pipdata recode
#' spec to produce a release-specific registry. Only variables listed in the
#' `piptm.optional_vars` R option are admitted as optional variables.
#' Special-case variables (`welfare`, `pov_status`, `weight`) are always
#' included regardless of the option.
#'
#' When `piptm.optional_vars` is `NULL`, the registry will contain only the
#' special-case variables and a warning is emitted. Variables in the option
#' that have no YAML entry are warned about and skipped. Variables in the YAML
#' that are not in the option are silently excluded (informed, not warned).
#'
#' @param release Character scalar release identifier.
#' @param registry_dir Character scalar directory where `<release>.json` is
#'   written. Defaults to `.piptm_env$registry_dir`.
#' @param verbose Logical; passed to `pipload::pip_read()`.
#'
#' @return Invisibly returns the in-memory registry list.
#' @export
build_variable_registry <- function(release, registry_dir = NULL, verbose = TRUE) {

  # --- Argument validation --------------------------------------------------
  if (!is.character(release) || length(release) != 1L || is.na(release) || !nzchar(release)) {
    cli::cli_abort("{.arg release} must be a non-empty character scalar.")
  }

  if (is.null(registry_dir)) {
    registry_dir <- .piptm_env$registry_dir
  }

  if (is.null(registry_dir) || !nzchar(registry_dir)) {
    cli::cli_abort(
      c(
        "Registry directory is not configured.",
        "i" = "Pass {.arg registry_dir} explicitly or set {.envvar PIPTM_REGISTRY_DIR}."
      )
    )
  }

  if (!dir.exists(registry_dir)) {
    dir.create(registry_dir, recursive = TRUE, showWarnings = FALSE)
  }

  if (!requireNamespace("yaml", quietly = TRUE)) {
    cli::cli_abort(
      c(
        "Package {.pkg yaml} is required to build the variable registry.",
        "i" = "Install it with {.code install.packages('yaml')}."
      )
    )
  }

  # --- Load YAML spec -------------------------------------------------------
  spec_path <- system.file("extdata", "tm_variable_spec.yaml", package = "piptm")
  if (!nzchar(spec_path) || !file.exists(spec_path)) {
    cli::cli_abort("Could not find {.file inst/extdata/tm_variable_spec.yaml}.")
  }

  spec      <- yaml::read_yaml(spec_path)
  spec_vars <- spec$variables
  if (is.null(spec_vars) || !length(spec_vars)) {
    cli::cli_abort("UI spec has no {.field variables} entries.")
  }

  # --- Load pipdata recode spec ---------------------------------------------
  pip_dict <- pipload::pip_read(
    id      = "recode_spec",
    format  = "qs2",
    alias   = "pip_inv",
    verbose = verbose
  )

  pip_vars <- pip_dict$variables
  if (is.null(pip_vars)) pip_vars <- list()

  # --- Read allowlist option ------------------------------------------------
  # Special-case vars are always processed regardless of the option.
  # Optional vars are admitted only when listed in piptm.optional_vars.
  # When the option is NULL, no optional vars are admitted.
  optional_vars <- getOption("piptm.optional_vars")

  if (is.null(optional_vars)) {
    cli::cli_warn(
      c(
        "!" = "Option {.code piptm.optional_vars} is NULL.",
        "i" = "Registry will contain only special-case variables (welfare, pov_status, weight).",
        "i" = "Set the option to admit optional breakdown dimensions."
      )
    )
    admitted_optional <- character(0L)
  } else {
    admitted_optional <- as.character(optional_vars)

    # Warn for vars in the allowlist that have no YAML entry
    missing_from_yaml <- setdiff(admitted_optional, names(spec_vars))
    if (length(missing_from_yaml) > 0L) {
      cli::cli_warn(
        c(
          "Variable(s) in {.code piptm.optional_vars} have no entry in tm_variable_spec.yaml:",
          setNames(missing_from_yaml, rep("!", length(missing_from_yaml))),
          "i" = "These variables will be skipped."
        )
      )
      admitted_optional <- setdiff(admitted_optional, missing_from_yaml)
    }

    # Inform about YAML vars present but excluded by the allowlist
    yaml_non_special  <- setdiff(names(spec_vars), .SPECIAL_CASE_VARNAMES)
    excluded_from_opt <- setdiff(yaml_non_special, admitted_optional)
    if (length(excluded_from_opt) > 0L) {
      cli::cli_inform(
        c(
          "i" = "{length(excluded_from_opt)} YAML variable(s) excluded by {.code piptm.optional_vars}:",
          setNames(excluded_from_opt, rep("*", length(excluded_from_opt)))
        )
      )
    }
  }

  # --- Build registry: special-case vars + admitted optional vars -----------
  varnames_to_build <- c(.SPECIAL_CASE_VARNAMES, admitted_optional)
  # Keep only those actually present in spec_vars
  varnames_to_build <- intersect(varnames_to_build, names(spec_vars))

  registry        <- lapply(varnames_to_build, function(varname) {
    .build_registry_entry(varname, spec_vars[[varname]], pip_vars)
  })
  names(registry) <- varnames_to_build
  registry        <- Filter(Negate(is.null), registry)

  # --- Write JSON -----------------------------------------------------------
  out_path <- file.path(registry_dir, paste0(release, ".json"))
  out_json <- jsonlite::toJSON(
    registry,
    auto_unbox = FALSE,
    null       = "null",
    pretty     = TRUE
  )
  writeLines(out_json, con = out_path, useBytes = TRUE)

  cli::cli_inform("Wrote variable registry to {.file {out_path}}.")
  invisible(registry)
}


#' Load all variable registry JSON files into package environment
#'
#' @param registry_dir Character scalar path to registry directory.
#'
#' @return Invisibly `NULL`.
#' @keywords internal
piptm_load_registry <- function(registry_dir) {
  .piptm_env$registries <- list()

  if (!is.character(registry_dir) || length(registry_dir) != 1L || is.na(registry_dir) || !nzchar(registry_dir)) {
    return(invisible(NULL))
  }

  if (!dir.exists(registry_dir)) {
    packageStartupMessage(
      "[piptm] Variable registry directory not found: ",
      registry_dir
    )
    return(invisible(NULL))
  }

  files <- list.files(
    registry_dir,
    pattern   = "\\.json$",
    full.names = TRUE
  )

  for (file in files) {
    release_id <- tools::file_path_sans_ext(basename(file))

    parsed <- tryCatch(
      jsonlite::read_json(file, simplifyVector = FALSE),
      error = function(e) {
        packageStartupMessage(
          "[piptm] Failed to parse variable registry file ",
          file, ": ", conditionMessage(e)
        )
        NULL
      }
    )

    if (is.null(parsed)) next

    .piptm_env$registries[[release_id]] <- parsed
  }

  invisible(NULL)
}


#' Return the loaded variable registry for a release
#'
#' @param release Optional release ID; defaults to `piptm_current_release()`.
#'
#' @return Named list variable registry for `release`.
#' @export
piptm_variable_registry <- function(release = NULL) {
  if (is.null(release)) {
    release <- piptm_current_release()
  }

  if (is.null(release) || !nzchar(release)) {
    cli::cli_abort(
      c(
        "No current release is set.",
        "i" = "Pass {.arg release} explicitly or load manifests first."
      )
    )
  }

  registries <- .piptm_env$registries
  if (is.null(registries)) registries <- list()

  if (!release %in% names(registries)) {
    cli::cli_abort(
      c(
        "No variable registry found for release {.val {release}}.",
        "i" = "Available releases: {.val {sort(names(registries))}}"
      )
    )
  }

  registries[[release]]
}


#' List analysis variables for the UI (Decision 2)
#'
#' Returns registry entries with role `analysis_var` mapped to the API shape
#' expected by the front-end: `varname`, `label`, `type`, `poverty_line_slider`,
#' and `stat_groups`.
#'
#' @param release Optional release ID; defaults to `piptm_current_release()`.
#' @return A list of named lists suitable for JSON serialization.
#' @export
piptm_analysis_variables <- function(release = NULL) {
  reg <- piptm_variable_registry(release)

  out <- lapply(reg, function(entry) {
    if (!"analysis_var" %in% entry$roles) return(NULL)
    list(
      varname             = entry$varname,
      label               = entry$ui_label,
      type                = entry$tm_type,
      poverty_line_slider = identical(entry$tm_type, "poverty"),
      stat_groups         = if (is.null(entry$stat_groups)) character(0L) else entry$stat_groups
    )
  })

  Filter(Negate(is.null), out)
}


#' List filter categories for the UI (Decision 1)
#'
#' Returns registry entries with role `filter` mapped to the API shape
#' expected by the front-end: `varname`, `label`, `subcategories` (list of
#' `{code, label}` entries).
#'
#' @param release Optional release ID; defaults to `piptm_current_release()`.
#' @return A list of named lists suitable for JSON serialization.
#' @export
piptm_filter_categories <- function(release = NULL) {
  reg <- piptm_variable_registry(release)

  out <- lapply(reg, function(entry) {
    if (!"filter" %in% entry$roles) return(NULL)
    subcats <- entry$categories
    if (is.null(subcats)) subcats <- list()
    list(
      varname       = entry$varname,
      label         = entry$ui_label,
      subcategories = subcats
    )
  })

  Filter(Negate(is.null), out)
}


#' List layout covariates for the UI (Decision 3)
#'
#' Returns registry entries with role `covariate` mapped to the API shape
#' expected by the front-end: `varname`, `label`, `n_categories`,
#' `pov_status_mutex`, and `poverty_line_slider`.
#'
#' @param release Optional release ID; defaults to `piptm_current_release()`.
#' @return A list of named lists suitable for JSON serialization.
#' @export
piptm_layout_covariates <- function(release = NULL) {
  reg <- piptm_variable_registry(release)

  out <- lapply(reg, function(entry) {
    if (!"covariate" %in% entry$roles) return(NULL)
    list(
      varname             = entry$varname,
      label               = entry$ui_label,
      n_categories        = if (is.null(entry$n_categories)) NULL else entry$n_categories,
      pov_status_mutex    = identical(entry$varname, "pov_status"),
      poverty_line_slider = identical(entry$tm_type, "poverty")
    )
  })

  Filter(Negate(is.null), out)
}
