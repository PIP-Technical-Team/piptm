# Static AST Coverage Audit for User-Facing API Error Messages
#
# Plan: .cg-docs/plans/2026-11-08-user-facing-api-error-messages.md (Step 5)
#
# Enforces:
#   1. Every `piptm_abort()` call across `R/` and `inst/plumber/` explicitly
#      passes a non-NULL, non-empty `user_message` named argument.
#   2. Zero raw `cli_abort()` / `cli::cli_abort()` calls exist in
#      `inst/plumber/*.R`, `pip_lookup()`, `piptm_manifest()`, or
#      `resolve_release()`.
#   3. Purely internal computation/formatter/startup files are listed in an
#      explicit internal-file allowlist (protected at runtime by
#      `.PIPTM_GENERIC_USER_ERROR` / `.PIPTM_DOWNLOAD_PAYLOAD_USER_ERROR`).
#   4. In mixed files (`R/errors.R`, `R/table_maker.R`, `R/load_data.R`,
#      `R/measures.R`, `R/manifest.R`, `R/description_builder.R`), any
#      remaining raw `cli_abort()` call must match an explicit per-site
#      allowlist keyed by `(file, enclosing_function, message_snippet)`.

.is_nonempty_user_message_ast <- function(um_node) {
  if (is.null(um_node)) {
    return(FALSE)
  }
  if (is.character(um_node)) {
    return(length(um_node) == 1L && !is.na(um_node) && nzchar(trimws(um_node)))
  }
  TRUE
}

.collect_abort_calls <- function(expr,
                                 file_label,
                                 enclosing_fn = "<top-level>") {
  out <- list()

  walk <- function(node, current_fn) {
    if (!is.call(node)) return(invisible(NULL))

    head_txt <- paste(deparse(node[[1L]]), collapse = "")

    # Track top-level or nested `name <- function(...) ...` assignments
    if (head_txt %in% c("<-", "=") &&
        length(node) >= 3L &&
        is.symbol(node[[2L]]) &&
        is.call(node[[3L]]) &&
        identical(as.character(node[[3L]][[1L]]), "function")) {
      fn_name <- as.character(node[[2L]])
      walk(node[[3L]], fn_name)
      return(invisible(NULL))
    }

    if (head_txt %in% c("cli_abort", "cli::cli_abort", "piptm_abort", "piptm:::piptm_abort")) {
      arg_names <- names(node)
      um_node <- if (!is.null(arg_names) && "user_message" %in% arg_names) {
        node[["user_message"]]
      } else {
        NULL
      }
      has_user_msg <- .is_nonempty_user_message_ast(um_node)
      code_txt <- paste(deparse(node, width.cutoff = 500L), collapse = " ")
      out[[length(out) + 1L]] <<- list(
        file             = file_label,
        fn               = current_fn,
        call_type        = if (grepl("piptm_abort$", head_txt)) "piptm_abort" else "cli_abort",
        has_user_message = has_user_msg,
        code             = code_txt
      )
    }

    for (i in seq_along(node)) {
      walk(node[[i]], current_fn)
    }
  }

  for (el in as.list(expr)) {
    walk(el, enclosing_fn)
  }

  out
}

# Purely internal/startup/formatter files protected by the runtime fallback
.INTERNAL_FILE_ALLOWLIST <- c(
  "R/api.R",
  "R/compute_inequality.R",
  "R/compute_measures.R",
  "R/compute_poverty.R",
  "R/compute_shares.R",
  "R/compute_summary_stats.R",
  "R/format_table_output.R",
  "R/registry.R",
  "R/schema.R",
  "R/table_flatten.R",
  "R/table_response.R"
)

# Per-site allowlist of internal programmer-contract / schema-corruption checks
# inside mixed files (`file`, `fn`, `snippet`).
.MIXED_FILE_SITE_ALLOWLIST <- list(
  list(file = "R/errors.R", fn = "piptm_abort", snippet = "user_message} must be"),
  list(file = "R/errors.R", fn = "piptm_abort", snippet = "class = unique(c(class"),
  list(file = "R/table_maker.R", fn = "table_maker", snippet = "pip_id} must be provided"),
  list(file = "R/table_params.R", fn = ".partition_surveys_by_dimensions", snippet = "Manifest contains duplicate entries"),
  list(file = "R/table_maker.R", fn = "compute_core", snippet = "missing expected metadata columns"),
  list(file = "R/table_maker.R", fn = "compute_core", snippet = "returned inconsistent metadata"),
  list(file = "R/table_maker.R", fn = "compute_core", snippet = "surveyid_year metadata must be numeric"),
  list(file = "R/table_maker.R", fn = "compute_core", snippet = "pop_share_threshold"),
  list(file = "R/load_data.R", fn = ".build_parquet_paths", snippet = "No Parquet files found"),
  list(file = "R/load_data.R", fn = ".build_parquet_paths", snippet = "Multiple Parquet files found"),
  list(file = "R/load_data.R", fn = "load_survey_microdata", snippet = "cols} must be NULL"),
  list(file = "R/load_data.R", fn = "load_survey_microdata", snippet = "Multiple manifest entries found"),
  list(file = "R/load_data.R", fn = "load_survey_microdata", snippet = "Arrow root is not configured"),
  list(file = "R/load_data.R", fn = "load_survey_microdata", snippet = "Arrow query returned 0 rows for"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "cols} must be NULL"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "filter_base} must be NULL"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "filter_base} must have non-empty"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "must include at least one value"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "has non-integer value"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "entries_dt} has 0 rows"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "Arrow root is not configured"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "Surveys have different full column names"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "Loaded unexpected survey"),
  list(file = "R/load_data.R", fn = "load_surveys", snippet = "Expected welfare column"),
  list(file = "R/measures.R", fn = ".classify_measures", snippet = "measures} must be a non-empty"),
  list(file = "R/measures.R", fn = ".validate_by", snippet = "by} must be"),
  list(file = "R/measures.R", fn = ".validate_by", snippet = "Could not derive valid dimensions"),
  list(file = "R/manifest.R", fn = ".load_manifests", snippet = "Manifest directory does not exist"),
  list(file = "R/manifest.R", fn = "set_arrow_root", snippet = "Arrow root directory does not exist")
)

.is_allowlisted_cli_abort <- function(rec) {
  if (rec$file %in% .INTERNAL_FILE_ALLOWLIST) {
    return(TRUE)
  }
  for (entry in .MIXED_FILE_SITE_ALLOWLIST) {
    if (identical(rec$file, entry$file) &&
        identical(rec$fn, entry$fn) &&
        grepl(entry$snippet, rec$code, fixed = TRUE)) {
      return(TRUE)
    }
  }
  FALSE
}

test_that("AST coverage audit: all piptm_abort() calls supply user_message and no unallowlisted cli_abort() calls exist", {
  pkg_root <- tryCatch(
    rprojroot::find_package_root_file(),
    error = function(e) normalizePath(testthat::test_path("..", ".."), mustWork = FALSE)
  )
  skip_if(!dir.exists(file.path(pkg_root, "R")), "Package source R/ directory not available")

  r_files <- sort(list.files(file.path(pkg_root, "R"), pattern = "\\.R$", full.names = TRUE))
  plumber_files <- sort(list.files(file.path(pkg_root, "inst", "plumber"), pattern = "\\.R$", full.names = TRUE))

  norm_root <- paste0(gsub("\\\\", "/", normalizePath(pkg_root, winslash = "/")), "/")
  all_records <- list()
  for (f in c(r_files, plumber_files)) {
    norm_f <- gsub("\\\\", "/", normalizePath(f, winslash = "/"))
    rel <- if (startsWith(norm_f, norm_root)) {
      substr(norm_f, nchar(norm_root) + 1L, nchar(norm_f))
    } else {
      norm_f
    }
    parsed <- parse(file = f, keep.source = TRUE, encoding = "UTF-8")
    all_records <- c(all_records, .collect_abort_calls(parsed, rel))
  }

  # 1. Every piptm_abort() call must include a non-NULL, non-empty user_message
  piptm_calls <- Filter(function(r) identical(r$call_type, "piptm_abort"), all_records)
  expect_gt(length(piptm_calls), 20L)
  missing_um <- Filter(function(r) !isTRUE(r$has_user_message), piptm_calls)
  expect_length(missing_um, 0L)

  # 2. Zero raw cli_abort() calls in inst/plumber/*.R, pip_lookup, piptm_manifest, or resolve_release
  cli_calls <- Filter(function(r) identical(r$call_type, "cli_abort"), all_records)
  forbidden_fns <- Filter(
    function(r) {
      startsWith(r$file, "inst/plumber/") ||
        r$fn %in% c("pip_lookup", "piptm_manifest", "resolve_release")
    },
    cli_calls
  )
  expect_length(forbidden_fns, 0L)

  # 3. Every remaining cli_abort() call must match the explicit allowlist
  unallowlisted <- Filter(function(r) !.is_allowlisted_cli_abort(r), cli_calls)
  if (length(unallowlisted) > 0L) {
    details <- vapply(
      unallowlisted,
      function(r) sprintf("%s (%s): %s", r$file, r$fn, r$code),
      character(1L)
    )
    fail(paste(
      c(
        "Found unallowlisted raw cli_abort() call(s) in user-facing files.",
        "Use piptm_abort(..., user_message = '...') instead:",
        details
      ),
      collapse = "\n"
    ))
  }
  expect_length(unallowlisted, 0L)
})

test_that("AST coverage audit helper flags unmigrated cli_abort() and piptm_abort() missing/NULL/blank user_message", {
  bad_expr <- parse(text = '
    table_maker <- function() {
      cli_abort("unmapped error in table_maker")
      piptm_abort("dev message without user_message")
      piptm_abort("dev message with NULL", user_message = NULL)
      piptm_abort("dev message with blank", user_message = "   ")
    }
  ')
  recs <- .collect_abort_calls(bad_expr, "R/table_maker.R")
  expect_length(recs, 4L)

  expect_identical(recs[[1L]]$call_type, "cli_abort")
  expect_false(.is_allowlisted_cli_abort(recs[[1L]]))

  expect_identical(recs[[2L]]$call_type, "piptm_abort")
  expect_false(recs[[2L]]$has_user_message)

  expect_identical(recs[[3L]]$call_type, "piptm_abort")
  expect_false(recs[[3L]]$has_user_message)

  expect_identical(recs[[4L]]$call_type, "piptm_abort")
  expect_false(recs[[4L]]$has_user_message)
})
