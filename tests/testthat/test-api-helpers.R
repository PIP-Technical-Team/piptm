# Tests for inst/plumber/helpers.R
#
# Plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md (Step 1)
#
# The helpers live in the plumber environment (sourced, not namespace-exported).
# We source the file directly so tests can call the helpers without a running
# plumber server.

helpers_path <- system.file("plumber", "helpers.R", package = "piptm")
if (!nzchar(helpers_path)) {
  # Dev / in-source testing: derive from package root
  helpers_path <- file.path(
    rprojroot::find_package_root_file(), "inst", "plumber", "helpers.R"
  )
}
source(helpers_path)

.vti <- function(...) {
  args <- list(...)
  if (is.null(args$analysis_var)) {
    args$analysis_var <- "welfare"
  }
  do.call(validate_table_input, args)
}

# ── api_response() ─────────────────────────────────────────────────────────────

test_that("api_response() returns the correct envelope shape", {
  payload <- list(a = 1L, b = "x")
  res     <- api_response(payload)

  expect_named(res, c("status", "data", "warnings", "errors", "meta"))
  expect_identical(res$status,   "success")
  expect_identical(res$data,     payload)
  expect_identical(res$warnings, character())
  expect_identical(res$errors,   character())
  expect_identical(res$meta,     list())
})

test_that("api_response() passes through warnings and meta", {
  res <- api_response(
    data     = 42L,
    warnings = c("w1", "w2"),
    meta     = list(release = "20260401_TEST", n_surveys = 3L)
  )

  expect_identical(res$warnings, c("w1", "w2"))
  expect_identical(res$meta$release, "20260401_TEST")
  expect_identical(res$meta$n_surveys, 3L)
})

# ── api_error() ────────────────────────────────────────────────────────────────

test_that("api_error() sets res$status and returns error envelope", {
  fake_res <- list(status = 200L)
  out      <- api_error(c("bad input"), 400L, fake_res)

  # Envelope shape
  expect_named(out, c("status", "data", "warnings", "errors", "meta"))
  expect_identical(out$status,   "error")
  expect_null(out$data)
  expect_identical(out$warnings, character())
  expect_identical(out$errors,   c("bad input"))
  expect_identical(out$meta,     list())
})

test_that("api_error() mutates res$status in the caller's environment", {
  fake_res        <- new.env(parent = emptyenv())
  fake_res$status <- 200L

  api_error("oops", 422L, fake_res)
  expect_identical(fake_res$status, 422L)
})

# ── resolve_release() ──────────────────────────────────────────────────────────

test_that("resolve_release(NULL) returns the current release", {
  cr  <- piptm::piptm_current_release()
  out <- resolve_release(NULL)
  expect_identical(out, cr)
})

test_that("resolve_release() with a known release returns it unchanged", {
  known <- names(piptm::piptm_manifests())
  skip_if(length(known) == 0L, "No manifests loaded — skipping")

  out <- resolve_release(known[[1L]])
  expect_identical(out, known[[1L]])
})

test_that("resolve_release() aborts on an unknown release", {
  expect_error(resolve_release("bogus_release_XYZ"), class = "rlang_error")
})

# ── validate_table_input() ─────────────────────────────────────────────────────

test_that("validate_table_input() passes with minimal valid inputs", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean"
  )
  expect_true(result$valid)
  expect_length(result$errors, 0L)
  expect_null(result$poverty_line)
})

test_that("validate_table_input() passes with poverty measure and valid poverty_line", {
  result <- .vti(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    analysis_var  = "pov_status",
    measures      = c("headcount", "mean"),
    poverty_line  = 2.15
  )
  expect_true(result$valid)
  expect_identical(result$poverty_line, 2.15)
})

test_that("validate_table_input() coerces character poverty_line to numeric", {
  result <- .vti(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    analysis_var  = "pov_status",
    measures      = "headcount",
    poverty_line  = "2.15"
  )
  expect_true(result$valid)
  expect_equal(result$poverty_line, 2.15)
})

test_that("validate_table_input() rejects more than 15 pip_ids", {
  ids    <- paste0("SUR_", 1:16, "_ABC_INC_ALL")
  result <- .vti(pip_id = ids, measures = "mean")

  expect_false(result$valid)
  expect_true(any(grepl("15", result$errors)))
})

# P1.9 — boundary: exactly 15 pip_ids must pass the length check
test_that("validate_table_input() accepts exactly 15 pip_ids (boundary)", {
  ids    <- paste0("ARM_2012_ILCS_CON_ALL_", seq_len(15L))
  result <- .vti(pip_id = ids, measures = "mean")

  # Length check passes; other errors (e.g. format) may exist but not length
  expect_false(any(grepl("15", result$errors)))
})

# P1.10 — pip_id allowlist: path-traversal and shell-injection attempts are rejected
test_that("validate_table_input() rejects pip_ids that fail the allowlist pattern", {
  bad_ids <- c(
    "../etc/passwd",          # path traversal
    "ARM_2012_ILCS; rm -rf",  # shell injection
    "ARM 2012 ILCS CON ALL",  # spaces
    ""                        # empty string
  )
  for (bad in bad_ids) {
    result <- .vti(pip_id = bad, measures = "mean")
    expect_false(result$valid, info = paste("Should reject:", bad))
    expect_true(
      any(grepl("pip_id", result$errors, ignore.case = TRUE)),
      info = paste("Error should mention pip_id for:", bad)
    )
  }
})

test_that("validate_table_input() rejects unknown measure names", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = c("mean", "nonexistent_measure")
  )
  expect_false(result$valid)
  expect_true(any(grepl("nonexistent_measure", result$errors)))
})

test_that("validate_table_input() rejects non-numeric poverty_line string", {
  result <- .vti(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    analysis_var  = "pov_status",
    measures      = "headcount",
    poverty_line  = "abc"
  )
  expect_false(result$valid)
  expect_true(any(grepl("poverty_line", result$errors, ignore.case = TRUE)))
})

test_that("validate_table_input() rejects non-positive poverty_line", {
  result <- .vti(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    analysis_var  = "pov_status",
    measures      = "headcount",
    poverty_line  = -1.0
  )
  expect_false(result$valid)
  expect_true(any(grepl("positive", result$errors)))
})

test_that("validate_table_input() rejects unknown dimension in a slot", {
  result <- .vti(
    pip_id      = "ARM_2012_ILCS_CON_ALL",
    measures    = "mean",
    column      = "gender",
    row         = "not_a_dim"
  )
  expect_false(result$valid)
  expect_true(any(grepl("not_a_dim", result$errors)))
})

test_that("validate_table_input() accepts valid slot dimensions", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = "gender",
    row      = "area"
  )
  expect_true(result$valid)
  expect_identical(result$by, c("gender", "area"))
})

test_that("validate_table_input() rejects NULL pip_id", {
  result <- .vti(pip_id = NULL, measures = "mean")
  expect_false(result$valid)
})

test_that("validate_table_input() rejects NULL measures", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = NULL
  )
  expect_false(result$valid)
})

# ── validate_table_input() — ppp param ────────────────────────────────────────

test_that("validate_table_input() accepts ppp = NULL (default)", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = NULL
  )
  expect_true(result$valid)
  expect_null(result$ppp)
})

test_that("validate_table_input() coerces character ppp to integer", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = "2017"
  )
  expect_true(result$valid)
  expect_identical(result$ppp, 2017L)
})

test_that("validate_table_input() accepts integer ppp", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = 2011L
  )
  expect_true(result$valid)
  expect_identical(result$ppp, 2011L)
})

test_that("validate_table_input() rejects non-numeric ppp string", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = "abc"
  )
  expect_false(result$valid)
  expect_true(any(grepl("ppp", result$errors, ignore.case = TRUE)))
})

test_that("validate_table_input() rejects negative ppp", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = -2017L
  )
  expect_false(result$valid)
  expect_true(any(grepl("positive", result$errors)))
})

test_that("validate_table_input() rejects ppp = 0", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = 0L
  )
  expect_false(result$valid)
  expect_true(any(grepl("positive", result$errors)))
})

test_that("validate_table_input() rejects ppp with length > 1", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = c(2011L, 2017L)
  )
  expect_false(result$valid)
  expect_true(any(grepl("scalar", result$errors)))
})

# P1.5 — NA input
test_that("validate_table_input() rejects NA ppp", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = NA
  )
  expect_false(result$valid)
  expect_true(any(grepl("ppp", result$errors, ignore.case = TRUE)))
  expect_null(result$ppp)
})

# P1.6 — empty character vector
test_that("validate_table_input() rejects character(0) ppp", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = character(0L)
  )
  expect_false(result$valid)
  expect_true(any(grepl("scalar", result$errors)))
  expect_null(result$ppp)
})

# P1.7 — error accumulation: invalid ppp + invalid pip_id
test_that("validate_table_input() accumulates errors from multiple params including ppp", {
  result <- .vti(
    pip_id   = NULL,
    measures = "mean",
    ppp      = "abc"
  )
  expect_false(result$valid)
  expect_true(length(result$errors) >= 2L)
  expect_null(result$ppp)
})

# P2.2 — decimal string: documents current (pre-P1.1-fix) behaviour.
# P1.1 digit-only guard: decimal strings must be rejected (not silently truncated).
test_that("validate_table_input() rejects decimal string ppp '2017.5'", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = "2017.5"
  )
  expect_false(result$valid)
  expect_null(result$ppp)
  expect_match(result$errors, "digits only", all = FALSE)
})

# P2.3 — return list shape contract
test_that("validate_table_input() return list has the expected field shape", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean"
  )
  expect_named(
    result,
    c("valid", "errors", "poverty_line", "ppp", "pop_share_threshold", "by", "slots"),
    ignore.order = FALSE
  )
})

# P1.1 digit-only guard: hex strings must be rejected (not silently accepted as 2047L).
test_that("validate_table_input() rejects hex string ppp '0x7FF'", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    ppp      = "0x7FF"
  )
  expect_false(result$valid)
  expect_null(result$ppp)
  expect_match(result$errors, "digits only", all = FALSE)
})

# ── validate_lookup_input() ────────────────────────────────────────────────────

test_that("validate_lookup_input() passes with valid equal-length inputs", {
  result <- validate_lookup_input(
    country_code = c("ARM", "BOL"),
    year         = c(2012L, 2000L),
    welfare_type = c("CON", "INC")
  )
  expect_true(result$valid)
  expect_length(result$errors, 0L)
})

test_that("validate_lookup_input() rejects mismatched vector lengths", {
  result <- validate_lookup_input(
    country_code = c("ARM", "BOL"),
    year         = 2012L,
    welfare_type = c("CON", "INC")
  )
  expect_false(result$valid)
  expect_true(any(grepl("same length", result$errors)))
})

test_that("validate_lookup_input() rejects invalid welfare_type values", {
  result <- validate_lookup_input(
    country_code = "ARM",
    year         = 2012L,
    welfare_type = "BOTH"
  )
  expect_false(result$valid)
  expect_true(any(grepl("BOTH", result$errors)))
})

test_that("validate_lookup_input() rejects non-integer-coercible year", {
  result <- validate_lookup_input(
    country_code = "ARM",
    year         = "twenty-twelve",
    welfare_type = "CON"
  )
  expect_false(result$valid)
  expect_true(any(grepl("integer", result$errors)))
})

test_that("validate_lookup_input() rejects NULL country_code", {
  result <- validate_lookup_input(
    country_code = NULL,
    year         = 2012L,
    welfare_type = "CON"
  )
  expect_false(result$valid)
})

# ── validate_table_input() — slot assembly ────────────────────────────────────

.slot_names <- c("column", "row", "superColumn", "superRow")

.expected_by <- function(slots) {
  filled <- Filter(Negate(is.null), slots)
  by <- unname(unlist(filled, use.names = FALSE))
  if (length(by) == 0L) NULL else by
}

test_that("validate_table_input() returns a fixed four-key slots list", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = "gender"
  )
  expect_identical(names(result$slots), .slot_names)
  expect_identical(result$slots$column, "gender")
  expect_null(result$slots$row)
  expect_null(result$slots$superColumn)
  expect_null(result$slots$superRow)
  expect_identical(result$by, .expected_by(result$slots))
})

test_that("validate_table_input() treats all-empty slots as a national request", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean"
  )
  expect_true(result$valid)
  expect_null(result$by)
  expect_identical(names(result$slots), .slot_names)
  expect_true(all(vapply(result$slots, is.null, logical(1))))
})

test_that("validate_table_input() rejects a repeated slot value", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = c("gender", "area")
  )
  expect_false(result$valid)
  expect_true(any(grepl("column", result$errors, ignore.case = TRUE)))
  expect_true(any(grepl("single variable", result$errors)))
})

test_that("validate_table_input() rejects the same variable in two slots", {
  result <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = "gender",
    row      = "gender"
  )
  expect_false(result$valid)
  expect_true(any(grepl("gender", result$errors)))
})

test_that("validate_table_input() rejects a gapped slot assignment", {
  gapped <- .vti(
    pip_id       = "ARM_2012_ILCS_CON_ALL",
    measures     = "mean",
    column       = "gender",
    superColumn  = "area"
  )
  expect_false(gapped$valid)
  expect_true(any(grepl("row", gapped$errors)))

  contiguous <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = "gender",
    row      = "area"
  )
  expect_true(contiguous$valid)
  expect_identical(contiguous$by, c("gender", "area"))
})

test_that("validate_table_input() does not describe a repeated slot as empty", {
  result <- .vti(
    pip_id       = "ARM_2012_ILCS_CON_ALL",
    measures     = "mean",
    column       = c("gender", "area"),
    superColumn  = "educat4"
  )
  expect_false(result$valid)
  expect_true(any(grepl("single variable", result$errors)))
  expect_false(any(grepl("`column` is empty", result$errors, ignore.case = TRUE)))
})

test_that("validate_table_input() trims slot values and treats whitespace as unfilled", {
  padded <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = " gender "
  )
  expect_true(padded$valid)
  expect_identical(padded$slots$column, "gender")
  expect_identical(padded$by, "gender")

  blank <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = "  "
  )
  expect_true(blank$valid)
  expect_null(blank$by)
  expect_null(blank$slots$column)
})

test_that("validate_table_input() preserves exact slot order for 3 and 4 slots and checks non-adjacent gaps/duplicates", {
  three_slots <- .vti(
    pip_id      = "ARM_2012_ILCS_CON_ALL",
    measures    = "mean",
    column      = "wquintile",
    row         = "gender",
    superColumn = "educat4"
  )
  expect_true(three_slots$valid)
  expect_identical(three_slots$by, c("wquintile", "gender", "educat4"))

  four_slots <- .vti(
    pip_id      = "ARM_2012_ILCS_CON_ALL",
    measures    = "mean",
    column      = "wquintile",
    row         = "gender",
    superColumn = "educat4",
    superRow    = "area"
  )
  expect_true(four_slots$valid)
  expect_identical(four_slots$by, c("wquintile", "gender", "educat4", "area"))

  gap_supercol <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = "gender",
    row      = "area",
    superRow = "educat4"
  )
  expect_false(gap_supercol$valid)
  expect_true(any(grepl("`superColumn` is empty", gap_supercol$errors)))

  dup_nonadj <- .vti(
    pip_id      = "ARM_2012_ILCS_CON_ALL",
    measures    = "mean",
    column      = "gender",
    row         = "area",
    superColumn = "gender"
  )
  expect_false(dup_nonadj$valid)
  expect_true(any(grepl("column, superColumn", dup_nonadj$errors)))
})

test_that("validate_table_input() accepts flat arrays and rejects objects or nested arrays", {
  nested_arr2 <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = list(c("gender", "area"))
  )
  expect_false(nested_arr2$valid)
  expect_true(any(grepl("single variable", nested_arr2$errors)))

  json_object <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = list(value = "gender")
  )
  expect_false(json_object$valid)
  expect_true(any(grepl("character string or flat array", json_object$errors)))

  nested_list <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = list(list("gender"))
  )
  expect_false(nested_list$valid)
  expect_true(any(grepl("character string or flat array", nested_list$errors)))

  matrix_wrapped <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = matrix("gender", nrow = 1L, ncol = 1L)
  )
  expect_false(matrix_wrapped$valid)
  expect_true(any(grepl("character string or flat array", matrix_wrapped$errors)))

  nested_arr_dim <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = list(matrix("gender", nrow = 1L, ncol = 1L))
  )
  expect_false(nested_arr_dim$valid)
  expect_true(any(grepl("character string or flat array", nested_arr_dim$errors)))

  # Exercise the actual jsonlite-decoded shapes rather than hand-built R lists,
  # since jsonlite's simplification can differ subtly from manual construction.
  decoded_empty_object <- jsonlite::fromJSON('{"column": {}}', simplifyVector = TRUE)
  empty_object <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = decoded_empty_object$column
  )
  expect_false(empty_object$valid)
  expect_true(any(grepl("character string or flat array", empty_object$errors)))

  decoded_blank_key_object <- jsonlite::fromJSON('{"column": {"": "gender"}}', simplifyVector = TRUE)
  blank_key_object <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = decoded_blank_key_object$column
  )
  expect_false(blank_key_object$valid)
  expect_true(any(grepl("character string or flat array", blank_key_object$errors)))

  decoded_nested_array <- jsonlite::fromJSON('{"column": [["gender"]]}', simplifyVector = TRUE)
  decoded_nested <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = decoded_nested_array$column
  )
  expect_false(decoded_nested$valid)
  expect_true(any(grepl("character string or flat array", decoded_nested$errors)))

  decoded_empty_array <- jsonlite::fromJSON('{"column": []}', simplifyVector = TRUE)
  empty_array <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = decoded_empty_array$column
  )
  expect_true(empty_array$valid)
  expect_null(empty_array$by)

  empty_list <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = list(character(0))
  )
  expect_true(empty_list$valid)
  expect_null(empty_list$by)

  non_char <- .vti(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    column   = 123L
  )
  expect_false(non_char$valid)
  expect_true(any(grepl("character string", non_char$errors)))

  dup_analysis <- .vti(
    analysis_var = "pov_status",
    pip_id       = "ARM_2012_ILCS_CON_ALL",
    measures     = "headcount",
    poverty_line = 2.15,
    column       = "pov_status"
  )
  expect_false(dup_analysis$valid)
  expect_true(any(grepl("analysis_var.*cannot also be used", dup_analysis$errors)))
})

# ── capture_with_warnings() ────────────────────────────────────────────────────

test_that("capture_with_warnings() returns result and empty warnings on clean expr", {
  out <- capture_with_warnings(1L + 1L)
  expect_identical(out$result, 2L)
  expect_identical(out$warnings, character())
  expect_null(out$error)
})

test_that("capture_with_warnings() collects base R warnings", {
  out <- capture_with_warnings({
    warning("test warning 1")
    warning("test warning 2")
    42L
  })
  expect_identical(out$result, 42L)
  expect_length(out$warnings, 2L)
  expect_true(any(grepl("test warning 1", out$warnings)))
  expect_null(out$error)
})

test_that("capture_with_warnings() collects cli warnings", {
  out <- capture_with_warnings({
    cli::cli_warn("cli warning here")
    "done"
  })
  expect_identical(out$result, "done")
  expect_true(any(grepl("cli warning here", out$warnings)))
  expect_null(out$error)
})

test_that("capture_with_warnings() catches stop() errors", {
  out <- capture_with_warnings(stop("something broke"))
  expect_null(out$result)
  expect_true(grepl("something broke", out$error))
})

test_that("capture_with_warnings() catches cli_abort() errors", {
  out <- capture_with_warnings(cli::cli_abort("abort message"))
  expect_null(out$result)
  expect_true(grepl("abort message", out$error))
})

test_that("capture_with_warnings() catches resolve_release() abort for bogus release", {
  out <- capture_with_warnings(resolve_release("this_is_not_a_real_release"))
  expect_null(out$result)
  expect_false(is.null(out$error))
  expect_true(nzchar(out$error))
})
