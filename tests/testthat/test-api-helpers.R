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
  result <- validate_table_input(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean"
  )
  expect_true(result$valid)
  expect_length(result$errors, 0L)
  expect_null(result$poverty_lines)
})

test_that("validate_table_input() passes with poverty measures and valid poverty_lines", {
  result <- validate_table_input(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    measures      = c("headcount", "mean"),
    poverty_lines = c(2.15, 3.65)
  )
  expect_true(result$valid)
  expect_identical(result$poverty_lines, c(2.15, 3.65))
})

test_that("validate_table_input() coerces character poverty_lines to numeric", {
  result <- validate_table_input(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    measures      = "headcount",
    poverty_lines = c("2.15", "3.65")
  )
  expect_true(result$valid)
  expect_equal(result$poverty_lines, c(2.15, 3.65))
})

test_that("validate_table_input() rejects more than 15 pip_ids", {
  ids    <- paste0("SUR_", 1:16, "_ABC_INC_ALL")
  result <- validate_table_input(pip_id = ids, measures = "mean")

  expect_false(result$valid)
  expect_true(any(grepl("15", result$errors)))
})

# P1.9 — boundary: exactly 15 pip_ids must pass the length check
test_that("validate_table_input() accepts exactly 15 pip_ids (boundary)", {
  ids    <- paste0("ARM_2012_ILCS_CON_ALL_", seq_len(15L))
  result <- validate_table_input(pip_id = ids, measures = "mean")

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
    result <- validate_table_input(pip_id = bad, measures = "mean")
    expect_false(result$valid, info = paste("Should reject:", bad))
    expect_true(
      any(grepl("pip_id", result$errors, ignore.case = TRUE)),
      info = paste("Error should mention pip_id for:", bad)
    )
  }
})

test_that("validate_table_input() rejects unknown measure names", {
  result <- validate_table_input(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = c("mean", "nonexistent_measure")
  )
  expect_false(result$valid)
  expect_true(any(grepl("nonexistent_measure", result$errors)))
})

test_that("validate_table_input() rejects non-numeric poverty_lines string", {
  result <- validate_table_input(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    measures      = "headcount",
    poverty_lines = "abc"
  )
  expect_false(result$valid)
  expect_true(any(grepl("abc", result$errors)))
})

test_that("validate_table_input() rejects non-positive poverty_lines", {
  result <- validate_table_input(
    pip_id        = "ARM_2012_ILCS_CON_ALL",
    measures      = "headcount",
    poverty_lines = c(2.15, -1.0)
  )
  expect_false(result$valid)
  expect_true(any(grepl("positive", result$errors)))
})

test_that("validate_table_input() rejects unknown dimension in `by`", {
  result <- validate_table_input(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    by       = c("gender", "not_a_dim")
  )
  expect_false(result$valid)
  expect_true(any(grepl("not_a_dim", result$errors)))
})

test_that("validate_table_input() accepts valid `by` dimensions", {
  result <- validate_table_input(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = "mean",
    by       = c("gender", "area")
  )
  expect_true(result$valid)
})

test_that("validate_table_input() rejects NULL pip_id", {
  result <- validate_table_input(pip_id = NULL, measures = "mean")
  expect_false(result$valid)
})

test_that("validate_table_input() rejects NULL measures", {
  result <- validate_table_input(
    pip_id   = "ARM_2012_ILCS_CON_ALL",
    measures = NULL
  )
  expect_false(result$valid)
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
