# Tests for manifest.R and zzz.R
# Plan: .cg-docs/plans/2026-04-03-manifest-generation-version-partition.md
#       (Step 4)
#
# Test coverage:
#   - .load_manifests(): parses fixture JSON into named list of data.tables
#   - piptm_manifest(): returns correct data.table, errors on missing release
#   - piptm_manifest() defaulting to current release
#   - set_manifest_dir(): re-scans on call, updates .piptm_env
#   - set_arrow_root(): updates .piptm_env$arrow_root
#   - Accessor functions: piptm_manifest_dir, piptm_arrow_root,
#     piptm_manifests, piptm_current_release

library(data.table)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#' Write a minimal fixture manifest JSON to a temp directory
#'
#' @param dir          Directory in which to write.
#' @param release      Release ID string (e.g. "20260206").
#' @param current      Logical. Write this as current_release.json too?
#' @param entries      List of entry lists. NULL uses a built-in default.
#'
#' @return Path to the written manifest file.
write_fixture_manifest <- function(dir,
                                   release  = "20260206",
                                   current  = FALSE,
                                   entries  = NULL) {
  if (is.null(entries)) {
    entries <- list(
      list(
        pip_id         = "COL_2010_ECH_INC_ALL",
        survey_id      = "COL_2010_ECH_V01_M_V02_A_GMD_ALL",
        country_code   = "COL",
        year           = 2010L,
        welfare_type   = "INC",
        version        = "v01_v02",
        survey_acronym = "ECH",
        module         = "ALL",
        dimensions     = c("gender", "area")
      )
    )
  }

  manifest <- list(
    release      = release,
    generated_at = "2026-04-03T17:00:00Z",
    entries      = entries
  )

  path <- file.path(dir, paste0("manifest_", release, ".json"))
  writeLines(
    jsonlite::toJSON(manifest, pretty = TRUE, auto_unbox = TRUE),
    con = path
  )

  if (current) {
    pointer <- list(
      current_release = release,
      updated_at      = "2026-04-03T17:00:00Z"
    )
    writeLines(
      jsonlite::toJSON(pointer, pretty = TRUE, auto_unbox = TRUE),
      con = file.path(dir, "current_release.json")
    )
  }

  path
}

# ---------------------------------------------------------------------------
# .load_manifests()
# ---------------------------------------------------------------------------

test_that(".load_manifests() parses a valid manifest into data.table in env", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)

  piptm:::.load_manifests(tmp)

  manifests <- piptm::piptm_manifests()
  expect_true("20260206" %in% names(manifests))

  dt <- manifests[["20260206"]]
  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 1L)
  expect_named(dt,
    c("pip_id", "survey_id", "country_code", "year", "welfare_type",
      "version", "survey_acronym", "module", "dimensions"),
    ignore.order = FALSE
  )
  expect_identical(dt$pip_id,        "COL_2010_ECH_INC_ALL")
  expect_identical(dt$country_code,  "COL")
  expect_identical(dt$year,          2010L)
  expect_identical(dt$welfare_type,  "INC")
  expect_identical(dt$version,       "v01_v02")
  expect_identical(dt$dimensions[[1L]], c("gender", "area"))
})

test_that(".load_manifests() loads multiple manifests correctly", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206")
  write_fixture_manifest(tmp, release = "20260315",
    entries = list(list(
      pip_id         = "BOL_2020_EH_CON_ALL",
      survey_id      = "BOL_2020_EH_V01_M_V04_A_GMD_ALL",
      country_code   = "BOL",
      year           = 2020L,
      welfare_type   = "CON",
      version        = "v01_v04",
      survey_acronym = "EH",
      module         = "ALL",
      dimensions     = character(0)
    ))
  )

  piptm:::.load_manifests(tmp)

  manifests <- piptm::piptm_manifests()
  expect_length(manifests, 2L)
  expect_true("20260206" %in% names(manifests))
  expect_true("20260315" %in% names(manifests))

  # BOL entry has empty dimensions
  bol_dt <- manifests[["20260315"]]
  expect_equal(length(bol_dt$dimensions[[1L]]), 0L)
})

test_that(".load_manifests() sets current_release from pointer file", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206")
  write_fixture_manifest(tmp, release = "20260315")

  # Write pointer pointing to the older release
  pointer <- list(current_release = "20260206", updated_at = "2026-04-01T00:00:00Z")
  writeLines(
    jsonlite::toJSON(pointer, auto_unbox = TRUE),
    file.path(tmp, "current_release.json")
  )

  piptm:::.load_manifests(tmp)

  expect_identical(piptm::piptm_current_release(), "20260206")
})

test_that(".load_manifests() falls back to latest release when no pointer file", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206")
  write_fixture_manifest(tmp, release = "20260315")
  # No current_release.json

  # Reset current_release to NULL before test to avoid state bleed
  env <- piptm:::.piptm_env
  old_cr <- env$current_release
  on.exit(env$current_release <- old_cr)
  env$current_release <- NULL

  piptm:::.load_manifests(tmp)

  # Should default to lexicographically latest
  expect_identical(piptm::piptm_current_release(), "20260315")
})

test_that(".load_manifests() errors on non-existent directory", {
  expect_error(
    piptm:::.load_manifests("/nonexistent/path"),
    regexp = "does not exist"
  )
})

test_that(".load_manifests() warns and skips unreadable JSON files", {
  tmp <- withr::local_tempdir()
  # Write a valid manifest
  write_fixture_manifest(tmp, release = "20260206")
  # Write a corrupted JSON
  writeLines("{ NOT VALID JSON +++", file.path(tmp, "manifest_broken.json"))

  expect_warning(
    piptm:::.load_manifests(tmp),
    regexp = "Could not parse"
  )

  manifests <- piptm::piptm_manifests()
  expect_true("20260206" %in% names(manifests))
  # The broken file key won't be there
  expect_false("broken" %in% names(manifests))
})

test_that(".load_manifests() handles manifest with no entries (empty array)", {
  tmp <- withr::local_tempdir()

  manifest <- list(
    release      = "20260101",
    generated_at = "2026-01-01T00:00:00Z",
    entries      = list()
  )
  writeLines(
    jsonlite::toJSON(manifest, pretty = TRUE, auto_unbox = TRUE),
    file.path(tmp, "manifest_20260101.json")
  )

  piptm:::.load_manifests(tmp)

  dt <- piptm::piptm_manifests()[["20260101"]]
  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 0L)
})

# ---------------------------------------------------------------------------
# piptm_manifest()
# ---------------------------------------------------------------------------

test_that("piptm_manifest() returns correct data.table for a named release", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)
  piptm:::.load_manifests(tmp)

  dt <- piptm::piptm_manifest("20260206")
  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 1L)
  expect_identical(dt$pip_id, "COL_2010_ECH_INC_ALL")
})

test_that("piptm_manifest() defaults to current release when release is NULL", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)
  piptm:::.load_manifests(tmp)

  dt <- piptm::piptm_manifest()  # should use current = "20260206"
  expect_s3_class(dt, "data.table")
  expect_identical(dt$pip_id, "COL_2010_ECH_INC_ALL")
})

test_that("piptm_manifest() errors when release not found", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)
  piptm:::.load_manifests(tmp)

  expect_error(
    piptm::piptm_manifest("99991231"),
    regexp = "not found"
  )
})

test_that("piptm_manifest() errors when no current release and release = NULL", {
  # Reset env to empty state
  .piptm_env <- piptm:::.piptm_env
  old_manifests       <- .piptm_env$manifests
  old_current_release <- .piptm_env$current_release
  on.exit({
    .piptm_env$manifests       <- old_manifests
    .piptm_env$current_release <- old_current_release
  })

  .piptm_env$manifests       <- list()
  .piptm_env$current_release <- NULL

  expect_error(
    piptm::piptm_manifest(),
    regexp = "No current release"
  )
})

# ---------------------------------------------------------------------------
# set_manifest_dir()
# ---------------------------------------------------------------------------

test_that("set_manifest_dir() rescans and updates manifest cache", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)

  # Start from an empty state
  .piptm_env <- piptm:::.piptm_env
  old_manifests <- .piptm_env$manifests
  on.exit(.piptm_env$manifests <- old_manifests)
  .piptm_env$manifests <- list()

  piptm::set_manifest_dir(tmp)

  expect_true("20260206" %in% names(piptm::piptm_manifests()))
  expect_identical(piptm::piptm_manifest_dir(), tmp)
})

# ---------------------------------------------------------------------------
# set_arrow_root()
# ---------------------------------------------------------------------------

test_that("set_arrow_root() updates .piptm_env$arrow_root", {
  tmp <- withr::local_tempdir()

  old_root <- piptm::piptm_arrow_root()
  on.exit({
    env <- getNamespace("piptm")$.piptm_env
    env$arrow_root <- old_root
  })

  piptm::set_arrow_root(tmp)
  expect_identical(piptm::piptm_arrow_root(), tmp)
})

test_that("set_arrow_root() errors when path does not exist", {
  expect_error(
    piptm::set_arrow_root("/nonexistent/path/to/arrow"),
    regexp = "does not exist"
  )
})

# ---------------------------------------------------------------------------
# Accessor functions: piptm_manifest_dir, piptm_manifests, piptm_current_release
# ---------------------------------------------------------------------------

test_that("piptm_manifest_dir() returns the directory set by set_manifest_dir()", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)
  piptm::set_manifest_dir(tmp)
  expect_identical(piptm::piptm_manifest_dir(), tmp)
})

test_that("piptm_manifests() returns a named list", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)
  piptm::set_manifest_dir(tmp)
  manifests <- piptm::piptm_manifests()
  expect_type(manifests, "list")
  expect_true(length(manifests) >= 1L)
})

test_that("piptm_current_release() returns a character scalar after loading", {
  tmp <- withr::local_tempdir()
  write_fixture_manifest(tmp, release = "20260206", current = TRUE)
  piptm::set_manifest_dir(tmp)
  expect_identical(piptm::piptm_current_release(), "20260206")
})
