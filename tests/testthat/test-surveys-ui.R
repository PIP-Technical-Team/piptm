test_that("piptm_surveys_ui returns expected shape for a simple manifest", {
  skip_on_cran()

  tmp <- tempfile("manifest_dir")
  dir.create(tmp)

  release <- "TESTUI1"
  entries <- list(list(
    pip_id = "AAA_2000_TEST_INC_ALL",
    survey_id = "S1",
    country_code = "AAA",
    country_name = "Testland",
    year = 2000,
    welfare_type = "INC",
    version = "v01",
    survey_acronym = "TEST",
    module = "ALL",
    dimensions = c("gender", "area"),
    welfare_vars = c("welfare_ppp_2021_01_02"),
    ppp_sort = 2021
  ))

  write_fixture_manifest_tm(tmp, release, entries, set_current = TRUE)
  piptm::set_manifest_dir(tmp)

  out <- piptm::piptm_surveys_ui(release)
  expect_type(out, "list")
  expect_length(out, 1L)

  srv <- out[[1L]]
  expect_named(
    srv,
    c("pip_id", "country_code", "country_label", "year",
      "welfare_type", "welfare_label", "version")
  )
  expect_false("dimensions" %in% names(srv))

  expect_identical(srv$pip_id, "AAA_2000_TEST_INC_ALL")
  expect_identical(srv$country_label, "Testland")
  expect_identical(srv$welfare_label, "Income")
  expect_identical(srv$version, "v01")
})

test_that("piptm_surveys_ui handles empty manifests", {
  skip_on_cran()

  tmp <- tempfile("manifest_dir")
  dir.create(tmp)
  release <- "TESTUI_EMPTY"

  write_fixture_manifest_tm(tmp, release, list(), set_current = TRUE)
  piptm::set_manifest_dir(tmp)

  expect_identical(piptm::piptm_surveys_ui(release), list())
})

test_that("piptm_surveys_ui labels welfare types and sorts pip_ids", {
  skip_on_cran()

  tmp <- tempfile("manifest_dir")
  dir.create(tmp)
  release <- "TESTUI_LABELS"
  entry <- function(pip_id, welfare_type) {
    list(
      pip_id = pip_id,
      survey_id = pip_id,
      country_code = "AAA",
      country_name = "Testland",
      year = 2000,
      welfare_type = welfare_type,
      version = "v01",
      survey_acronym = "TEST",
      module = "ALL",
      dimensions = character(0),
      welfare_vars = c("welfare_ppp_2021_01_02"),
      ppp_sort = 2021
    )
  }
  entries <- list(
    entry("zzz_2000_TEST_INC_ALL", "INC"),
    entry("AAA_2000_TEST_CON_ALL", "CON"),
    entry("MID_2000_TEST_OTHER_ALL", "OTHER")
  )

  write_fixture_manifest_tm(tmp, release, entries, set_current = TRUE)
  piptm::set_manifest_dir(tmp)
  out <- piptm::piptm_surveys_ui(release)

  expect_identical(
    vapply(out, `[[`, character(1), "pip_id"),
    c("AAA_2000_TEST_CON_ALL", "MID_2000_TEST_OTHER_ALL",
      "zzz_2000_TEST_INC_ALL")
  )
  expect_identical(vapply(out, `[[`, character(1), "welfare_label"),
                   c("Consumption", "OTHER", "Income"))
  expect_true(all(vapply(out, function(x) !"dimensions" %in% names(x),
                         logical(1))))
})
