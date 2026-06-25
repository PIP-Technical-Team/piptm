test_that("piptm_surveys_ui returns expected shape for a simple manifest", {
  skip_on_cran()

  tmp <- tempfile("manifest_dir")
  dir.create(tmp)

  release <- "TESTUI1"
  entries <- list(list(
    pip_id = "AAA_2000_TEST_INC_ALL",
    survey_id = "S1",
    country_code = "AAA",
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
  expect_true(all(c("pip_id", "country_code", "country_label", "year",
                    "welfare_type", "dimensions") %in% names(srv)))

  expect_identical(srv$pip_id, "AAA_2000_TEST_INC_ALL")
  # country_label falls back to code when no countrycode package
  expect_true(is.character(srv$country_label) && nzchar(srv$country_label))

  expect_type(srv$dimensions, "character")
  expect_length(srv$dimensions, 2L)
  expect_identical(srv$dimensions[[1]], "gender")
})
