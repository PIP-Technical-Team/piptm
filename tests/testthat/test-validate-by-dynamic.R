test_that(".validate_by accepts dynamic covariates from layout", {
  rel <- activate_test_registry("TEST_RELEASE")
  expect_no_error(piptm:::.validate_by("wquintile", release = rel))
})
