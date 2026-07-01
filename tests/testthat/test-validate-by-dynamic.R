test_that(".validate_by accepts dynamic covariates from layout", {
  expect_no_error(piptm:::.validate_by("wquintile"))
})
