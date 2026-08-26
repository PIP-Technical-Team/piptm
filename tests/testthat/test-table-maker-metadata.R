# Test: table_maker Metadata Capture
# Requirement: R1, R2 — include_metadata parameter and backward compatibility
# Phase: 1

test_that("include_metadata parameter validation", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_DATA_DIR", "")), 
              "Data directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  # Get a test pip_id
  manifest <- piptm_manifest(release)
  skip_if(nrow(manifest) == 0, "No surveys in manifest")
  test_pip_id <- manifest$pip_id[1]
  
  # Valid non-logical should error
  expect_error(
    table_maker(
      pip_id = test_pip_id,
      analysis_var = "welfare",
      measures = "mean",
      include_metadata = "yes"
    ),
    class = "simpleError"
  )
  
  # Empty logical should error
  expect_error(
    table_maker(
      pip_id = test_pip_id,
      analysis_var = "welfare",
      measures = "mean",
      include_metadata = logical(0)
    ),
    class = "simpleError"
  )
})

test_that("include_metadata = FALSE returns data.table (default behavior)", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_DATA_DIR", "")), 
              "Data directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  manifest <- piptm_manifest(release)
  skip_if(nrow(manifest) == 0, "No surveys in manifest")
  test_pip_id <- manifest$pip_id[1]
  
  result <- table_maker(
    pip_id = test_pip_id,
    analysis_var = "welfare",
    measures = "mean",
    include_metadata = FALSE
  )
  
  expect_s3_class(result, "data.table")
  expect_true("measure" %in% names(result))
  expect_true("value" %in% names(result))
})

test_that("include_metadata = TRUE returns list with data and description_metadata", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_DATA_DIR", "")), 
              "Data directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  manifest <- piptm_manifest(release)
  skip_if(nrow(manifest) == 0, "No surveys in manifest")
  test_pip_id <- manifest$pip_id[1]
  
  result <- table_maker(
    pip_id = test_pip_id,
    analysis_var = "welfare",
    measures = "mean",
    include_metadata = TRUE
  )
  
  expect_type(result, "list")
  expect_true("data" %in% names(result))
  expect_true("description_metadata" %in% names(result))
  
  # data field should be a data.table
  expect_s3_class(result$data, "data.table")
  expect_true("measure" %in% names(result$data))
  expect_true("value" %in% names(result$data))
  
  # description_metadata field should be a list
  expect_type(result$description_metadata, "list")
})

test_that("include_metadata default (missing) returns data.table", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_DATA_DIR", "")), 
              "Data directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  manifest <- piptm_manifest(release)
  skip_if(nrow(manifest) == 0, "No surveys in manifest")
  test_pip_id <- manifest$pip_id[1]
  
  # Call without include_metadata parameter
  result <- table_maker(
    pip_id = test_pip_id,
    analysis_var = "welfare",
    measures = "mean"
  )
  
  expect_s3_class(result, "data.table")
  expect_false(is.list(result) && "data" %in% names(result))
})
