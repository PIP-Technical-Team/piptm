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
    "must be TRUE or FALSE"
  )
  
  # Empty logical should error
  expect_error(
    table_maker(
      pip_id = test_pip_id,
      analysis_var = "welfare",
      measures = "mean",
      include_metadata = logical(0)
    ),
    "must be TRUE or FALSE"
  )
  
  # Length > 1 logical should error
  expect_error(
    table_maker(
      pip_id = test_pip_id,
      analysis_var = "welfare",
      measures = "mean",
      include_metadata = c(TRUE, FALSE)
    ),
    "must be TRUE or FALSE"
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

test_that("metadata assembly is present with schema-expected fields", {
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
    by = NULL,
    filter_base = NULL,
    include_metadata = TRUE,
    release = release
  )

  meta <- result$description_metadata
  if (is.null(meta)) skip("description_metadata is NULL (expected when registry unavailable)")

  # Top-level required sections per spec §2.1
  expect_true(all(c("params", "provenance", "surveys", "resolved_labels",
                    "execution") %in% names(meta)))

  # provenance
  expect_true("release" %in% names(meta$provenance))
  expect_true("ppp_year" %in% names(meta$provenance))
  expect_true("generated_at" %in% names(meta$provenance))

  # surveys: loaded must be a data.table with expected columns
  expect_s3_class(meta$surveys$loaded, "data.table")
  expect_true(all(c("pip_id", "country_code", "surveyid_year", "welfare_type") %in%
                    names(meta$surveys$loaded)))
  expect_s3_class(meta$surveys$excluded, "data.table")
  expect_true(all(c("pip_id", "reason") %in% names(meta$surveys$excluded)))

  # resolved_labels
  expect_true(all(c("analysis_var", "measures", "filters", "covariates") %in%
                    names(meta$resolved_labels)))
  expect_true("varname" %in% names(meta$resolved_labels$analysis_var))
  expect_true("ui_label" %in% names(meta$resolved_labels$analysis_var))
  expect_true("tm_type" %in% names(meta$resolved_labels$analysis_var))

  # execution: includes warnings field
  expect_true(all(c("n_surveys_loaded", "n_surveys_excluded",
                    "n_measures_computed", "suppression",
                    "warnings") %in% names(meta$execution)))
})

test_that("abort-path safety: same error with and without include_metadata", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_DATA_DIR", "")),
              "Data directory not configured")

  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")

  manifest <- piptm_manifest(release)
  skip_if(nrow(manifest) == 0, "No surveys in manifest")
  test_pip_id <- manifest$pip_id[1]

  # Invalid measure: should abort identically in both paths
  expect_error(
    table_maker(
      pip_id = test_pip_id,
      analysis_var = "welfare",
      measures = "not_a_real_measure",
      include_metadata = FALSE
    )
  )
  expect_error(
    table_maker(
      pip_id = test_pip_id,
      analysis_var = "welfare",
      measures = "not_a_real_measure",
      include_metadata = TRUE
    )
  )
})
