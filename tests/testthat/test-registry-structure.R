# Test: Registry Structure Validation
# Requirement: R11 — Registry function return structures match spec §5.1
# Phase: 0

test_that("piptm_variable_registry returns correct structure", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_REGISTRY_DIR", "")), 
              "Registry directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  reg <- piptm_variable_registry(release)
  
  # Should return a named list
  expect_type(reg, "list")
  expect_true(length(names(reg)) > 0)
  
  # Each entry should have the required fields per spec §5.1
  for (varname in names(reg)) {
    if (identical(varname, "measure_spec")) {
      next
    }
    entry <- reg[[varname]]
    
    expect_true("varname" %in% names(entry), 
                info = paste(varname, "missing 'varname'"))
    expect_true("ui_label" %in% names(entry),
                info = paste(varname, "missing 'ui_label'"))
    expect_true("tm_type" %in% names(entry),
                info = paste(varname, "missing 'tm_type'"))
    expect_true("roles" %in% names(entry),
                info = paste(varname, "missing 'roles'"))
    expect_true("stat_groups" %in% names(entry),
                info = paste(varname, "missing 'stat_groups'"))
    
    expect_true("n_categories" %in% names(entry),
                info = paste(varname, "missing 'n_categories'"))
  }
})

test_that("piptm_stat_groups returns correct structure", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_REGISTRY_DIR", "")), 
              "Registry directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  groups <- piptm_stat_groups(release)
  
  # Should return a list of groups
  expect_type(groups, "list")
  expect_true(length(groups) > 0)
  
  # Each group should have: group, group_label, measures
  for (i in seq_along(groups)) {
    group <- groups[[i]]
    
    expect_true("group" %in% names(group),
                info = paste("Group", i, "missing 'group'"))
    expect_true("group_label" %in% names(group),
                info = paste("Group", i, "missing 'group_label'"))
    expect_true("measures" %in% names(group),
                info = paste("Group", i, "missing 'measures'"))
    
    # Each measure should have: measure, label
    if (length(group$measures) > 0) {
      for (j in seq_along(group$measures)) {
        measure <- group$measures[[j]]
        expect_true("measure" %in% names(measure),
                    info = paste("Group", i, "measure", j, "missing 'measure'"))
        expect_true("label" %in% names(measure),
                    info = paste("Group", i, "measure", j, "missing 'label'"))
      }
    }
  }
})

test_that("piptm_filter_categories returns correct structure", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_REGISTRY_DIR", "")), 
              "Registry directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  cats <- piptm_filter_categories(release)
  
  # Should return a list of filters
  expect_type(cats, "list")
  
  # Each filter should have: varname, label, subcategories
  for (i in seq_along(cats)) {
    filter <- cats[[i]]
    
    expect_true("varname" %in% names(filter),
                info = paste("Filter", i, "missing 'varname'"))
    expect_true("label" %in% names(filter),
                info = paste("Filter", i, "missing 'label'"))
    expect_true("subcategories" %in% names(filter),
                info = paste("Filter", i, "missing 'subcategories'"))
    
    # Each subcategory should have: code, label
    if (length(filter$subcategories) > 0) {
      for (j in seq_along(filter$subcategories)) {
        subcat <- filter$subcategories[[j]]
        expect_true("code" %in% names(subcat),
                    info = paste("Filter", i, "subcat", j, "missing 'code'"))
        expect_true("label" %in% names(subcat),
                    info = paste("Filter", i, "subcat", j, "missing 'label'"))
      }
    }
  }
})

test_that("piptm_layout_covariates returns correct structure", {
  skip_if_not(dir.exists(Sys.getenv("PIPTM_REGISTRY_DIR", "")), 
              "Registry directory not configured")
  
  release <- piptm_current_release()
  skip_if(is.null(release) || !nzchar(release), "No current release set")
  
  covs <- piptm_layout_covariates(release)
  
  # Should return a list of covariates
  expect_type(covs, "list")
  
  # Each covariate should have: varname, label, n_categories
  for (i in seq_along(covs)) {
    cov <- covs[[i]]
    
    expect_true("varname" %in% names(cov),
                info = paste("Covariate", i, "missing 'varname'"))
    expect_true("label" %in% names(cov),
                info = paste("Covariate", i, "missing 'label'"))
    expect_true(
      "n_categories" %in% names(cov),
      info = paste("Covariate", i, "missing 'n_categories'")
    )
    expect_true(
      is.numeric(cov$n_categories),
      info = paste("Covariate", i, "has non-numeric 'n_categories'")
    )
  }
})
