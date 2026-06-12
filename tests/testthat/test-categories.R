library(testthat)

# ══════════════════════════════════════════════════════════════════════════════
# pip_tablemaker_categories() — top-level structure
# ══════════════════════════════════════════════════════════════════════════════

test_that("pip_tablemaker_categories() returns a list", {
  cats <- pip_tablemaker_categories()
  expect_type(cats, "list")
})

test_that("pip_tablemaker_categories() returns exactly 24 entries", {
  cats <- pip_tablemaker_categories()
  expect_length(cats, 24L)
})

test_that("every entry has exactly the fields varname, label, subcategories", {
  cats <- pip_tablemaker_categories()
  for (entry in cats) {
    expect_named(entry, c("varname", "label", "subcategories"), ignore.order = FALSE)
  }
})

test_that("every varname is a non-empty character scalar", {
  cats <- pip_tablemaker_categories()
  for (entry in cats) {
    expect_type(entry$varname, "character")
    expect_length(entry$varname, 1L)
    expect_gt(nchar(entry$varname), 0L)
  }
})

test_that("every label is a non-empty character scalar", {
  cats <- pip_tablemaker_categories()
  for (entry in cats) {
    expect_type(entry$label, "character")
    expect_length(entry$label, 1L)
    expect_gt(nchar(entry$label), 0L)
  }
})

test_that("every subcategories field is a non-empty list", {
  cats <- pip_tablemaker_categories()
  for (entry in cats) {
    expect_type(entry$subcategories, "list")
    expect_gt(length(entry$subcategories), 0L)
  }
})

test_that("every subcategory entry has exactly the fields value and label", {
  cats <- pip_tablemaker_categories()
  for (entry in cats) {
    for (sub in entry$subcategories) {
      expect_named(sub, c("value", "label"), ignore.order = FALSE)
    }
  }
})

test_that("every subcategory label is a non-empty character scalar", {
  cats <- pip_tablemaker_categories()
  for (entry in cats) {
    for (sub in entry$subcategories) {
      expect_type(sub$label, "character")
      expect_length(sub$label, 1L)
      expect_gt(nchar(sub$label), 0L)
    }
  }
})

test_that("all varnames are unique", {
  cats    <- pip_tablemaker_categories()
  varnames <- vapply(cats, `[[`, character(1L), "varname")
  expect_equal(length(varnames), length(unique(varnames)))
})

# ══════════════════════════════════════════════════════════════════════════════
# Canonical ordering
# ══════════════════════════════════════════════════════════════════════════════

test_that("varnames appear in canonical display order", {
  cats     <- pip_tablemaker_categories()
  varnames <- vapply(cats, `[[`, character(1L), "varname")
  expected <- c(
    # demographic
    "gender", "area", "age_group",
    # education
    "educat4", "educat5", "educat7",
    # household
    "hsize_group",
    # infrastructure
    "imp_wat_rec", "imp_san_rec", "electricity",
    # labour — lstatus
    "lstatus", "lstatus_year",
    # labour — empstat
    "empstat", "empstat_2", "empstat_year", "empstat_2_year",
    # labour — industrycat4
    "industrycat4", "industrycat4_2", "industrycat4_year", "industrycat4_2_year",
    # labour — industrycat10
    "industrycat10", "industrycat10_2", "industrycat10_year", "industrycat10_2_year"
  )
  expect_equal(varnames, expected)
})

# ══════════════════════════════════════════════════════════════════════════════
# Track 1 — Factor variables (character values)
# ══════════════════════════════════════════════════════════════════════════════

get_entry <- function(cats, varname) {
  cats[[which(vapply(cats, `[[`, character(1L), "varname") == varname)]]
}

test_that("gender has 2 subcategories with correct values and labels", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "gender")
  expect_length(entry$subcategories, 2L)
  expect_equal(entry$subcategories[[1L]]$value, "male")
  expect_equal(entry$subcategories[[1L]]$label, "Male")
  expect_equal(entry$subcategories[[2L]]$value, "female")
  expect_equal(entry$subcategories[[2L]]$label, "Female")
})

test_that("area has 2 subcategories with correct values and labels", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "area")
  expect_length(entry$subcategories, 2L)
  expect_equal(entry$subcategories[[1L]]$value, "urban")
  expect_equal(entry$subcategories[[1L]]$label, "Urban")
  expect_equal(entry$subcategories[[2L]]$value, "rural")
  expect_equal(entry$subcategories[[2L]]$label, "Rural")
})

test_that("educat4 has 4 subcategories with correct values and labels", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "educat4")
  expect_length(entry$subcategories, 4L)
  values <- vapply(entry$subcategories, `[[`, character(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values[[1L]], "No education")
  expect_equal(values[[2L]], "Primary only")
  expect_equal(values[[3L]], "Secondary (complete or incomplete)")
  expect_equal(values[[4L]], "Tertiary")
  # labels are identical to values for Track 1
  expect_equal(labels[[1L]], "No education")
  expect_equal(labels[[3L]], "Secondary (complete or incomplete)")
  expect_equal(labels[[4L]], "Tertiary")
})

test_that("educat5 has 5 subcategories, with remapped labels where applicable", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "educat5")
  expect_length(entry$subcategories, 5L)
  values <- vapply(entry$subcategories, `[[`, character(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values[[3L]], "Primary complete but secondary incomplete")
  expect_equal(labels[[3L]], "Primary complete, secondary incomplete")
  expect_equal(values[[5L]], "Some tertiary/post-secondary")
  expect_equal(labels[[5L]], "Some tertiary or post-secondary")
})

test_that("educat7 has 7 subcategories", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "educat7")
  expect_length(entry$subcategories, 7L)
  values <- vapply(entry$subcategories, `[[`, character(1L), "value")
  expect_equal(
    values,
    c(
      "No education", "Primary incomplete", "Primary complete",
      "Secondary incomplete", "Secondary complete",
      "Some tertiary", "Tertiary complete"
    )
  )
})

test_that("Track 1 factor variables all use character subcategory values", {
  cats    <- pip_tablemaker_categories()
  track1  <- c("gender", "area", "educat4", "educat5", "educat7")
  for (vn in track1) {
    entry <- get_entry(cats, vn)
    for (sub in entry$subcategories) {
      expect_type(sub$value, "character")
    }
  }
})

# ══════════════════════════════════════════════════════════════════════════════
# Track 2 — Statically binned variables
# ══════════════════════════════════════════════════════════════════════════════

test_that("age_group has 4 subcategories with correct bin labels", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "age_group")
  expect_length(entry$subcategories, 4L)
  values <- vapply(entry$subcategories, `[[`, character(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values, c("0-14", "15-24", "25-64", "65+"))
  expect_equal(labels, c("0 to 14", "15 to 24", "25 to 64", "65 and above"))
})

test_that("hsize_group has 4 subcategories with correct bin labels", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "hsize_group")
  expect_length(entry$subcategories, 4L)
  values <- vapply(entry$subcategories, `[[`, character(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values, c("1", "2-3", "4-6", "7+"))
  expect_equal(labels, c("1 person", "2 to 3 persons", "4 to 6 persons", "7 or more persons"))
})

test_that("raw column names age and hsize are NOT present", {
  cats     <- pip_tablemaker_categories()
  varnames <- vapply(cats, `[[`, character(1L), "varname")
  expect_false("age"   %in% varnames)
  expect_false("hsize" %in% varnames)
})

test_that("Track 2 binned variables use character subcategory values", {
  cats   <- pip_tablemaker_categories()
  track2 <- c("age_group", "hsize_group")
  for (vn in track2) {
    entry <- get_entry(cats, vn)
    for (sub in entry$subcategories) {
      expect_type(sub$value, "character")
    }
  }
})

# ══════════════════════════════════════════════════════════════════════════════
# Track 3 — Binary integer indicators
# ══════════════════════════════════════════════════════════════════════════════

test_that("binary indicator variables have exactly 2 subcategories (0/1)", {
  cats   <- pip_tablemaker_categories()
  track3 <- c("imp_wat_rec", "imp_san_rec", "electricity")
  for (vn in track3) {
    entry  <- get_entry(cats, vn)
    expect_length(entry$subcategories, 2L)
    values <- vapply(entry$subcategories, `[[`, integer(1L), "value")
    labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
    expect_equal(values, c(0L, 1L))
    expect_equal(labels, c("No", "Yes"))
  }
})

test_that("Track 3 variables use integer subcategory values", {
  cats   <- pip_tablemaker_categories()
  track3 <- c("imp_wat_rec", "imp_san_rec", "electricity")
  for (vn in track3) {
    entry <- get_entry(cats, vn)
    for (sub in entry$subcategories) {
      expect_type(sub$value, "integer")
    }
  }
})

test_that("imp_wat_rec has label 'Access to improved water'", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "imp_wat_rec")
  expect_equal(entry$label, "Access to improved water")
})

test_that("imp_san_rec has label 'Access to improved sanitation'", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "imp_san_rec")
  expect_equal(entry$label, "Access to improved sanitation")
})

test_that("electricity has label 'Access to electricity'", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "electricity")
  expect_equal(entry$label, "Access to electricity")
})

# ══════════════════════════════════════════════════════════════════════════════
# Track 4 — Integer-coded GMD labour variables
# ══════════════════════════════════════════════════════════════════════════════

test_that("lstatus has 3 subcategories with correct integer values", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "lstatus")
  expect_length(entry$subcategories, 3L)
  values <- vapply(entry$subcategories, `[[`, integer(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values, 1L:3L)
  expect_equal(labels, c("Employed", "Unemployed", "Out of the labour force"))
})

test_that("lstatus and lstatus_year share the same subcategory values", {
  cats   <- pip_tablemaker_categories()
  day7   <- get_entry(cats, "lstatus")$subcategories
  year12 <- get_entry(cats, "lstatus_year")$subcategories
  expect_identical(day7, year12)
})

test_that("empstat has 5 subcategories with correct integer values", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "empstat")
  expect_length(entry$subcategories, 5L)
  values <- vapply(entry$subcategories, `[[`, integer(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values, 1L:5L)
  expect_equal(
    labels,
    c(
      "Paid employee", "Employer", "Own-account worker",
      "Contributing family worker", "Other"
    )
  )
})

test_that("all four empstat variants share the same subcategories", {
  cats     <- pip_tablemaker_categories()
  ref      <- get_entry(cats, "empstat")$subcategories
  variants <- c("empstat_2", "empstat_year", "empstat_2_year")
  for (vn in variants) {
    expect_identical(get_entry(cats, vn)$subcategories, ref)
  }
})

test_that("industrycat4 has 4 subcategories with correct integer values", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "industrycat4")
  expect_length(entry$subcategories, 4L)
  values <- vapply(entry$subcategories, `[[`, integer(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values, 1L:4L)
  expect_equal(labels, c("Agriculture", "Industry", "Services", "Other"))
})

test_that("all four industrycat4 variants share the same subcategories", {
  cats     <- pip_tablemaker_categories()
  ref      <- get_entry(cats, "industrycat4")$subcategories
  variants <- c("industrycat4_2", "industrycat4_year", "industrycat4_2_year")
  for (vn in variants) {
    expect_identical(get_entry(cats, vn)$subcategories, ref)
  }
})

test_that("industrycat10 has 10 subcategories with correct integer values", {
  cats  <- pip_tablemaker_categories()
  entry <- get_entry(cats, "industrycat10")
  expect_length(entry$subcategories, 10L)
  values <- vapply(entry$subcategories, `[[`, integer(1L), "value")
  labels <- vapply(entry$subcategories, `[[`, character(1L), "label")
  expect_equal(values, 1L:10L)
  expect_equal(
    labels,
    c(
      "Agriculture, fishing, forestry", "Mining", "Manufacturing",
      "Utilities", "Construction", "Commerce",
      "Transport and communications", "Finance and business services",
      "Public administration", "Other services"
    )
  )
})

test_that("all four industrycat10 variants share the same subcategories", {
  cats     <- pip_tablemaker_categories()
  ref      <- get_entry(cats, "industrycat10")$subcategories
  variants <- c("industrycat10_2", "industrycat10_year", "industrycat10_2_year")
  for (vn in variants) {
    expect_identical(get_entry(cats, vn)$subcategories, ref)
  }
})

test_that("Track 4 labour variables use integer subcategory values", {
  cats   <- pip_tablemaker_categories()
  track4 <- c(
    "lstatus", "lstatus_year",
    "empstat", "empstat_2", "empstat_year", "empstat_2_year",
    "industrycat4",      "industrycat4_2",
    "industrycat4_year", "industrycat4_2_year",
    "industrycat10",      "industrycat10_2",
    "industrycat10_year", "industrycat10_2_year"
  )
  for (vn in track4) {
    entry <- get_entry(cats, vn)
    for (sub in entry$subcategories) {
      expect_true(
        is.integer(sub$value),
        info = paste0(vn, " subcategory value is not integer")
      )
    }
  }
})

# ══════════════════════════════════════════════════════════════════════════════
# Labour variable labels
# ══════════════════════════════════════════════════════════════════════════════

test_that("lstatus label is 'Labour status (7-day)'", {
  cats  <- pip_tablemaker_categories()
  expect_equal(get_entry(cats, "lstatus")$label, "Labour status (7-day)")
})

test_that("lstatus_year label is 'Labour status (12-month)'", {
  cats  <- pip_tablemaker_categories()
  expect_equal(get_entry(cats, "lstatus_year")$label, "Labour status (12-month)")
})

test_that("empstat label is 'Employment status, primary job (7-day)'", {
  cats  <- pip_tablemaker_categories()
  expect_equal(
    get_entry(cats, "empstat")$label,
    "Employment status, primary job (7-day)"
  )
})

test_that("empstat_2 label is 'Employment status, secondary job (7-day)'", {
  cats  <- pip_tablemaker_categories()
  expect_equal(
    get_entry(cats, "empstat_2")$label,
    "Employment status, secondary job (7-day)"
  )
})

test_that("industrycat10_2_year has the correct label", {
  cats  <- pip_tablemaker_categories()
  expect_equal(
    get_entry(cats, "industrycat10_2_year")$label,
    "Industry (10 categories), secondary job (12-month)"
  )
})

# ══════════════════════════════════════════════════════════════════════════════
# Idempotence — repeated calls return identical results
# ══════════════════════════════════════════════════════════════════════════════

test_that("pip_tablemaker_categories() is idempotent", {
  expect_identical(pip_tablemaker_categories(), pip_tablemaker_categories())
})
