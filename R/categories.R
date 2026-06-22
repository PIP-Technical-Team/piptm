# Category catalogue for the Table Maker sample-base filter panel
#
# Exports:
#   pip_tablemaker_categories()  — static catalogue of all filterable
#                                  categorical variables and their subcategories

NULL

# ── Internal subcategory constants (shared across variable families) ───

#' Subcategories for the gender variable
#'
#' @keywords internal
.GENDER_SUBCATS <- list(
  list(value = 1L, label = "Male"),
  list(value = 2L, label = "Female")
)

#' Subcategories for the area variable
#'
#' @keywords internal
.AREA_SUBCATS <- list(
  list(value = 1L, label = "Urban"),
  list(value = 2L, label = "Rural")
)

#' Subcategories for the age_group variable
#'
#' @keywords internal
.AGE_GROUP_SUBCATS <- list(
  list(value = 1L, label = "0 to 14"),
  list(value = 2L, label = "15 to 24"),
  list(value = 3L, label = "25 to 64"),
  list(value = 4L, label = "65 and above")
)

#' Subcategories for the educat4 variable
#'
#' @keywords internal
.EDUCAT4_SUBCATS <- list(
  list(value = 1L, label = "No education"),
  list(value = 2L, label = "Primary (complete or incomplete)"),
  list(value = 3L, label = "Secondary (complete or incomplete)"),
  list(value = 4L, label = "Tertiary (complete or incomplete)")
)

#' Subcategories for the educat5 variable
#'
#' @keywords internal
.EDUCAT5_SUBCATS <- list(
  list(value = 1L, label = "No education"),
  list(value = 2L, label = "Primary incomplete"),
  list(value = 3L, label = "Primary complete, secondary incomplete"),
  list(value = 4L, label = "Secondary complete"),
  list(value = 5L, label = "Some tertiary or post-secondary")
)

#' Subcategories for the educat7 variable
#'
#' @keywords internal
.EDUCAT7_SUBCATS <- list(
  list(value = 1L, label = "No education"),
  list(value = 2L, label = "Primary incomplete"),
  list(value = 3L, label = "Primary complete"),
  list(value = 4L, label = "Secondary incomplete"),
  list(value = 5L, label = "Secondary complete"),
  list(value = 6L, label = "Higher than secondary but not university"),
  list(value = 7L, label = "University incomplete or complete")
)

#' Subcategories for the hsize_group variable
#'
#' @keywords internal
.HSIZE_GROUP_SUBCATS <- list(
  list(value = 1L, label = "1 person"),
  list(value = 2L, label = "2 to 3 persons"),
  list(value = 3L, label = "4 to 6 persons"),
  list(value = 4L, label = "7 or more persons")
)

#' Subcategories for the lstatus (labour status) variable family
#'
#' @keywords internal
.LSTATUS_SUBCATS <- list(
  list(value = 1L, label = "Employed"),
  list(value = 2L, label = "Unemployed"),
  list(value = 3L, label = "Out of the labour force")
)

#' Subcategories for the empstat (employment status) variable family
#'
#' @keywords internal
.EMPSTAT_SUBCATS <- list(
  list(value = 1L, label = "Paid employee"),
  list(value = 2L, label = "Non-paid employee"),
  list(value = 3L, label = "Employer"),
  list(value = 4L, label = "Self-employed"),
  list(value = 5L, label = "Other, workers not classifiable by status")
)

#' Subcategories for the industrycat4 (4-category industry) variable family
#'
#' @keywords internal
.INDUSTRYCAT4_SUBCATS <- list(
  list(value = 1L, label = "Agriculture"),
  list(value = 2L, label = "Industry"),
  list(value = 3L, label = "Services"),
  list(value = 4L, label = "Other")
)

#' Subcategories for the industrycat10 (10-category industry) variable family
#'
#' @keywords internal
.INDUSTRYCAT10_SUBCATS <- list(
  list(value =  1L, label = "Agriculture, hunting, fishing, etc."),
  list(value =  2L, label = "Mining"),
  list(value =  3L, label = "Manufacturing"),
  list(value =  4L, label = "Public utility services"),
  list(value =  5L, label = "Construction"),
  list(value =  6L, label = "Commerce"),
  list(value =  7L, label = "Transport"),
  list(value =  8L, label = "Financial"),
  list(value =  9L, label = "Public administration"),
  list(value = 10L, label = "Other services")
)

#' Subcategories for the wquintile (welfare quintile) variable
#'
#' @keywords internal
.WQUINTILE_SUBCATS <- list(
  list(value = 1L, label = "Q1 (bottom 20%)"),
  list(value = 2L, label = "Q2"),
  list(value = 3L, label = "Q3"),
  list(value = 4L, label = "Q4"),
  list(value = 5L, label = "Q5 (top 20%)")
)

# ── Exported function ─────────────────────────────────────────────────────

#' Full catalogue of categorical variables for the Table Maker sample-base
#' filter panel
#'
#' @description
#' Returns the static catalogue of all categorical variables available in the
#' PIP microdata that the Table Maker UI uses to build its sample-base filter
#' panel (Step 2, first decision: "What sample base do I want to analyse?").
#'
#' The UI renders each variable as a filter with toggleable subcategory chips,
#' all selected by default. The user can deselect subcategories to restrict
#' the sample base; multiple active filters are combined as intersections at
#' query time. The catalogue is fully static and always returns the complete
#' universe of variables regardless of which surveys were selected in Step 1.
#' Whether a specific variable is present in a given survey is handled by the
#' backend at query time, not by this endpoint.
#'
#' @details
#' All categorical variables are represented as integer-coded factors.
#' The `value` field always corresponds to the underlying integer code stored
#' in the data, while `label` provides the human-readable text displayed in
#' the UI.
#'
#' Variables belong to four tracks:
#'
#' * **Track 1 — Factor variables** (`gender`, `area`, `educat4`,
#'   `educat5`, `educat7`, `age_group`, `hsize_group`): represented as
#'   integer-coded factors.
#' * **Track 2 — Binary indicators** (`imp_wat_rec`, `imp_san_rec`,
#'   `electricity`): stored as `0`/`1` integers.
#' * **Track 3 — GMD labour variables** (`lstatus` family,
#'   `empstat` family, `industrycat4` family, `industrycat10` family):
#'   standardised GMD codebook integers.
#' * **Track 4 — Welfare quintile** (`wquintile`): integer 1–5
#'   where 1 = bottom 20% and 5 = top 20%.
#'
#' @return A list of entries in canonical display order
#'   (demographic → education → household → welfare → infrastructure → labour).
#'   Each entry is a named list with three fields:
#'   \describe{
#'     \item{`varname`}{Character scalar: column name as it exists at query
#'       time.}
#'     \item{`label`}{Character scalar: human-readable variable name for the
#'       UI.}
#'     \item{`subcategories`}{List of named lists, each with:
#'       `value` — the underlying integer code stored in the data;
#'       `label` — the human-readable subcategory label shown in the UI.}
#'   }
#'
#' @family measures
#'
#' @examples
#' cats <- pip_tablemaker_categories()
#' length(cats)
#' cats[[1L]]$varname
#' cats[[1L]]$subcategories[[1L]]$value  # 1L
#' cats[[1L]]$subcategories[[1L]]$label  # "Male"
#'
#' @export
pip_tablemaker_categories <- function() {
  list(
    # ── Demographic ───────────────────────────────────────────────────────
    list(
      varname       = "gender",
      label         = "Gender",
      subcategories = .GENDER_SUBCATS
    ),
    list(
      varname       = "area",
      label         = "Area",
      subcategories = .AREA_SUBCATS
    ),
    list(
      varname       = "age_group",
      label         = "Age group",
      subcategories = .AGE_GROUP_SUBCATS
    ),

    # ── Education ────────────────────────────────────────────────────────
    list(
      varname       = "educat4",
      label         = "Education level (4 groups)",
      subcategories = .EDUCAT4_SUBCATS
    ),
    list(
      varname       = "educat5",
      label         = "Education level (5 groups)",
      subcategories = .EDUCAT5_SUBCATS
    ),
    list(
      varname       = "educat7",
      label         = "Education level (7 groups)",
      subcategories = .EDUCAT7_SUBCATS
    ),

    # ── Household ────────────────────────────────────────────────────────
    list(
      varname       = "hsize_group",
      label         = "Household size",
      subcategories = .HSIZE_GROUP_SUBCATS
    ),

    # ── Welfare ──────────────────────────────────────────────────────────
    list(
      varname       = "wquintile",
      label         = "Welfare quintile",
      subcategories = .WQUINTILE_SUBCATS
    ),

    # ── Infrastructure ───────────────────────────────────────────────────
    list(
      varname = "imp_wat_rec",
      label   = "Access to improved water",
      subcategories = list(
        list(value = 0L, label = "No"),
        list(value = 1L, label = "Yes")
      )
    ),
    list(
      varname = "imp_san_rec",
      label   = "Access to improved sanitation",
      subcategories = list(
        list(value = 0L, label = "No"),
        list(value = 1L, label = "Yes")
      )
    ),
    list(
      varname = "electricity",
      label   = "Access to electricity",
      subcategories = list(
        list(value = 0L, label = "No"),
        list(value = 1L, label = "Yes")
      )
    ),

    # ── Labour: lstatus family ──────────────────────────────────────────
    list(
      varname       = "lstatus",
      label         = "Labour status (7-day)",
      subcategories = .LSTATUS_SUBCATS
    ),
    list(
      varname       = "lstatus_year",
      label         = "Labour status (12-month)",
      subcategories = .LSTATUS_SUBCATS
    ),

    # ── Labour: empstat family ──────────────────────────────────────────
    list(
      varname       = "empstat",
      label         = "Employment status, primary job (7-day)",
      subcategories = .EMPSTAT_SUBCATS
    ),
    list(
      varname       = "empstat_2",
      label         = "Employment status, secondary job (7-day)",
      subcategories = .EMPSTAT_SUBCATS
    ),
    list(
      varname       = "empstat_year",
      label         = "Employment status, primary job (12-month)",
      subcategories = .EMPSTAT_SUBCATS
    ),
    list(
      varname       = "empstat_2_year",
      label         = "Employment status, secondary job (12-month)",
      subcategories = .EMPSTAT_SUBCATS
    ),

    # ── Labour: industrycat4 family ─────────────────────────────────────
    list(
      varname       = "industrycat4",
      label         = "Industry (4 categories), primary job (7-day)",
      subcategories = .INDUSTRYCAT4_SUBCATS
    ),
    list(
      varname       = "industrycat4_2",
      label         = "Industry (4 categories), secondary job (7-day)",
      subcategories = .INDUSTRYCAT4_SUBCATS
    ),
    list(
      varname       = "industrycat4_year",
      label         = "Industry (4 categories), primary job (12-month)",
      subcategories = .INDUSTRYCAT4_SUBCATS
    ),
    list(
      varname       = "industrycat4_2_year",
      label         = "Industry (4 categories), secondary job (12-month)",
      subcategories = .INDUSTRYCAT4_SUBCATS
    ),

    # ── Labour: industrycat10 family ────────────────────────────────────
    list(
      varname       = "industrycat10",
      label         = "Industry (10 categories), primary job (7-day)",
      subcategories = .INDUSTRYCAT10_SUBCATS
    ),
    list(
      varname       = "industrycat10_2",
      label         = "Industry (10 categories), secondary job (7-day)",
      subcategories = .INDUSTRYCAT10_SUBCATS
    ),
    list(
      varname       = "industrycat10_year",
      label         = "Industry (10 categories), primary job (12-month)",
      subcategories = .INDUSTRYCAT10_SUBCATS
    ),
    list(
      varname       = "industrycat10_2_year",
      label         = "Industry (10 categories), secondary job (12-month)",
      subcategories = .INDUSTRYCAT10_SUBCATS
    )
  )
}