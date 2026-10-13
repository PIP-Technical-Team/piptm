library(piptm)
library(jsonlite)

json_str <- '{
  "description_metadata": {
    "params": {
      "pip_id": ["COL_2008_GEIH_INC_ALL", "BRA_2004_PNAD_INC_ALL"],
      "analysis_var": ["emp_others"],
      "measures": ["target_survey_share"],
      "poverty_line": {},
      "by": ["gender"],
      "filter_base": {},
      "ppp": [2021],
      "release": ["20260401_TEST"],
      "pop_share_threshold": [0.01],
      "include_metadata": [true]
    },
    "provenance": {
      "release": ["20260401_TEST"],
      "ppp_year": [2021],
      "generated_at": ["2026-10-13 14:34:10 UTC"]
    },
    "surveys": {
      "loaded": [
        {"pip_id": "BRA_2004_PNAD_INC_ALL", "country_code": "BRA", "surveyid_year": 2004, "welfare_type": "INC"},
        {"pip_id": "COL_2008_GEIH_INC_ALL", "country_code": "COL", "surveyid_year": 2008, "welfare_type": "INC"}
      ],
      "excluded": []
    },
    "resolved_labels": {
      "analysis_var": {
        "varname": [["emp_others"]],
        "ui_label": [["Employment in other sectors"]],
        "tm_type": [["binary"]]
      },
      "measures": [
        {"measure": "target_survey_share", "ui_label": "Target share in sample base", "stat_group": "shares"}
      ],
      "filters": {},
      "covariates": [
        {"slot": "rows", "varname": "gender", "ui_label": "Gender", "n_categories": 2}
      ]
    },
    "execution": {
      "n_surveys_loaded": [2],
      "n_surveys_excluded": [0],
      "n_filters_applied": [0],
      "n_measures_computed": [1],
      "suppression": {
        "triggered": [false],
        "threshold": [0.01],
        "n_cells_suppressed": [0],
        "suppressed_cells": {}
      },
      "warnings": {}
    }
  }
}'

# Mimic plumber default JSON body parsing (NOT simplifying to data.frame),
# which is what @parser json uses under the hood.
body <- jsonlite::fromJSON(json_str, simplifyVector = TRUE)

cat("class of resolved_labels$measures:\n")
print(class(body$description_metadata$resolved_labels$measures))
print(body$description_metadata$resolved_labels$measures)

cat("\n--- Attempting build_description_model ---\n")
result <- tryCatch({
  model <- piptm::build_description_model(
    description_metadata = body$description_metadata,
    params = body$description_metadata$params
  )
  html <- piptm::render_description_html(model)
  cat("SUCCESS\n")
  html
}, error = function(e) {
  cat("ERROR:\n")
  print(e)
  NULL
})
