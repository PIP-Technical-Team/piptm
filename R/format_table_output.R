#' Format table_maker output for API presentation
#'
#' Formats numeric values from [table_maker()] as character strings conforming
#' to the `/table` API presentation contract:
#' \itemize{
#'   \item Masked cells with suppression flag `"NR"` have `value = "NR"`.
#'   \item Percentage-scale measures (`gini`, `headcount`, `poverty_gap`,
#'     `severity`, `pop_share`, `target_within_group_share`,
#'     `target_survey_share`) are multiplied by 100 and formatted with 2 decimal
#'     places (e.g. `0.067` becomes `"6.70"`).
#'   \item Count measures (`pop_poverty`, `nobs`) are formatted as whole
#'     numbers with 0 decimal places.
#'   \item All other finite numeric values (`mean`, `median`, `sd`, `var`, `min`,
#'     `max`, `p10`, `p25`, `p75`, `p90`, `sum`, `mld`, `watts`) are formatted
#'     with 2 decimal places.
#'   \item Genuine missing (`NA`), `NaN`, `Inf`, and `-Inf` values are converted
#'     to `NA_character_` (serializing to `null` in JSON).
#'   \item The internal `flag` column is dropped from the returned table.
#' }
#'
#' @param dt A [data.table::data.table()] output from [table_maker()],
#'   containing at least `measure`, `value`, and `flag` columns.
#'
#' @return A new [data.table::data.table()] with `value` formatted as character
#'   and `flag` removed. The input table is never modified by reference.
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' res <- table_maker(pip_id = "COL_2010_ECH_INC_ALL", analysis_var = "welfare", measures = "gini")
#' formatted <- format_table_output(res)
#' }
format_table_output <- function(dt) {
  if (!data.table::is.data.table(dt)) {
    cli::cli_abort("{.arg dt} must be a data.table.")
  }

  required_cols <- c("measure", "value", "flag")
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0L) {
    cli::cli_abort(
      c(
        "{.arg dt} must contain columns {.val measure}, {.val value}, and {.val flag}.",
        "i" = "Missing column{?s}: {.val {missing_cols}}."
      )
    )
  }

  out <- data.table::copy(dt)

  if (nrow(out) == 0L) {
    out[, value := as.character(value)]
    out[, flag := NULL]
    return(out)
  }

  formatted_val <- rep(NA_character_, nrow(out))

  # 1. NR masked cells
  is_nr <- !is.na(out$flag) & out$flag == "NR"
  formatted_val[is_nr] <- "NR"

  # 2. Finite numeric values (guard against NA, NaN, Inf, -Inf)
  val_num <- out$value
  is_finite <- !is_nr & !is.na(val_num) & is.finite(val_num)

  if (any(is_finite)) {
    meas <- out$measure

    idx_whole <- is_finite & (meas %chin% c("pop_poverty", "nobs"))
    idx_pct   <- is_finite & (meas %chin% .PCT_MEASURES)
    idx_other <- is_finite & !idx_whole & !idx_pct

    if (any(idx_whole)) {
      formatted_val[idx_whole] <- formatC(
        round(val_num[idx_whole]),
        format = "f",
        digits = 0L
      )
    }

    if (any(idx_pct)) {
      formatted_val[idx_pct] <- formatC(
        val_num[idx_pct] * 100,
        format = "f",
        digits = 2L
      )
    }

    if (any(idx_other)) {
      formatted_val[idx_other] <- formatC(
        val_num[idx_other],
        format = "f",
        digits = 2L
      )
    }
  }

  out[, value := formatted_val]
  out[, flag := NULL]
  out
}
