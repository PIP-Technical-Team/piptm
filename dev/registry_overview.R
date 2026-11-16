# Dev tool: HTML overview of the Table Maker catalogue as the endpoints serve it.
# Usage (from package root): Rscript dev/registry_overview.R [release]
#
# Content comes from the functions the plumber endpoints call, so the page
# matches GET /analysis-variables, GET /categories, GET /covariates, and
# GET /statistics. The raw registry JSON is not walked: jsonlite boxes every
# scalar as a length-1 array, and measure_spec sits beside the variables.
suppressMessages(devtools::load_all(".", quiet = TRUE))
args <- commandArgs(trailingOnly = TRUE)
rel <- if (length(args) && nzchar(args[1])) args[1] else NULL

analysis    <- piptm_analysis_variables(rel)
filters     <- piptm_filter_categories(rel)
covariates  <- piptm_layout_covariates(rel)
stat_groups <- piptm_stat_groups(rel)

s <- function(x) {
  if (is.null(x) || length(x) == 0L) return("")
  paste(as.character(unlist(x, use.names = FALSE)), collapse = ", ")
}
esc <- function(x) {
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  gsub(">", "&gt;", x, fixed = TRUE)
}
shown_release <- if (is.null(rel)) piptm_current_release() else rel

group_label <- stats::setNames(
  vapply(stat_groups, function(g) s(g$group_label), ""),
  vapply(stat_groups, function(g) s(g$group), "")
)
group_chip <- function(keys) {
  keys <- as.character(unlist(keys, use.names = FALSE))
  if (!length(keys)) return("<span class='none'>none</span>")
  paste(vapply(keys, function(k) {
    lab <- group_label[[k]]
    if (is.null(lab) || !nzchar(lab)) lab <- k
    sprintf("<span class='chip' title='%s'>%s</span>", esc(k), esc(lab))
  }, ""), collapse = " ")
}
type_badge <- function(type) {
  sprintf("<span class='t t-%s'>%s</span>", esc(s(type)), esc(s(type)))
}

cat_rows <- function(items) {
  paste(vapply(items, function(k)
    sprintf("<tr><td><code>%s</code></td><td>%s</td></tr>",
            esc(s(k$code)), esc(s(k$label))), ""), collapse = "")
}

filter_cards <- vapply(filters, function(e) {
  vn <- s(e$varname)
  body <- cat_rows(e$subcategories)
  if (!nzchar(body)) body <- "<tr><td colspan='2' class='none'>no categories</td></tr>"
  sprintf(
    "<div class='card' id='%s'><h3><code>%s</code> &mdash; %s</h3><table class='small'><tr><th>Code</th><th>Label</th></tr>%s</table></div>",
    esc(vn), esc(vn), esc(s(e$label)), body
  )
}, "")

mrows <- paste(unlist(lapply(stat_groups, function(g) {
  measures <- g$measures
  if (is.null(measures) || !length(measures)) {
    return(sprintf(
      "<tr><td>%s</td><td colspan='2'><code>%s</code></td></tr>",
      esc(s(g$group_label)), esc(s(g$group))
    ))
  }
  vapply(measures, function(m)
    sprintf("<tr><td>%s</td><td><code>%s</code></td><td>%s</td></tr>",
            esc(s(g$group_label)), esc(s(m$measure)), esc(s(m$label))), "")
})), collapse = "")

html <- paste0(
  "<!doctype html><meta charset='utf-8'><title>piptm registry</title><style>
body{font-family:Segoe UI,Arial,sans-serif;margin:24px;color:#222;background:#f6f7f9}
h1{margin-bottom:4px}h2{margin-top:28px}table{border-collapse:collapse;background:#fff;margin:8px 0 24px}
th,td{border:1px solid #d5d9df;padding:5px 10px;text-align:left;font-size:13px;vertical-align:top}th{background:#0b4f8a;color:#fff}
tr:nth-child(even) td{background:#f3f6fa}.n{text-align:right}.small{min-width:360px}
.card{background:#fff;border:1px solid #d5d9df;border-radius:6px;padding:8px 14px;margin:10px 12px 10px 0;display:inline-block;vertical-align:top;min-width:380px}
.card table{margin:6px 0}.meta{font-size:12px;color:#666}.none{color:#999;font-style:italic}
.chip{display:inline-block;font-size:11px;padding:1px 7px;margin:1px 2px 1px 0;border-radius:9px;background:#e7eef6;color:#0b4f8a;border:1px solid #c5d4e6}
.t{font-size:11px;padding:2px 7px;border-radius:9px;color:#fff;background:#777}
.t-categorical{background:#2a9d8f}.t-binary{background:#e9a23b}.t-continuous{background:#4c78a8}.t-welfare{background:#8e5ea2}.t-poverty{background:#d1495b}
</style><h1>Table Maker catalogue</h1><div class='meta'>Release: ", esc(shown_release),
  " &mdash; ", length(analysis), " analysis variables, ", length(filters),
  " sample-base filters, ", length(covariates), " layout covariates, ",
  length(stat_groups), " statistic groups. Sourced from the functions behind ",
  "<code>/analysis-variables</code>, <code>/categories</code>, ",
  "<code>/covariates</code>, and <code>/statistics</code>.</div>",

  "<h2>Analysis variables <span class='meta'>/analysis-variables</span></h2>",
  "<table><tr><th>Variable</th><th>Label</th><th>Type</th><th>Stat groups</th></tr>",
  paste(vapply(analysis, function(e)
    sprintf("<tr><td><code>%s</code></td><td>%s</td><td>%s</td><td>%s</td></tr>",
            esc(s(e$varname)), esc(s(e$label)), type_badge(e$type),
            group_chip(e$stat_groups)), ""), collapse = ""),
  "</table>",

  "<h2>Sample-base filters <span class='meta'>/categories</span></h2>",
  "<table><tr><th>Variable</th><th>Label</th><th>#Categories</th></tr>",
  paste(vapply(filters, function(e)
    sprintf("<tr><td><a href='#%s'><code>%s</code></a></td><td>%s</td><td class='n'>%s</td></tr>",
            esc(s(e$varname)), esc(s(e$varname)), esc(s(e$label)),
            length(e$subcategories)), ""), collapse = ""),
  "</table>", paste(filter_cards, collapse = ""),

  "<h2>Layout covariates <span class='meta'>/covariates</span></h2>",
  "<table><tr><th>Variable</th><th>Label</th><th>#Categories</th></tr>",
  paste(vapply(covariates, function(e)
    sprintf("<tr><td><code>%s</code></td><td>%s</td><td class='n'>%s</td></tr>",
            esc(s(e$varname)), esc(s(e$label)), esc(s(e$n_categories))), ""),
    collapse = ""),
  "</table>",

  "<h2>Statistics <span class='meta'>/statistics</span></h2>",
  "<table><tr><th>Group</th><th>Measure key</th><th>Label</th></tr>",
  mrows, "</table>"
)

out <- file.path(Sys.getenv("TEMP"), "piptm_registry_overview.html")
writeLines(html, out, useBytes = TRUE)
cat(out, "\n")
