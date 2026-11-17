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
  "<!doctype html><meta charset='utf-8'><title>Table Maker Catalogue</title><style>
:root {
  --default-font: \"Open Sans\", system-ui, -apple-system, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial, sans-serif;
  --background-color: #F7F8F8;
  --default-color: #1a1f24;
  --heading-color: #044368;
  --muted-color: #485664;
  --accent-color: #0ea2bd;
  --primary-blue: #00689D;
  --surface-color: #ffffff;
  --tint-color: #e8f2f8;
  --border-color: #dee2e6;
}
body {
  font-family: var(--default-font);
  margin: 0;
  padding: 24px 16px;
  color: var(--default-color);
  background: var(--background-color);
  line-height: 1.5;
}
.pip-catalogue-card {
  max-width: 1120px;
  margin: 0 auto;
  background: var(--surface-color);
  border: 1px solid var(--border-color);
  border-radius: 6px;
  padding: 28px 32px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
}
.pip-catalogue-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 12px;
  margin: 0 0 12px 0;
  padding: 0 0 14px 0;
  border-bottom: 2px solid var(--primary-blue);
}
h1 {
  margin: 0;
  font-size: 24px;
  font-weight: 700;
  color: var(--heading-color);
  letter-spacing: -0.01em;
}
h2 {
  margin: 28px 0 12px 0;
  padding-left: 10px;
  border-left: 3px solid var(--accent-color);
  font-size: 18px;
  font-weight: 600;
  color: var(--heading-color);
  line-height: 1.35;
}
h3 {
  margin: 4px 0 8px 0;
  font-size: 14px;
  font-weight: 600;
  color: var(--heading-color);
}
table {
  border-collapse: collapse;
  background: var(--surface-color);
  margin: 8px 0 24px;
  width: 100%;
  font-size: 14px;
}
th, td {
  border: 1px solid var(--border-color);
  padding: 8px 12px;
  text-align: left;
  font-size: 13px;
  vertical-align: top;
}
th {
  background: var(--tint-color);
  color: var(--heading-color);
  border-bottom: 2px solid var(--primary-blue);
  font-weight: 600;
}
tr:nth-child(even) td {
  background: var(--background-color);
}
tr:hover td {
  background: var(--tint-color);
}
.n {
  text-align: right;
}
.small {
  min-width: 340px;
}
.card {
  background: var(--surface-color);
  border: 1px solid var(--border-color);
  border-top: 3px solid var(--primary-blue);
  border-radius: 6px;
  padding: 10px 14px;
  margin: 10px 12px 10px 0;
  display: inline-block;
  vertical-align: top;
  min-width: 360px;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
}
.card table {
  margin: 6px 0;
}
.meta {
  font-size: 13px;
  color: var(--muted-color);
  font-weight: 400;
}
.none {
  color: #8b95a1;
  font-style: italic;
}
a {
  color: var(--primary-blue);
  text-decoration: none;
}
a:hover {
  text-decoration: underline;
}
code {
  background: var(--tint-color);
  color: var(--heading-color);
  padding: 1px 5px;
  border-radius: 4px;
  font-size: 12px;
}
.chip {
  display: inline-block;
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  margin: 1px 3px 1px 0;
  border-radius: 10px;
  background: var(--tint-color);
  color: var(--heading-color);
  border: 1px solid #b8d8e8;
}
.t {
  display: inline-block;
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 10px;
  color: #fff;
  background: var(--muted-color);
}
.t-categorical { background: #0ea2bd; }
.t-binary      { background: #00689D; }
.t-continuous  { background: #044368; }
.t-welfare     { background: #1b6ca8; }
.t-poverty     { background: #b85d19; }
</style><div class='pip-catalogue-card'><div class='pip-catalogue-header'><h1>Table Maker Catalogue</h1>",
  "<a href='https://pip.worldbank.org/home' target='_blank' rel='noopener noreferrer' style='display:inline-flex;align-items:center;'>",
  "<img src='https://pip.worldbank.org/assets/WB-PIP-E-horizontal-RGB-high.png' alt='World Bank PIP' style='width:260px;max-width:100%;height:auto;border:0;' /></a></div>",
  "<div class='meta'>Release: <b>", esc(shown_release),
  "</b> &mdash; ", length(analysis), " analysis variables, ", length(filters),
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
  mrows, "</table></div>"
)

out <- file.path(Sys.getenv("TEMP"), "piptm_registry_overview.html")
writeLines(html, out, useBytes = TRUE)
cat(out, "\n")
