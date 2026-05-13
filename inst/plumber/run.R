# Convenience launcher for the piptm Plumber API
#
# Usage:
#   Rscript inst/plumber/run.R
#   Rscript inst/plumber/run.R --port 8888
#
# Environment variables (optional — override defaults):
#   PIPTM_API_PORT  : port to listen on (default: 8080)
#   PIPTM_API_HOST  : host to bind to   (default: 0.0.0.0)

if (!requireNamespace("piptm", quietly = TRUE)) {
  stop(
    "Package 'piptm' is not installed. ",
    "Run `pak::pak('PIP-Technical-Team/piptm')` or `devtools::load_all()` first."
  )
}

library(piptm)

port <- as.integer(Sys.getenv("PIPTM_API_PORT", unset = "8080"))
host <- Sys.getenv("PIPTM_API_HOST", unset = "0.0.0.0")

piptm::run_api(port = port, host = host)
