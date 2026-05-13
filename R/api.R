# API launch helpers
#
# Plan: .cg-docs/plans/2026-05-11-api-service-plumber-v2.md (Step 5)
#
# Exports:
#   run_api()  — start the Plumber API server

#' Start the piptm Plumber API server
#'
#' Loads the Plumber router from `inst/plumber/plumber.R` and starts
#' listening for HTTP requests.  This is a blocking call — the function
#' does not return until the server is stopped (Ctrl+C / SIGINT).
#'
#' The router exposes eight endpoints:
#'
#' | Endpoint | Method | Description |
#' |---|---|---|
#' | `/table` | GET, POST | Compute poverty / inequality / welfare measures |
#' | `/lookup` | GET | Resolve country-year-welfare triplets to pip_ids |
#' | `/surveys` | GET | Full manifest rows for a release |
#' | `/releases` | GET | List all loaded release IDs and current release |
#' | `/measures` | GET | List measure names and computation families |
#' | `/dimensions` | GET | List valid disaggregation dimension names |
#' | `/health` | GET | Server health check |
#'
#' All responses follow a structured envelope:
#' `{ status, data, warnings, errors, meta }`.
#'
#' The server must be able to reach the Arrow repository and manifest
#' directory configured via [set_arrow_root()] / [set_manifest_dir()] or the
#' `PIPTM_ARROW_ROOT` / `PIPTM_MANIFEST_DIR` environment variables.
#'
#' @param port Integer. TCP port to listen on. Defaults to `8080`. Override
#'   at deployment time via the `PIPTM_API_PORT` environment variable:
#'   `port = as.integer(Sys.getenv("PIPTM_API_PORT", unset = "8080"))`.
#' @param host Character scalar. IP address to bind to. Defaults to
#'   `"0.0.0.0"` (all interfaces). Use `"127.0.0.1"` to restrict to
#'   localhost only.
#' @param quiet Logical. When `TRUE`, suppresses the plumber startup banner.
#'   Defaults to `FALSE`.
#'
#' @return Called for its side effect (blocking HTTP server). Returns
#'   invisibly when the server shuts down.
#'
#' @section Known limitations (v1):
#' - Single-threaded: concurrent requests queue behind each other. Response
#'   time target is ≤3 seconds for 15 surveys; larger batches may exceed
#'   this.
#' - No authentication or rate limiting.
#' - No multi-worker / load balancing support.
#'
#' @family api
#' @export
#' @examples
#' \dontrun{
#' # Start on the default port (8080):
#' piptm::run_api()
#'
#' # Start on a custom port, localhost only:
#' piptm::run_api(port = 9000L, host = "127.0.0.1")
#'
#' # Or use the command-line launcher:
#' # Rscript inst/plumber/run.R
#' }
run_api <- function(port = 8080L, host = "0.0.0.0", quiet = FALSE) {
  if (!requireNamespace("plumber", quietly = TRUE)) {
    cli::cli_abort(
      c(
        "Package {.pkg plumber} is required to start the API server.",
        "i" = "Install it with {.code install.packages(\"plumber\")}."
      )
    )
  }

  plumber_file <- system.file("plumber", "plumber.R", package = "piptm")
  if (!nzchar(plumber_file)) {
    cli::cli_abort(
      c(
        "Could not locate {.path inst/plumber/plumber.R}.",
        "i" = "Ensure {.pkg piptm} is installed or loaded via {.code devtools::load_all()}."
      )
    )
  }

  pr <- plumber::plumb(plumber_file)
  pr$run(host = host, port = as.integer(port), quiet = quiet)

  invisible(NULL)
}
