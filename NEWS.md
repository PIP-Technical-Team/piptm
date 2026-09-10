# piptm (development version)

## piptm 0.1.0.9000

### Breaking changes

- `table_maker()` now requires `analysis_var` as an explicit input parameter.
- `table_maker()` and `/table` use scalar `poverty_line` (singular) instead of repeatable `poverty_lines`.
- `/table` input validation now enforces `poverty_line` when `analysis_var = "pov_status"` or when `"pov_status"` is present in `by`.
