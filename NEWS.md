# piptm (development version)

## piptm 0.1.0.9000

### Breaking changes

- `table_maker()` now requires `analysis_var` as an explicit input parameter.
- `table_maker()` and `/table` use scalar `poverty_line` (singular) instead of repeatable `poverty_lines`.
- `/table` input validation now enforces `poverty_line` when `analysis_var = "pov_status"` or when `"pov_status"` is present in `by`.
- `table_maker()` replaces row-dropping cell suppression with cell-level masking (`value = NA`, `flag = "NR"`) at default `pop_share_threshold = 0.001` (0.1%), and removes the `population` column.
- The `/table` API endpoint now returns `value` as formatted strings via `format_table_output()`, with percentage measures scaled by 100 with 2 decimal places, whole-number counts, `"NR"` for masked cells, and `null` for genuine missing values. The `flag` and `population` columns are not included in `/table` response data.
- Added `format_table_output()` to format `table_maker()` data.tables according to the API presentation contract.
