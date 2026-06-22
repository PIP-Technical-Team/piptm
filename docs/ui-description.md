# Table Maker: UI Description

## Overview

Table Maker is a tool that lets a user assemble a cross-tabulated summary table from household welfare survey data. The experience is structured as a three-step wizard. A stepper is displayed at the top of every screen, showing three labeled steps with a clear visual indication of which step is currently active and which steps have already been completed.

The three steps are:

- **Step 1:** "Which surveys do I want to analyze?"
- **Step 2:** "How do I want to measure, and how do I want to cut the data?"
- **Step 3:** "Here are my results."

Each step answers one question and only unlocks the next once that question is resolved. The user progresses from selecting data, to defining analysis and cuts, to viewing results.

---

## Global Rules and Constraints (Reflected Throughout the UI)

The following constraints are enforced across all steps and surfaced to the user through live counters and visual feedback:

- A user may select at most 15 surveys.
- A user may place at most one variable in each of the four layout slots, for a maximum of four layout variables in total.
- A user may select at most four statistics, which appear together inside every table cell.
- All statistics are sample-weighted automatically. No weighting control is exposed to the user.
- The poverty line is always expressed in international PPP dollars per day. No currency choice is offered.

---

## Step 1: Which Surveys Do I Want to Analyze?

Step 1 presents a matrix grid of all available surveys, allowing the user to select country, year, and welfare type combinations.

### Grid Structure

The grid is organized as follows:

- **Rows** are organized by country and welfare type (income or consumption). The tool uses 13 countries and years spanning 2008 to 2023, producing 16 year columns.
- The **first two columns** of the grid display the country name and the welfare type. A country that has surveys of both income and consumption types shows two rows — one for income and one for consumption. A country with only one welfare type shows a single row.
- Each **year cell** displays a checkbox when a survey exists for that country, year, and welfare type combination, and displays a dash when no survey exists for that combination.
- Every cell has a **tooltip** that describes variable availability, sample coverage, and relevant metadata for that observation.

### Toolbar (Above the Grid, Left to Right)

- A **country search box**.
- A **Region filter chip**.
- A **dual-handle year range slider** that narrows the visible year columns.
- An **"Available Variables" filter chip group**. Selecting one or more variables filters the grid to show only surveys that contain all of the selected variables.
- A **"Clear all" button**.
- A live **"N / 15 selected" counter**.

### Row and Header Behavior

- Each row has a **row checkbox** that selects every available year in that row. The row checkbox operates per welfare type row, not per country. A **selected-over-available count** is shown on the right side of each row.
- The **year header row** uses two-digit year labels. The first two digits are stacked above the last two digits to save horizontal space (for example, "20" stacked over "08" for the year 2008).
- Row backgrounds **alternate in shading** for readability.

### Footer

- The current selection count is displayed.
- A primary button labeled **"Continue to variables"** is shown. This button remains disabled until at least one survey is selected. The grid is the only way to leave Step 1.

---

## Step 2: How Do I Want to Measure, and How Do I Want to Cut the Data?

Step 2 presents three decisions in sequence. Each decision is revealed as the prior one is addressed. Once revealed, all three decisions remain visible and editable simultaneously.

### Decision 1: What Sample Base Do I Want to Analyze?

By default, the whole survey sample is used. The UI shows a list of all categorical variables available across the selected surveys. The user selects the variables they want to filter on. For each chosen variable, its subcategories are shown with every subcategory selected by default, allowing the user to deselect subcategories they wish to exclude.

A variable used here as a filter may also be used later as a layout slot in Decision 3. This can produce empty categories in the final table, which is acceptable expected behavior.

**Worked example supported by the UI:** the user picks "Secondary education completed", which has subcategories "completed" and "not completed", and keeps only "not completed". The user also picks "Age group" and keeps only "20 to 65". The resulting sample base is the intersection of those choices.

### Decision 2: Which Statistics Do I Want to Compute?

The user chooses a single analysis variable from a **searchable dropdown**. Once a variable is chosen, a **grouped statistic dropdown** appears. The user selects up to four statistics of that one variable.

Selected statistics appear as **removable chips** in a selected statistics panel. The chips can be **reordered by dragging**. Because there is one analysis variable, every chip reads as the statistic applied to that variable (for example: Mean(Income), Median(Income), Gini(Income)).

**Grouped statistic dropdown contents:**

- *Shares and Counts:* Share (%)
- *Summary Statistics:* Mean, Median, Sum, Standard Deviation
- *Inequality:* Gini, Theil, Palma Ratio

**Statistic gating by variable type:**

- For a **continuous variable** (such as income or age), the full set of statistics above is available.
- For a **binary variable** (such as primary completion or employment status), only Share (%) is available.
- For **poverty status**, a **poverty line slider** expressed in PPP dollars per day is revealed. The available statistics become poverty rate, poverty gap, and poverty severity, which depend on the poverty line value.

Statistics that do not apply to the chosen variable are shown as **disabled**, with a short tooltip explaining why.

Deciles, quantiles, and named percentiles are not offered as statistics, as they are precalculated at the national level in the source data. Welfare quintiles appear only as a precalculated category available for filtering in Decision 1 and as a layout slot in Decision 3.

### Decision 3: How Do I Want to Slice the Table?

**Header text:** "How to slice the table."  
**Helper text:** "Drag covariates into the four role slots. The preview updates as you assign."

The layout, from left to right, consists of:

**An "Available" column** listing draggable covariates. The list includes items such as: Gender, Education level, Urban / Rural, Geographic region, Age group, Household size, Employment status. When a covariate has been dropped into a slot, it remains visible in this list but is **greyed out** with an **"in use" tag**.

**Four labeled role slots** arranged as a small grid:

- Slot labeled **"1 Columns"**
- Slot labeled **"2 Rows"**
- Slot labeled **"3 Super Columns"**
- Slot labeled **"4 Super Rows"**

Each slot accepts at most one covariate. An empty slot displays faint **"drop here"** placeholder text. An assigned slot shows the covariate name and a small count of its categories on the right (for example, Gender shows 2, and Education level shows 4).

A **click-based or keyboard-based alternative** to dragging is also provided, so that slot assignment is accessible.

**Live table skeleton preview:** A live preview of the resulting table structure updates as covariates are assigned to slots. Two caption lines beneath the preview update live:

- A **cell definition line**, for example: "cell = poverty rate @ $6.8/day" — naming the active statistic and, when relevant, the poverty line.
- A **filter line**, for example: "filtered to Secondary education: Not completed" — restating the active sample base filters from Decision 1.

When one statistic is selected, a small note reads "× 1 value per cell", and the number updates with the count of selected statistics.

**Footer:** A primary button labeled **"Generate table."** A table can be generated even when no covariate is assigned to any slot. In that case, the statistics are estimated at the national level and the table is a single cell per statistic.

**Regarding poverty status:** Poverty status is treated distinctly throughout the tool. If poverty status is selected in Decision 2, it is disabled in Decision 3, and vice versa. If poverty status is selected in either decision, the poverty line slider becomes available to the user.

---

## Step 3: Here Are My Results

Step 3 displays the completed table.

When multiple surveys were selected, each survey has its own **tab**, and the user can flip between tabs to compare results. Each tab is a self-contained table for that one survey, so income-based and consumption-based surveys never share a cell.

### Summary Rail (Left Side)

A summary rail on the left reminds the user of every configuration choice made:

- The analysis variable
- The selected statistics
- The poverty line, if relevant
- The four slot assignments
- The selected surveys

### Result Area

- A **"measure" chip control** lets the user choose which of the selected statistics are currently displayed inside the cells — one or many at a time.
- **Export actions** available: CSV, XLSX, and copy.
- A way to **go back and edit the configuration** is provided, returning the user to the relevant earlier step with all their choices preserved.

---

## Table Layout Semantics

The four layout slots map onto the rendered table as follows. This mapping is reflected both in the Step 2 live preview and in the Step 3 result table.

- **Columns** is the innermost column split. Its categories become the bottom row of column headers.
- **Super Columns** is an outer column band sitting above Columns. Each Super Columns category spans the full set of Columns categories beneath it.
- **Rows** carries the left-side row labels, one row per category.
- **Super Rows** is an outer row band sitting beside Rows, grouping rows into blocks.

Every intersection cell lists the selected statistics together. The statistics are not nested into extra rows or columns — they sit inside the cell as a short stacked list.

**Concrete example rendered in the preview:**

- Super Columns = Urban / Rural → top band reads: Urban, then Rural
- Columns = Gender → beneath each of Urban and Rural, headers read: Female, then Male
- Rows = Education level → left labels read: None, Primary, Secondary, Tertiary
- Super Rows = empty

Each cell shows the selected statistics. With one statistic selected, the preview shows a small note reading "× 1 value per cell", and the number updates with the count of selected statistics.

---

## UI States and Microcopy

The following states are visible across the flow:

- The **"N / 15 selected" counter** at and near the selection cap, including a disabled state when a further selection would exceed 15.
- A slot in the **"drop here" empty state** and a slot in the **assigned state** showing a covariate name and category count.
- A covariate **greyed with an "in use" tag** once it has been assigned to a slot.
- A statistic shown as **disabled with an explanatory tooltip** because it does not apply to the chosen variable.
- The **poverty line slider** appearing only after poverty status is chosen as the analysis variable (in either Decision 2 or Decision 3).
- The **Step 3 measure chip** toggling which statistics appear inside the cells.

---

