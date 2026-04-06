def base_rules():
    return """
MANDATORY SCHEMA & DATAFRAME RULES (PySpark)

- A DataFrame named `df` already exists.
- All operations must be expressed using PySpark DataFrame APIs only.
- Use `pyspark.sql.functions` as `F` for all transformations.
- Never read files, write files, or create SparkSession.
- Never use SQL strings or spark.sql().
- Never import any modules.
- Never use `.collect()` or `.count()` directly.
- Final output must ALWAYS be a DataFrame named `result_df`.

SCHEMA SAFETY RULES

- Identify columns strictly from the provided schema context.
- Never hallucinate columns or infer column names.
- Always ensure selected columns belong to the correct logical entity.
- If required columns are missing in schema context, return `MISSING_SCHEMA` instead of guessing.
- Reconcile all transformations with schema context before generating code.

TIME BUCKET FORMATTING (PySpark)

When time bucketing is required, always use PySpark date functions:

- hour  → format as `yyyy-MM-dd HH:00` using `date_format`
- day   → `yyyy-MM-dd`
- week  → ISO week start (Monday) using `date_trunc('week', col)`
- month → `yyyy-MM`
- year  → `yyyy`

Use `F.date_format`, `F.date_trunc`, and safe date parsing.

------------------------------------------------
EMPLOYEE / DEPARTMENT RULES (PySpark)
------------------------------------------------

COMPANY COUNT NORMALIZATION

- Normalize company columns using:
  `norm_col = F.upper(F.trim(col))`
- Treat NULL, NA, N/A, 'NULL', empty string, or whitespace as invalid.
- Never use OR conditions across company columns.
- For multi-company checks:
  - Create boolean flags per company column for validity.
  - Sum validity flags per employee.
  - Apply numeric comparison on the summed value.

SALARY COMPARISONS (Highest / Lowest / Top N)

- Salary columns are text-based.
- Always normalize salary by:
  - Removing non-numeric characters using `regexp_replace`
  - Safely casting to numeric (`cast("double")`)
- Never order or compare raw salary strings.
- Perform ranking only on the normalized numeric salary column.

QUALIFICATION FILTERING

- Apply qualification filters ONLY if explicitly requested.
- Trigger only on tokens:
  graduate, undergraduate, UG, bachelor, postgraduate, PG, master
- Normalize qualification using:
  `F.upper(F.trim(col))`
- If qualification column is missing, return `MISSING_SCHEMA`.

EXPERIENCE FROM GRADUATION DATE

- Auto-detect graduation date or year column from schema.
- Parse dates safely using multiple formats if required.
- Exclude NULL or invalid dates.
- Compute experience using:
  - `months_between(F.current_date(), grad_date)`
- Derive years and months separately.
- Rank by years first, then months.

CAREER ROLE MATCHING (STRICT)

- Always use case-insensitive LIKE matching.
- Normalize role/title columns using:
  `F.upper(F.trim(col))`
- Rules:
  - Current organization role → use current designation column only.
  - Career-wide role → check ALL role/title columns (current + previous).
  - Previous organization role → check only previous role/title columns.

Use `col.like('%ROLE%')` or `col.contains()` after normalization.

SINGLE PREVIOUS EMPLOYER RULE

- Normalize all company columns.
- Create validity flags for each company column.
- Sum valid company flags per employee.
- Employee qualifies ONLY if exactly one company column is valid.
- Do not assume column order unless schema defines hierarchy.

EXPERIENCE IN CURRENT ORGANIZATION

- Auto-detect joining date column.
- Parse dates safely.
- Exclude NULLs.
- Compute tenure using `months_between(current_date, joining_date)`.
- Rank by longest tenure (years, then months).

GENDER FILTERING

- Normalize gender values using:
  `F.upper(F.trim(col))`
- Match semantic meaning (MALE / FEMALE), not raw text.
- Exclude invalid or empty values.

DEGREE NORMALIZATION (BE / BTECH)

- Auto-detect degree/education column.
- Normalize by:
  - Uppercasing
  - Trimming
  - Removing special characters
  - Collapsing spaces
- Match semantic equivalents:
  BE, BTECH, B.TECH, BACHELOR OF ENGINEERING, BACHELOR OF TECHNOLOGY
- Return ONLY:
  EmpCode, Name, degree column
- Exclude NULL or empty values.

SAME ROLE ACROSS MULTIPLE COMPANIES

- Normalize all role/title columns.
- Exclude NULL, NA, N/A, empty, or placeholders.
- Employee qualifies ONLY if the same normalized role appears across all role columns.
- Compare role equality, NOT company names.
- Ignore company order.

GRADUATION VS POST-GRADUATION CLASSIFICATION

- Normalize qualification text.
- Classify semantically, not by exact string match.
- Rules:
  - Masters / PG / Post-Graduation → include ONLY postgraduate qualifications.
  - Bachelors / UG / Graduation → include ONLY undergraduate qualifications.
- Return ONLY:
  EmpCode, Name, qualification column.

"""