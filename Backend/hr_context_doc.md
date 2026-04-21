# HR Domain — Semantic Layer for LLM-Driven Analytics

> **Purpose:** This document is a domain-level semantic layer for the Human Resources (HR) function. It is uploaded alongside HR datasets so that an LLM can generate accurate PySpark queries, produce meaningful analytical insights, and recommend relevant charts — without inventing meanings, making unsafe assumptions, or producing misleading conclusions.
>
> This document is **dataset-agnostic**. It defines HR domain concepts, business logic, safe reasoning patterns, and guardrails that apply regardless of which specific HR dataset is provided.

---

## Table of Contents

1. [Domain Summary](#1-domain-summary)
2. [Core Entities](#2-core-entities)
3. [Measurable Fields (Metrics)](#3-measurable-fields-metrics)
4. [Dimensions (Grouping Fields)](#4-dimensions-grouping-fields)
5. [Business Definitions](#5-business-definitions)
6. [Policies and Thresholds](#6-policies-and-thresholds)
7. [KPIs and How to Compute Them](#7-kpis-and-how-to-compute-them)
8. [Aggregation Rules](#8-aggregation-rules)
9. [Interpretation Rules](#9-interpretation-rules)
10. [Cause-and-Effect Relationships](#10-cause-and-effect-relationships)
11. [Executive Priorities](#11-executive-priorities)
12. [Preferred Comparison Patterns](#12-preferred-comparison-patterns)
13. [Time-Based Analysis Rules](#13-time-based-analysis-rules)
14. [Chart and Visualization Guidance](#14-chart-and-visualization-guidance)
15. [Red Flag Indicators](#15-red-flag-indicators)
16. [Safe vs. Unsafe Assumptions](#16-safe-vs-unsafe-assumptions)
17. [Role-Specific Interpretation](#17-role-specific-interpretation)
18. [Guardrails and Hard Rules](#18-guardrails-and-hard-rules)
19. [Metric Dependency Rules](#19-metric-dependency-rules)
20. [Fallback Rules When Required Data Is Missing](#20-fallback-rules-when-required-data-is-missing)
21. [Synonym Mapping](#21-synonym-mapping)
22. [Confidence Levels for Inference](#22-confidence-levels-for-inference)
23. [Unsupported Analysis Rules](#23-unsupported-analysis-rules)
24. [Default Time Windows](#24-default-time-windows)
25. [Join Priority Rules](#25-join-priority-rules)

---

## 1. Domain Summary

**Domain:** Human Resources (HR) Analytics

HR analytics focuses on understanding workforce composition, performance, compensation, attrition, hiring, attendance, and development — with the goal of supporting data-driven decisions that affect people, costs, and organizational effectiveness.

Analysis in this domain typically answers three levels of questions:

| Level | Question Type | Example |
|---|---|---|
| **Descriptive** | What happened? | What is the attrition rate by department this year? |
| **Diagnostic** | Why did it happen? | Which departments have high attrition and low promotion rates? |
| **Prescriptive** | What should we do? | Which roles are at succession risk and need immediate pipeline development? |

The LLM should always attempt to answer at the highest level the data supports, without overreaching beyond what is available.

---

## 2. Core Entities

These are the primary objects in the HR domain. When generating queries, these entities are typically rows in fact or dimension tables.

| Entity | Description | Typical Identifier |
|---|---|---|
| **Employee** | An individual on the organization's payroll or workforce | `employee_id` |
| **Department** | An organizational unit grouping employees by function | `department`, `department_id` |
| **Manager** | An employee who directly supervises other employees | `manager_id`, `manager_name` |
| **Job Role / Designation** | The title or role classification of an employee | `designation`, `job_role`, `job_title` |
| **Business Unit** | A larger organizational division containing multiple departments | `business_unit` |
| **Location** | Geographic office or region where the employee is based | `location`, `office_location` |
| **Hiring Source** | The channel through which an employee was recruited | `hiring_source`, `source_of_hire` |
| **Cost Center** | The financial unit an employee's salary is charged to | `cost_center` |

> **Rule:** `employee_id` is a unique identifier. It must **never** be aggregated (SUM, AVG, COUNT DISTINCT as a measure). It is used only for row-level filtering, joins, or headcount counting via `COUNT(*)`.

---

## 3. Measurable Fields (Metrics)

These are numeric fields that can be aggregated. Each field has a defined aggregation method and interpretation context.

| Field | Typical Column Names | Default Aggregation | Unit | Notes |
|---|---|---|---|---|
| **Salary** | `salary`, `basic_salary`, `ctc` | AVG or SUM | Currency | Use AVG for comparisons; SUM for total payroll cost |
| **Bonus** | `bonus`, `bonus_amount` | AVG or SUM | Currency | Often a percentage of salary; context-dependent |
| **Incentives** | `incentive`, `variable_pay` | AVG or SUM | Currency | Common in sales roles; may distort averages if not segmented |
| **Experience** | `total_experience`, `years_of_experience` | AVG | Years | Often pre-hire experience; distinguish from tenure |
| **Tenure** | `tenure`, `years_at_company`, `months_at_company` | AVG | Years or Months | Time since joining the current organization |
| **Attendance** | `attendance_percentage`, `attendance_rate` | AVG | Percentage (%) | Never SUM; averaging across employees is correct |
| **Overtime Hours** | `overtime_hours`, `ot_hours` | AVG or SUM | Hours | SUM for total load; AVG per employee for workload analysis |
| **Performance Score** | `performance_score`, `performance_rating` | AVG | Score or Rating | Distribution analysis (e.g., histogram) is often more useful than average alone |
| **Training Hours** | `training_hours`, `training_completed` | AVG or SUM | Hours | SUM for total investment; AVG for per-employee effort |
| **Attrition Count** | `attrition`, `exited`, `is_attrition` | COUNT or SUM | Count | Binary flag (1 = exited) or categorical; never AVG |
| **Headcount** | Derived from COUNT(*) | COUNT | Count | Total active employees in a segment |
| **Promotion Count** | `promoted`, `is_promoted` | COUNT or SUM | Count | Binary flag indicating if promoted in the review period |
| **Salary Hike %** | `salary_hike`, `hike_percentage` | AVG | Percentage (%) | Analyze by department and designation for fairness |

---

## 4. Dimensions (Grouping Fields)

Dimensions are categorical fields used to segment and filter data. These are the primary axes for comparison and drill-down.

| Dimension | Typical Column Names | Description |
|---|---|---|
| **Department** | `department`, `dept_name` | Primary segmentation for most HR metrics |
| **Designation / Job Level** | `designation`, `job_level`, `grade` | Used to compare within a career level |
| **Gender** | `gender` | Diversity and pay equity analysis |
| **Location** | `location`, `city`, `region` | Geographic workforce distribution |
| **Employment Type** | `employment_type` | Full-time, part-time, contract, etc. |
| **Joining Year / Cohort** | `joining_year`, `joining_date` | Hiring trend and cohort analysis |
| **Manager** | `manager_name`, `manager_id` | Manager-level performance benchmarking |
| **Business Unit** | `business_unit`, `bu` | Cross-unit comparison. Note: `division` is ambiguous — see §21 for resolution logic |
| **Hiring Source** | `source_of_hire` | Evaluating recruitment channel effectiveness |
| **Age Band** | `age_group`, `age_band` | Generational workforce segmentation |
| **Performance Band** | `performance_band` | High / Mid / Low performer segmentation |
| **Attrition Status** | `attrition`, `is_active`, `employment_status` | Active vs. exited employees |
| **Tenure Band** | Derived: e.g., `< 1 yr`, `1–3 yrs`, `3–5 yrs`, `5+ yrs` | Career stage segmentation |

---

## 5. Business Definitions

These are the authoritative definitions for HR terms. The LLM must use these definitions — not general-purpose interpretations — when analyzing HR data.

### Attrition Types

| Term | Definition |
|---|---|
| **Total Attrition** | All employees who left the organization in a period, regardless of reason |
| **Voluntary Attrition** | Employees who resigned of their own accord |
| **Involuntary Attrition** | Employees terminated, laid off, or asked to leave by the organization |
| **Regrettable Attrition** | Voluntary exits of employees the organization wanted to retain — typically high performers or critical roles |
| **Non-Regrettable Attrition** | Exits of low performers or poor cultural fits, considered acceptable or healthy |

> **Critical Rule:** Do not assume all attrition is negative. Non-regrettable attrition may be a sign of healthy performance management.

### Talent Classification

| Term | Definition |
|---|---|
| **High Performer** | An employee whose performance score falls in the top tier (e.g., top 20% or rating of 4–5 on a 5-point scale) |
| **Critical Talent** | Employees in roles that are hard to replace due to rare skills, domain expertise, or strategic importance — irrespective of current performance rating |
| **Bench Strength** | The number of ready-now or near-ready successors available for key roles |
| **Succession Risk** | A role or position where no qualified internal successor exists if the current holder exits |
| **Leadership Pipeline** | The pool of employees being groomed for future leadership or management responsibilities |

### Compensation

| Term | Definition |
|---|---|
| **Salary Compression** | When pay differences between levels (e.g., junior vs. senior) are smaller than expected, often due to market salary inflation for new hires outpacing internal raises |
| **Compensation Band** | The defined salary range (minimum, midpoint, maximum) for a given designation or job level |
| **Pay Equity** | Equal pay for equivalent roles, experience, and performance — analyzed across gender, location, or other dimensions |
| **Variable Pay** | Non-fixed compensation such as bonuses, incentives, and commissions — common in sales and executive roles |

### Workforce Metrics

| Term | Definition |
|---|---|
| **Internal Mobility** | Movement of employees within the organization — lateral moves, promotions, or role changes |
| **Employee Engagement** | A measure of how emotionally invested an employee is in their work and organization; typically collected via surveys |
| **Workforce Productivity** | Output or value generated per employee — often approximated using revenue per headcount when revenue data is available |
| **Span of Control** | The number of direct reports a manager is responsible for |
| **Time to Fill** | The number of days between a job opening and an offer acceptance |
| **Offer Acceptance Rate** | Percentage of offers extended that are accepted by candidates |

---

## 6. Policies and Thresholds

These are common organizational rules that govern HR decisions. When specific thresholds are not provided in the dataset or prompt, use these as default reasoning guidelines — and flag when actual policy values should be confirmed.

### Promotion Eligibility
- Employees are typically eligible for promotion after a minimum tenure in their current designation (commonly 12–24 months).
- High performers are usually prioritized for promotion in review cycles.
- Promotion without performance improvement may indicate process gaps.

### Performance Ratings
- Performance scores typically follow a scale (e.g., 1–5, or bands: Below Expectations / Meets Expectations / Exceeds Expectations).
- If a 5-point scale is used: 1–2 = Low, 3 = Average, 4–5 = High.
- Forced distribution policies may cap the percentage of top-rated employees per department.

### Compensation Bands
- Employees paid above the maximum of their band may be flagged for salary review.
- Employees at the minimum of their band for 2+ years may be at retention risk.
- New hires frequently enter at or above mid-band due to market conditions, contributing to compression.

### Bonus and Incentive Eligibility
- Employees who have not completed their probation period are typically ineligible for bonuses.
- Employees on a performance improvement plan (PIP) may be ineligible for variable pay.
- Prorated bonuses may apply for employees who joined mid-cycle.

### Attendance Thresholds
- Attendance below 80% (or a defined threshold) may trigger HR action in most organizations.
- Leave-adjusted attendance (approved leaves excluded) should be compared separately from raw attendance.

### Span of Control
- Typical healthy span: 5–10 direct reports per manager.
- More than 15 direct reports may indicate management overload.
- Fewer than 3 direct reports may indicate over-management or under-utilization of leadership capacity.

### Training Requirements
- Mandatory training completion (e.g., compliance, safety) is often tracked with a 100% target.
- Low training hours may indicate lack of development investment or disengagement.

---

## 7. KPIs and How to Compute Them

These are the standard metrics used in HR reporting. Always use these formulas unless the dataset provides an alternative definition.

| KPI | Formula | Notes |
|---|---|---|
| **Attrition Rate** | `(Employees Exited in Period / Average Headcount in Period) × 100` | Annualize if computing for a sub-year period |
| **Regrettable Attrition Rate (as share of exits)** | `(Regrettable Exits / Total Exits) × 100` | Answers: "Of all who left, what proportion were regrettable?" |
| **Regrettable Attrition Rate (as share of headcount)** | `(Regrettable Exits / Average Headcount in Period) × 100` | Answers: "What percentage of the workforce did we regrettably lose?" Preferred for workforce risk reporting |
| **Headcount** | `COUNT(employee_id)` filtered to active employees using the employment status field (see §21 synonym: `employment_type` / `attrition`) | Always filter for active employees unless analyzing exits |
| **Average Salary** | `AVG(salary)` grouped by relevant dimension | Use median for skewed distributions |
| **Total Payroll Cost** | `SUM(salary + bonus + incentives)` | Useful for cost center or department budgeting |
| **Average Tenure** | `AVG(tenure_in_months / 12)` | Segment by department, designation, or gender |
| **Promotion Rate** | `(Employees Promoted / Eligible Employees) × 100` | Eligibility criteria must be applied before computing |
| **Internal Mobility Rate** | `(Internal Transfers or Role Changes / Total Employees) × 100` | A higher rate is usually a positive indicator |
| **Attendance Rate** | `AVG(attendance_percentage)` | Never SUM; averages across individuals |
| **Gender Diversity Ratio** | `COUNT(gender = 'Female') / COUNT(*) × 100` | Compute at department, level, and leadership layers |
| **Training Hours per Employee** | `SUM(training_hours) / COUNT(employee_id)` | Filter by department or role for targeted insights |
| **Offer Acceptance Rate** | `(Offers Accepted / Offers Extended) × 100` | Requires recruitment pipeline data |
| **Revenue per Employee** | `Total Revenue / Active Headcount` | Requires revenue data; proxy for workforce productivity |

---

## 8. Aggregation Rules

These rules govern how fields must be treated in PySpark or any SQL-like query to avoid incorrect results.

| Field Type | Correct Treatment | Never Do This |
|---|---|---|
| `employee_id` | Use in COUNT(*), GROUP BY for uniqueness, or joins only | SUM, AVG, or use as a measure |
| `salary` | AVG for benchmarking; SUM for payroll totals | SUM when comparing groups of different sizes |
| `attendance_percentage` | AVG across employees | SUM (produces meaningless totals) |
| `performance_score` | AVG for group comparison; distribution for spread | Binary filter without acknowledging distribution |
| `attrition` (binary flag) | SUM to count exits; use `(SUM(attrition) / COUNT(employee_id)) × 100` to compute attrition rate | AVG of a binary flag produces the same numeric result but is semantically unclear — use the explicit formula from §7 instead |
| `tenure` | AVG for group comparison; segment into bands for distribution | Treat raw months as a category |
| Percentages in general | Never SUM unless they are counts disguised as percentages | Do not add 80% + 90% and report 170% |
| Missing values | Treat as unknown; flag in output | Treat as zero unless explicitly justified |

---

## 9. Interpretation Rules

These rules prevent the LLM from drawing simplistic or misleading conclusions from the data.

### Attrition Interpretation
- **High attrition does not always mean poor culture.** Restructuring cycles, economic downturns, or end-of-project completions can cause legitimate spikes.
- **Stable headcount does not mean zero attrition.** If new hires exactly replace exits, headcount appears unchanged while attrition is high.
- **Attrition requires a time window.** A single snapshot cannot determine attrition; at minimum, an entry date and exit date or a period flag is needed.

### Compensation Interpretation
- **High salary does not mean overpayment.** It may reflect retention bonuses, rare skill premiums, or long tenure with consistent hikes.
- **Low salary does not mean underpayment.** Recent hires at entry-level and individuals in lower-cost locations may have legitimately lower pay.
- **Salary comparison without designation control is misleading.** Always compare salary within the same designation or job level.

### Attendance Interpretation
- **Low attendance does not imply absenteeism.** Employees on approved leave, medical leave, or work-from-home arrangements may appear to have low attendance depending on how data is captured.
- **100% attendance may not indicate ideal productivity** — it may also reflect a culture of fear or lack of leave utilization.

### Performance Interpretation
- **Low performance scores for recently promoted or transferred employees may be role-transition-related**, not indicative of poor capability.
- **High performance scores with high attrition in a group** may indicate unmet career expectations rather than job dissatisfaction.

### Tenure Interpretation
- **Long tenure with no promotion is not inherently negative.** Specialized individual contributors or employees in stable roles may choose not to advance.
- **Short average tenure is concerning only if accompanied by regrettable exits.** In consulting or project-based roles, shorter tenures may be the norm.

---

## 10. Cause-and-Effect Relationships

These are the business relationships commonly observed in HR. Use these to enrich analytical insights beyond surface-level descriptions.

| Cause | Likely Effect | Insight Type |
|---|---|---|
| Delayed promotions | Increased voluntary attrition, especially among high performers | Retention Risk |
| Low manager quality | Higher voluntary exits in affected teams | Manager Effectiveness |
| Poor internal mobility | Mid-tenure attrition (3–5 year band) | Career Development |
| High overtime over sustained periods | Declining engagement and increased attrition risk — burnout cannot be confirmed from overtime data alone, but sustained overwork is a known precursor worth flagging | Workload and Wellbeing |
| Salary compression | Retention risk for senior employees who see new hires at similar pay | Compensation Fairness |
| Low training investment | Stagnant performance and reduced employee development satisfaction | Capability Building |
| Weak succession planning | Leadership continuity risk when key roles are vacated | Succession and Risk |
| High hiring volume without productivity improvement | Overstaffing or onboarding/integration inefficiency | Workforce Productivity |
| Promotion without salary adjustment | Short-term engagement spike followed by attrition | Compensation Alignment |
| Low engagement scores sustained over time | Leading indicator of voluntary attrition in 6–12 months | Predictive Risk |

---

## 11. Executive Priorities

When generating insights or summarizing findings, prioritize metrics and signals in the following order unless the user specifies otherwise. This reflects what senior HR leadership and business executives typically act on.

**Tier 1 — Highest Priority:**
- Regrettable attrition (especially among high performers and critical talent)
- Succession risk for leadership and critical roles
- Pay equity gaps by gender, location, and designation — i.e., whether equivalent roles are paid equally across groups

**Tier 2 — High Priority:**
- Manager effectiveness (attrition and engagement by manager)
- Internal mobility and career development patterns
- Compensation competitiveness relative to bands

**Tier 3 — Operational Priority:**
- Overall headcount trends and hiring velocity
- Attendance and overtime patterns
- Training completion and effectiveness

**Tier 4 — Supplementary:**
- Gender diversity *ratio* (headcount representation by gender across departments and levels) — distinct from pay equity, which is Tier 1
- Hiring source effectiveness
- Offer acceptance rates

> **Note on gender:** Pay *equity* (are women and men paid equally for equivalent roles?) is a Tier 1 concern. Gender *representation* (what proportion of the workforce or leadership is female?) is Tier 4. These are different questions and must not be conflated.

---

## 12. Preferred Comparison Patterns

These are the most analytically meaningful comparisons in HR. Prefer these patterns over arbitrary groupings.

| Comparison | Why It Is Meaningful |
|---|---|
| Department vs. Department | Identifies organizational units with performance, attrition, or cost outliers |
| Designation vs. Designation | Controls for seniority and role type in compensation and performance analysis |
| Manager vs. Manager (within department) | Isolates management quality from structural factors |
| High Performers vs. Average Performers | Reveals differential treatment in pay, promotion, and development |
| New Hires (< 1 yr tenure) vs. Long-Tenure Employees | Highlights onboarding effectiveness and salary compression |
| Active vs. Exited Employees | Enables attrition profiling and exit analysis |
| Business Unit vs. Business Unit | Strategic workforce alignment across divisions |
| Location vs. Location | Identifies geographic compensation disparities and attendance norms |
| Gender by Department and Level | Diversity equity analysis |
| Cohort Year (Joining Year) | Tracks how groups hired at different times perform and retain over time |

> **Avoid:** Comparing employees of different designations on raw salary without normalization. Avoid grouping by unique identifiers like `employee_id` when aggregating.

---

## 13. Time-Based Analysis Rules

HR metrics must be interpreted with a clear time context. Without it, conclusions are unreliable.

| Metric | Recommended Time Granularity | Notes |
|---|---|---|
| **Attrition** | Monthly, Quarterly, or Annual | To annualize a sub-period rate: `(Exits / Avg Headcount) × (12 / Months in Period) × 100` — see §24 for the full formula |
| **Hiring / Headcount Growth** | Monthly or Quarterly | Align with fiscal planning cycles |
| **Performance** | Review cycle (semi-annual or annual) | Do not compare across different cycles without normalization |
| **Promotions** | Annual review cycle | Promotions outside cycles may indicate exceptional cases |
| **Salary Hikes** | Annual or per appraisal cycle | Compare hike percentages within the same appraisal year |
| **Training Hours** | Monthly or Annual | Seasonal spikes (e.g., new hire onboarding) are normal |
| **Attendance** | Monthly | Yearly averages can mask seasonal dips |
| **Overtime** | Monthly | Sustained overtime (3+ months) is more concerning than a one-month spike |
| **Tenure** | Point-in-time snapshot | Report as of a specific date; cohort analysis adds richer context |

> **Rule:** When the user does not specify a time period, apply the defaults defined in §24. If the dataset does not cover the default window, use the maximum available range and state this explicitly.

---

## 14. Chart and Visualization Guidance

When generating or recommending visualizations, match the chart type to the analytical question.

| Question Type | Recommended Chart | Notes |
|---|---|---|
| Distribution of a numeric field (salary, tenure, score) | Histogram or Box Plot | Reveals skewness, outliers, and spread |
| Category comparison (department, location) | Bar Chart (horizontal for many categories) | Sort by value descending for readability |
| Trend over time | Line Chart | Use for attrition, headcount, hiring by month/quarter |
| Proportion (gender ratio, attrition type breakdown) | Pie Chart or Donut Chart | Limit to < 5 categories; otherwise use stacked bar |
| Two-variable relationship (salary vs. performance) | Scatter Plot | Add regression line for trend visibility |
| Performance distribution by segment | Grouped Bar Chart or Stacked Bar | Useful for high/mid/low performer splits |
| Attrition by multiple dimensions | Heatmap | Rows = department, Columns = tenure band or designation |
| Manager comparison | Horizontal Bar Chart sorted by metric | One bar per manager within a department |
| Cohort analysis | Line Chart with one line per cohort | X-axis = months since joining, Y-axis = retention rate |
| Headcount change (hiring vs. attrition) | Waterfall Chart | Shows net headcount movement clearly |
| Salary band positioning | Box Plot or Dot Plot with band overlays | Show individual positions relative to band min/mid/max |

> **Default output:** If the user asks for "insights" or "analysis" without specifying a chart, generate both a textual summary and a chart recommendation with the appropriate type and axes.

---

## 15. Red Flag Indicators

These are early warning signals that leadership should be alerted to proactively. When the data shows any of the following patterns, surface them as priority insights.

| Red Flag | What It May Indicate |
|---|---|
| Stable headcount + rising regrettable attrition | Organization is rehiring to replace talent it is losing — costly and disruptive |
| High engagement scores + rising exits | Engagement surveys may be lagging indicators; deeper investigation needed |
| Fast hiring + stagnant or declining productivity | Onboarding issues, role clarity gaps, or overstaffing |
| Promotion growth + no retention improvement | Promotions are not addressing the root cause of attrition |
| High compensation increases + no performance improvement | Compensation is being used reactively, not strategically |
| Strong attendance + low performance | Presence without productivity; warrants investigation into role fit, workload clarity, or capability gaps — engagement cannot be confirmed as the cause without survey data |
| Single manager with disproportionately high attrition | Likely management quality issue isolated to one team |
| High attrition in the 1–2 year tenure band | Onboarding and early-career development are failing |
| High attrition in the 5–7 year tenure band | Mid-career stagnation; promotion or mobility pipeline is broken |
| Gender pay gap widening over appraisal cycles | Systemic compensation bias that compounds over time |
| Zero internal mobility over 2+ review cycles | Career development pathways are not functioning |

---

## 16. Safe vs. Unsafe Assumptions

### Safe Assumptions (the LLM may apply these by default)

- Salary is best compared within the same designation or job level.
- Attendance should be averaged, not summed, across employees.
- Performance scores are averaged at group level; distributions are preferred for deeper analysis.
- Department and designation are the strongest segmentation axes for most HR questions.
- `employee_id` is a unique row identifier and should never be used as a metric.
- Attrition analysis requires a defined time period; point-in-time data alone is insufficient.
- When a field is missing for some rows, the LLM should note the gap rather than impute a value.

### Unsafe Assumptions (the LLM must NEVER make these)

| Unsafe Assumption | Why It Is Wrong |
|---|---|
| Missing attendance value = absenteeism | The field may simply not be captured; do not impute intent |
| High salary = overpayment | May reflect retention strategy, role criticality, or long tenure |
| Low salary = underpayment | May be justified by role level, location, or recent hire timing |
| Attrition always means voluntary resignation | Could be involuntary, retirement, contract end, or death |
| Employee dissatisfaction can be inferred from exit data alone | Exits have multiple causes; assuming dissatisfaction is a leap |
| Missing values should be treated as zero | Nulls often mean data was not captured, not that the value is zero |
| All employees in the same department have the same role | Departments are heterogeneous; designation matters |
| High overtime = poor time management | May reflect seasonal peaks, project deadlines, or understaffing |
| Low training hours = disengagement | May reflect no available training programs, not lack of interest |
| Same performance score = same output value | Output value varies significantly by role and seniority |

---

## 17. Role-Specific Interpretation

Different roles in the organization are measured and compared differently. The LLM must account for role context when generating insights.

| Role Type | Key Considerations |
|---|---|
| **Sales / Business Development** | High variable pay (incentives, commissions) is normal and expected; base salary comparisons alone are misleading. Target achievement and quota attainment are primary performance indicators. |
| **Technical / Engineering Roles** | Critical talent classification is common; market salary premiums may justify above-band compensation. Hard to backfill quickly — succession risk is elevated. |
| **Leadership / Executive Roles** | Compensation exceptions are more frequent and often board-approved. Succession planning is a Tier 1 priority. Span of control norms differ from individual contributors. |
| **Support / Administrative Functions** | Performance is often measured differently from revenue-generating roles. Salary bands may be tighter. Lower promotion frequency is structurally normal. |
| **Operations / Manufacturing Roles** | Attendance and overtime metrics carry higher operational weight than in office roles. Safety-related training completion is critical. |
| **New Hires (< 6 months)** | Performance scores during probation are early signals, not definitive assessments. Attrition in this band is often related to role fit, onboarding quality, or expectation mismatch. |
| **Contract / Contingent Workers** | Typically excluded from attrition rate calculations, promotion analysis, and compensation band comparisons unless explicitly instructed otherwise — contract end is not the same as voluntary or involuntary attrition. Always filter by `employment_type` before running workforce metrics and state whether contractors are included or excluded. |
| **Individual Contributors (IC) vs. Managers** | ICs should not be penalized for long tenure without promotion if they are not on a management track. Manager metrics (team attrition, team performance) are distinct from IC metrics. |

---

## 18. Guardrails and Hard Rules

These are non-negotiable rules the LLM must follow when generating PySpark queries, analysis, or insights.

### Query Generation Rules
- Never use `employee_id` as a measure or aggregate it.
- Never SUM percentages (attendance, performance scores expressed as %).
- Always filter for the relevant population (e.g., active employees only, unless analyzing exits).
- When grouping by a dimension, ensure the dimension has low enough cardinality to be meaningful (not grouping by a near-unique field like `employee_name`).
- When joining tables, always validate that the join key is unambiguous and specify the join type explicitly.
- Never treat NULL as zero in calculations — use `COALESCE` only when it is logically justified and stated.

### Insight Generation Rules
- Do not state causation where only correlation is observed. Use language like "associated with" or "tends to correlate with" rather than "causes."
- Do not flag a metric as a red flag without qualifying context. Always say "this warrants further investigation" rather than drawing a firm conclusion.
- If data is insufficient to answer a question (e.g., no exit reason field for attrition type analysis), state this explicitly rather than guessing.
- When multiple interpretations are possible, surface the most likely one and note alternatives.

### Output Format Rules
- Always state the time period of analysis in the output.
- Always state the filters applied (e.g., "Active employees only," "Department: Engineering").
- When presenting a metric, always include the base population size (e.g., "Average salary is ₹X (n = 240 employees)").
- Do not produce a visualization recommendation without describing the axes and what the chart will show.

---

## 19. Metric Dependency Rules

Before computing any metric, the LLM must verify that the required fields are present in the dataset. If any required field is absent, the LLM must apply the relevant fallback rule from Section 20 — never attempt to compute the metric silently with incomplete inputs.

---

### Attrition Rate
**Required:**
- An attrition indicator field (binary flag, exit date, or employment status)
- A defined time period (start and end date, or a period column)
- A way to establish active headcount (either a status field or a snapshot date)

**Optional but improves accuracy:**
- Exit reason or exit type (voluntary vs. involuntary)
- Joining date (to compute tenure at exit)

---

### Regrettable Attrition Rate
**Required:**
- Attrition indicator (as above)
- At least one of: performance score, performance band, or a critical role indicator

**Cannot compute without:** Both attrition data and a quality/criticality signal. Attrition alone is insufficient.

---

### Promotion Readiness / Eligibility
**Required:**
- Tenure in current designation or role
- Performance score or rating (current or most recent cycle)
- Current designation (to apply band-appropriate eligibility thresholds)

**Optional but strengthens analysis:**
- Last promotion date
- Succession plan or readiness flag

---

### Compensation Fairness / Pay Equity
**Required:**
- Compensation field (salary, CTC, or equivalent)
- Designation or job level (role normalization)
- At least one equity dimension: gender, location, or employment type

**Strongly preferred:**
- Performance score (to distinguish merit-based pay differences from structural gaps)
- Tenure (to account for experience-based variation)

**Cannot compute fairly without:** Designation control. Cross-role salary comparison without normalization is misleading.

---

### Retention Risk Scoring

Retention risk is not a single metric — it is a cumulative picture built from however many signals are available. The more signals present, the sharper the picture. The fewer signals, the more the output shifts from a specific risk score to a directional insight. **Never refuse to assess retention risk just because not all signals are available. Always work with what exists and be explicit about what is and is not factored in.**

#### Risk Signals — assess each one independently if the data exists

| Signal | What to Look For | Risk Direction |
|---|---|---|
| **Tenure band** | Employees in the 1–2 yr or 5–7 yr bands have historically higher exit rates | Medium-High risk if present |
| **Performance trend** | Declining performance scores over the last 2 cycles | High risk signal |
| **Promotion recency** | No promotion in 2+ eligible cycles despite high performance | High risk signal |
| **Salary competitiveness** | Pay at or below band minimum for 2+ years. If compensation bands are unavailable (see §20 fallback), use pay below the average for the same designation as a proxy | Medium-High risk signal |
| **Internal mobility** | No lateral moves or role changes in 3+ years | Medium risk signal |
| **Overtime trend** | Sustained high overtime (3+ months) | Medium risk signal |
| **Attendance trend** | Declining attendance without approved leave explanation | Medium risk signal |
| **Manager attrition rate** | Employee's manager has a team attrition rate above department average | Medium-High risk signal |
| **Engagement score** | Score below department average or declining over time | High risk signal |
| **Department attrition rate** | Employee is in a high-attrition department | Contextual risk signal |
| **Peer exits** | Multiple peers in same role/team have exited recently | Medium-High risk signal |

#### How to combine signals

- **If hard attrition data exists** (exit flags, exit dates, reason codes): compute actual attrition rate first, then use signals to explain and segment it. Hard data takes precedence over signal-based inference.
- **If no hard attrition data exists**: use available signals to produce a qualitative risk tier — High / Medium / Low — based on how many and which signals are present. Do not fabricate a percentage.
- **Cumulative signal logic:**
  - 3 or more High risk signals present → surface as **High retention risk**, describe which signals are firing
  - 1–2 High risk signals or 3+ Medium signals → surface as **Medium retention risk**, describe the pattern
  - Only Medium signals with no High signals → surface as **Low-Medium retention risk**, note it as early warning
  - No signals or insufficient data → state data is insufficient for risk assessment; do not guess
- Always list which signals were assessed, which were present in the data, and which were absent. Transparency about coverage is required.

**Required for any retention risk output:**
- At least 2 signals from the table above must be assessable from the dataset. A single signal in isolation is insufficient to establish a risk tier — it is a point of interest, not a pattern.

**Hard data that changes everything (if present, compute directly, do not estimate):**
- Attrition flag or exit date → compute actual attrition rate by segment
- Exit reason codes → split voluntary vs. involuntary and identify regrettable exits
- Engagement survey scores → compute engagement-attrition correlation directly

---

### Manager Effectiveness
**Required:**
- Manager identifier linked to employee records
- At least one team-level metric: team attrition rate, team performance average, or team engagement score

**Cannot infer from:** Individual employee data alone. Effectiveness requires aggregated team-level patterns attributed to a specific manager.

---

### Internal Mobility Rate
**Required:**
- Role history or designation change records
- Employee identifier (to track the same individual across roles)
- Time period

**Cannot compute without:** Historical role or designation data. A single-snapshot dataset cannot measure mobility.

---

### Succession Readiness
**Required:**
- Key role identification (either flagged in the dataset or defined by designation level)
- Potential successors identified (via performance band, readiness flag, or succession plan field)
- Tenure and designation of candidates

**Cannot compute without:** At least a key role indicator and a candidate pool signal.

---

### Training Effectiveness
**Required:**
- Training hours or training completion data
- Performance scores (pre and post training, or at least post-training)
- Time reference (training period and performance review period)

**Weak analysis only:** If only training hours are available without linked performance change, the analysis can only describe investment, not impact.

---

### Workforce Productivity
**Required:**
- Active headcount
- A productivity proxy — ideally revenue, output units, or project delivery metrics

**Note:** Pure HR datasets rarely contain revenue. When unavailable, explicitly state that productivity analysis is limited to input-side metrics (headcount, hours) only.

---

## 20. Fallback Rules When Required Data Is Missing

When a field required for a metric is absent from the dataset, apply the appropriate fallback below. Always explicitly state in the output which fallback was used and why the result is an approximation, not a definitive measure.

---

| Required Data Missing | Fallback Approach | Output Caveat to State |
|---|---|---|
| **Compensation bands** | Use average salary by designation as a proxy benchmark | "Formal compensation band data is unavailable. Comparison uses internal average salary by designation as a proxy." |
| **Attrition flag** | Infer exits using the exit date field (see §21 synonym: `exit_date`) where it is not null, or the employment status field (see §21 synonym: `attrition`) where the value indicates resignation, termination, or inactivity | "Attrition is inferred from available exit date or status fields. Results may exclude employees whose status was not updated in the dataset." |
| **Exit reason / attrition type** | Report total attrition only; do not split into voluntary vs. involuntary | "Exit reason is unavailable. Voluntary vs. involuntary breakdown cannot be computed." |
| **Performance scores** | Use tenure and designation stagnation as weak proxies for performance signals only | "Performance data is unavailable. Indicators are based on tenure and career progression as weak proxies." |
| **Promotion history** | Use tenure in current designation as a weak indicator of stagnation only | "Promotion history is unavailable. Long tenure in the same designation is used as an indirect signal only — not a confirmed absence of promotion." |
| **Manager hierarchy** | Avoid manager effectiveness analysis entirely | "Manager hierarchy data is unavailable. Team-level or manager-level analysis cannot be performed." |
| **Engagement scores** | Do not skip retention risk analysis. Instead, assess all other available signals from the §19 Retention Risk signal table and produce a qualitative risk tier. State which signals were assessed and which were absent. | "Engagement scores are unavailable. Retention risk is assessed using available signals — [list which ones]. Risk tier is qualitative, not a computed score." |
| **Multiple retention signals missing** | If fewer than 2 signals from the §19 table are assessable, state that data coverage is insufficient for a reliable risk tier rather than producing a guess | "Insufficient signal coverage for retention risk assessment. Only [X] of [Y] risk indicators are available in this dataset." |
| **Joining date / tenure** | Omit tenure-based segmentation; note its absence | "Joining date or tenure is unavailable. Cohort and career-stage analysis cannot be performed." |
| **Location data** | Skip geographic comparisons; do not impute location from other fields | "Location data is unavailable. Geographic workforce distribution and location-based pay comparisons are excluded." |
| **Training data** | Skip training effectiveness and development investment analysis | "Training data is unavailable. Learning and development analysis is excluded from this output." |
| **Succession or readiness flags** | Use performance band + tenure as a weak succession proxy | "Succession readiness flags are unavailable. High performers with sufficient tenure in senior roles are used as an approximate candidate pool." |

---

## 21. Synonym Mapping

Different datasets use different column names for the same concept. When the LLM encounters a field not listed in the schema, it should attempt to match it against these synonyms before asking for clarification or failing.

This mapping is directional: all synonyms on the right map to the canonical concept on the left.

---

### Employee Identifier
**Canonical concept:** `employee_id`
**Also seen as:** `emp_id`, `empcode`, `associate_id`, `worker_id`, `staff_id`, `personnel_id`, `resource_id`, `emp_no`, `employee_code`

---

### Compensation / Pay
**Canonical concept:** `salary`
**Also seen as:** `ctc`, `annual_ctc`, `compensation`, `fixed_pay`, `annual_pay`, `base_salary`, `basic_pay`, `gross_salary`, `monthly_salary`, `pay`, `remuneration`, `total_compensation`, `package`

---

### Joining / Hire Date
**Canonical concept:** `joining_date`
**Also seen as:** `doj`, `date_of_joining`, `hire_date`, `start_date`, `employment_start_date`, `onboard_date`, `joining_dt`, `date_hired`

---

### Designation / Job Level
**Canonical concept:** `designation`
**Also seen as:** `role`, `title`, `job_title`, `position`, `job_role`, `grade`, `level`, `band`, `job_band`, `rank`, `seniority_level`

---

### Attrition / Exit Indicator
**Canonical concept:** `attrition`
**Also seen as:** `exit_date`, `date_of_exit`, `termination_date`, `last_working_day`, `lwd`, `resignation_date`, `separated`, `exited`, `is_attrition`, `churned`, `employee_status` (where value = 'Resigned' / 'Terminated' / 'Inactive')

---

### Department
**Canonical concept:** `department`
**Also seen as:** `dept`, `dept_name`, `team`, `function`, `business_function`, `cost_center_name`
**Note:** `division` is ambiguous — it may refer to a department or a business unit depending on the organization. Before flagging ambiguity, inspect the column values: if they contain functional team names (e.g., "Engineering", "Sales", "HR", "Finance") treat it as `department`; if they contain strategic or geographic unit names (e.g., "APAC", "Retail Banking", "North America", "Consumer Division") treat it as `business_unit`. Only flag ambiguity if the values are unclear or mixed after inspection.

---

### Manager
**Canonical concept:** `manager_id`
**Also seen as:** `reporting_manager`, `direct_manager`, `supervisor`, `team_lead`, `line_manager`, `manager_name`, `reports_to`, `mgr_id`

---

### Performance Score
**Canonical concept:** `performance_score`
**Also seen as:** `rating`, `performance_rating`, `appraisal_score`, `annual_rating`, `perf_score`, `review_score`, `goal_score`, `kra_rating`, `performance_band`

---

### Attendance
**Canonical concept:** `attendance_percentage`
**Also seen as:** `attendance`, `attendance_rate`, `presence_rate`, `days_present_pct`, `attendance_pct`, `working_days_attended_pct`

---

### Tenure / Years at Company
**Canonical concept:** `tenure`
**Also seen as:** `years_of_service`, `service_length`, `company_tenure`, `emp_tenure`, `tenure_years`, `tenure_months`, `months_at_company`, `length_of_service`

---

### Employment Type
**Canonical concept:** `employment_type`
**Also seen as:** `emp_type`, `worker_type`, `contract_type`, `engagement_type`, `employee_category`, `payroll_type`

---

### Business Unit
**Canonical concept:** `business_unit`
**Also seen as:** `bu`, `vertical`, `segment`, `division`, `strategic_unit`, `line_of_business`, `lob`

---

### Age / Date of Birth
**Canonical concept:** `age`
**Also seen as:** `dob`, `date_of_birth`, `birth_date`, `employee_age`, `age_years`, `age_as_of_date`
**Note:** `age` is typically derived from `date_of_birth` at query time. `age_group` or `age_band` are pre-bucketed dimension fields — treat them as dimensions, not measures.

---

### Training Hours
**Canonical concept:** `training_hours`
**Also seen as:** `learning_hours`, `training_completed`, `l&d_hours`, `development_hours`, `training_hrs`, `courses_completed`

---

### Bonus
**Canonical concept:** `bonus`
**Also seen as:** `bonus_amount`, `annual_bonus`, `performance_bonus`, `incentive_pay`, `variable_bonus`, `payout`

---

## 22. Confidence Levels for Inference

When the LLM derives an insight or computes a metric, it must internally assess and explicitly communicate the confidence level of the conclusion. This prevents users from treating approximations as verified facts.

Use the following framework consistently.

---

### Confidence Level Definitions

| Level | Meaning | When to Use |
|---|---|---|
| **High Confidence** | Directly computed from explicit, complete, and unambiguous data | Required fields are all present, clean, and unambiguous |
| **Medium Confidence** | Computed from available data with minor gaps or indirect indicators | One required field is missing but can be reasonably inferred; or data is present but coverage is partial |
| **Low Confidence** | Approximated from weak proxies; result direction may be correct but magnitude is unreliable | Key required fields are absent and substituted with proxies |
| **Cannot Compute** | Insufficient data to produce even an approximation; analysis should not proceed | Multiple required fields are missing; any output would be speculative |

---

### Confidence Examples by Metric

| Metric | Scenario | Confidence Level |
|---|---|---|
| **Attrition Rate** | Explicit exit date + active/inactive status + defined period | High |
| **Attrition Rate** | Employee status field exists but has nulls for some rows | Medium |
| **Attrition Rate** | No exit date or status; inferred from absence in recent records | Low |
| **Regrettable Attrition** | Attrition flag + performance band both present | High |
| **Regrettable Attrition** | Attrition flag present; performance inferred from tenure stagnation | Low |
| **Pay Equity** | Salary + designation + gender all present | High |
| **Pay Equity** | Salary + gender present but no designation normalization | Low — cross-role comparison without control |
| **Retention Risk** | Hard attrition data present (exit flags/dates/reasons) + multiple signals | High — compute directly from data |
| **Retention Risk** | No hard attrition data; 3+ High risk signals present from §19 signal table | Medium-High — qualitative risk tier, state signals firing |
| **Retention Risk** | No hard attrition data; 1–2 High risk signals or 3+ Medium signals | Medium — directional insight, list signals assessed |
| **Retention Risk** | No hard attrition data; only Medium signals, no High signals | Low-Medium — early warning only, not a risk verdict |
| **Retention Risk** | Fewer than 2 signals assessable from §19 table | Cannot Compute — state data coverage is insufficient |
| **Promotion Readiness** | Tenure + performance score + designation all present | High |
| **Promotion Readiness** | Tenure present; performance absent | Low |
| **Manager Effectiveness** | Team attrition + team performance attributed to manager | High |
| **Manager Effectiveness** | Only headcount per manager available | Low |
| **Succession Readiness** | Readiness flags + performance band + tenure | High |
| **Succession Readiness** | Performance band + tenure as proxy; no explicit flags | Medium |

---

### Output Labeling Convention

When presenting findings, always append the confidence level inline. Use plain language:

- "**[High Confidence]** Attrition rate in the Engineering department is 18.4% for FY2024."
- "**[Medium Confidence]** Estimated regrettable attrition is approximately 40% of total exits, based on available performance ratings covering 72% of exited employees."
- "**[Low Confidence]** Compensation fairness analysis is approximate — designation data is missing for 35% of employees, so cross-role normalization is incomplete."
- "**[Cannot Compute]** Manager effectiveness analysis cannot be performed — manager hierarchy data is not present in this dataset."

---

## 23. Unsupported Analysis Rules

These are analyses the LLM must decline to perform or must heavily qualify, because the required causal or contextual data is structurally unavailable in typical HR datasets. Attempting these without qualification produces hallucinated insights that appear confident but are unreliable.

---

### Never Infer Without Explicit Data

| Inference | Why It Cannot Be Drawn | What to Say Instead |
|---|---|---|
| **Employee dissatisfaction** from exits alone | Exits have many causes — better offers, relocation, health, retirement, restructuring. Dissatisfaction is only one. | "Exit patterns are visible; the cause of exits cannot be determined without exit survey or reason data." |
| **Promotion denial** from tenure alone | Long tenure in a role may reflect employee preference, role structure, or policy — not necessarily a withheld promotion. | "No promotion activity is observed in the available period; whether promotion was denied or not pursued is unknown." |
| **Pay inequity** without role-normalized comparison | Salary differences across groups are not inequitable by definition — they may reflect role, level, and experience differences. | "Pay differences exist between groups; equity analysis requires designation-controlled comparison before conclusions can be drawn." |
| **Manager quality** from individual employee data | A single employee's poor performance or exit does not characterize a manager's effectiveness. | "Manager-level conclusions require aggregated team patterns across multiple employees and time periods." |
| **Burnout** from overtime data alone | High overtime may be temporary, seasonal, or voluntarily taken. Burnout is a psychological state requiring engagement or health data to infer. | "Elevated overtime hours are observed; whether this has led to burnout or disengagement cannot be confirmed without engagement data." |
| **Individual-level future attrition prediction** without a predictive model | Labelling specific employees as "at risk" based on demographic or profile similarity to past exits is individual-level profiling, not prediction — it is unfair and unreliable. This is distinct from group-level retention risk patterns (e.g., "employees in the 5–7 year tenure band in Department X show multiple risk signals"), which are supported by §19. | "Group-level risk patterns are presented. Individual employees are not labelled as at-risk — that requires a validated predictive model." |
| **Engagement levels** from attendance or performance alone | Attendance and performance are output metrics, not direct measures of emotional engagement. | "Engagement data is unavailable. Attendance and performance are used as partial proxies only." |
| **Culture or morale** from any quantitative HR data | Culture and morale are qualitative constructs that require survey instruments, not transaction data. | "Quantitative HR data cannot characterize organizational culture or team morale directly." |

---

## 24. Default Time Windows

When a user's question does not specify a time period, apply these defaults automatically. Always state the default time window used in the output.

If the dataset does not contain sufficient historical data to cover the default window, use the maximum available range and state this explicitly.

---

| Analysis Type | Default Time Window | Rationale |
|---|---|---|
| **Attrition Rate** | Last 12 months (rolling) | Annual attrition is the industry standard reporting period |
| **Regrettable Attrition** | Last 12 months | Aligns with annual review and exit cycles |
| **Hiring / Headcount Growth** | Last 12 months, broken by quarter | Captures seasonal hiring patterns |
| **Promotion Analysis** | Last completed review cycle (typically annual) | Promotions are cycle-bound events |
| **Compensation Comparison** | Current snapshot (most recent data) | Salaries are a point-in-time measure |
| **Salary Hike Analysis** | Last appraisal cycle | Hike percentages are cycle-specific |
| **Retention Risk** | Rolling 6–12 months | Leading indicators require a recency window |
| **Attendance Trend** | Last 6 months, broken by month | Monthly patterns reveal seasonality and anomalies |
| **Overtime Analysis** | Last 6 months | Sustained overtime (3+ months) is the meaningful signal |
| **Training Effectiveness** | Before vs. after the most recent training cycle | Requires a before/after comparison window |
| **Tenure Distribution** | Current snapshot | Tenure is measured as of today or the dataset snapshot date |
| **Cohort Analysis** | All available years grouped by joining year | Cohorts require full lifecycle data |
| **Performance Trend** | Last 2 completed review cycles | Single-cycle performance is less reliable; trends are stronger signals |
| **Gender Diversity** | Current snapshot | Diversity is typically reported at a point in time |

---

### Annualizing Sub-Period Attrition

When only a partial-year dataset is available, annualize attrition using this formula:

```
Annualized Attrition Rate = (Exits in Period / Average Headcount in Period) × (12 / Months in Period) × 100
```

Always state: *"This is an annualized estimate based on X months of data."*

---

## 25. Join Priority Rules

When the dataset involves multiple tables or when a question requires combining data across multiple HR data sources, apply joins in the following priority order. This ensures that the employee record remains the anchor and that joins do not inadvertently inflate or reduce the employee population.

---

### Anchor Table

**Always start from the employee table.** The employee table is the fact anchor. All other tables join to it. Never start a join from a secondary table (e.g., performance or training) as the base, as this may silently exclude employees who have no records in that table.

```
employee (base)
    → designation / job level
    → department
    → manager hierarchy
    → compensation / salary history
    → performance scores
    → attendance records
    → training records
    → promotion history
    → attrition / exit records
    → engagement survey responses
```

---

### Join Type Rules

| Join Scenario | Recommended Join Type | Reason |
|---|---|---|
| Employee → Department | LEFT JOIN | All employees must be included, even if department mapping is missing |
| Employee → Manager | LEFT JOIN | Some employees (e.g., the CEO, or data gaps) may have no manager |
| Employee → Performance | LEFT JOIN | Not all employees may have a performance record in every cycle |
| Employee → Compensation | LEFT JOIN | Historical records may be absent for recent hires |
| Employee → Attrition Records | LEFT JOIN | Active employees have no exit record; inner join would exclude them |
| Employee → Training | LEFT JOIN | Employees who attended no training must still appear in headcount |
| Employee → Engagement Survey | LEFT JOIN | Survey response rates are rarely 100% |
| Attrition Records → Employee (for exit analysis) | INNER JOIN or LEFT JOIN on exit table | Depends on whether you want all exits or only those with full employee profiles |

> **Rule:** Default to LEFT JOIN when joining from the employee anchor. Switch to INNER JOIN only when the question explicitly filters to employees who have records in both tables (e.g., "among employees who have both a performance score and a salary record").

---

### Deduplication After Joins

After any join, verify that the employee count in the result matches the expected headcount. If the join produces more rows than employees, a one-to-many relationship exists (e.g., multiple performance records per employee per cycle). Apply aggregation or filter to the most recent or relevant record before proceeding.

```python
# PySpark example: take most recent performance record per employee
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

w = Window.partitionBy("employee_id").orderBy(desc("review_date"))
performance_latest = performance_df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
```

---

### Multi-Table Analysis Join Order Example

For a query requiring: attrition rate by department, segmented by performance band —

```
1. Start: employee table (active + exited)
2. JOIN department (LEFT JOIN on department_id)
3. JOIN performance — most recent record per employee (LEFT JOIN on employee_id)
4. JOIN attrition records (LEFT JOIN on employee_id)
5. Compute: attrition flag from exit records
6. Group by: department, performance_band
7. Compute: attrition rate = COUNT(exits) / COUNT(employee_id) × 100
```

Never join performance → attrition directly without the employee anchor, as it would exclude employees missing from one of those tables.

---

*End of HR Semantic Layer Document*

*This document should be re-uploaded alongside any HR dataset provided to the LLM. It does not replace dataset-specific schema documentation but complements it with domain reasoning that schema alone cannot provide.*
