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
| **Business Unit** | `business_unit`, `division` | Cross-unit comparison |
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
| **Regrettable Attrition Rate** | `(Regrettable Exits / Total Exits) × 100` | Requires performance or criticality data to compute |
| **Headcount** | `COUNT(employee_id) WHERE is_active = 1` | Always filter for active employees unless analyzing exits |
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
| `attrition` (binary flag) | SUM to count exits; AVG to compute attrition rate | Treat as a continuous measure |
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
| High overtime over sustained periods | Declining engagement and eventual burnout exits | Workload and Wellbeing |
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
- Pay equity gaps (gender, location, designation)

**Tier 2 — High Priority:**
- Manager effectiveness (attrition and engagement by manager)
- Internal mobility and career development patterns
- Compensation competitiveness relative to bands

**Tier 3 — Operational Priority:**
- Overall headcount trends and hiring velocity
- Attendance and overtime patterns
- Training completion and effectiveness

**Tier 4 — Supplementary:**
- Gender diversity ratios by department and level
- Hiring source effectiveness
- Offer acceptance rates

> When asked for "top insights" or "what should leadership focus on," always lead with Tier 1 items if the data supports them.

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
| **Attrition** | Monthly, Quarterly, or Annual | Annualize monthly rates: `monthly_rate × 12` |
| **Hiring / Headcount Growth** | Monthly or Quarterly | Align with fiscal planning cycles |
| **Performance** | Review cycle (semi-annual or annual) | Do not compare across different cycles without normalization |
| **Promotions** | Annual review cycle | Promotions outside cycles may indicate exceptional cases |
| **Salary Hikes** | Annual or per appraisal cycle | Compare hike percentages within the same appraisal year |
| **Training Hours** | Monthly or Annual | Seasonal spikes (e.g., new hire onboarding) are normal |
| **Attendance** | Monthly | Yearly averages can mask seasonal dips |
| **Overtime** | Monthly | Sustained overtime (3+ months) is more concerning than a one-month spike |
| **Tenure** | Point-in-time snapshot | Report as of a specific date; cohort analysis adds richer context |

> **Rule:** When the user does not specify a time period, use the most recent complete period available in the data. Always state the period in the output.

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
| Strong attendance + low performance | Presence without productivity; may indicate disengagement or role mismatch |
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

