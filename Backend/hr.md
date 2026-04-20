This dataset belongs to Human Resources (HR) analytics.

Typical analysis focuses on employee performance, compensation, attrition, department-level insights, hiring trends, attendance, and workforce planning.

Common entities include:
- Employee
- Department
- Manager
- Job Role
- Location
- Business Unit
- Hiring Source

Common measurable fields:
- Salary
- Bonus
- Incentives
- Experience
- Tenure
- Attendance Percentage
- Overtime Hours
- Performance Score
- Training Hours
- Attrition Count

Dimensions:
Common grouping fields:
- Department
- Designation
- Gender
- Location
- Employment Type
- Joining Year
- Manager
- Business Unit

Business rules:
- employee_id should never be aggregated
- salary is usually analyzed using AVG or SUM depending on use case
- attrition is analyzed as employee exits over a period
- tenure is usually measured in months or years
- attendance should be averaged, not summed
- performance scores are usually averaged

Common KPIs:
- Attrition Rate = exited employees / total employees
- Average Salary by Department
- Headcount by Department
- Average Tenure
- Attendance Trend
- Performance Distribution
- Gender Diversity Ratio

Preferred analysis patterns:
- compare departments using averages and counts
- avoid grouping by unique identifiers like employee_id
- use department and designation for segmentation
- use joining date for hiring trends

Guardrails:
- do not treat employee_id as a measure
- do not sum percentages unless explicitly required
- do not assume missing values are zero
- do not infer attrition unless resignation/exit indicators exist