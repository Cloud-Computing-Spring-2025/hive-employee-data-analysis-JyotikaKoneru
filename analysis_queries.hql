CREATE TABLE hue__tmp_employees (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary INT,
    project STRING,
    join_date STRING,
    department STRING
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs:///user/hive/employees.csv' INTO TABLE hue__tmp_employees;

CREATE TABLE hue__tmp_departments (
    dept_id INT,
    department_name STRING,
    location STRING
) ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs:///user/hive/departments.csv' INTO TABLE hue__tmp_departments;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = non-strict;

CREATE TABLE employees_partitioned (
    emp_id INT,
    name STRING,
    age INT,
    job_role STRING,
    salary INT,
    project STRING,
    join_date STRING
) 
PARTITIONED BY (department STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS PARQUET;

-- Add multiple partitions in a single command
ALTER TABLE employees_partitioned 
ADD PARTITION (department='HR')
PARTITION (department='Engineering')
PARTITION (department='Marketing')
PARTITION (department='Finance')
PARTITION (department='Sales');


INSERT OVERWRITE TABLE employees_partitioned PARTITION (department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM hue__tmp_employees;

SELECT DISTINCT department FROM employees_partitioned;


-- Retrieve all employees who joined after 2015
INSERT OVERWRITE DIRECTORY '/user/hive/output/joined_after_2015'
SELECT * FROM employees_partitioned WHERE year(join_date) > 2015;

-- Find the average salary of employees in each department
INSERT OVERWRITE DIRECTORY '/user/hive/output/avg_salary_every_dept'
SELECT department, AVG(salary) AS avg_salary FROM employees_partitioned GROUP BY department;

-- Identify employees working on the 'Alpha' project
INSERT OVERWRITE DIRECTORY '/user/hive/output/alpha_people'
SELECT * FROM employees_partitioned WHERE project = 'Alpha';

-- Count the number of employees in each job role
INSERT OVERWRITE DIRECTORY '/user/hive/output/count_employees_every_role'
SELECT job_role, COUNT(*) AS count FROM employees_partitioned GROUP BY job_role;

-- Retrieve employees whose salary is above the average salary of their department
INSERT OVERWRITE DIRECTORY '/user/hive/output/salary_above_average_salary_departmentwise'
SELECT * FROM employees_partitioned e 
WHERE salary > (SELECT AVG(salary) FROM employees_partitioned WHERE department = e.department);

-- Find the department with the highest number of employees
INSERT OVERWRITE DIRECTORY '/user/hive/output/highest_number_employees_department_name'
SELECT department, COUNT(*) AS employee_count 
FROM employees_partitioned GROUP BY department 
ORDER BY employee_count DESC LIMIT 1;

-- Check for employees with null values in any column and exclude them from analysis
INSERT OVERWRITE DIRECTORY '/user/hive/output/check_employees_null_remove'
SELECT * FROM employees_partitioned WHERE emp_id IS NOT NULL AND name IS NOT NULL AND age IS NOT NULL 
AND job_role IS NOT NULL AND salary IS NOT NULL AND project IS NOT NULL AND join_date IS NOT NULL AND department IS NOT NULL;

-- Join the employees and departments tables to display employee details along with department locations
INSERT OVERWRITE DIRECTORY '/user/hive/output/emp_details_with_dept_locations'
SELECT e.*, d.location FROM employees_partitioned e INNER JOIN hue__tmp_departments d ON e.department = d.department_name;

-- Rank employees within each department based on salary
INSERT OVERWRITE DIRECTORY '/user/hive/output/rank_employees_withins_departments'
SELECT emp_id, name, department, salary, 
RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank 
FROM employees_partitioned;

-- Find the top 3 highest-paid employees in each department
INSERT OVERWRITE DIRECTORY '/user/hive/output/top3_high_pay_deptwise'
SELECT * FROM (
    SELECT emp_id, name, department, salary, 
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
    FROM employees_partitioned
) ranked WHERE rank <= 3;