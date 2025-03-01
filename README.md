# Hive - Employee and Department Data Analysis

This repository contains Hive scripts and queries designed to analyze employee and department data using partitioned tables.

## Objectives
### Problem Statement
You are provided with two datasets: `employees.csv` and `departments.csv`. The tasks involve:

1. Loading `employees.csv` into a temporary Hive table and then transforming it into a partitioned table.
2. Loading `departments.csv` into a Hive table.
3. Performing various queries to analyze employee and department data.

## Setup and Execution

### 1. **Start the Hadoop Cluster**
To initiate the Hadoop cluster, execute:

```bash
docker compose up -d
```

### 2. **Load Data into Temporary Hive Tables**

```sql
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
```

### 3. **Create Partitioned Table and Insert Data**

```sql
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

ALTER TABLE employees_partitioned 
ADD PARTITION (department='HR')
PARTITION (department='Engineering')
PARTITION (department='Marketing')
PARTITION (department='Finance')
PARTITION (department='Sales');

INSERT OVERWRITE TABLE employees_partitioned PARTITION (department)
SELECT emp_id, name, age, job_role, salary, project, join_date, department 
FROM employees_temp;

SELECT DISTINCT department FROM employees_partitioned;
```

### 4. **Querying the Data**

#### Retrieve employees who joined after 2015
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/joined_after_2015'
SELECT * FROM employees_partitioned WHERE year(join_date) > 2015;
```

#### Find the average salary per department
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/avg_salary_every_dept'
SELECT department, AVG(salary) AS avg_salary FROM employees_partitioned GROUP BY department;
```

#### Identify employees working on the 'Alpha' project
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/alpha_people'
SELECT * FROM employees_partitioned WHERE project = 'Alpha';
```

#### Count employees per job role
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/count_employees_every_role'
SELECT job_role, COUNT(*) AS count FROM employees_partitioned GROUP BY job_role;
```

#### Retrieve employees earning above the average salary of their department
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/salary_above_average_salary_departmentwise'
SELECT * FROM employees_partitioned e 
WHERE salary > (SELECT AVG(salary) FROM employees_partitioned WHERE department = e.department);
```

#### Find the department with the highest number of employees
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/highest_number_employees_department_name'
SELECT department, COUNT(*) AS employee_count 
FROM employees_partitioned GROUP BY department 
ORDER BY employee_count DESC LIMIT 1;
```

#### Exclude employees with null values
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/check_employees_null_remove'
SELECT * FROM employees_partitioned WHERE emp_id IS NOT NULL AND name IS NOT NULL AND age IS NOT NULL 
AND job_role IS NOT NULL AND salary IS NOT NULL AND project IS NOT NULL AND join_date IS NOT NULL AND department IS NOT NULL;
```

#### Join employees and departments to get department locations
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/emp_details_with_dept_locations'
SELECT e.*, d.location FROM employees_partitioned e INNER JOIN departments_temp d ON e.department = d.department_name;
```

#### Rank employees within each department by salary
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/rank_employees_withins_departments'
SELECT emp_id, name, department, salary, 
RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank 
FROM employees_partitioned;
```

#### Find the top 3 highest-paid employees in each department
```sql
INSERT OVERWRITE DIRECTORY '/user/hive/output/top3_high_pay_deptwise'
SELECT * FROM (
    SELECT emp_id, name, department, salary, 
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank 
    FROM employees_partitioned
) ranked WHERE rank <= 3;
```

### 5. **Execute Queries and Save Output**
Run each command individually. The outputs of the commands are stored in the output folder with the given names.


### 6. **Access Hive Server Container**
```bash
docker exec -it hive-server /bin/bash
```

### 7. **Copy Output from HDFS to Local Filesystem (Inside the Container)**
```bash
hdfs dfs -get /user/hive/output /tmp/output
```

### 8. **Exit the Container**
```bash
exit
```

### 9. **Check Current Working Directory on the Host**
```bash
pwd
```

### 11. **Copy Output Files from Docker Container to Host Machine**
```bash
docker cp hive-server:/tmp/output /workspaces/hive-employee-data-analysis-JyotikaKoneru
```

### 12. **Commit Changes to GitHub**
```bash
git status
git add .
git commit -m "Queries and Outputs for data analysis"
git push origin master
```

### Challenges Faced
1. **Hue Server Downtime** : The Hue server repeatedly went down, causing interruptions.
   - **Resolution**: Restarting the Hue server using `docker restart hue`, monitoring logs with `docker logs hue -f`, and ensuring sufficient memory allocation for Docker.
2. **Repeated Folder Formation** :Unintended multiple folder generations while handling output directories.
   - **Resolution**: Carefully structuring Hive queries and verifying paths before running export commands to prevent duplicate folder creation.
3. **File Path Setting Issues** : Difficulty in properly setting up file paths for data extraction and storage.
   - **Resolution**: Using absolute paths, verifying HDFS directory structure with `hdfs dfs -ls /user/hive/output/`, and ensuring correct permissions before executing copy commands.  
4. **Configuring Hive with Partitioning:** Setting up and managing partitioned tables required careful handling to ensure efficient query execution.
5. **Handling Large Datasets:** Processing employee and department data in Hive required optimizations to improve performance.
6. **Dynamic Partitioning Issues:** Enabling and correctly implementing dynamic partitioning posed challenges in data insertion.
7. **Join Performance Optimization:** Optimizing the join operations between `employees_partitioned` and `departments_temp` to improve query speed.
8. **Extracting Meaningful Insights:** Designing queries to extract relevant insights, such as salary comparisons and department-based rankings, required iterative refinements.
