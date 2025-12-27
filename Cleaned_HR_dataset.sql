USE HR_dataset;
SET sql_safe_updates = 0

CREATE TABLE HR2 LIKE HR;

INSERT HR2
SELECT *
FROM HR;

SELECT *
FROM HR2;

-- Data Cleaning 
-- 1. Change the column name
ALTER TABLE HR2
CHANGE COLUMN id emp_id VARCHAR(20) NULL;
# Check data type
DESCRIBE HR2;


-- 2. Dealing with date-time column
## birthdate column
SELECT birthdate
FROM HR2;

UPDATE HR2
SET birthdate = CASE
	WHEN birthdate LIKE '%/%' THEN DATE_FORMAT(STR_TO_DATE(birthdate, '%m/%d/%Y'), '%Y-%m-%d')
    WHEN birthdate LIKE '%-%' THEN DATE_FORMAT(STR_TO_DATE(birthdate, '%m-%d-%Y'), '%Y-%m-%d')
    # This is the date format in the raw data >> (birthdate, '%m/%d/%Y') and (birthdate, '%m-%d-%Y')
    # But this is the date format that we want >> '%Y-%m-%d'
    ELSE NULL
END;

## hire_date column
UPDATE HR2
SET hire_date = CASE
	WHEN hire_date LIKE '%/%' THEN DATE_FORMAT(STR_TO_DATE(hire_date, '%m/%d/%Y'), '%Y-%m-%d')
    WHEN hire_date LIKE '%-%' THEN DATE_FORMAT(STR_TO_DATE(hire_date, '%m-%d-%Y'), '%Y-%m-%d')
    # This is the date format in the raw data >> (birthdate, '%m/%d/%Y') and (birthdate, '%m-%d-%Y')
    # But this is the date format that we want >> '%Y-%m-%d'
    ELSE NULL
END;

## termdate column
SELECT termdate
FROM HR2;

UPDATE HR2
SET termdate = date(str_to_date(termdate, '%Y-%m-%d %H:%i:%s UTC'))
WHERE termdate IS NOT NULL AND termdate != '';

UPDATE HR2
SET termdate = CASE
 WHEN termdate = '' THEN NULL
 ELSE termdate
END; 

# Change data type to date type
ALTER TABLE HR2
MODIFY COLUMN birthdate DATE;

ALTER TABLE HR2
MODIFY COLUMN hire_date DATE;

ALTER TABLE HR2
MODIFY COLUMN termdate DATE;

-- 3. Create new column 'age'
ALTER TABLE HR2
ADD COLUMN age INT;

UPDATE HR2
SET age = timestampdiff(YEAR, birthdate, CURDATE());

SELECT birthdate, age
FROM HR2;

-- 4. Data validation
SELECT
min(age) AS youngest,
max(age) AS oldest
FROM HR2;

SELECT count(*)
FROM HR2
WHERE age < 18; # There are 967 rows that has age under 18, so we will exclude these rows when we do the analysis

SELECT *
FROM HR2
WHERE age < 18;

#The reason age is showing in negative is instead of 1960 it is mentioned as 2060 , so you can use DATE_SUB to subtract 100 years.
UPDATE HR2
SET birthdate = DATE_SUB(birthdate, INTERVAL 100 YEAR)
WHERE birthdate >= '2060-01-01' AND birthdate < '2070-01-01';

UPDATE HR2
SET age = timestampdiff(YEAR, birthdate, CURDATE());

# ---------------------------------------------------------------------------------------------------------------------------------------------------------

# Data Analysis
-- 1. What is the gender breakdown of employees in the company?
SELECT gender, count(*) AS count
FROM HR2
WHERE termdate IS NULL # which represent the current emplyoyees in this company
GROUP BY gender;

-- 2. What is the race breakdown of employees in the company?
SELECT race, count(*) AS count
FROM HR2
WHERE termdate IS NULL
GROUP BY race
ORDER BY count(*) DESC;

-- 3. What is the age distribution of employees in the company?
SELECT
	min(age) AS youngest,
    max(age) AS oldest
FROM HR2
WHERE termdate IS NULL;

SELECT CASE 
	WHEN age >= 18 AND age <= 24 THEN '18-24'
    WHEN age >= 25 AND age <= 34 THEN '25-34'
    WHEN age >= 35 AND age <= 44 THEN '35-44'
    WHEN age >= 45 AND age <= 54 THEN '45-54'
    WHEN age >= 55 AND age <= 64 THEN '55-64'
    ELSE '65+'
	END AS age_group, gender,
	count(*) AS count
FROM HR2
WHERE termdate IS NULL
GROUP BY age_group, gender
ORDER BY age_group, gender;

-- 4. How many employees work at headquaters vs. remote location?
SELECT * 
FROM HR2;

SELECT location, count(*) AS count
FROM HR2
WHERE termdate IS NULL
GROUP BY location;

-- 5. What is the average length of employment for employees who have been terminated?
SELECT
	ROUND(AVG(DATEDIFF(termdate, hire_date))/365,0) AS avg_length_employment
FROM  HR2
WHERE termdate IS NOT NULL AND termdate <= CURDATE();

-- 6. How does the gender distribution vary accross departments?
SELECT department, gender, count(*) AS count
FROM HR2
WHERE termdate IS NULL
GROUP BY department, gender
ORDER BY department;

-- 7. What is the distribution of job titles across the company?
SELECT jobtitle, count(*) AS count
FROM HR2
WHERE termdate IS NULL
GROUP BY jobtitle
ORDER BY jobtitle DESC;

-- 8. Which department has the highest turnover rate?
SELECT department,
		total_count,
        terminated_count,
        terminated_count/total_count AS terminated_rates
FROM (
	SELECT department, count(*) AS total_count,
	SUM(CASE WHEN termdate IS NOT NULL AND termdate <= CURDATE() THEN 1 ELSE 0 END) AS terminated_count
	FROM HR2
	GROUP BY department
    ) AS subquery
ORDER BY terminated_rates DESC;

-- 9. What is the distribution of employees across locations by state?
SELECT location_state, count(*) AS count
FROM HR2
WHERE termdate IS NULL
GROUP BY location_state
ORDER BY count DESC;

-- 10. How has the company's employee count changed over time based on hire and term dates?
SELECT year,
hires,
terminations,
hires - terminations AS net_change,
round(((hires - terminations)/hires)*100, 2) AS net_change_percent
FROM (
	SELECT YEAR(hire_date) AS year,
	count(*) AS hires,
	SUM(CASE WHEN termdate IS NOT NULL AND termdate <= CURDATE() THEN 1 ELSE 0 END) AS terminations
	FROM HR2
    GROUP BY YEAR(hire_date)
    ) AS subquery
ORDER BY year ASC;

-- 11. What is the tenure distribution for each department?
SELECT department, ROUND(AVG(DATEDIFF(termdate, hire_date))/365, 0) AS avg_tenure
FROM HR2
WHERE termdate IS NOT NULL AND termdate <= CURDATE()
GROUP BY department
ORDER BY department;



    
    
