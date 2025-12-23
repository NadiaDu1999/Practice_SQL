-- Cleaning Data Processes
-- 1. Removing duplicates
-- 2. Standardizing the data
-- 3. Handling Null or blank values
-- 4. Removing any comlumns

-- First, after converting CSV to JSON, Null become None 
-- Change None to Null 
UPDATE
	layoffs
SET
	company = CASE company WHEN 'None' THEN NULL ELSE company END,
    location = CASE location WHEN 'None' THEN NULL ELSE location END,
    industry = CASE industry WHEN 'None' THEN NULL ELSE industry END,
    total_laid_off = CASE total_laid_off WHEN 'None' THEN NULL ELSE total_laid_off END,
    percentage_laid_off = CASE percentage_laid_off WHEN 'None' THEN NULL ELSE percentage_laid_off END,
    `date` = CASE `date` WHEN 'None' THEN NULL ELSE `date` END,
    stage = CASE stage WHEN 'None' THEN NULL ELSE stage END,
    country = CASE country WHEN 'None' THEN NULL ELSE country END,
    funds_raised_millions = CASE funds_raised_millions WHEN 'None' THEN NULL ELSE funds_raised_millions END;

SELECT *
FROM layoffs;

# copy layoffs table and called 'layoffs_staging' because we will change a lot of things in the table, so we need to have raw data available
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

# Check NULL
SELECT *
FROM layoffs_staging
WHERE company = 'NULL'
   OR location = 'NULL'
   OR industry = 'NULL'
   OR total_laid_off = 'NULL'
   OR percentage_laid_off = 'NULL'
   OR `date` = 'NULL'
   OR stage = 'NULL'
   OR country = 'NULL'
   OR funds_raised_millions = 'NULL';

# Change 'NULL' (text) into real SQL NULL in MySQL
UPDATE layoffs_staging
SET company = NULL
WHERE company IN ('', 'NULL');

UPDATE layoffs_staging
SET location = NULL
WHERE location IN ('', 'NULL');

UPDATE layoffs_staging
SET industry = NULL
WHERE industry IN ('', 'NULL');

UPDATE layoffs_staging
SET total_laid_off = NULL
WHERE total_laid_off IN ('', 'NULL');

UPDATE layoffs_staging
SET funds_raised_millions = NULL
WHERE funds_raised_millions IN ('', 'NULL');

UPDATE layoffs_staging
SET percentage_laid_off = NULL
WHERE percentage_laid_off IN ('', 'NULL');

UPDATE layoffs_staging
SET `date` = NULL
WHERE `date` IN ('', 'NULL');

UPDATE layoffs_staging
SET stage = NULL
WHERE stage IN ('', 'NULL');

UPDATE layoffs_staging
SET country = NULL
WHERE country IN ('', 'NULL');

# --------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 1. REMOVE DUPLICATES
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions)
FROM layoffs_staging;
# if row_num > 1 means that row is duplicated and can be removed

WITH duplicate_cte AS  # “Create a temporary result set that I can query right after.” It exists only for this query and is not stored permanently.
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1

# confirm if they are really duplicated
SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS  # “Create a temporary result set that I can query right after.” It exists only for this query and is not stored permanently.
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
DELETE # This will throw us an error
FROM duplicate_cte
WHERE row_num > 1

-- Solution
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Delete duplicates
DELETE
FROM layoffs_staging2
WHERE row_num > 1; # Done

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;
# ----------------------------------------------------------------------------------------------------------------------------------------------------
-- 2. Standardizing the data
SELECT company, TRIM(company) # 'TRIM' takes off the white space at the beginning or atthe end
from layoffs_staging2

UPDATE layoffs_staging2
SET company = TRIM(company); 

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

# Change other crypto.. to 'Crypto'
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; # Done

# Change United States. to United States
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; # There are 2 Unites States (and United States.)

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; # Done

# Change the date format
SELECT `date`
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') # 'STR_TO_DATE converts the date column to the default date format in MySQL
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = CASE
	WHEN `date` LIKE '%/%/%' THEN STR_TO_DATE(`date`, '%m/%d/%Y') # There is 'NULL' still in date column
    ELSE `date`
END; # Done

# Change date to date type (not it's text)
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; # Done
# ----------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. Handling Null or blank values
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb'; # So, now we want the null value to be 'Travel' industry for Airbnb

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

# Reaplce value
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; # Done

# Bally company also blank
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL;

SELECT *
FROM layoffs_staging2
WHERE company = "Bally's Interactive";
# We cannot replace any values becasue we do not have the information.
# ------------------------------------------------------------------------------------------------------------------------------------------------
-- 4. Removing any comlumns

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Delete rows
DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; # Done

# Drop column row_num
ALTER TABLE layoffs_staging2
DROP COLUMN row_num; # Done

SELECT *
FROM layoffs_staging2;























	


