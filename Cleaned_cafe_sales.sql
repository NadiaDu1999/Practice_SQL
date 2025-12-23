# Look at dataset overall
SELECT *
FROM sales;

# Create a copy table
CREATE TABLE sales_copy
LIKE sales;

INSERT sales_copy
SELECT *
FROM sales;

SELECT *
FROM sales_copy;

-- Data Cleaning
-- 1. Change column names
ALTER TABLE sales_copy
RENAME COLUMN `Transaction ID` TO Transaction_id;

ALTER TABLE sales_copy
RENAME COLUMN `Price Per Unit` TO Price_per_unit;

ALTER TABLE sales_copy
RENAME COLUMN `Total Spent` TO Total_spent;

ALTER TABLE sales_copy
RENAME COLUMN `Payment Method` TO Payment_method;

ALTER TABLE sales_copy
RENAME COLUMN `Transaction Date` TO Transaction_date; # Done!

SELECT *
FROM sales_copy;
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 2. Removing any columns or rows
# 2.1 If Transaction _id is None, they will be removed
SELECT *
FROM sales_copy
WHERE Transaction_id = 'NULL'
OR Transaction_id IS NULL
OR Transaction_id = ''
OR Transaction_id = 'UNKNOWN'
OR Transaction_id = 'ERROR';

CREATE TABLE `sales_copy2` (
  `Transaction_id` text,
  `Item` text,
  `Quantity` text,
  `Price_per_unit` text,
  `Total_spent` text,
  `Payment_method` text,
  `Location` text,
  `Transaction_date` text,
  `rn` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# 2.2 Remove Duplicate Rows
INSERT INTO sales_copy2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY Transaction_id, Item, Quantity, Price_per_unit,Total_spent, Payment_method, Location, Transaction_date) AS rn
FROM sales_copy;

DELETE
FROM sales_copy2
WHERE rn > 1;

SELECT *
FROM sales_copy2;

# 2.3 Remove rows that is not give us any information
DELETE
FROM sales_copy2
WHERE Item IN ('', 'ERROR', 'UNKNOWN')
AND Price_per_unit IN ('', 'ERROR', 'UNKNOWN')
AND Total_spent IN ('', 'ERROR', 'UNKNOWN');

DELETE
FROM sales_copy2
WHERE Item IN ('', 'ERROR', 'UNKNOWN')
AND Price_per_unit IN ('', 'ERROR', 'UNKNOWN')
AND Quantity IN ('', 'ERROR', 'UNKNOWN');

# 2.4 Remove unnecessary column
ALTER TABLE sales_copy2
DROP COLUMN rn;
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 3. Dealing with missing values
# 3.1 Impute missing values in Item column using Price_per_unit column
SELECT DISTINCT Item
FROM sales_copy2;

SELECT Item, Price_per_unit, Quantity, Total_spent
FROM sales_copy2
GROUP BY Item, Price_per_unit, Quantity, Total_spent;
# Now we know.. 
# Tea = $1.5
# Salad = $5
# Cookie = $1
# Coffee = $2
# Juice  = $3
# Smoothie = $4
# Cake = $3
# Sandwich = $4

UPDATE sales_copy2 t1
JOIN sales_copy2 t2
	ON t1.Price_per_unit = t2.Price_per_unit
SET t1.Item = t2.Item
WHERE t1.Item IN ('', 'ERROR', 'UNKNOWN')
AND t2.Item NOT IN ('', 'ERROR', 'UNKNOWN')
AND t1.Price_per_unit NOT IN ('', 'ERROR', 'UNKNOWN')
AND t2.Price_per_unit NOT IN ('', 'ERROR', 'UNKNOWN');

# 3.2 Impute missing values in Price_per_unit column
UPDATE sales_copy2 t1
JOIN sales_copy2 t2
	ON t1.Item = t2.Item
SET t1.Price_per_unit = t2.Price_per_unit
WHERE t1.Price_per_unit IN ('', 'ERROR', 'UNKNOWN')
AND t2.Price_per_unit NOT IN ('', 'ERROR', 'UNKNOWN')
AND t1.Item NOT IN ('', 'ERROR', 'UNKNOWN')
AND t2.Item NOT IN ('', 'ERROR', 'UNKNOWN');

# There are some missing values in Price_per_unit and Item still
SELECT *
FROM sales_copy2
WHERE Price_per_unit IN ('', 'ERROR', 'UNKNOWN');

# Now we impute the missing values using "Total_spent / Quantity"
UPDATE sales_copy2
SET Price_per_unit = Total_spent / Quantity
WHERE Price_per_unit IN ('', 'ERROR', 'UNKNOWN');

SELECT *
FROM sales_copy2;

# 3.3 Impute missing values in Quantity column
SELECT *
FROM sales_copy2
WHERE Quantity IN ('', 'ERROR', 'UNKNOWN');

UPDATE sales_copy2
SET Quantity = Total_spent / Price_per_unit
WHERE Quantity IN ('', 'ERROR', 'UNKNOWN');

# 3.4 Impute missing values in Total_spent column
SELECT *
FROM sales_copy2
WHERE Total_spent IN ('', 'ERROR', 'UNKNOWN');

UPDATE sales_copy2
SET Total_spent = Quantity * Price_per_unit
WHERE Total_spent IN ('', 'ERROR', 'UNKNOWN');

# Fill up more information for Item column base on Price_per_unit (leave out the rows that have $3 or $4 dollard because we do not know which item it belongs to
SELECT *
FROM sales_copy2
WHERE Item IN ('', 'ERROR', 'UNKNOWN');

UPDATE sales_copy2
SET Item = CASE
	WHEN Price_per_unit = '5' THEN 'Salad'
    WHEN Price_per_unit = '1.5' THEN 'Tea'
    WHEN Price_per_unit = '1' THEN 'Cookie'
    WHEN Price_per_unit = '2' THEN 'Coffee'
    ELSE Item
END;
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 4. Check data consistency
# Check if Total_spent = Price_per_unit * Quantity
SELECT
  CASE
    WHEN (Total_spent - Price_per_unit * Quantity) = 0 THEN 'Correct'
    ELSE 'Incorrect'
  END AS check_result
FROM sales_copy2;

# Check if all items have correct price
SELECT DISTINCT Item, Price_per_unit
FROM sales_copy2;
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 5. Standardizing the data
# Change '', ERROR and UNKNOWN to NULL
UPDATE sales_copy2
SET Transaction_date = CASE
	WHEN Transaction_date = '' OR Transaction_date = 'ERROR' OR Transaction_date = 'UNKNOWN'
    THEN NULL ELSE Transaction_date END,
    Item = CASE
    WHEN Item = '' OR Item = 'ERROR' OR Item = 'UNKNOWN'
    THEN NULL ELSE Item END,
    Quantity = CASE
    WHEN Quantity = '' OR Quantity = 'ERROR' OR Quantity = 'UNKNOWN'
    THEN NULL ELSE Quantity END,
    Price_per_unit = CASE
    WHEN Price_per_unit = '' OR Price_per_unit = 'ERROR' OR Price_per_unit = 'UNKNOWN'
    THEN NULL ELSE Price_per_unit END,
    Total_spent = CASE
    WHEN Total_spent = '' OR Total_spent = 'ERROR' OR Total_spent = 'UNKNOWN'
    THEN NULL ELSE Total_spent END,
    Payment_method = CASE
    WHEN Payment_method = '' OR Payment_method = 'ERROR' OR Payment_method = 'UNKNOWN'
    THEN NULL ELSE Payment_method END,
    Location = CASE
    WHEN Location = '' OR Location = 'ERROR' OR Location= 'UNKNOWN'
    THEN NULL ELSE Location END,
    Transaction_date =  CASE
    WHEN Transaction_date = '' OR Transaction_date ='ERROR' OR Transaction_date = 'UNKNOWN'
    THEN NULL ELSE Transaction_date END;
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 6. Change data type/format
# Change to mySQL date format
SELECT *
FROM sales_copy2;

UPDATE sales_copy2
SET Transaction_date = CASE
	WHEN Transaction_date LIKE '%/%/%' THEN  STR_TO_DATE(Transaction_date, '%Y/%m/%d')
    ELSE Transaction_date
END;

# Chage to date type
ALTER TABLE sales_copy2
MODIFY COLUMN Transaction_date DATE;
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

# Note that since we have many NULLs across multiple variables (most of them categorical), 
# I decided to retain them rather than remove the rows, because removing them would significantly reduce 
# the dataset size and potentially bias the analysis.
    











