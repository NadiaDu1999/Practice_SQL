set sql_safe_updates = 0;

SELECT *
FROM nf2;

create table nf2_copy
LIKE nf2;

insert nf2_copy
select *
from nf2;

select *
from nf2_copy; 

-- Data cleaning
# 1. Check/remove duplicate rows
# 2. Populate missing value
# 3. Drop unnecessary column
# 4. Delaing with date column

# ------------------------------------------------------------------------------------------------------------------------------------------------------

-- 1. Check/remove duplicate rows
SELECT show_id, count(*)
FROM nf2_copy
GROUP BY show_id
ORDER BY show_id DESC; # There is no duplicate rows

-- 2. Populate missing value
# Check for missing values in each column
SELECT SUM(CASE WHEN show_id IS NULL OR show_id = '' THEN 1 ELSE 0 END) AS id_nulls,
		SUM(CASE WHEN `type` IS NULL OR `type` = '' THEN 1 ELSE 0 END) AS type_nulls,
        SUM(CASE WHEN title IS NULL OR title = '' THEN 1 ELSE 0 END) AS title_nulls,
        SUM(CASE WHEN director IS NULL OR director = '' THEN 1 ELSE 0 END) AS director_nulls,
		SUM(CASE WHEN `cast` IS NULL OR `cast` = '' THEN 1 ELSE 0 END) AS cast_nulls,
		SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END) AS country_nulls,
        SUM(CASE WHEN date_added IS NULL OR date_added = '' THEN 1 ELSE 0 END) AS date_added_nulls,
        SUM(CASE WHEN release_year IS NULL OR release_year = '' THEN 1 ELSE 0 END) AS release_year_nulls,
        SUM(CASE WHEN rating IS NULL OR rating = '' THEN 1 ELSE 0 END) AS rating_nulls,
        SUM(CASE WHEN duration IS NULL OR duration = '' THEN 1 ELSE 0 END) AS duration_nulls,
        SUM(CASE WHEN listed_in IS NULL OR listed_in = '' THEN 1 ELSE 0 END) AS listed_in_nulls,
        SUM(CASE WHEN `description` IS NULL OR `description` = '' THEN 1 ELSE 0 END) AS description_nulls
FROM nf2_copy;

# 2.1 Populate missing values in director column
WITH director_freq AS
	(
	SELECT `cast`,
			director,
            count(*) AS freq,
            RANK() OVER(PARTITION BY `cast` ORDER BY count(*) DESC) AS rnk
    FROM nf2_copy
    WHERE director != ''
    GROUP BY `cast`, director
    )
    
    SELECT *
    FROM director_freq
    WHERE rnk =1;

CREATE temporary TABLE cast_director_lookup AS
SELECT `cast`, director
FROM (
	SELECT `cast`,
			director,
            count(*) AS freq,
            RANK() OVER(PARTITION BY `cast` ORDER BY count(*) DESC) AS rnk # For each cast, rank directors from most frequent to least frequent.
    FROM nf2_copy
    WHERE director != ''
    GROUP BY `cast`, director
    ) t
WHERE rnk =1;

UPDATE nf2_copy n
JOIN cast_director_lookup l
  ON n.`cast` = l.`cast`
SET n.director = l.director
WHERE (n.director = '' OR n.director IS NULL);

SELECT count(*) as missing
FROM nf2_copy
WHERE director = '';

# For the rest in director column, I will put them as 'Not provided.'
UPDATE nf2_copy
SET director = 'Not provided'
WHERE director = '';

# 2.2 Populate missing value in country column
SELECT *
FROM nf2_copy;

WITH director_country AS
	(
    SELECT director,
    country,
    count(*) AS freq,
    RANK() OVER(PARTITION BY director ORDER BY count(*) DESC) as rnk
    FROM nf2_copy
    WHERE country != '' AND director != 'Not provided'
    GROUP BY director, country
    )
SELECT director, country, freq, rnk
FROM director_country
WHERE rnk =1;

CREATE TEMPORARY TABLE director_country_lookup AS
SELECT director, country
FROM (
    SELECT director,
    country,
    count(*) AS freq,
    RANK() OVER(PARTITION BY director ORDER BY count(*) DESC) as rnk
    FROM nf2_copy
    WHERE country != '' AND director != 'Not provided'
    GROUP BY director, country
    ) t
WHERE rnk=1;

UPDATE nf2_copy n
JOIN director_country_lookup l
ON n.director = l.director
SET n.country = l.country
WHERE n.country = '' 
AND n.director != 'Not provided';

# For the rest nulls in country column, will be replaced as 'Not provided'
UPDATE nf2_copy
SET country = 'Not provided'
WHERE country = '';

# Moreover, there are more than 1 value in country column. So, I choose only the first country and remove the rest just to make it easy for visualization and analysis
SELECT SUBSTRING_INDEX(country, ', ', 1)
FROM nf2_copy;

UPDATE nf2_copy
SET country = SUBSTRING_INDEX(country, ', ', 1)
WHERE country LIKE '%,%';

# 2.3 Remove nulls in date_added, rating and duration columns
# date_added has only 10 nulls
# rating has only 4 nulls
# duration has only 3 nulls
DELETE 
FROM nf2_copy
WHERE date_added = '';

DELETE 
FROM nf2_copy 
WHERE rating = '';

DELETE 
FROM nf2_copy 
WHERE duration = '';

SELECT *
FROm nf2_copy;

-- 3. Drop uneccessary columns
ALTER TABLE nf2_copy
DROP COLUMN `cast`;

ALTER TABLE nf2_copy
DROP COLUMN `description`;

-- 4. Delaing with date column
UPDATE nf2_copy
SET date_added = CASE
	WHEN date_added LIKE '%, %' THEN STR_TO_DATE(date_added, '%M %d, %Y')
    ELSE date_added
END;

# Change to date type
ALTER TABLE nf2_copynf2
MODIFY COLUMN date_added DATE;







  






























