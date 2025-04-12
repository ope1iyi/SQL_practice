CREATE TABLE layoffs (
	company char(40),
	location varchar(40),
	industry varchar(40),
	total_laid_off varchar(10),
	percentage_laid_off varchar(10),
	date varchar(18),
	stage varchar(30),
	country varchar(30),
	funds_raised_millions varchar(20)
);



copy layoffs(company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) 
FROM 'C:\Users\Scientist\Downloads\layoffs.csv' 
DELIMITER ','
CSV HEADER
NULL AS 'NULL';


SELECT *
FROM layoffs

-- 1. REMOVE DUPLICATES
-- 2. sTANDARDDISED THE DATA
-- 3. NULL VALUES OR BLANK VALUES
-- 4. REMOVE UNNECESSARY COLUMNS


--CREATING ANOTHER TABLE TO DO OUR ANALYSIS ON 
CREATE TABLE layoffs_staging (LIKE layoffs INCLUDING ALL);

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;


-- CHECKING FOR DUPLICATES IN LAYOFFS_STAGING
-- row_number over partition by all fields to point out the duplicates
-- anyone with row_num greater than 1 is a duplicate
WITH duplicate_cte AS (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as Row_num
	FROM layoffs_staging
	) 
	
-- CREATING A NEW STAGING TABLE TO REMOVE ALL DUPLICATES
CREATE TABLE staging2 (LIKE layoffs_staging INCLUDING ALL);
ALTER TABLE staging2 ADD COLUMN RowNum bigint;

INSERT INTO staging2
SELECT *,
   ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as Row_num
 FROM layoffs_staging;

-- DELETING THE DUPLICATES FROM STAGING2 TABLE
DELETE
FROM staging2
WHERE Rownum > 1;

SELECT *
FROM staging2;



-- STANDARDINZING THE DATA
-- This process is basically just scanning some columns to check 
-- for anomalities
UPDATE staging2
SET company = TRIM(company);

UPDATE staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';

UPDATE staging2
SET country = TRIM(country, '.')
WHERE country like 'United States%';

UPDATE staging2
set date = TO_DATE(date, 'MM/DD/YYYY');


ALTER TABLE staging2
ALTER COLUMN date TYPE DATE USING TO_DATE(date, 'YYYY-MM-DD');

SELECT *
FROM STAGING2
WHERE total_laid_off IS null
AND percentage_laid_off IS null;

SELECT *
FROM STAGING2
WHERE industry IS null OR industry = ''

--POPULATE INDUSTRY FIELD USING THE SAME COMPANY IN THE SAME LOCATION

SELECT * FROM STAGING2 T1
INNER JOIN STAGING2 T2
	ON T1.company = T2.company
WHERE (t1.industry IS null OR T1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE staging2 t1
SET industry = NULL
WHERE industry ='';


UPDATE staging2 t1
SET industry = t2.industry
FROM staging2 t2
WHERE t1.company = t2.company
   AND t1.industry IS NULL
   AND t2.industry IS NOT NULL;

select * from staging2 where location='Providence';


-- CHECKING COLUMNS
SELECT *
FROM STAGING2
WHERE total_laid_off IS null
AND percentage_laid_off IS null;

DELETE
FROM STAGING2
WHERE total_laid_off IS null
AND percentage_laid_off IS null;

ALTER TABLE staging2
DROP COLUMN rownum

ALTER TABLE staging2
ALTER COLUMN percentage_laid_off TYPE numeric USING percentage_laid_off::numeric;

ALTER TABLE staging2
ALTER COLUMN funds_raised_millions TYPE numeric USING funds_raised_millions::numeric;

select * from staging2;

--EDA
-- total_laid_off is in the wrong datatype

SELECT max(total_laid_off), min(total_laid_off)
FROM staging2

select * from layoffs









