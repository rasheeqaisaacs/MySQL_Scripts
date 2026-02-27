-- Data Cleaning

SELECT *
FROM layoffs1;

-- 1. Remove Duplicates
-- 2. Standardise the Data
-- 3. Null Values or blank values
-- 4. Remove any columns (if not needed)


-- REMOVING DUPLICATES

CREATE TABLE layoffs_staging2
LIKE layoffs1;

SELECT *
FROM layoffs_staging2;

INSERT layoffs_staging2
SELECT *
FROM layoffs1;

SELECT COUNT(*) FROM layoffs1;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging2;


WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
funds_raised_millions) AS row_num
FROM layoffs_staging2
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging2
where company = 'Casper';


-- Create table to het rid of extra rows that are duplicates 
-- row_num was added manually
CREATE TABLE `layoffs_staging4` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- empty table
SELECT *
FROM layoffs_staging4
WHERE row_num > 1;

-- insert original data into table created above
INSERT INTO layoffs_staging4
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, 
funds_raised_millions) AS row_num
FROM layoffs_staging2;

-- identify duplicates
SELECT *
FROM layoffs_staging4
WHERE row_num > 1;

-- deleting duplicates
DELETE
FROM layoffs_staging4
WHERE row_num > 1;

SELECT *
FROM layoffs_staging4;


-- Standardising data

SELECT company, TRIM(company)
FROM layoffs_staging4;

UPDATE layoffs_staging4
SET company = TRIM(company);


SELECT DISTINCT industry
FROM layoffs_staging4
ORDER BY 1;

SELECT *
FROM layoffs_staging4
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging4
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT industry
FROM layoffs_staging4;


SELECT DISTINCT location
FROM layoffs_staging4
ORDER BY 1;


SELECT *
FROM layoffs_staging4
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging4
ORDER BY 1;

-- updating table with new info
UPDATE layoffs_staging4
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- changed the date format
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging4;

UPDATE layoffs_staging4
SET `date` = 
	CASE
		WHEN `date` IS NOT NULL AND `date` != 'NULL' THEN str_to_date(`date`, '%m/%d/%Y')
		ELSE NULL
END;

-- Update column to correct date
UPDATE layoffs_staging4
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging4;

ALTER TABLE layoffs_staging4
MODIFY COLUMN `date` DATE;


-- CHANGE COLUMN TO INT

ALTER TABLE layoffs_staging4 ADD COLUMN new_total_laid_off INT;

UPDATE layoffs_staging4
SET new_total_laid_off =
  CASE
    WHEN total_laid_off REGEXP '^-?[0-9]+$'
    THEN CAST(total_laid_off AS SIGNED)
    ELSE NULL
  END;


ALTER TABLE layoffs_staging4 ADD COLUMN new_funds_raised_millions INT;

UPDATE layoffs_staging4
SET new_funds_raised_millions =
  CASE
    WHEN funds_raised_millions REGEXP '^-?[0-9]+$'
    THEN CAST(funds_raised_millions AS SIGNED)
    ELSE NULL
  END;
  

ALTER TABLE layoffs_staging4 ADD COLUMN new_percentage_laid_off INT;

UPDATE layoffs_staging4
SET new_percentage_laid_off =
  CASE
    WHEN percentage_laid_off REGEXP '^-?[0-9]+$'
    THEN CAST(percentage_laid_off AS SIGNED)
    ELSE NULL
  END;

SELECT *
FROM layoffs_staging4;


-- NULLS and BLANK VALUES (Populating these values)

SELECT *
FROM layoffs_staging4
WHERE new_total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging4
WHERE industry = 'NULL'
OR industry = '';

SELECT *
FROM layoffs_staging4
WHERE company = 'Airbnb';


SELECT t1.industry, t2.industry
FROM layoffs_staging4 t1
JOIN layoffs_staging4 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


-- Changing BLANKS to NULLS

UPDATE layoffs_staging4
SET industry = null
WHERE industry = '';

 
UPDATE layoffs_staging4 t1
JOIN layoffs_staging4 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- formatted like this because we use part of the name not full like Airbnb
-- Bally is left NULL since this was the company only laid off this one
SELECT *
FROM layoffs_staging4
WHERE company LIKE 'Bally%';   


SELECT *
FROM layoffs_staging4;


-- DELETE ROWS

DELETE
FROM layoffs_staging4
WHERE new_total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging4;

ALTER TABLE layoffs_staging4
DROP column row_num;





