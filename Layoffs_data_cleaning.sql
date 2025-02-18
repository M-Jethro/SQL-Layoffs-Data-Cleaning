-- Dataset Source: https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- Data availability: From 11 March 2020 (when COVID-19 was declared a pandemic) to 20 July 2024.

-- Selecting all records from the layoffs table to inspect the data.
SELECT * 
FROM world_layoffs.layoffs;

-- Creating a staging table as a copy of the original layoffs table.
DROP TABLE IF EXISTS layoffs_staging;
CREATE TABLE layoffs_staging 
LIKE layoffs;

-- Inserting all records from layoffs into the staging table for transformations.
INSERT layoffs_staging 
SELECT *
FROM layoffs;

-- Verifying that data has been inserted correctly.
SELECT *
FROM layoffs_staging;

-- Assigning a row number to identify duplicate records based on key attributes.
SELECT *,
ROW_NUMBER() 
    OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

-- Using a CTE to identify duplicate records by assigning row numbers.
WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() 
            OVER (
                PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
    FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;  -- Filtering out only the duplicate rows.

-- Checking for layoffs related to specific companies.
SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo';

SELECT *
FROM layoffs_staging
WHERE company = 'Beyond Meat';

-- Creating a new staging table with an adjusted schema.
DROP TABLE IF EXISTS layoffs_staging2;
CREATE TABLE `layoffs_staging2` (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` TEXT,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised` TEXT,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Verifying the newly created table.
SELECT *
FROM layoffs_staging2;

-- Inserting data into layoffs_staging2 while generating row numbers for duplicate detection.
INSERT INTO layoffs_staging2
SELECT *,
    ROW_NUMBER() 
        OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised) AS row_num
FROM layoffs_staging;

-- Checking for duplicate records after insertion.
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Disabling safe update mode to allow DELETE operations.
SET SQL_SAFE_UPDATES = 0;

-- Removing duplicate rows from layoffs_staging2.
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Verifying that duplicates have been removed.
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Converting date values from text format to proper DATETIME format.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%d/%m/%Y');

-- Modifying the table structure to set proper data types.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATETIME,
MODIFY COLUMN `row_num` INT DEFAULT NULL;

-- Standardizing company names by trimming whitespace.
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- Checking distinct values for industry, location, and country for data consistency.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY location;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Inspecting the date column to confirm transformations.
SELECT `date`
FROM layoffs_staging2;

-- Identifying records with missing values in key columns.
SELECT *
FROM layoffs_staging2
WHERE (total_laid_off IS NULL OR total_laid_off = '')
AND (percentage_laid_off IS NULL OR percentage_laid_off = '');

-- Removing records that contain missing values in key columns.
DELETE
FROM layoffs_staging2
WHERE (total_laid_off IS NULL OR total_laid_off = '')
AND (percentage_laid_off IS NULL OR percentage_laid_off = '');

-- Verifying that missing value records have been removed.
SELECT *
FROM layoffs_staging2;

-- Dropping the row_num column after deduplication is complete.
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Final verification of the cleaned dataset.
SELECT *
FROM layoffs_staging2;
