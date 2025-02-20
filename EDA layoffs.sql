-- Retrieving all data from the table for exploratory analysis.
SELECT *
FROM layoffs_staging2;

-- Finding the maximum number of employees laid off in a single event and the highest percentage of layoffs at a company.
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Identifying companies that laid off their entire workforce (100% layoffs), sorted by total layoffs.
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Cleaning up the dataset by trimming any leading or trailing spaces in numeric columns.
UPDATE layoffs_staging2
SET percentage_laid_off = TRIM(percentage_laid_off),
    total_laid_off = TRIM(total_laid_off);

-- Converting percentage_laid_off to DECIMAL and total_laid_off to INT to ensure numeric operations work correctly.
ALTER TABLE layoffs_staging2 
MODIFY COLUMN percentage_laid_off DECIMAL(5,2) NULL,
MODIFY COLUMN total_laid_off INT NULL;

-- Ensuring percentage filtering and sorting work properly by explicitly casting them as numeric values.
SELECT * 
FROM layoffs_staging2 
WHERE CAST(percentage_laid_off AS DECIMAL(10,2)) = 1
ORDER BY CAST(total_laid_off AS SIGNED) DESC;

-- Checking the table structure to confirm column data types.
DESC layoffs_staging2;

-- Identifying any non-numeric values in the total_laid_off and percentage_laid_off columns.
SELECT total_laid_off 
FROM layoffs_staging2 
WHERE total_laid_off REGEXP '[^0-9]';

SELECT percentage_laid_off 
FROM layoffs_staging2 
WHERE percentage_laid_off REGEXP '[^0-9.]';

-- Replacing non-numeric values in numeric columns with NULL to allow successful data type conversion.
UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off REGEXP '[^0-9]';

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off REGEXP '[^0-9.]';

-- Ensuring empty strings are converted to NULL values for consistency.
UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = '';

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = '';

-- Modifying table structure after cleaning data.
ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT NULL,
MODIFY COLUMN percentage_laid_off DECIMAL(5,2) NULL;

-- Finding companies with 100% layoffs sorted by total layoffs.
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Ranking companies by total layoffs.
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

-- Finding the earliest and most recent layoff events in the dataset.
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Identifying industries most affected by layoffs.
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

-- Identifying countries most affected by layoffs.
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

-- Identifying yearly layoff trends.
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

-- Finding the months with the highest layoffs across all years.
SELECT 
    YEAR(`date`) AS layoff_year, 
    MONTHNAME(`date`) AS layoff_month, 
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY layoff_year, layoff_month
ORDER BY total_layoffs DESC;

-- Examining layoffs by company funding stage.
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

-- Aggregating layoffs by month.
SELECT 
    SUBSTRING(`date`, 1,7) AS layoff_month, 
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY layoff_month
ORDER BY 1 ASC;

-- Calculating rolling total layoffs over time.
WITH Rolling_total AS (
    SELECT 
        SUBSTRING(`date`, 1,7) AS layoff_month, 
        SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
    GROUP BY layoff_month
    ORDER BY 1 ASC
)
SELECT `layoff_month`, total_layoffs,
    SUM(total_layoffs) OVER(ORDER BY `layoff_month`) AS rolling_total
FROM Rolling_total;

-- Calculating rolling layoffs per country.
WITH Rolling_total AS (
    SELECT 
        SUBSTRING(`date`, 1,7) AS layoff_month, 
        country,
        SUM(total_laid_off) AS total_layoffs
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
    GROUP BY layoff_month, country
    ORDER BY layoff_month ASC
)
SELECT 
    layoff_month, 
    country,
    total_layoffs,
    SUM(total_layoffs) OVER(PARTITION BY country ORDER BY layoff_month) AS rolling_total
FROM Rolling_total;

-- Examining layoffs at the company level per year.
SELECT company, YEAR (`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR (`date`)
ORDER BY 3 DESC;

-- Ranking companies with the most layoffs per year.
WITH Company_per_year (company, years, total_laid_off) AS (
    SELECT company, YEAR (`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
Company_Year_Rank AS (
    SELECT *,
        DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM Company_per_year
    WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE ranking <= 5;

-- Examining layoffs based on company funding levels.
SELECT 
    CASE 
        WHEN funds_raised = '' OR funds_raised IS NULL THEN 'No Funding Data'
        WHEN funds_raised < 50 THEN 'Low Funding (<$50M)'
        WHEN funds_raised BETWEEN 50 AND 200 THEN 'Medium Funding ($50M-$200M)'
        ELSE 'High Funding (>$200M)'
    END AS funding_category,
    SUM(total_laid_off) AS total_layoffs
FROM layoffs_staging2
GROUP BY funding_category
ORDER BY total_layoffs DESC;

SELECT *
FROM layoffs_staging2;

