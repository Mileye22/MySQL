USE world_layoffs;

SELECT COUNT(*)
FROM layoffs;

DROP TABLE IF EXISTS layoffs_staging;

CREATE TABLE layoffs_staging 
SELECT * FROM layoffs;

SELECT *
FROM layoffs_staging;

DROP TABLE IF EXISTS lay;
CREATE TABLE lay
LIKE layoffs;

INSERT INTO lay
SELECT * FROM layoffs;

SELECT *
FROM lay;

SELECT DISTINCT(company), COUNT(DISTINCT(company))
FROM lay
GROUP BY 1 
ORDER BY 2;

SELECT DISTINCT(industry), COUNT(DISTINCT(industry))
FROM lay
GROUP BY 1 
ORDER BY 2 DESC;

UPDATE lay
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(country), COUNT(DISTINCT(country))
FROM lay
GROUP BY 1 
ORDER BY 2 DESC;

UPDATE lay
SET country = 'United States'
WHERE country LIKE 'United St%';

SELECT DISTINCT(stage), COUNT(DISTINCT(stage))
FROM lay
GROUP BY 1 
ORDER BY 2 DESC;

WITH duplicate_cte AS
(SELECT *, ROW_NUMBER() OVER(PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, lay.date , stage, country, funds_raised_millions) AS row_num
FROM lay)

SELECT *
FROM duplicate_cte
WHERE company IN (SELECT company
FROM duplicate_cte
WHERE row_num > 1);

WITH duplicate_cte2 AS
(SELECT *, ROW_NUMBER() OVER(PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, lay.date , stage, country, funds_raised_millions) AS row_num
FROM lay)

SELECT *
FROM duplicate_cte2
WHERE row_num > 1;

DROP TABLE IF EXISTS lay_staging;
CREATE TABLE lay_staging
SELECT *, ROW_NUMBER() OVER(PARTITION BY 
company, location, industry, total_laid_off, percentage_laid_off, lay.date , stage, country, funds_raised_millions) AS row_num
FROM lay;

SELECT *
FROM lay_staging
WHERE row_num > 1;

DELETE
FROM lay_staging
WHERE row_num > 1;

SELECT *
FROM lay_staging;

-- DROPPED DUPLICATES

-- Normalizing data types

UPDATE lay_staging
SET company = TRIM(TRAILING '.' FROM Company);

UPDATE lay_staging
SET location = TRIM(TRAILING '.' FROM location);

UPDATE lay_staging
SET industry = TRIM(TRAILING '.' FROM industry);

UPDATE lay_staging
SET country = TRIM(TRAILING '.' FROM country);

SELECT lay_staging.date , STR_TO_DATE(lay_staging.date, '%m/%d/%Y')
FROM lay_staging;

UPDATE lay_staging
SET lay_staging.date = STR_TO_DATE(lay_staging.date, '%m/%d/%Y');

ALTER TABLE lay_staging
MODIFY COLUMN `date` DATE;

UPDATE lay
SET lay.date = STR_TO_DATE(lay.date, '%m/%d/%Y');

ALTER TABLE lay
MODIFY COLUMN `date` DATE;

ALTER TABLE lay_staging
DROP COLUMN row_num;

SELECT *
FROM lay_staging
WHERE industry IS NULL or industry = '' ;

WITH industry_null_cte AS
(SELECT * 
FROM
lay_staging
WHERE company IN (SELECT company
FROM lay_staging
WHERE industry IS NULL or industry = ''))

SELECT * 
FROM industry_null_cte;
 
 UPDATE lay_staging
 SET industry = NULL
 WHERE industry = '';
 
 SELECT t1.industry, t2.industry
FROM lay_staging t1
JOIN lay_staging t2
ON t1.company = t2.company
AND t2.location =t1.location
WHERE t1.industry IS  NULL
AND t2.industry IS NOT NULL;
 
 UPDATE lay_staging t1
 JOIN lay_staging t2
 ON t1.company = t2.company
AND t2.location =t1.location
 SET t1.industry = t2.industry
 WHERE t1.industry IS  NULL
AND t2.industry IS NOT NULL;
 
 SELECT COUNT(*) null_count
 FROM lay_staging
 WHERE funds_raised_millions IS NOT NULL;
 
 SELECT *
 FROM lay_staging
 LIMIT 1;
 


-- DATA ANALYSIS
-- I want to do data analysis for employee_lay_off data accross country
-- industry and company over 3 years from (2020 - 2023)
-- Some questions i need answweres includes Which industry, country, company has the highest lay_offs
-- What are the funding raised by this companys, which industry have the highest fundings and country
-- is funding a necessity for lay_off in companies
-- What are the lay_offs over time across countries(top 10)

 ;

USE world_layoffs;
SELECT *
FROM lay_staging;

SELECT COUNT(*)
FROM lay_staging
WHERE lay_staging.total_laid_off IS NULL; 
-- 739

SELECT COUNT(*)
FROM lay_staging
WHERE lay_staging.percentage_laid_off IS NULL;
-- 784
;
SELECT COUNT(*)
FROM lay_staging;
-- 2361;

SELECT COUNT(DISTINCT(company))
FROM lay_staging;
-- 1888 
;

SELECT COUNT(DISTINCT(company))
FROM lay_staging
WHERE lay_staging.total_laid_off IS NULL
AND  lay_staging.percentage_laid_off IS NULL;
-- 361 ;

SELECT COUNT(DISTINCT(company))
FROM lay_staging
WHERE lay_staging.total_laid_off IS NOT NULL
AND lay_staging.percentage_laid_off IS NOT NULL
-- 1194
;



WITH cte_1 AS 
(SELECT *
FROM lay_staging
WHERE lay_staging.total_laid_off IS NULL
AND lay_staging.percentage_laid_off IS NULL),
cte_2 AS 
(SELECT *
FROM lay_staging
WHERE lay_staging.total_laid_off IS NOT NULL
AND lay_staging.percentage_laid_off IS NOT NULL) 

SELECT DISTINCT(cte_2.company)
FROM cte_2
LEFT JOIN cte_1
ON cte_1.company = cte_2.company
WHERE cte_1.company IS NOT NULL;
-- 45 rows to be dropped

SELECT *
FROM lay_staging
WHERE lay_staging.total_laid_off IS NULL
AND lay_staging.percentage_laid_off IS NULL;


START TRANSACTION;
DELETE
FROM lay_staging
WHERE lay_staging.total_laid_off IS NULL
AND lay_staging.percentage_laid_off IS NULL;
SELECT FOUND_ROWS();
-- 361 
-- If something is wrong, rollback the changes
ROLLBACK;
-- If everything looks good, commit the changes
COMMIT;



SELECT DISTINCT(industry),
SUM(total_laid_off) OVER (PARTITION BY industry) total_laid_off
FROM lay_staging
ORDER BY 2 DESC
LIMIT 10;

SELECT DISTINCT(country),
SUM(total_laid_off) OVER (PARTITION BY country) total_laid_off
FROM lay_staging
ORDER BY 2 DESC
LIMIT 10;

SELECT DISTINCT(YEAR(`date`)) `Year`, company, SUM(funds_raised_millions) OVER (PARTITION BY company ORDER BY YEAR(`date`)) funds,
SUM(total_laid_off) OVER (PARTITION BY company ORDER BY YEAR(`date`)) total_laid_off
FROM lay_staging
ORDER BY 3 DESC ,1
LIMIT 20;


SELECT industry, SUM(total_laid_off)
FROM lay_staging
GROUP BY 1
ORDER BY 1 DESC;

SELECT country, SUM(total_laid_off) total_laid_off, SUM(funds_raised_millions) AS funds_Millions 
FROM lay_staging
WHERE total_laid_off IS NOT NULL 
GROUP BY 1
HAVING funds_Millions < 2000
ORDER BY 2 DESC, 3 DESC;

SELECT YEAR(`date`) `Year`, industry, SUM(total_laid_off)
FROM lay_staging
WHERE YEAR(`date`) = 2023
GROUP BY 1,2 
ORDER BY 3 DESC, 2 DESC
LIMIT 10;

SELECT YEAR(`date`) `Year`, country, SUM(total_laid_off), SUM(funds_raised_millions)
FROM lay_staging
GROUP BY 1,2 
ORDER BY 3 DESC, 2 DESC
LIMIT 10;

SELECT YEAR(`date`) `Year`, MONTH(`date`) `Month`, company, SUM(total_laid_off)
FROM lay_staging
GROUP BY 1,2,3 
ORDER BY 3 DESC, 1, 2 ASC;

SELECT stage, SUM(total_laid_off), SUM(funds_raised_millions)
FROM lay_staging
GROUP BY 1
ORDER BY 3 DESC;

SELECT company, stage, SUM(total_laid_off), SUM(funds_raised_millions)
FROM lay_staging
GROUP BY 1, 2;

SELECT company, SUM(total_laid_off), SUM(funds_raised_millions)
FROM lay_staging
WHERE stage LIKE 'Post%'
GROUP BY 1
ORDER BY 2 DESC;

SELECT *
FROM lay_staging
WHERE company = 'Ola';

SELECT *
FROM lay_staging
WHERE country LIKE 'Niger%';

SELECT *
FROM lay_staging
WHERE country LIKE 'United S%' AND industry ='Data'
ORDER BY 3;