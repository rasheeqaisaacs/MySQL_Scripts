-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging4;

SELECT MAX(new_total_laid_off), MAX(percentage_laid_off) 
FROM layoffs_staging4;


SELECT *
FROM layoffs_staging4
WHERE percentage_laid_off = 1
ORDER BY new_funds_raised_millions DESC;


SELECT company, SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY company
ORDER BY 2 DESC;


SELECT MIN(`DATE`), MAX(`DATE`)
FROM layoffs_staging4;


SELECT industry, SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY industry
ORDER BY 2 DESC;


SELECT country, SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY country
ORDER BY 2 DESC;


SELECT YEAR (`date`), SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


SELECT stage, SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY stage
ORDER BY 2 DESC;

SELECT substring(`DATE`, 1,7) AS `MONTH`, SUM(new_total_laid_off)
FROM layoffs_staging4
WHERE substring(`DATE`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
;


WITH Rolling_Total AS
(
SELECT substring(`DATE`, 1,7) AS `MONTH`, SUM(new_total_laid_off) AS total_off
FROM layoffs_staging4
WHERE substring(`DATE`, 1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off
,SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total;


SELECT company, SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR( `date`), SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year (company, years, total_laid_off) AS
(
SELECT company, YEAR( `date`), SUM(new_total_laid_off)
FROM layoffs_staging4
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
;