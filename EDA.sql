-- exploratory data analysis
select * from layoffs_staging2;

	-- total laid off by company
select company, sum(total_laid_off) from layoffs_staging2 group by company order by 2 desc;

	-- minimum and maximum dates
select min(`date`), max(`date`) from layoffs_staging2;

	-- total laid off by industry
select industry, sum(total_laid_off) from layoffs_staging2 group by industry order by 2 desc;

	-- total laid off by country
select country, sum(total_laid_off) from layoffs_staging2 group by country order by 2 desc;

	-- total laid off by year
select year(`date`), sum(total_laid_off) from layoffs_staging2 group by year(`date`) order by 2 desc;

	-- total laid off by stage
select stage, sum(total_laid_off) from layoffs_staging2 group by stage order by 2 desc;

	-- cumulative summation of laid off by month and years
with rolling_total as(
select substr(`date`,1,7) as `month`, sum(total_laid_off) as laid_off from layoffs_staging2 
where `date` is not null group by `month` order by 1 
)
select `month`, laid_off, sum(laid_off) over (order by `month`) as rolling_total from rolling_total;

	-- partitioning by year and ranking laid offs by companies  
with company_year as(
select company, year(`date`) as `year`, sum(total_laid_off) as laid_off from layoffs_staging2
 group by company, year(`date`) ),
company_year_rank as(
select *, dense_rank() over(partition by `year` order by laid_off desc) as `rank` from company_year 
where `year` is not null)
select * from company_year_rank where `rank` <= 5;

