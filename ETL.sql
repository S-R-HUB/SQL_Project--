select * from layoffs;

-- copying data to a separate table for data cleaning
create table layoffs_staging like layoffs;
insert into layoffs_staging select * from layoffs;
select * from layoffs_staging;

-- performing data cleaning
	-- adding row number to remove duplicates
with  duplicates as(select *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) from layoffs_staging)
select * from duplicates where row_num>1;

	-- creating a separate table and copying data from layoffs_staging table to remove duplicates using row numbers
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
  `row_number` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2 select *, row_number() over(partition by company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) from layoffs_staging;
select * from layoffs_staging2;
delete from layoffs_staging2 where `row_number` > 1;
select * from layoffs_staging2 where `row_number` > 1;

-- standardizing data
select distinct company from layoffs_staging2;
update layoffs_staging2 set company= trim(company);

select distinct industry from layoffs_staging2 order by 1;
update layoffs_staging2 set industry= 'Crypto' where industry like 'Crypto%';

select distinct location from layoffs_staging2 order by 1;
select distinct country from layoffs_staging2 order by 1;
update layoffs_staging2 set country= trim(trailing '.' from country)  where country like 'United States%';

-- changing date column to date format
select `date`, str_to_date(`date`, '%m/%d/%Y') from layoffs_staging2;
update layoffs_staging2 set `date`= str_to_date(`date`, '%m/%d/%Y');
alter table layoffs_staging2 modify `date` date;
select `date` from layoffs_staging2 order by 1;


-- dealing with null and blank values
select * from layoffs_staging2 where industry='';
select * from layoffs_staging2 where company='airbnb';
select * from layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company;
update layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company=t2.company set t1.industry = t2.industry 
where t1.industry = '' and t2.industry <>'';

delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

-- remove any unwanted columns
alter table layoffs_staging2 drop column `row_number`;
select * from layoffs_staging2;


