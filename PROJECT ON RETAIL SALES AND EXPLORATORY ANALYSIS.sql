SELECT * FROM new_schema.retail_sales_dataset;


## data cleaning 
## Remove duplicates
## Standandize data
## Null values and rows


create table retail_sales
like retail_sales_dataset;


select*
from retail_sales;


select*,
ROW_NUMBER () OVER(
PARTITION BY  Gender, Age, 'Date') as row_num
from  retail_sales;


with duplicate_cts as
(
select*,
ROW_NUMBER () OVER(
PARTITION BY  Gender, Age, Price_per_Unit, Product_Category, Quantity, Total_Amount, 'Date') as row_num
from  retail_sales
)


select*
from duplicate_cts
where row_num > 1;

select*
from retail_sales
where Product_Category= 'electronics';



with duplicate_cts as
(
select*,
ROW_NUMBER () OVER(
PARTITION BY  Gender, Age, Price_per_Unit, Product_Category, Quantity, Total_Amount, 'Date') as row_num
from  retail_sales
)
delete
from duplicate_cts
where row_num > 1;

CREATE TABLE `retail_sales3` (
  `Transaction_ID` int DEFAULT NULL,
  `Date` datetime DEFAULT NULL,
  `Customer ID` text,
  `Gender` text,
  `Age` int DEFAULT NULL,
  `Product_Category` text,
  `Quantity` int DEFAULT NULL,
  `Price_per_Unit` int DEFAULT NULL,
  `Total_Amount` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select*
from retail_sales3; 

insert into retail_sales3
select*,
ROW_NUMBER () OVER(
PARTITION BY  Gender, Age, Price_per_Unit, Product_Category, Quantity, Total_Amount, 'Date') as row_num
from  retail_sales;


delete
from retail_sales3
where row_num >1;

select*
from retail_sales3;


##  sql safe update mode
set SQL_SAFE_UPDATES = 0;


#standardizing data
select product_category, trim(product_category)
from retail_sales3;

update retail_sales3
set product_category = trim(product_category);

select distinct total_amount
from retail_sales3
order by 1;


select distinct product_category
from retail_sales3
order by 1;

select*
FROM retail_sales3;


select date('2023-05-23 00:00:00') as date_only
from retail_sales3;

alter table retail_sales3
modify column date_time DATE;

select date
from retail_sales3;

select*
from retail_sales3
where Transaction_ID is null;


alter table retail_sales3
drop column row_num;

select*
from retail_sales3;


alter table retail_sales3
drop column Transaction_ID;

alter table retail_sales3
add column transaction_id int not null auto_increment primary key;


##Exploratory Analysis

select*
from retail_sales3;

select MAX(total_amount)
from retail_sales3;


select*
from retail_sales3
order by Total_Amount desc;


select product_category, sum(price_per_unit)
from retail_sales3
group by Product_Category
order by 2 desc;


select `date`, sum(price_per_unit)
from retail_sales3
group by `date`;


select year(`date`), sum(price_per_unit)
from retail_sales3
group by year(`date`);


select `date`, sum(Quantity)
from retail_sales3
group by `date`;

select*
from retail_sales3;

select  quantity, sum(Total_Amount)
from retail_sales3
group by quantity;

select substring(`date`,4,2) as `month`, sum(total_amount)
from retail_sales3
group by `month`;

select Quantity,  year(`date`), sum(Total_Amount)
from retail_sales3
group by Quantity, year(`date`)
order by 1 desc;
