-- table - sales_data
-- date, region, product, sales_amount
-- partition the table by the region col using hive querey

using ft_hive;

INSERT OVERWRITE TABLE sales_data PARTITION(region) select region from sales_data; 