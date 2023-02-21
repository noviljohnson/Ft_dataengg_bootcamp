CREATE TABLE IF NOT EXISTS RATINGS (userid int, movieid int, rating float,timestmp date )
    COMMENT 'Movie ratings'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE;

    
LOAD DATA LOCAL INPATH '/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/assignments/hadoop/ratings.csv' OVERWRITE INTO TABLE ratings;

select rating, count(*)
from ratings
group by rating;


CREATE EXTERNAL TABLE IF NOT EXISTS EMPLOYEE_EXT (eid int, name String, salary String, designation String)
COMMENT 'Employee details'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '/user/training/empmgmt/input';