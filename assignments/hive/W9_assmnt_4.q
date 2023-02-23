USE hive_training;

# Create partition table
CREATE TABLE IF NOT EXISTS EMPLOYEE_PART (eid int, name String, salary String, designation String)
COMMENT 'EMPLOYEE_PART details'
PARTITIONED BY (yoj String)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;