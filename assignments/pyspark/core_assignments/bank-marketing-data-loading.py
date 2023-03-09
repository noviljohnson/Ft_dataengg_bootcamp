# Assignment #4

# 	Enhance above Bank Marketing Campaign Data Analysis PySpark Application to perform the data processing
# 	as pipeline of four different stages viz. Loading, Validation, Transformation and Expot each as separate PySpark applicaiton.

# 	Create PySpark Application - bank-marketing-data-loading.py. Perform below operations.
# 	a) Load Bank Marketing Campaign Data csv file from local to HDFS file system under '/user/training/bankmarketing/raw'
# 	b) Load Bank Marketing Campaign Data from HDFS file system under '/user/training/bankmarketing/raw' and create DataFrame
# 	c) Convert the data into parquet format and write into HDFS file system under '/user/training/bankmarketing/staging'
# 	d) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/success' once the data loading job completed successfully
# 	f) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/error' once the data loading job is failed due to data error
	

