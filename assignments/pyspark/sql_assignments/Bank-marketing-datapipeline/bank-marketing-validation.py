# Create PySpark Application - bank-marketing-validation.py. Perform below operations.
# 	a) Load Bank Marketing Campaign Data in Parquet format from HDFS file system under '/user/training/bankmarketing/staging'
# 	b) Remove all 'unknown' job records 
# 	c) Replace 'unknown' contact nos with 1234567890 and 'unknown' poutcome with 'na'
# 	d) Write the output as Parquet format into HDFS file system under '/user/training/bankmarketing/validated'
# 	e) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/success' once the validation job completed successfully
# 	f) Data should be moved to '/user/training/bankmarketing/staging/yyyymmdd/error' once the validation job is failed due to data error

import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.functions import regexp_replace

import subprocess

from datetime import datetime

spark = SparkSession.builder.master("local").appName("bankValidationApp").getOrCreate()



date = datetime.now().strftime('%Y-%m-%d')
try:
	bank_parquet_df = spark.read.parquet("hdfs://localhost:9000/user/training/bankmarketing/staging/*.parquet")


	job_filter = bank_parquet_df.filter(bank_parquet_df['job'] != 'unknown')

	contact_filter = job_filter.withColumn('contact',regexp_replace('contact','unknown','1234567890'))

	poutcome_filter = contact_filter.withColumn('poutcome',regexp_replace('poutcome','unknown','na'))


	poutcome_filter.write.parquet('hdfs://localhost:9000/user/training/bankmarketing/validated')


	poutcome_filter.write.mode('overwrite').csv(path = f'hdfs://localhost:9000//user/training/bankmarketing/staging/{date}/success',header=True)

except Exception as e:
	print(e)
	#if there is any error in the file reading create a error directory
	subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/error".split())

	# move the data to error folder
	subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/raw/bankmarketdata.csv /user/training/bankmarketing/staging/{date}/error".split())



