# Assignment #4

# 	Enhance above Bank Marketing Campaign Data Analysis PySpark Application to perform the data processing
# 	as pipeline of four different stages viz. Loading, Validation, Transformation and Expot each as separate PySpark applicaiton.

# 	Create PySpark Application - bank-marketing-data-loading.py. Perform below operations.
# 	a) Load Bank Marketing Campaign Data csv file from local to HDFS file system under '/user/training/bankmarketing/raw'
# 	b) Load Bank Marketing Campaign Data from HDFS file system under '/user/training/bankmarketing/raw' and create DataFrame
# 	c) Convert the data into parquet format and write into HDFS file system under '/user/training/bankmarketing/staging'
# 	d) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/success' once the data loading job completed successfully
# 	f) Data should be moved to '/user/training/bankmarketing/raw/yyyymmdd/error' once the data loading job is failed due to data error


import pyspark
from pyspark.sql import SparkSession

import subprocess
from datetime import datetime

# creating spark app
spark = SparkSession.builder.master("local").appName('bankdataLoadApp').getOrCreate()

#create bankmarketing directory
subprocess.run("hadoop fs -mkdir /user/training/bankmarketing".split())

#create raw directory
subprocess.run("hadoop fs -mkdir /user/training/bankmarketing/raw".split())

#export  the data file from local to bankmarketing/raw
subprocess.run("hadoop fs -copyFromLocal /home/noviljohnson/bankmarketing/bankmarketdata.csv /user/training/bankmarketing/raw".split())

# create date
date = datetime.now().strftime("%Y-%m-%d")


try :

	HDFS_PATH ='hdfs://localhost:9000/user/training/bankmarketing/raw/bankmarketdata.csv'

	# read the csv file
	bank_df =  spark.read.csv(path=HDFS_PATH,header=True,sep=';',inferSchema=True)

	#convert the dataframe into parquet file export into staging directory
	bank_df.write.parquet('hdfs://localhost:9000/user/training/bankmarketing/staging/')

	# create a directory with date
	subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/".split())

	# create a success directory
	subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/success".split())

	# move the data to success folder
	subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/raw/bankmarketdata.csv /user/training/bankmarketing/staging/{date}/success".split())


except Exception as e:
	print(e)

	#if there is any error in the file reading create a error directory
	subprocess.run(f"hadoop fs -mkdir /user/training/bankmarketing/staging/{date}/error".split())

	# move the data to error folder
	subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/raw/bankmarketdata.csv /user/training/bankmarketing/staging/{date}/error".split())

