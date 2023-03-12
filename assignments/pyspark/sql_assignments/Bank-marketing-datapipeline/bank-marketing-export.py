# Create PySpark Application - bank-marketing-export.py. Perform below operations.
# 	a) Load processed Bank Marketing Campaign Data in Avro format from HDFS file system under '/user/training/bankmarketing/processed'
# 	b) Export the data into RDBMS (MySQL DB) under bankmaketing schema and subcription_count table
# 	c) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/success' once the export job completed successfully
# 	d) Data should be moved to '/user/training/bankmarketing/processed/yyyymmdd/error' once the export job is failed

'''
Spark dont have packages internally to handle AVRO files. So we need to pass those packages explicitly in spark-submit

Exporting to RDBMS(MySQL)
*************************
1.) Make use the database is present in MySQL
2.) Spark will automatically creates the table mentioned in the export command under the mentioned
	database.
3.) To run the spark job with mysql export we need to pass the sql connector jar files explicitly

spark-submit \
--packages org.apache.spark:spark-avro_2.12:3.3.2 \
--jars "/home/bigdata/mysql-connector-j-8.0.32/mysql-connector-j-8.0.32.jar" bank_export.py


'''

import pyspark
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


from datetime import datetime



spark = SparkSession.builder.master('local').appName("bankmarketingExportapp").getOrCreate()


date =  datetime.now().strftime('%Y-%m-%d')

subprocess.run(f'hadoop fs -mkdir /user/training/bankmarketing/processed/{date}'.split())

try:
	avro_df = spark.read.format("avro").load("hdfs://localhost:9000/user/training/bankmarketing/processed/*.avro")

	# avro_df.write.mode('append') \
	#     .format("jdbc") \
	#     .option("url", "jdbc:mysql://localhost/bankmarketing") \
	#     .option("driver", "com.mysql.jdbc.Driver") \
	#     .option("dbtable", "subscription_count") \
	#     .option("user", "sqoop") \
	#     .option("password", "sqoop") \
	#     .save()
	avro_df.write \
        .format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/pyspark_training") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("dbtable", "subscription_count") \
        .option("user", "pyspark") \
        .option("password", "pyspark") \
        .save()
 
	avro_df.write.mode('overwrite').csv(path=f'hdfs://localhost:9000/user/training/bankmarketing/processed/{date}/success',header=True)
except Exception as e:
	print(e)

	subprocess.run(f'hadoop fs -mkdir /user/training/bankmarketing/processed/{date}/error'.split())

	subprocess.run(f"hadoop fs -mv /user/training/bankmarketing/processed/*.avro /user/training/bankmarketing/processed/{date}/error".split())




